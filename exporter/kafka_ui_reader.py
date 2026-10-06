"""Lectura de detecciones a través de la API REST de UI for Apache Kafka.

Útil cuando el puerto de Kafka no es accesible pero sí la interfaz web (p. ej. :8080).
Soporta la API nueva (`/messages/v2`, kafbat/kafka-ui) y la antigua (`/messages`,
provectus/kafka-ui); se prueba primero la nueva y, si no existe, la antigua.
"""

import json
import logging
from datetime import datetime

import requests

from .detections import parse_message, select_window, topic_name

log = logging.getLogger(__name__)

HTTP_TIMEOUT_S = 60


class KafkaUIError(RuntimeError):
    pass


def iso_to_ms(value):
    """'2026-10-05T11:19:54.246Z' -> 1791199194246 (None si no se puede)."""
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
    except (TypeError, ValueError):
        return None


def iter_sse_events(response):
    """Itera los eventos JSON de una respuesta text/event-stream."""
    data = []
    for raw in response.iter_lines(decode_unicode=True):
        if raw is None:
            continue
        line = raw.rstrip("\r")
        if line.startswith("data:"):
            data.append(line[5:].lstrip())
        elif line == "" and data:
            yield json.loads("\n".join(data))
            data = []
    if data:
        yield json.loads("\n".join(data))


class KafkaUIDetectionsReader:
    """Misma interfaz que `DetectionsReader`, pero vía HTTP contra UI for Apache Kafka."""

    def __init__(self, base_url, cluster=None, username=None, password=None,
                 margin=5, value_serde=None, verify_ssl=True, session=None):
        self.base_url = base_url.rstrip("/")
        self.cluster = cluster
        self.margin = margin
        self.value_serde = value_serde
        self.session = session or requests.Session()
        self.session.verify = verify_ssl
        self._use_v2 = None  # se detecta en la primera petición
        if username:
            self._login(username, password or "")

    # --- API pública -----------------------------------------------------------------

    def fetch(self, guid, timestamp, before=5, after=5):
        cluster = self._cluster()
        topic = topic_name(guid)
        partitions = self._partitions(cluster, topic)

        found = {}
        for partition in partitions:
            for backward, limit in ((True, before + self.margin), (False, after + self.margin)):
                if limit <= 0:
                    continue
                for msg in self._messages(cluster, topic, partition, timestamp, backward, limit):
                    det = self._to_detection(msg)
                    if det is not None:
                        found[(det.partition, det.offset)] = det
        return select_window(found.values(), timestamp, before, after)

    # --- HTTP ------------------------------------------------------------------------

    def _url(self, path):
        return f"{self.base_url}{path}"

    def _login(self, username, password):
        # Autenticación LOGIN_FORM (Spring Security): deja la cookie de sesión.
        resp = self.session.post(
            self._url("/login"),
            data={"username": username, "password": password},
            allow_redirects=False,
            timeout=HTTP_TIMEOUT_S,
        )
        location = resp.headers.get("Location", "")
        if resp.status_code >= 400 or "error" in location:
            raise KafkaUIError("Login en UI for Apache Kafka fallido: revisa usuario y contraseña")

    def _get_json(self, path, **params):
        resp = self.session.get(self._url(path), params=params, timeout=HTTP_TIMEOUT_S)
        if resp.status_code in (401, 403):
            raise KafkaUIError(
                f"Acceso denegado ({resp.status_code}) en {path}: usa --ui-user/--ui-password"
            )
        if resp.status_code == 404:
            raise KafkaUIError(f"No encontrado: {path}")
        resp.raise_for_status()
        try:
            return resp.json()
        except ValueError:
            raise KafkaUIError(
                f"Respuesta no JSON en {path}: ¿la URL es la de UI for Apache Kafka? "
                "¿Hace falta login (--ui-user/--ui-password)?"
            ) from None

    def _cluster(self):
        if self.cluster:
            return self.cluster
        names = [c["name"] for c in self._get_json("/api/clusters")]
        if len(names) == 1:
            self.cluster = names[0]
            return self.cluster
        raise KafkaUIError(
            f"Hay {len(names)} clusters, indica uno con --ui-cluster: {', '.join(names)}"
        )

    def _partitions(self, cluster, topic):
        try:
            details = self._get_json(f"/api/clusters/{cluster}/topics/{topic}")
        except KafkaUIError as exc:
            if "No encontrado" in str(exc):
                raise KafkaUIError(f"El topic '{topic}' no existe en el cluster '{cluster}'") from None
            raise
        partitions = [p["partition"] for p in details.get("partitions") or []]
        if not partitions:
            partitions = list(range(details.get("partitionCount") or 1))
        return sorted(partitions)

    def _messages(self, cluster, topic, partition, timestamp, backward, limit):
        path = f"/api/clusters/{cluster}/topics/{topic}/messages"
        common = {"limit": limit}
        if self.value_serde:
            common["valueSerde"] = self.value_serde

        if self._use_v2 is not False:
            params = {
                **common,
                "mode": "TO_TIMESTAMP" if backward else "FROM_TIMESTAMP",
                "timestamp": timestamp,
                "partitions": partition,
            }
            resp = self._stream(path + "/v2", params)
            if resp.status_code == 404 and self._use_v2 is None:
                resp.close()
                log.info("API /messages/v2 no disponible, se usa /messages")
                self._use_v2 = False
            else:
                self._use_v2 = True
                return self._read_stream(resp)

        params = {
            **common,
            "seekType": "TIMESTAMP",
            "seekTo": f"{partition}::{timestamp}",
            "seekDirection": "BACKWARD" if backward else "FORWARD",
        }
        return self._read_stream(self._stream(path, params))

    def _stream(self, url_path, params):
        return self.session.get(
            self._url(url_path),
            params=params,
            headers={"Accept": "text/event-stream"},
            stream=True,
            timeout=HTTP_TIMEOUT_S,
        )

    @staticmethod
    def _read_stream(resp):
        with resp:
            if resp.status_code >= 400:
                raise KafkaUIError(
                    f"Error {resp.status_code} leyendo mensajes: {resp.text[:500]}"
                )
            messages = []
            for event in iter_sse_events(resp):
                kind = event.get("type")
                if kind == "MESSAGE" and event.get("message"):
                    messages.append(event["message"])
                elif kind == "DONE":
                    break
            return messages

    @staticmethod
    def _to_detection(msg):
        # provectus usa "content"; kafbat usa "value".
        value = msg.get("value", msg.get("content"))
        if value is None:
            return None
        return parse_message(
            value, msg.get("partition", -1), msg.get("offset", -1), iso_to_ms(msg.get("timestamp"))
        )
