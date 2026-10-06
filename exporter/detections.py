"""Modelo de los mensajes de detecciones y lógica común a los lectores."""

import json
import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class Detection:
    """Un mensaje de detecciones ya parseado."""

    frame_timestamp: int
    payload: dict
    partition: int = -1
    offset: int = -1
    objects: list = field(init=False)

    def __post_init__(self):
        self.objects = self.payload.get("Objects") or []


def topic_name(guid: str) -> str:
    return f"{guid}_detections"


def has_objects(detection: Detection) -> bool:
    return len(detection.objects) > 0


def select_window(detections, timestamp: int, before: int, after: int):
    """Devuelve (anteriores, posteriores) respecto a `timestamp`.

    Anteriores: los `before` mensajes con FrameTimestamp < timestamp más cercanos.
    Posteriores: los `after` mensajes con FrameTimestamp >= timestamp más cercanos.
    Ambas listas se devuelven en orden cronológico.
    """
    ordered = sorted(detections, key=lambda d: (d.frame_timestamp, d.partition, d.offset))
    prev = [d for d in ordered if d.frame_timestamp < timestamp]
    nxt = [d for d in ordered if d.frame_timestamp >= timestamp]
    return (prev[-before:] if before > 0 else []), nxt[:after]


def parse_message(value: bytes, partition: int, offset: int, kafka_ts: int):
    try:
        payload = json.loads(value)
    except (TypeError, ValueError):
        log.warning("Mensaje no JSON en partición %s offset %s, se ignora", partition, offset)
        return None
    if not isinstance(payload, dict):
        return None
    frame_ts = payload.get("FrameTimestamp")
    if frame_ts is None:
        frame_ts = kafka_ts
    if frame_ts is None:
        log.warning("Mensaje sin timestamp en partición %s offset %s, se ignora", partition, offset)
        return None
    return Detection(int(frame_ts), payload, partition, offset)
