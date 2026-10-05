"""Lectura de los mensajes de detecciones alrededor de un timestamp."""

import json
import logging
import time
from dataclasses import dataclass, field

from confluent_kafka import Consumer, KafkaException, TopicPartition

log = logging.getLogger(__name__)

POLL_TIMEOUT_S = 1.0
READ_TIMEOUT_S = 15.0
METADATA_TIMEOUT_S = 10.0


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
    return Detection(int(frame_ts), payload, partition, offset)


class DetectionsReader:
    """Lee de `<guid>_detections` los mensajes cercanos a un timestamp.

    Para cada partición se usa `offsets_for_times` para localizar el primer offset
    con timestamp de Kafka >= timestamp y se lee una ventana de mensajes alrededor.
    `margin` amplía esa ventana para tolerar diferencias entre el timestamp de Kafka
    y el FrameTimestamp del mensaje.
    """

    def __init__(self, kafka_config: dict, margin: int = 5):
        self.kafka_config = {
            "group.id": "detections-exporter",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            **kafka_config,
        }
        self.margin = margin

    def fetch(self, guid: str, timestamp: int, before: int = 5, after: int = 5):
        consumer = Consumer(self.kafka_config)
        try:
            topic = topic_name(guid)
            partitions = self._partitions(consumer, topic)
            detections = []
            for partition in partitions:
                detections.extend(
                    self._read_partition(consumer, topic, partition, timestamp, before, after)
                )
        finally:
            consumer.close()
        return select_window(detections, timestamp, before, after)

    @staticmethod
    def _partitions(consumer, topic):
        metadata = consumer.list_topics(topic, timeout=METADATA_TIMEOUT_S)
        topic_meta = metadata.topics.get(topic)
        if topic_meta is None or topic_meta.error is not None or not topic_meta.partitions:
            raise KafkaException(f"El topic '{topic}' no existe o no tiene particiones")
        return sorted(topic_meta.partitions)

    def _read_partition(self, consumer, topic, partition, timestamp, before, after):
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=METADATA_TIMEOUT_S
        )
        if high <= low:
            return []

        found = consumer.offsets_for_times(
            [TopicPartition(topic, partition, timestamp)], timeout=METADATA_TIMEOUT_S
        )[0]
        # Sin mensajes con timestamp >= timestamp: el punto de corte es el final.
        pivot = high if found.offset < 0 else found.offset

        start = max(low, pivot - before - self.margin)
        end = min(high, pivot + after + self.margin)  # exclusivo
        if end <= start:
            return []

        consumer.assign([TopicPartition(topic, partition, start)])
        detections = []
        next_offset = start
        deadline = time.monotonic() + READ_TIMEOUT_S
        while next_offset < end:
            if time.monotonic() > deadline:
                log.warning(
                    "Timeout leyendo partición %s (offset %s/%s)", partition, next_offset, end
                )
                break
            msg = consumer.poll(POLL_TIMEOUT_S)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            next_offset = msg.offset() + 1
            if msg.offset() >= end:
                break
            detection = parse_message(msg.value(), partition, msg.offset(), msg.timestamp()[1])
            if detection is not None:
                detections.append(detection)
        consumer.unassign()
        return detections
