"""CLI: python -m exporter.main <guid> <timestamp> [opciones]"""

import argparse
import logging
from pathlib import Path

from .detections import has_objects
from .drawing import draw_objects
from .image_source import get_image as default_get_image

log = logging.getLogger(__name__)

BEFORE = "anterior"
AFTER = "posterior"


def export(guid, timestamp, before, after, output_dir, get_image=default_get_image):
    """Pinta y guarda cada detección con objetos. Devuelve las rutas generadas."""
    out = Path(output_dir) / guid / str(timestamp)
    out.mkdir(parents=True, exist_ok=True)

    paths = []
    for position, detections in ((BEFORE, before), (AFTER, after)):
        kept = [d for d in detections if has_objects(d)]
        log.info("%s: %d mensajes, %d con objetos", position, len(detections), len(kept))
        for index, det in enumerate(kept, start=1):
            image = get_image(guid, det.frame_timestamp)
            annotated = draw_objects(
                image,
                det.objects,
                det.payload.get("FrameWidth"),
                det.payload.get("FrameHeight"),
            )
            path = out / f"{position}_{index:02d}_{det.frame_timestamp}.jpg"
            annotated.save(path, quality=95)
            paths.append(path)
    return paths


def run(guid, timestamp, reader, output_dir="output", window=5, get_image=default_get_image):
    """`reader`: DetectionsReader (Kafka directo) o KafkaUIDetectionsReader (vía web)."""
    before, after = reader.fetch(guid, timestamp, before=window, after=window)
    return export(guid, timestamp, before, after, output_dir, get_image)


def parse_kafka_config(pairs):
    config = {}
    for pair in pairs or []:
        key, sep, value = pair.partition("=")
        if not sep:
            raise argparse.ArgumentTypeError(f"Se esperaba clave=valor: {pair!r}")
        config[key] = value
    return config


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("guid")
    parser.add_argument("timestamp", type=int, help="Timestamp en milisegundos")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                        help="Conexión directa a Kafka (si no se usa --kafka-ui)")
    parser.add_argument("-X", dest="kafka_config", action="append", metavar="CLAVE=VALOR",
                        help="Propiedad extra de librdkafka (repetible)")
    parser.add_argument("--window", type=int, default=5,
                        help="Mensajes anteriores y posteriores a leer (por defecto 5)")
    parser.add_argument("--margin", type=int, default=5,
                        help="Offsets extra a leer por partición alrededor del timestamp")
    parser.add_argument("-o", "--output-dir", default="output")

    ui = parser.add_argument_group("UI for Apache Kafka (en lugar de conexión directa)")
    ui.add_argument("--kafka-ui", metavar="URL",
                    help="URL de la interfaz web, p. ej. http://10.98.120.24:8080")
    ui.add_argument("--ui-cluster", help="Nombre del cluster (obligatorio si hay varios)")
    ui.add_argument("--ui-user", help="Usuario si la web pide login")
    ui.add_argument("--ui-password", help="Contraseña si la web pide login")
    ui.add_argument("--value-serde", help="Serde del valor (por defecto, automático)")
    ui.add_argument("--insecure", action="store_true",
                    help="No verificar el certificado HTTPS de la web")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.kafka_ui:
        from .kafka_ui_reader import KafkaUIDetectionsReader

        reader = KafkaUIDetectionsReader(
            args.kafka_ui, cluster=args.ui_cluster, username=args.ui_user,
            password=args.ui_password, margin=args.margin, value_serde=args.value_serde,
            verify_ssl=not args.insecure,
        )
    else:
        from .kafka_reader import DetectionsReader

        kafka_config = {"bootstrap.servers": args.bootstrap_servers,
                        **parse_kafka_config(args.kafka_config)}
        reader = DetectionsReader(kafka_config, margin=args.margin)

    paths = run(args.guid, args.timestamp, reader, args.output_dir, args.window)
    for path in paths:
        print(path)


if __name__ == "__main__":
    main()
