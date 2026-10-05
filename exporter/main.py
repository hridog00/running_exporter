"""CLI: python -m exporter.main <guid> <timestamp> [opciones]"""

import argparse
import logging
from pathlib import Path

from .drawing import draw_objects
from .image_source import get_image as default_get_image
from .kafka_reader import DetectionsReader, has_objects

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


def run(guid, timestamp, kafka_config, output_dir="output", window=5, margin=5,
        get_image=default_get_image):
    reader = DetectionsReader(kafka_config, margin=margin)
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
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("-X", dest="kafka_config", action="append", metavar="CLAVE=VALOR",
                        help="Propiedad extra de librdkafka (repetible)")
    parser.add_argument("--window", type=int, default=5,
                        help="Mensajes anteriores y posteriores a leer (por defecto 5)")
    parser.add_argument("--margin", type=int, default=5,
                        help="Offsets extra a leer por partición alrededor del timestamp")
    parser.add_argument("-o", "--output-dir", default="output")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    kafka_config = {"bootstrap.servers": args.bootstrap_servers,
                    **parse_kafka_config(args.kafka_config)}
    paths = run(args.guid, args.timestamp, kafka_config, args.output_dir,
                args.window, args.margin)
    for path in paths:
        print(path)


if __name__ == "__main__":
    main()
