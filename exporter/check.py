"""Diagnóstico de conexión: python -m exporter.check [opciones]

Comprueba paso a paso la conexión a Kafka (directa) y/o a UI for Apache Kafka y
explica en qué paso falla.
"""

import argparse
import socket
import sys

TCP_TIMEOUT_S = 5
METADATA_TIMEOUT_S = 15


def ok(msg):
    print(f"  [OK]    {msg}")


def fail(msg, hint=None):
    print(f"  [FALLO] {msg}")
    if hint:
        print(f"          -> {hint}")


def split_host_port(address, default_port=9092):
    host, sep, port = address.strip().rpartition(":")
    if not sep:
        return address.strip(), default_port
    return host.strip("[]"), int(port)


def check_tcp(host, port):
    """Abre una conexión TCP. Devuelve True si el puerto acepta conexiones."""
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        fail(f"No se resuelve el nombre '{host}': {exc}",
             "Comprueba el nombre o añádelo a C:\\Windows\\System32\\drivers\\etc\\hosts")
        return False

    last_error = None
    for family, socktype, proto, _, addr in infos:
        try:
            with socket.socket(family, socktype, proto) as sock:
                sock.settimeout(TCP_TIMEOUT_S)
                sock.connect(addr)
            ok(f"Puerto abierto en {host}:{port} ({addr[0]})")
            return True
        except OSError as exc:
            last_error = exc

    if isinstance(last_error, socket.timeout):
        fail(f"Sin respuesta de {host}:{port} en {TCP_TIMEOUT_S}s",
             "Firewall o VPN bloqueando, o IP incorrecta")
    elif isinstance(last_error, ConnectionRefusedError):
        fail(f"Conexión rechazada en {host}:{port}",
             "La máquina responde pero nada escucha en ese puerto: el puerto no es el correcto")
    else:
        fail(f"No se puede conectar a {host}:{port}: {last_error}")
    return False


def check_kafka(bootstrap, extra_config, guid=None):
    print(f"\n== Kafka directo: {bootstrap}")

    print("\nPaso 1. Conexión de red (TCP) al bootstrap")
    reachable = [a for a in bootstrap.split(",") if check_tcp(*split_host_port(a))]
    if not reachable:
        return False

    print("\nPaso 2. Hablar con Kafka (pedir metadatos)")
    try:
        from confluent_kafka.admin import AdminClient
    except ImportError:
        fail("No está instalado confluent-kafka", "pip install -e .")
        return False

    errors = []
    config = {
        "bootstrap.servers": bootstrap,
        "socket.timeout.ms": 10000,
        "error_cb": lambda err: errors.append(str(err)),
        **extra_config,
    }
    try:
        metadata = AdminClient(config).list_topics(timeout=METADATA_TIMEOUT_S)
    except Exception as exc:  # noqa: BLE001 - queremos mostrar cualquier error
        fail(f"Kafka no responde: {exc}")
        for err in dict.fromkeys(errors):
            print(f"          {err}")
        print("          -> Si el puerto está abierto pero esto falla, lo normal es que el broker")
        print("             use SSL/SASL: prueba con -X security.protocol=SSL (o SASL_SSL ...)")
        return False
    ok(f"Kafka responde (cluster {metadata.cluster_id}, {len(metadata.topics)} topics)")

    print("\nPaso 3. Brokers anunciados (a estos se conecta el programa después)")
    all_ok = True
    for broker in sorted(metadata.brokers.values(), key=lambda b: b.id):
        print(f"  Broker {broker.id}: {broker.host}:{broker.port}")
        if not check_tcp(broker.host, broker.port):
            all_ok = False
    if not all_ok:
        print("          -> El broker anuncia una dirección no accesible desde este equipo.")
        print("             Hay que cambiar advertised.listeners en Kafka o ejecutar en el servidor.")
        return False

    detections = sorted(t for t in metadata.topics if t.endswith("_detections"))
    print(f"\nTopics *_detections: {len(detections)}")
    for name in detections[:20]:
        print(f"  {name}")
    if len(detections) > 20:
        print(f"  ... y {len(detections) - 20} más")

    if guid:
        check_topic(f"{guid}_detections", set(metadata.topics))
    return True


def check_topic(topic, topics):
    print(f"\nTopic buscado: {topic}")
    if topic in topics:
        ok("Existe")
        return True
    similar = [t for t in topics if t.lower() == topic.lower()]
    if similar:
        fail("No existe con esas mayúsculas/minúsculas",
             f"El nombre real es '{similar[0]}': escribe el guid igual")
    else:
        fail("No existe", "Revisa el guid")
    return False


def check_kafka_ui(url, username=None, password=None, cluster=None, guid=None,
                   verify_ssl=True):
    print(f"\n== UI for Apache Kafka: {url}")
    from urllib.parse import urlparse

    parsed = urlparse(url)
    print("\nPaso 1. Conexión de red (TCP) a la web")
    if not check_tcp(parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80)):
        return False

    print("\nPaso 2. API de la web")
    from .kafka_ui_reader import KafkaUIDetectionsReader, KafkaUIError

    try:
        reader = KafkaUIDetectionsReader(url, cluster=cluster, username=username,
                                         password=password, verify_ssl=verify_ssl)
        clusters = reader._get_json("/api/clusters")
    except Exception as exc:  # noqa: BLE001
        fail(str(exc))
        return False
    ok("La API responde")

    print("\nPaso 3. Clusters configurados en la web")
    for c in clusters:
        print(f"  {c.get('name')}: estado {c.get('status')}, "
              f"{c.get('brokerCount')} brokers, {c.get('topicCount')} topics")
    if not clusters:
        fail("La web no tiene clusters configurados")
        return False
    try:
        name = reader._cluster()
    except KafkaUIError as exc:
        fail(str(exc))
        return False

    try:
        brokers = reader._get_json(f"/api/clusters/{name}/brokers")
        print(f"\nBrokers de '{name}' (dirección real de Kafka, útil para --bootstrap-servers):")
        for b in brokers:
            print(f"  Broker {b.get('id')}: {b.get('host')}:{b.get('port')}")
    except Exception as exc:  # noqa: BLE001
        print(f"  (no se pudieron listar los brokers: {exc})")

    if guid:
        topic = f"{guid}_detections"
        print(f"\nTopic buscado: {topic}")
        try:
            reader._partitions(name, topic)
            ok("Existe")
        except Exception as exc:  # noqa: BLE001
            fail(str(exc), "Revisa el guid (mayúsculas/minúsculas incluidas)")
            return False
    return True


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap-servers", help="Comprobar Kafka directo, p. ej. 10.98.120.24:9092")
    parser.add_argument("-X", dest="kafka_config", action="append", default=[],
                        metavar="CLAVE=VALOR", help="Propiedad extra de librdkafka (repetible)")
    parser.add_argument("--kafka-ui", metavar="URL", help="Comprobar la web, p. ej. http://10.98.120.24:8080")
    parser.add_argument("--ui-cluster")
    parser.add_argument("--ui-user")
    parser.add_argument("--ui-password")
    parser.add_argument("--insecure", action="store_true")
    parser.add_argument("--guid", help="Comprobar además que existe <guid>_detections")
    args = parser.parse_args(argv)

    if not args.bootstrap_servers and not args.kafka_ui:
        parser.error("indica --bootstrap-servers y/o --kafka-ui")

    results = []
    if args.bootstrap_servers:
        from .main import parse_kafka_config

        results.append(check_kafka(args.bootstrap_servers,
                                   parse_kafka_config(args.kafka_config), args.guid))
    if args.kafka_ui:
        results.append(check_kafka_ui(args.kafka_ui, args.ui_user, args.ui_password,
                                      args.ui_cluster, args.guid, not args.insecure))

    print("\nResultado:", "TODO OK" if all(results) else "HAY FALLOS (ver arriba)")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
