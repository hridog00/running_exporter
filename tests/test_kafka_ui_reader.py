import json
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from exporter.kafka_ui_reader import KafkaUIDetectionsReader, KafkaUIError, iso_to_ms

TOPIC = "g_detections"
# Partición 0: timestamps pares de 20 en 20; partición 1: impares desplazados 10.
DATA = {0: list(range(0, 400, 20)), 1: list(range(10, 400, 20))}
OBJ = {"Label": "persona", "ClassId": 1, "Score": 0.9, "Tlwh": [1, 2, 3, 4]}


def message(partition, offset, ts, field):
    payload = {"FrameTimestamp": ts, "Objects": [OBJ], "InvalidObjects": []}
    iso = datetime.fromtimestamp(ts / 1000, timezone.utc).isoformat().replace("+00:00", "Z")
    return {"partition": partition, "offset": offset, "timestamp": iso,
            field: json.dumps(payload)}


def select(partition, ts, backward, limit):
    rows = [(i, t) for i, t in enumerate(DATA[partition])]
    if backward:
        rows = [r for r in rows if r[1] < ts][::-1]
    else:
        rows = [r for r in rows if r[1] >= ts]
    return rows[:limit]


def make_handler(state):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _json(self, code, body):
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            form = parse_qs(self.rfile.read(length).decode())
            ok = form.get("username") == ["u"] and form.get("password") == ["p"]
            self.send_response(302)
            self.send_header("Location", "/" if ok else "/login?error")
            if ok:
                self.send_header("Set-Cookie", "SESSION=abc; Path=/")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):
            url = urlparse(self.path)
            q = parse_qs(url.query)
            state["paths"].append(url.path)
            if state["auth"] and "SESSION=abc" not in (self.headers.get("Cookie") or ""):
                return self._json(401, {})
            if url.path == "/api/clusters":
                return self._json(200, [{"name": "local"}])
            if url.path == f"/api/clusters/local/topics/{TOPIC}":
                return self._json(200, {"partitions": [{"partition": p} for p in DATA]})
            if url.path.endswith("/messages/v2"):
                if not state["v2"]:
                    return self._json(404, {})
                partition = int(q["partitions"][0])
                ts = int(q["timestamp"][0])
                backward = q["mode"][0] == "TO_TIMESTAMP"
                field = "value"
            elif url.path.endswith("/messages"):
                partition, ts = map(int, q["seekTo"][0].split("::"))
                assert q["seekType"] == ["TIMESTAMP"]
                backward = q["seekDirection"][0] == "BACKWARD"
                field = "content"
            else:
                return self._json(404, {})
            rows = select(partition, ts, backward, int(q["limit"][0]))
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            events = [{"type": "PHASE", "phase": {"name": "x"}}]
            events += [{"type": "MESSAGE", "message": message(partition, i, t, field)}
                       for i, t in rows]
            events.append({"type": "DONE"})
            for ev in events:
                self.wfile.write(f"data:{json.dumps(ev)}\n\n".encode())

    return Handler


@pytest.fixture
def ui_server():
    state = {"v2": True, "auth": False, "paths": []}
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(state))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    state["url"] = f"http://127.0.0.1:{server.server_port}"
    yield state
    server.shutdown()


@pytest.mark.parametrize("v2", [True, False])
def test_fetch_window_v2_and_legacy(ui_server, v2):
    ui_server["v2"] = v2
    reader = KafkaUIDetectionsReader(ui_server["url"], margin=0)
    before, after = reader.fetch("g", 200)
    assert [d.frame_timestamp for d in before] == [150, 160, 170, 180, 190]
    assert [d.frame_timestamp for d in after] == [200, 210, 220, 230, 240]
    assert all(d.objects for d in before + after)
    used_legacy = any(p.endswith("/messages") for p in ui_server["paths"])
    assert used_legacy is (not v2)


def test_login(ui_server):
    ui_server["auth"] = True
    with pytest.raises(KafkaUIError, match="Acceso denegado"):
        KafkaUIDetectionsReader(ui_server["url"]).fetch("g", 200)
    with pytest.raises(KafkaUIError, match="Login"):
        KafkaUIDetectionsReader(ui_server["url"], username="u", password="mal")
    before, after = KafkaUIDetectionsReader(ui_server["url"], username="u", password="p").fetch("g", 200)
    assert len(before) == 5 and len(after) == 5


def test_unknown_topic(ui_server):
    with pytest.raises(KafkaUIError, match="no existe"):
        KafkaUIDetectionsReader(ui_server["url"]).fetch("otro", 200)


def test_iso_to_ms():
    assert iso_to_ms("2026-10-05T11:19:54.246Z") == 1791199194246
    assert iso_to_ms(None) is None


def test_check_kafka_ui(ui_server, capsys):
    from exporter.check import main

    assert main(["--kafka-ui", ui_server["url"], "--guid", "g"]) == 0
    assert main(["--kafka-ui", ui_server["url"], "--guid", "otro"]) == 1
    assert "no existe" in capsys.readouterr().out


def test_check_kafka_port_closed(capsys):
    import socket

    from exporter.check import main

    with socket.socket() as s:  # puerto libre sin nadie escuchando
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    assert main(["--bootstrap-servers", f"127.0.0.1:{port}"]) == 1
    assert "rechazada" in capsys.readouterr().out


def test_check_topic_case_mismatch(capsys):
    from exporter.check import check_topic

    assert not check_topic("ABC_detections", {"abc_detections"})
    assert "abc_detections" in capsys.readouterr().out


def test_cli_list_only_and_blank_image(ui_server, tmp_path, capsys):
    from exporter.main import main

    main(["g", "200", "--kafka-ui", ui_server["url"], "--list-only", "-o", str(tmp_path)])
    out = capsys.readouterr().out
    assert "Anteriores: 5" in out and "Posteriores: 5" in out
    assert not any(tmp_path.iterdir())

    main(["g", "200", "--kafka-ui", ui_server["url"], "--blank-image", "-o", str(tmp_path)])
    files = sorted(p.name for p in (tmp_path / "g" / "200").iterdir())
    assert len(files) == 10 and files[0].startswith("anterior_01_")
