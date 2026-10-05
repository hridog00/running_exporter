import json

from PIL import Image

from exporter import kafka_reader
from exporter.drawing import draw_objects
from exporter.kafka_reader import Detection, DetectionsReader, has_objects, select_window
from exporter.main import export

OBJ = {"Label": "persona", "AreaId": 67, "ClassId": 1, "Score": 0.946,
       "Tlwh": [165, 50, 65, 197], "KeyPoints": []}


def payload(ts, objects):
    return {"FrameTimestamp": ts, "FrameCount": ts, "FrameWidth": 1080, "FrameHeight": 1920,
            "Objects": objects, "InvalidObjects": []}


def det(ts, objects=()):
    return Detection(ts, payload(ts, list(objects)))


def test_select_window_takes_closest_before_and_after():
    dets = [det(t) for t in range(0, 200, 10)]
    before, after = select_window(dets, 100, 5, 5)
    assert [d.frame_timestamp for d in before] == [50, 60, 70, 80, 90]
    assert [d.frame_timestamp for d in after] == [100, 110, 120, 130, 140]


def test_select_window_at_edges():
    dets = [det(t) for t in (1, 2, 3)]
    before, after = select_window(dets, 2, 5, 5)
    assert [d.frame_timestamp for d in before] == [1]
    assert [d.frame_timestamp for d in after] == [2, 3]


def test_has_objects():
    assert has_objects(det(1, [OBJ]))
    assert not has_objects(det(1))
    assert not has_objects(Detection(1, {"Objects": None}))


def test_draw_objects_scales_to_image():
    img = Image.new("RGB", (540, 960), "white")
    out = draw_objects(img, [OBJ], 1080, 1920)
    assert out.size == img.size
    # Borde izquierdo de la caja escalado a la mitad (165/2 ≈ 82)
    assert out.getpixel((83, 100)) != (255, 255, 255)
    assert img.getpixel((83, 100)) == (255, 255, 255)


def test_export_only_keeps_messages_with_objects(tmp_path):
    before = [det(10, [OBJ]), det(20)]
    after = [det(30), det(40, [OBJ, OBJ])]
    calls = []

    def fake_get_image(guid, ts):
        calls.append((guid, ts))
        return Image.new("RGB", (1080, 1920))

    paths = export("abc", 25, before, after, tmp_path, fake_get_image)
    assert calls == [("abc", 10), ("abc", 40)]
    assert [p.name for p in paths] == ["anterior_01_10.jpg", "posterior_01_40.jpg"]
    assert all(p.exists() for p in paths)


class FakeMessage:
    def __init__(self, partition, offset, value):
        self._p, self._o, self._v = partition, offset, value

    def error(self):
        return None

    def offset(self):
        return self._o

    def partition(self):
        return self._p

    def value(self):
        return self._v

    def timestamp(self):
        return (1, json.loads(self._v)["FrameTimestamp"])


class FakeConsumer:
    """Topic con 2 particiones; los mensajes de cada partición tienen timestamps crecientes."""

    def __init__(self, data):
        self.data = data  # {partition: [ts, ...]}
        self.queue = []

    def list_topics(self, topic, timeout=None):
        class Meta:
            pass

        tmeta = Meta()
        tmeta.error = None
        tmeta.partitions = {p: None for p in self.data}
        meta = Meta()
        meta.topics = {topic: tmeta}
        return meta

    def get_watermark_offsets(self, tp, timeout=None):
        return 0, len(self.data[tp.partition])

    def offsets_for_times(self, tps, timeout=None):
        tp = tps[0]
        tss = self.data[tp.partition]
        off = next((i for i, t in enumerate(tss) if t >= tp.offset), -1)
        return [kafka_reader.TopicPartition(tp.topic, tp.partition, off)]

    def assign(self, tps):
        tp = tps[0]
        self.queue = [
            FakeMessage(tp.partition, i, json.dumps(payload(t, [OBJ] if t % 20 else [])).encode())
            for i, t in enumerate(self.data[tp.partition]) if i >= tp.offset
        ]

    def poll(self, timeout):
        return self.queue.pop(0) if self.queue else None

    def unassign(self):
        self.queue = []

    def close(self):
        pass


def test_reader_merges_partitions(monkeypatch):
    data = {0: list(range(0, 400, 20)), 1: list(range(10, 400, 20))}
    monkeypatch.setattr(kafka_reader, "Consumer", lambda conf: FakeConsumer(data))
    before, after = DetectionsReader({}, margin=0).fetch("g", 200)
    assert [d.frame_timestamp for d in before] == [150, 160, 170, 180, 190]
    assert [d.frame_timestamp for d in after] == [200, 210, 220, 230, 240]


def test_reader_timestamp_after_end(monkeypatch):
    data = {0: [10, 20, 30]}
    monkeypatch.setattr(kafka_reader, "Consumer", lambda conf: FakeConsumer(data))
    before, after = DetectionsReader({}).fetch("g", 1000)
    assert [d.frame_timestamp for d in before] == [10, 20, 30]
    assert after == []
