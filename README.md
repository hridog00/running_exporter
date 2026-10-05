# running_exporter

Dado un `guid` y un `timestamp` (ms), lee del topic de Kafka `<guid>_detections`
los 5 mensajes anteriores y los 5 posteriores al timestamp, se queda solo con los
que tienen algún elemento en `Objects`, obtiene la imagen de cada frame con
`get_image(guid, FrameTimestamp)` y pinta cada objeto (caja `Tlwh` + `Label` y `Score`).

## Uso

```bash
pip install -r requirements.txt
python -m exporter.main <guid> <timestamp_ms> --bootstrap-servers kafka:9092 \
    [-X security.protocol=SASL_SSL -X sasl.mechanism=PLAIN ...] [-o output]
```

Genera una imagen por mensaje en `output/<guid>/<timestamp>/`:

```
anterior_01_<FrameTimestamp>.jpg ... anterior_NN_<FrameTimestamp>.jpg
posterior_01_<FrameTimestamp>.jpg ... posterior_NN_<FrameTimestamp>.jpg
```

Desde código, se puede inyectar la implementación real de `get_image`:

```python
from exporter.main import run
run(guid, ts, {"bootstrap.servers": "kafka:9092"}, get_image=mi_get_image)
```

o sustituir el stub de `exporter/image_source.py`. `get_image` puede devolver un
`PIL.Image`, bytes (JPEG/PNG) o un array numpy RGB.

## Criterios

- **Anterior**: `FrameTimestamp < timestamp`; **posterior**: `FrameTimestamp >= timestamp`.
- Primero se toman los 5 + 5 mensajes más cercanos y después se descartan los que
  tienen `Objects` vacío, así que puede haber menos de 5 imágenes por lado.
- Se soportan topics con varias particiones: en cada una se localiza el offset con
  `offsets_for_times` y se lee una ventana (`--margin` offsets extra para tolerar
  diferencias entre el timestamp de Kafka y `FrameTimestamp`); luego se ordena todo
  por `FrameTimestamp`.
- `Tlwh` = `[x, y, ancho, alto]` en píxeles del frame (`FrameWidth` x `FrameHeight`);
  si la imagen tiene otro tamaño, las cajas se reescalan.

## Tests

```bash
pip install pytest && python -m pytest
```
