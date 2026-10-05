"""Origen de las imágenes.

`get_image` es un punto de integración externo: aquí solo se declara la firma.
Sustituye esta implementación por la real (o pásala como parámetro a `run`).
Puede devolver un `PIL.Image.Image`, bytes de una imagen codificada (JPEG/PNG...)
o un array de numpy (H x W x 3, RGB).
"""


def get_image(guid: str, timestamp: int):
    raise NotImplementedError(
        "get_image(guid, timestamp) debe ser proporcionada por la integración real"
    )
