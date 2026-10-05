"""Pintado de los objetos detectados sobre una imagen."""

import colorsys
import io

from PIL import Image, ImageDraw, ImageFont


def to_pil(image) -> Image.Image:
    """Convierte lo que devuelva get_image (PIL, bytes o array numpy) a PIL RGB."""
    if isinstance(image, Image.Image):
        return image.convert("RGB")
    if isinstance(image, (bytes, bytearray)):
        return Image.open(io.BytesIO(image)).convert("RGB")
    if hasattr(image, "__array_interface__"):
        return Image.fromarray(image).convert("RGB")
    raise TypeError(f"Tipo de imagen no soportado: {type(image)!r}")


def color_for(class_id) -> tuple:
    """Color estable y distinto por ClassId (tonos repartidos con la razón áurea)."""
    try:
        cid = int(class_id)
    except (TypeError, ValueError):
        cid = 0
    hue = (cid * 0.618033988749895) % 1.0
    r, g, b = colorsys.hsv_to_rgb(hue, 0.85, 1.0)
    return round(r * 255), round(g * 255), round(b * 255)


def _font(size: int):
    try:
        return ImageFont.load_default(size=size)
    except TypeError:  # Pillow < 10.1
        return ImageFont.load_default()


def draw_objects(image, objects, frame_width=None, frame_height=None) -> Image.Image:
    """Dibuja cada objeto (caja Tlwh + "label score") y devuelve una imagen nueva.

    Tlwh = [x_izquierda, y_arriba, ancho, alto] en coordenadas del frame
    (FrameWidth x FrameHeight). Si la imagen tiene otro tamaño, se reescala.
    """
    img = to_pil(image).copy()
    draw = ImageDraw.Draw(img)

    sx = img.width / frame_width if frame_width else 1.0
    sy = img.height / frame_height if frame_height else 1.0
    line = max(2, round(min(img.width, img.height) / 300))
    font = _font(max(12, round(min(img.width, img.height) / 50)))

    for obj in objects:
        tlwh = obj.get("Tlwh") or []
        if len(tlwh) != 4:
            continue
        x, y, w, h = tlwh
        box = (x * sx, y * sy, (x + w) * sx, (y + h) * sy)
        color = color_for(obj.get("ClassId"))
        draw.rectangle(box, outline=color, width=line)

        score = obj.get("Score")
        text = obj.get("Label", "?")
        if score is not None:
            text = f"{text} {float(score):.2f}"
        left, top, right, bottom = draw.textbbox((0, 0), text, font=font)
        tw, th = right - left, bottom - top
        pad = 2
        # Etiqueta encima de la caja; si no cabe, por dentro.
        ty = box[1] - th - 2 * pad
        if ty < 0:
            ty = box[1]
        draw.rectangle((box[0], ty, box[0] + tw + 2 * pad, ty + th + 2 * pad), fill=color)
        draw.text((box[0] + pad - left, ty + pad - top), text, fill=(0, 0, 0), font=font)
    return img
