import qrcode
from qrcode.constants import ERROR_CORRECT_H
from PIL import Image, ImageDraw, ImageFont

def generate_qr_with_center_text(
    data: str,
    center_text: str,
    qr_box_size: int = 10,
    qr_border: int = 4,
    text_padding: int = 8,
    font_path: str = None,   # e.g., r"C:\Windows\Fonts\arial.ttf"
    font_size: int = 48,
    output_path: str = "qr_with_text.png"
):
    # 1) QR create with high error correction
    qr = qrcode.QRCode(
        version=None,                 # auto size
        error_correction=ERROR_CORRECT_H,
        box_size=qr_box_size,
        border=qr_border
    )
    qr.add_data(data)
    qr.make(fit=True)

    img = qr.make_image(fill_color="black", back_color="white").convert("RGB")

    # 2) Prepare drawing
    draw = ImageDraw.Draw(img)

    # 3) Choose font
    if font_path:
        font = ImageFont.truetype(font_path, font_size)
    else:
        # Fallback font
        font = ImageFont.load_default()

    # 4) Measure text size
    text_w, text_h = draw.textbbox((0,0), center_text, font=font)[2:]
    box_w = text_w + 2 * text_padding
    box_h = text_h + 2 * text_padding

    # 5) Compute center position
    img_w, img_h = img.size
    box_x = (img_w - box_w) // 2
    box_y = (img_h - box_h) // 2

    # 6) Draw white rectangle background for readability
    draw.rectangle(
        [(box_x, box_y), (box_x + box_w, box_y + box_h)],
        fill="white"
    )

    # 7) Draw text in center
    text_x = box_x + text_padding
    text_y = box_y + text_padding
    draw.text((text_x, text_y), center_text, fill="black", font=font)

    # 8) Save output
    img.save(output_path)
    print(f"Saved: {output_path}")

if __name__ == "__main__":
    # Example use
    generate_qr_with_center_text(
        data="https://qrorder.restroworks.com/redirect?id=6931302b6af8b8425e0816e9",
        center_text="T 1",                 # Beech ka number
        qr_box_size=10,
        qr_border=4,
        text_padding=10,
        font_path=r"C:\Windows\Fonts\arial.ttf",  # Windows 11 par common
        font_size=48,
        output_path="E:/Coffee Island/Menu Item Image/QR/Store QR/GK II/order_12345_qr.png"
    )