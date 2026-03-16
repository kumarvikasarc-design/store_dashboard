# utils/export_pdf.py
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
import pandas as pd

def df_to_pdf_bytes(df: pd.DataFrame) -> bytes:
    """
    Simple PDF generator: renders store_summary DataFrame into a basic PDF table.
    For complex layout replace with reportlab platypus or wkhtmltopdf.
    """
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    left = 20 * mm
    top = height - 20 * mm
    line_h = 8 * mm

    c.setFont("Helvetica-Bold", 14)
    c.drawString(left, top, "COGS Store Summary")
    c.setFont("Helvetica", 9)
    y = top - 15

    if df.empty:
        c.drawString(left, y, "No data")
        c.showPage()
        c.save()
        buf.seek(0)
        return buf.read()

    # Draw header
    cols = list(df.columns)
    x_positions = [left + i * (width - 40*mm) / max(1, len(cols)) for i in range(len(cols))]
    c.setFont("Helvetica-Bold", 8)
    for i, col in enumerate(cols):
        c.drawString(x_positions[i], y, str(col))
    y -= line_h

    c.setFont("Helvetica", 8)
    # Rows (limit to fit)
    rows_to_show = df.fillna("").astype(str).values.tolist()
    for r in rows_to_show:
        if y < 40:
            c.showPage()
            y = top - 20
        for i, cell in enumerate(r):
            c.drawString(x_positions[i], y, cell[:30])
        y -= line_h

    c.showPage()
    c.save()
    buf.seek(0)
    return buf.read()
