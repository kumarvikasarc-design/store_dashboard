import pandas as pd
import win32print

PRINTER_NAME = "ZDesigner ZD230-203dpi ZPL"
EXCEL_FILE = r"C:\Users\ACER\Desktop\Labels.xlsx"

# ==========================
# LOAD EXCEL (Vertical Format)
# ==========================
df = pd.read_excel(EXCEL_FILE)

data = dict(zip(df["Field"], df["Declaration"]))

product_name = data.get("Product Name", "")
net_qty = data.get("Net Quantity", "")
capacity = data.get("Capacity", "")
material = data.get("Material Composition", "")
mfg = data.get("Month & Year of Manufacture/Packing", "")
mrp = data.get("Maximum Retail Price (MRP)", "")
manufacturer = data.get("Manufactured / Packed by", "")
marketer = data.get("Marketed by", "")
consumer = data.get("Consumer Care Details", "")
country = data.get("Country of Origin", "")

# Clean MRP text (remove ₹ and extra text)
mrp_clean = str(mrp).replace("₹", "").replace("(Inclusive of all taxes)", "").strip()

# Format Mfg date to mmm-yyyy
try:
    mfg_formatted = pd.to_datetime(mfg).strftime("%b-%Y")
except:
    mfg_formatted = mfg  # if already text, keep as is
    
# ==========================
# ZPL WITH TABLE BORDER (2x3 inch)
# ==========================

zpl = f"""
^XA
^PW406
^LL609
^LH0,0
^CF0,22

^FO5,5^GB396,599,2^FS

^CF0,28
^FO0,15^FB406,1,0,C^FDPRODUCT DECLARATION^FS
^CF0,22

^FO10,50^GB386,40,1^FS
^FO20,60^FB360,2,5,L^FD{product_name}^FS

^FO10,95^GB186,35,1^FS
^FO20,105^FDNet Qty: {net_qty}^FS

^FO210,95^GB186,35,1^FS
^FO220,105^FDCapacity: {capacity}^FS

^FO10,130^GB386,35,1^FS
^FO20,140^FDMaterial: {material}^FS

^FO10,165^GB386,35,1^FS
^FO20,175^FDMfg: {mfg_formatted}^FS

^FO10,200^GB386,70,1^FS
^CF0,42
^FO0,210^FB406,1,0,C^FDMRP: Rs. {mrp_clean}^FS
^CF0,22
^FO0,245^FB406,1,0,C^FD(Inclusive of all taxes)^FS

^FO10,270^GB386,85,1^FS
^FO20,280^FB360,3,5,L^FDManufactured By:
{manufacturer}^FS

^FO10,355^GB386,120,1^FS
^FO20,365^FB360,4,5,L^FDMarketed By:
{marketer}^FS

^FO10,475^GB386,60,1^FS
^FO20,485^FB360,2,5,L^FD{consumer}^FS

^FO10,535^GB386,35,1^FS
^FO20,545^FDCountry of Origin: {country}^FS

^XZ
"""
# ==========================
# SEND TO PRINTER (ONE LABEL ONLY)
# ==========================
printer = win32print.OpenPrinter(PRINTER_NAME)
win32print.StartDocPrinter(printer, 1, ("Label", None, "RAW"))
win32print.StartPagePrinter(printer)
win32print.WritePrinter(printer, zpl.encode("utf-8"))
win32print.EndPagePrinter(printer)
win32print.EndDocPrinter(printer)
win32print.ClosePrinter(printer)

print("Label Printed Successfully!")
