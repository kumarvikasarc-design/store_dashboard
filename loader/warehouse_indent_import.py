import pandas as pd
from sqlalchemy import create_engine, text
import urllib
import os
import warnings
from sqlalchemy.exc import SAWarning
warnings.filterwarnings("ignore", category=SAWarning)

# ================= SQL =================
SQL_CONN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=coffee_island_analytics;"
    "Trusted_Connection=yes;"
)

params = urllib.parse.quote_plus(SQL_CONN)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True
)

BASE_PATH = r"C:\Users\ACER\store_dashboard\inventory\Indent_report"

# ================= HELPERS =================
def find_col(df, keys):
    for c in df.columns:
        for k in keys:
            if k.lower() in str(c).lower():
                return c
    return None

def clean_num(col):
    if col is None:
        return 0
    if isinstance(col, pd.DataFrame):
        col = col.iloc[:, 0]
    return pd.to_numeric(
        col.astype(str).str.replace(',', '').str.strip(),
        errors='coerce'
    ).fillna(0)

def pick_col(df, col):
    if col is None:
        return None
    data = df[col]
    if isinstance(data, pd.DataFrame):
        return data.iloc[:, 0]
    return data

# ===== duplicate file check =====
def file_already_imported(filename):
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM file_import_log WHERE file_name = :f"),
            {"f": filename}
        ).fetchone()
        return result[0] > 0

def log_file(filename):
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO file_import_log (file_name) VALUES (:f)"),
            {"f": filename}
        )

# ================= LOOP =================
for file in os.listdir(BASE_PATH):

    if not file.endswith(".csv"):
        continue

    if file_already_imported(file):
        print(f"⏩ Skipped already imported: {file}")
        continue

    print(f"\n📂 Importing: {file}")
    full = os.path.join(BASE_PATH, file)

    # 🔥 header always row 6
    try:
        df = pd.read_csv(full, skiprows=5, engine="python", encoding="utf-8")
    except:
        df = pd.read_csv(full, skiprows=5, engine="python", encoding="latin1")

    df = df.dropna(how="all")
    df.columns = df.columns.astype(str).str.strip()

    print("✔ Header loaded")

    # ===== column detect =====
    col_date = find_col(df, ["date"])
    col_wh = find_col(df, ["warehouse","supplier"])
    col_outlet = find_col(df, ["receiver"])
    col_indent = find_col(df, ["indent"])
    col_status = find_col(df, ["status"])
    col_itemcode = find_col(df, ["item code"])
    col_item = find_col(df, ["item name"])
    col_cat = find_col(df, ["category name"])
    col_super = find_col(df, ["super category"])
    col_uom = find_col(df, ["unit"])
    col_req = find_col(df, ["requested"])
    col_recv = find_col(df, ["received qty"])
    col_waste = find_col(df, ["wastage"])
    col_price = find_col(df, ["unitprice"])
    col_amt = find_col(df, ["amount"])

    print("Detected:", col_wh, col_item, col_req)

    if not col_item or not col_date:
        print("❌ Required columns missing -> skipped:", file)
        continue

    # ================= CLEAN =================
    df[col_date] = pd.to_datetime(df[col_date], dayfirst=True, errors="coerce")

    items = df[df[col_item].notna()].copy()

    if col_req: items[col_req] = clean_num(items[col_req])
    if col_recv: items[col_recv] = clean_num(items[col_recv])
    if col_waste: items[col_waste] = clean_num(items[col_waste])
    if col_price: items[col_price] = clean_num(items[col_price])
    if col_amt: items[col_amt] = clean_num(items[col_amt])

    # remove blank items
    items = items[items[col_item].astype(str).str.strip() != ""]

    # ================= FINAL =================
    final = pd.DataFrame({
        "indent_date": pick_col(items, col_date),
        "warehouse": pick_col(items, col_wh),
        "outlet_name": pick_col(items, col_outlet),
        "indent_number": pick_col(items, col_indent),
        "status": pick_col(items, col_status),

        "item_code": pick_col(items, col_itemcode),
        "item_name": pick_col(items, col_item),
        "category": pick_col(items, col_cat),
        "super_category": pick_col(items, col_super),

        "uom": pick_col(items, col_uom),
        "indent_qty": pick_col(items, col_req),
        "received_qty": pick_col(items, col_recv),
        "wastage_qty": pick_col(items, col_waste),

        "unit_price": pick_col(items, col_price),
        "total_amount": pick_col(items, col_amt)
    })

    final = final.dropna(subset=["item_name"])

    if final.empty:
        print("⚠ No valid rows:", file)
        continue

    final.to_sql("warehouse_indent", engine, if_exists="append", index=False)

    log_file(file)
    print(f"✅ Imported {len(final)} rows")

print("\n🏁 ALL FILES IMPORT COMPLETED")
