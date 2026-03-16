import pandas as pd
from sqlalchemy import create_engine, text
import urllib
import os

# ================= SQL CONNECTION =================
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

BASE_PATH = r"C:\Users\ACER\store_dashboard\inventory\warehouse_stockentry"

# ================= HELPERS =================
def find_col(df, keywords):
    for col in df.columns:
        for k in keywords:
            if k.lower() in col.lower():
                return col
    return None

def clean_numeric(col):
    return pd.to_numeric(col.astype(str).str.replace(',', '').str.strip(), errors='coerce').fillna(0)

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

# ================= LOOP FILES =================
for file in os.listdir(BASE_PATH):

    if not file.endswith(".csv"):
        continue

    if file_already_imported(file):
        print(f"⏩ Skipped: {file}")
        continue

    full_path = os.path.join(BASE_PATH, file)
    print(f"📂 Importing: {file}")

    df = pd.read_csv(full_path)
    df.columns = df.columns.str.strip()

    # -------- detect columns --------
    col_date = find_col(df, ["date"])
    col_invoice_date = find_col(df, ["invoice date"])
    col_wh = find_col(df, ["deployment", "warehouse"])
    col_supplier = find_col(df, ["vendor"])
    col_trn = find_col(df, ["transaction"])
    col_invoice = find_col(df, ["invoice number"])
    col_po = find_col(df, ["po number"])
    col_itemcode = find_col(df, ["item code"])
    col_item = find_col(df, ["item name"])
    col_cat = find_col(df, ["category"])
    col_super = find_col(df, ["super category"])
    col_qty = find_col(df, ["quantity"])
    col_uom = find_col(df, ["unit"])
    col_price = find_col(df, ["unit price"])
    col_charge = find_col(df, ["charges"])
    col_amount = find_col(df, ["amount"])
    col_gst = find_col(df, ["gst"])
    col_tax = find_col(df, ["total tax"])
    col_total = find_col(df, ["total"])

    df[col_date] = pd.to_datetime(df[col_date], dayfirst=True, errors="coerce")

    # ================= ITEM ROWS =================
    items = df[
        df[col_item].notna() &
        (df[col_item].astype(str).str.strip() != '-') &
        (pd.to_numeric(df[col_qty], errors="coerce") > 0)
    ].copy()

    if not items.empty:

        # ===== CLEAN ALL NUMERIC COLUMNS =====
        items[col_qty] = clean_numeric(items[col_qty])
        items[col_price] = clean_numeric(items[col_price])

        if col_amount:
            items[col_amount] = clean_numeric(items[col_amount])

        if col_tax:
            items[col_tax] = clean_numeric(items[col_tax])

        if col_total:
            items[col_total] = clean_numeric(items[col_total])

        if col_gst:
            items[col_gst] = clean_numeric(items[col_gst])


        items_df = pd.DataFrame({
            "entry_date": items[col_date],
            "invoice_date": pd.to_datetime(items[col_invoice_date], dayfirst=True, errors="coerce"),
            "warehouse": items[col_wh],
            "supplier": items[col_supplier],

            "transaction_number": items[col_trn],
            "invoice_no": items[col_invoice],
            "po_number": items[col_po],

            "item_code": items[col_itemcode],
            "item_name": items[col_item],
            "category": items[col_cat],
            "super_category": items[col_super],

            "qty_in": items[col_qty],
            "uom": items[col_uom],
            "unit_price": items[col_price],

            "charges_name": items[col_charge] if col_charge else None,
            "amount": items[col_amount] if col_amount else 0,
            "gst_rate": items[col_gst] if col_gst else 0,
            "tax": items[col_tax] if col_tax else 0,
            "total_amount": items[col_total] if col_total else 0
        })

        items_df.to_sql("warehouse_stockentry", engine, if_exists="append", index=False)

    # ================= FREIGHT =================
    if col_charge:
        charges = df[
            (df[col_item].isna() | (df[col_item].astype(str).str.strip() == '-')) &
            df[col_charge].notna()
        ].copy()

        if not charges.empty:

            if col_amount:
                charges[col_amount] = clean_numeric(charges[col_amount])


            charges_df = pd.DataFrame({
                "invoice_no": charges[col_invoice],
                "transaction_number": charges[col_trn],
                "warehouse": charges[col_wh],
                "supplier": charges[col_supplier],
                "charge_name": charges[col_charge],
                "charge_amount": charges[col_amount],
                "entry_date": charges[col_date]
            })

            charges_df.to_sql("warehouse_entry_charges", engine, if_exists="append", index=False)

    log_file(file)

print("✅ ENTRY IMPORT COMPLETED (PRO)")
