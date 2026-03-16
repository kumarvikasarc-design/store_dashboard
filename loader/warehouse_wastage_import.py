import pandas as pd
from sqlalchemy import create_engine
import urllib
import os

# ================= SQL =================

SQL_CONN = r"""
DRIVER={ODBC Driver 17 for SQL Server};
SERVER=localhost\SQLEXPRESS;
DATABASE=coffee_island_analytics;
Trusted_Connection=yes;
"""

params = urllib.parse.quote_plus(SQL_CONN)

engine = create_engine(
f"mssql+pyodbc:///?odbc_connect={params}",
fast_executemany=True
)

BASE_PATH = r"C:\Users\ACER\store_dashboard\inventory\warehouse_wastage"

# ================= HELPERS =================

def clean_num(col):
    return pd.to_numeric(
    col.astype(str).str.replace(',', '').str.strip(),
    errors="coerce"
    ).fillna(0)

def find_col(df, words):
    for c in df.columns:
        for w in words:
            if w in c.lower():
                return c
    return None

# ================= LOOP =================

for file in os.listdir(BASE_PATH):

    if not file.endswith(".csv"):
        continue

    print(f"\n📂 Importing wastage: {file}")
    full = os.path.join(BASE_PATH, file)

    # ===== read raw =====
    raw = pd.read_csv(
        full,
        header=None,
        engine="python",
        encoding="latin1",
        sep=None,
        on_bad_lines="skip"
    )


    # ================= EXTRACT HEADER INFO =================
    warehouse = None
    store_hdr = None
    user_hdr = None
    date_hdr = None

    for i in range(min(15, len(raw))):
        row = raw.iloc[i].astype(str).str.lower().tolist()

        if any("deployment name" in x or "warehouse" in x for x in row):
            if len(raw.iloc[i]) > 1:
                warehouse = raw.iloc[i,1]

        if any("store/kitchen" in x or "store" in x for x in row):
            if len(raw.iloc[i]) > 1:
                store_hdr = raw.iloc[i,1]

        if any("user" in x for x in row):
            if len(raw.iloc[i]) > 1:
                user_hdr = raw.iloc[i,1]

        if any("date" in x for x in row):
            if len(raw.iloc[i]) > 1:
                date_hdr = raw.iloc[i,1]

    warehouse = str(warehouse).strip() if warehouse else None
    store_hdr = str(store_hdr).strip() if store_hdr else None
    user_hdr = str(user_hdr).strip() if user_hdr else None
    date_hdr = pd.to_datetime(date_hdr, dayfirst=True, errors="coerce")

    print("HEADER →", warehouse, store_hdr, user_hdr, date_hdr)

    # ===== detect header row =====
    header_row = None
    for i in range(min(20, len(raw))):
        row = ",".join(raw.iloc[i].astype(str)).lower()
        if "item name" in row and "quantity" in row:
            header_row = i
            break

    if header_row is None:
        print("❌ header not found:", file)
        continue

    print("✔ Header found at row:", header_row+1)

    # ===== read actual table =====
    # ===== read actual table =====
    df = pd.read_csv(
        full,
        engine="python",
        encoding="latin1",
        sep=None,
        on_bad_lines="skip",
        header=0
    )

    # force correct column names
    df.columns = [
        "deployment_name","store_name","user_name","date",
        "transaction_number","item_code","item_name",
        "category","super_category","comment",
        "qty","unit","unit_price","amount","source"
    ][:len(df.columns)]

    df.columns = df.columns.str.strip().str.lower()

    # ================= DATE FIX =================
    df["date"] = df["date"].astype(str).str.strip()

    df["date"] = df["date"].str.replace(r'[^0-9\-/]', '', regex=True)

    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")

    # if still null → take header date
    if df["date"].isna().all():
        df["date"] = date_hdr

    # ================= WAREHOUSE FIX =================
    df["deployment_name"] = df["deployment_name"].astype(str).str.strip()

    # if deployment empty → use header warehouse
    if warehouse:
        df["deployment_name"] = df["deployment_name"].replace(
            ["", "nan", "None"],
            warehouse
        )

    # ================= CLEAN NUMBERS =================
    df["qty"] = clean_num(df["qty"])
    df["unit_price"] = clean_num(df["unit_price"])
    df["amount"] = clean_num(df["amount"])

    # ================= FINAL =================
    final = pd.DataFrame({
        "wastage_date": df["date"],
        "warehouse": df["deployment_name"],
        "store_name": df["store_name"],
        "user_name": df["user_name"],
        "transaction_number": df["transaction_number"],
        "item_code": df["item_code"],
        "item_name": df["item_name"],
        "category": df["category"],
        "super_category": df["super_category"],
        "comment": df["comment"],
        "qty": df["qty"],
        "uom": df["unit"],
        "unit_price": df["unit_price"],
        "amount": df["amount"],
        "source": file
    })

    final = final[final["item_name"].notna()]

    # trim text
    final["item_name"] = final["item_name"].astype(str).str[:390]
    final["comment"] = final["comment"].astype(str).str[:490]
    final["source"] = final["source"].astype(str).str[:490]

    # ===== duplicate check =====
    dup = pd.read_sql(f"""
    select count(*) c from warehouse_wastage 
    where source='{file}'
    """, engine)

    if dup.iloc[0,0] > 0:
        print("⚠️ Already imported:", file)
        continue

    final.to_sql("warehouse_wastage", engine, if_exists="append", index=False)

    print(f"✅ Imported {len(final)} rows")
    
print("\n🏁 WASTAGE IMPORT DONE")
