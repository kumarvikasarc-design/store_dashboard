import pandas as pd
from sqlalchemy import create_engine
import urllib
import os

# ================= SQL CONNECTION =================
SQL_CONN = r"""
DRIVER={ODBC Driver 17 for SQL Server};
SERVER=localhost\SQLEXPRESS;
DATABASE=coffee_island_analytics;
Trusted_Connection=yes;
"""

params = urllib.parse.quote_plus(SQL_CONN)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

BASE_PATH = r"C:\Users\ACER\store_dashboard\inventory\expiryreport"

# ================= HELPERS =================
def clean_num(col):
    return pd.to_numeric(
        col.astype(str)
        .str.replace(',', '')
        .str.replace('NA','')
        .str.replace('-','')
        .str.strip(),
        errors="coerce"
    ).fillna(0)

def fix_date(col):
    return pd.to_datetime(
        col.astype(str)
        .str.replace('-', '')
        .str.strip(),
        dayfirst=True,
        errors="coerce"
    )
#===== AUTO HEADER DETECT =====
def smart_read(file):

    # read raw
    if file.endswith(".xlsx"):
        temp = pd.read_excel(file, header=None)
    else:
        temp = pd.read_csv(file, header=None)

    header_row = None

    # find header row properly
    for i in range(20):
        row = temp.iloc[i].astype(str).str.lower()

        if (
            "warehouse" in row.to_string()
            and "transaction" in row.to_string()
            and "item" in row.to_string()
        ):
            header_row = i
            break

    if header_row is None:
        raise Exception("❌ HEADER NOT FOUND")

    print("✅ Header found at row:", header_row)

    # read again with correct header
    if file.endswith(".xlsx"):
        df = pd.read_excel(file, header=header_row)
    else:
        df = pd.read_csv(file, header=header_row)

    return df

def fix_date(col):
    return pd.to_datetime(
        col,
        format="%d-%m-%Y",
        errors="coerce"
    )

# ================= LOOP =================
for file in os.listdir(BASE_PATH):

    if not (file.endswith(".xlsx") or file.endswith(".csv")):
        continue

    print(f"\n📂 Importing expiry: {file}")
    full = os.path.join(BASE_PATH, file)

    # ===== ALWAYS SMART READ ===== 
    df = smart_read(full)

    # ===== CLEAN HEADERS =====
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r'\s+', ' ', regex=True)
    )

    # remove unnamed columns
    df = df.loc[:, ~df.columns.str.contains('^unnamed', case=False)]

    print("🧾 Columns:", df.columns.tolist())

    # ===== RENAME =====
    rename_map = {
        "warehouse":"warehouse",
        "entry no.":"entry_no",
        "transaction date":"transaction_date",
        "item code":"item_code",
        "item name":"item_name",
        "category":"category",
        "unit":"unit",
        "price":"price",
        "qty":"qty",
        "amount":"amount",
        "manufacture date":"manufacture_date",
        "duration":"duration",
        "expiry date":"expiry_date"
    }

    df.rename(columns=rename_map, inplace=True)
    # if item_code empty shift detected
    if "item_code" in df.columns:
        if df["item_code"].isna().sum() > len(df) * 0.8:
            print("⚠ Item code column empty → shifting fix")

        # shift columns right
        df["item_code"] = df["item_name"]
        df["item_name"] = df["category"]

    # ===== DATE FIX =====
    df["transaction_date"] = fix_date(df["transaction_date"])
    df["manufacture_date"] = fix_date(df["manufacture_date"])
    df["expiry_date"] = fix_date(df["expiry_date"])

    # ===== NUMERIC =====
    for c in ["price","qty","amount"]:
        if c in df.columns:
            df[c] = clean_num(df[c])

    df["source"] = file

    # ===== FINAL =====
    final_cols = [
        "warehouse","entry_no","transaction_date",
        "item_code","item_name","category",
        "unit","price","qty","amount",
        "manufacture_date","duration","expiry_date","source"
    ]

    final = df[final_cols].copy()
    final = final[final["item_name"].notna()]

    # ===== TEXT TRIM =====
    final["warehouse"] = final["warehouse"].astype(str).str[:200]
    final["entry_no"] = final["entry_no"].astype(str).str[:100]
    final["item_code"] = final["item_code"].astype(str).str[:100]
    final["item_name"] = final["item_name"].astype(str).str[:500]
    final["category"] = final["category"].astype(str).str[:300]
    final["unit"] = final["unit"].astype(str).str[:100]
    final["duration"] = final["duration"].astype(str).str[:100]
    final["source"] = final["source"].astype(str).str[:500]

    # ===== DUP CHECK =====
    dup = pd.read_sql(f"""
    SELECT COUNT(*) c FROM warehouse_item_expiry
    WHERE source='{file}'
    """, engine)

    if dup.iloc[0,0] > 0:
        print("⚠ Already imported:", file)
        continue

    # ===== INSERT =====
    final.to_sql(
        "warehouse_item_expiry",
        engine,
        if_exists="append",
        index=False,
        chunksize=2000
    )

    print(f"✅ Imported {len(final)} rows")

print("\n🏁 EXPIRY IMPORT DONE")
