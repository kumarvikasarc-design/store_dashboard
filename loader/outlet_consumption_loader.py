import pandas as pd
from sqlalchemy import create_engine, text
import urllib
import os
import warnings
from sqlalchemy.exc import SAWarning
warnings.filterwarnings("ignore", category=SAWarning)
# =====================================================
# SQL CONNECTION
# =====================================================
SQL_CONN = r"""
DRIVER={ODBC Driver 17 for SQL Server};
SERVER=localhost\SQLEXPRESS;
DATABASE=coffee_island_analytics;
Trusted_Connection=yes;
"""
params = urllib.parse.quote_plus(SQL_CONN)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

BASE_PATH = r"C:\Users\ACER\store_dashboard\inventory\consumption"

# =====================================================
# CLEANING FUNCTIONS
# =====================================================
def clean_num(col):
    return pd.to_numeric(
        col.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("NA", "", regex=False)
        .str.replace("PCS", "", regex=False)
        .str.strip(),
        errors="coerce"
    ).fillna(0)

def fix_date(col):
    return pd.to_datetime(
        col.astype(str)
        .str.replace("NA", "", regex=False)
        .str.replace("/", "-", regex=False)
        .str.strip(),
        format="%d-%m-%Y",
        errors="coerce"
    )

def safe_col(df, col):
    c = df.loc[:, df.columns == col]
    if c.shape[1] == 0:
        return None
    return c.iloc[:,0]

def read_enterprise_file(path):

    # ================= READ RAW SAFE =================
    if path.endswith(".csv"):
        temp = pd.read_csv(
            path,
            header=None,
            encoding="utf-8",
            engine="python",   # 🔥 important
            on_bad_lines="skip"  # skip broken rows
        )
    else:
        temp = pd.read_excel(path, header=None)

    # ================= FIND HEADER ROW =================
    header_row = None
    for i in range(15):
        row_values = temp.iloc[i].astype(str).str.lower().tolist()
        if any("item code" in x for x in row_values):
            header_row = i
            break

    if header_row is None:
        raise Exception("❌ Header row not found in file")

    print(f"✅ Header detected at row: {header_row}")

    # ================= READ AGAIN WITH HEADER =================
    if path.endswith(".csv"):
        df = pd.read_csv(
            path,
            header=header_row,
            encoding="utf-8",
            engine="python",
            on_bad_lines="skip"
        )
    else:
        df = pd.read_excel(path, header=header_row)

    return df

# =====================================================
# LOOP FILES
# =====================================================
for file in os.listdir(BASE_PATH):

    if not (file.endswith(".xlsx") or file.endswith(".csv")):
        continue

    print(f"\n📂 Processing: {file}")
    full_path = os.path.join(BASE_PATH, file)

    df = read_enterprise_file(full_path)

    # 🔥 Remove completely blank / merged columns like "Unnamed: 0"
    df = df.loc[:, ~df.columns.astype(str).str.contains('^unnamed', case=False)]

    # =====================================================
    # NORMALIZE HEADERS
    # =====================================================
    df.columns = (
        df.columns.astype(str)
        .str.lower()
        .str.replace(r'[^a-z0-9\s]', '', regex=True)
        .str.strip()
    )

    # remove duplicate columns (very important)
    df = df.loc[:, ~df.columns.duplicated(keep="first")].copy()

    # 🚨 FORCE REMOVE ALL DUPLICATE COLUMN NAMES COMPLETELY
    df = df.loc[:, ~df.columns.duplicated()]
# =====================================================
# REVISED COLUMN MAPPING (More Precise)
# =====================================================
# This dictionary maps the "cleaned" CSV header to your SQL column name
precise_mapping = {
    'deployment name': 'outlet_name',
    'storekitchen name': 'store_name',
    'item code': 'item_code',
    'item name': 'item_name',
    'category name': 'category',
    'super category name': 'super_category',
    'average price': 'avg_price',
    'opening date': 'opening_date',
    'closing date': 'closing_date',
    'opening qty': 'opening_qty',
    'purchase qty': 'purchase_qty',
    'consumption qty': 'consumption_qty',
    'wastage qty': 'wastage_qty',
    'stock out qty': 'stock_out_qty',
    'closing qty': 'closing_qty',
}

for file in os.listdir(BASE_PATH):
    if not (file.endswith(".xlsx") or file.endswith(".csv")): continue
    
    full_path = os.path.join(BASE_PATH, file)
    df = read_enterprise_file(full_path)

    # 1. Clean column names but keep spaces for mapping
    df.columns = df.columns.astype(str).str.lower().str.strip()
    
    # 2. Apply Mapping
    df = df.rename(columns=precise_mapping)

    # 3. Drop unwanted "Unnamed" columns
    df = df.loc[:, ~df.columns.str.contains('^unnamed')]

    # ... (Keep the date/num cleaning logic)

    # =====================================================
    # BUILD FINAL DF (With Safety Defaults)
    # =====================================================
    final = pd.DataFrame()

    # Use .get(col, default) to prevent KeyError and ensure 31 columns
    final["outlet_name"] = df.get("outlet_name", "Unknown")
    final["store_name"] = df.get("store_name", "Unknown")
    final["item_code"] = df.get("item_code", "NA")
    final["item_name"] = df.get("item_name", "NA")
    final["category"] = df.get("category", "NA")
    final["super_category"] = df.get("super_category", "NA")
    
    final["avg_price"] = clean_num(df.get("average price", 0)) # Using original name if rename failed
    if "avg_price" in df.columns: final["avg_price"] = clean_num(df["avg_price"])

    final["opening_date"] = fix_date(df.get("opening_date"))
    final["closing_date"] = fix_date(df.get("closing_date"))
    
    # Safe calculation for days
    final["consumption_days"] = 0
    mask = final["opening_date"].notna() & final["closing_date"].notna()
    final.loc[mask, "consumption_days"] = (final["closing_date"] - final["opening_date"]).dt.days

    # Quantities
    qty_cols = ["opening_qty", "purchase_qty", "consumption_qty", "wastage_qty", "stock_out_qty", "closing_qty"]
    for q in qty_cols:
        final[q] = clean_num(df.get(q, 0))

    # Fill missing required SQL columns with 0
    placeholder_cols = [
        "indent_receive_qty", "indent_dispatch_qty", "internal_receive_qty", 
        "internal_dispatch_qty", "stock_in_qty", "reuse_qty", "return_qty",
        "latest_physical_qty", "ideal_closing_qty", "physical_adjusted_closing"
    ]
    for p in placeholder_cols:
        final[p] = 0

    # Calculated Fields
    final["total_out_qty"] = final["consumption_qty"] + final["stock_out_qty"]
    final["consumption_amt"] = final["consumption_qty"] * final["avg_price"]
    final["wastage_amt"] = final["wastage_qty"] * final["avg_price"]
    final["closing_amt"] = final["closing_qty"] * final["avg_price"]

    final["source"] = file
    final["inserted_at"] = pd.Timestamp.now()
        
    print("Duplicate columns check:")
    print(df.columns[df.columns.duplicated()])

    print("Rows to upload:", len(final))
    # remove id column if exists in table
    with engine.connect() as conn:
        cols = pd.read_sql("SELECT TOP 0 * FROM outlet_consumption", conn).columns.tolist()

    if "id" in cols:
        print("Auto identity column detected → safe insert mode")

    # =====================================================
    # UPLOAD (FINAL FIX)
    # =====================================================
    try:
        # delete old data first
        with engine.connect() as conn:
            conn.execute(
                text("DELETE FROM outlet_consumption WHERE source = :src"),
                {"src": file}
            )
            conn.commit()

        print("Deleted old records")

        print("Inserting rows:", len(final))

        # INSERT separately (IMPORTANT)
        final.to_sql(
            "outlet_consumption",
            engine,   # 🔥 use engine NOT connection
            if_exists="append",
            index=False,
            chunksize=1000
        )

        print(f"✅ SUCCESS INSERT: {file}")

    except Exception as e:
        print("❌ INSERT ERROR:", e)



print("\n🏁 ALL FILES IMPORTED SUCCESSFULLY")
