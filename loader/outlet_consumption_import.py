import pandas as pd
from sqlalchemy import create_engine, text
import urllib
import os
import warnings
from sqlalchemy.exc import SAWarning
warnings.filterwarnings("ignore", category=SAWarning)

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
# REVISED CLEANING FUNCTIONS
# =====================================================
def clean_num(col):
    if col is None or not isinstance(col, pd.Series):
        return 0
    return pd.to_numeric(
        col.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("NA", "", regex=False)
        .str.replace("PCS", "", regex=False)
        .str.strip(),
        errors="coerce"
    ).fillna(0)

def fix_date(col):
    if col is None or not isinstance(col, pd.Series):
        return pd.NaT
    return pd.to_datetime(
        col.astype(str)
        .str.replace("NA", "", regex=False)
        .str.replace("/", "-", regex=False)
        .str.strip(),
        format="%d-%m-%Y",
        errors="coerce"
    )

def read_enterprise_file(path):
    # 🔥 FIX: Added index_col=False to prevent the first column from being skipped
    if path.endswith(".csv"):
        temp = pd.read_csv(
            path,
            header=None,
            encoding="utf-8",
            engine="python",
            on_bad_lines="skip",
            index_col=False  # <--- CRITICAL FIX
        )
    else:
        temp = pd.read_excel(path, header=None)

    # Find Header Row
    header_row = 0
    for i in range(min(15, len(temp))):
        row_values = temp.iloc[i].astype(str).str.lower().tolist()
        if any("item code" in x for x in row_values):
            header_row = i
            break

    # Read again with correct header
    if path.endswith(".csv"):
        df = pd.read_csv(
            path,
            header=header_row,
            encoding="utf-8",
            engine="python",
            on_bad_lines="skip",
            index_col=False  # <--- CRITICAL FIX
        )
    else:
        df = pd.read_excel(path, header=header_row)

    return df

# =====================================================
# LOOP FILES
# =====================================================
mapping = {
    "deployment": "outlet_name",
    "storekitchen": "store_name",
    "item code": "item_code",
    "item name": "item_name",
    "category": "category",
    "super category": "super_category",
    "average": "avg_price",
    "opening date": "opening_date",
    "closing date": "closing_date",
    "opening qty": "opening_qty",
    "purchase qty": "purchase_qty",
    "consumption qty": "consumption_qty",
    "wastage qty": "wastage_qty",
    "stock out qty": "stock_out_qty",
    "closing qty": "closing_qty",
}

for file in os.listdir(BASE_PATH):
    if not (file.endswith(".xlsx") or file.endswith(".csv")):
        continue

    print(f"\n📂 Processing: {file}")
    df = read_enterprise_file(os.path.join(BASE_PATH, file))
    
    # 1. Reset Index immediately to prevent duplicate label errors
    df = df.reset_index(drop=True)

    # 2. Normalize Headers
    df.columns = (
        df.columns.astype(str)
        .str.lower()
        .str.replace(r'[^a-z0-9\s]', '', regex=True)
        .str.strip()
    )

    # 3. Apply Mapping
    rename_dict = {}
    for k, v in mapping.items():
        for col in df.columns:
            if k in col:
                rename_dict[col] = v
                break
    df.rename(columns=rename_dict, inplace=True)

    # 4. Build Final DF
    final = pd.DataFrame(index=df.index)
    
    final["outlet_name"] = df.get("outlet_name", "Unknown").ffill()
    final["store_name"] = df.get("store_name", "Unknown").ffill()
    final["item_code"] = df.get("item_code", "NA")
    final["item_name"] = df.get("item_name", "NA")
    final["category"] = df.get("category", "NA")
    final["super_category"] = df.get("super_category", "NA")
    
    final["avg_price"] = clean_num(df.get("avg_price"))
    final["opening_date"] = fix_date(df.get("opening_date"))
    final["closing_date"] = fix_date(df.get("closing_date"))
    
    # Safe date calculation
    final["consumption_days"] = (final["closing_date"] - final["opening_date"]).dt.days
    final["consumption_days"] = final["consumption_days"].fillna(0).astype(int)

    # Quantities
    qty_cols = ["opening_qty", "purchase_qty", "consumption_qty", "wastage_qty", "stock_out_qty", "closing_qty"]
    for q in qty_cols:
        final[q] = clean_num(df.get(q))

    # Static Placeholders
    placeholders = ["indent_receive_qty", "indent_dispatch_qty", "internal_receive_qty", 
                    "internal_dispatch_qty", "stock_in_qty", "reuse_qty", "return_qty",
                    "latest_physical_qty", "ideal_closing_qty", "physical_adjusted_closing"]
    for p in placeholders:
        final[p] = 0

    # Amounts
    final["total_out_qty"] = final["consumption_qty"] + final["stock_out_qty"]
    final["consumption_amt"] = final["consumption_qty"] * final["avg_price"]
    final["wastage_amt"] = final["wastage_qty"] * final["avg_price"]
    final["closing_amt"] = final["closing_qty"] * final["avg_price"]

    final["source"] = file
    final["inserted_at"] = pd.Timestamp.now()

    # 5. Filter and Clean
    final = final[final["item_name"].astype(str).str.len() > 2].copy()
    final = final[~final["category"].astype(str).str.contains("CAPEX", case=False, na=False)].copy()
    final = final.reset_index(drop=True)

    # =====================================================
    # UPLOAD
    # =====================================================
    try:
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM outlet_consumption WHERE source = :src"), {"src": file})
            conn.commit()
        
        final.to_sql("outlet_consumption", engine, if_exists="append", index=False, chunksize=1000)
        print(f"✅ SUCCESS: {file} ({len(final)} rows)")
    except Exception as e:
        print(f"❌ INSERT ERROR: {e}")

print("\n🏁 ALL FILES IMPORTED SUCCESSFULLY")