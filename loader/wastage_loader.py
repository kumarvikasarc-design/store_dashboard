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
    """Safely converts strings with commas/symbols to floats."""
    if col is None: return 0.0
    return pd.to_numeric(
        col.astype(str).str.replace(r'[^\d.]', '', regex=True), 
        errors="coerce"
    ).fillna(0.0)

def normalize_header(col):
    return str(col).lower().strip().replace("_", " ").replace(".", "")

def map_columns(df):
    """
    Robustly maps raw headers to standard names using a two-tier priority system.
    This prevents 'unit' from accidentally matching 'unit price'.
    """
    expected = {
        "deployment_name": ["deployment", "warehouse"],
        "store_name": ["store", "kitchen"],
        "user_name": ["user"],
        "date": ["date"],
        "transaction_number": ["transaction"],
        "item_code": ["item code", "code"],
        "item_name": ["item", "product"],
        "category": ["category name", "category"],
        "super_category": ["super category"],
        "comment": ["comment", "remarks"],
        "qty": ["quantity", "qty"],
        "unit": ["unit", "uom"],
        "unit_price": ["unit price", "price", "rate", "cost"],
        "amount": ["amount", "total", "value"],
        "source": ["source"]
    }

    mapping = {}
    remaining_cols = list(df.columns)
    
    # Priority 1: Exact or highly specific matches
    for target in ["unit_price", "super_category", "category", "deployment_name", "store_name", "item_name"]:
        keywords = expected[target]
        for col in remaining_cols:
            norm = normalize_header(col)
            if any(k == norm for k in keywords):
                mapping[col] = target
                remaining_cols.remove(col)
                break
    
    # Priority 2: General keyword matching for remaining columns
    for target, keywords in expected.items():
        if target in mapping.values(): continue
        for col in remaining_cols[:]:
            norm = normalize_header(col)
            if any(k in norm for k in keywords):
                mapping[col] = target
                remaining_cols.remove(col)
                break
    
    return df.rename(columns=mapping)

def import_wastage_file(file):
    full = os.path.join(BASE_PATH, file)
    
    # 1. Use index_col=False to prevent 'Deployment Name' from becoming an index
    # 2. engine="python" helps handle varying column counts per row
    df = pd.read_csv(full, index_col=False, encoding="latin1", engine="python")
    
    # Remove entirely empty columns (often caused by trailing commas in CSV)
    df = df.dropna(axis=1, how='all')

    # Map columns dynamically
    df = map_columns(df)

    # Clean Dates: Standardize to YYYY-MM-DD
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")

    # Clean Numbers
    for col in ["qty", "unit_price", "amount"]:
        if col in df.columns:
            df[col] = clean_num(df[col])

    # Construct Final DataFrame for SQL
    final = pd.DataFrame({
        "wastage_date": df.get("date"),
        "warehouse": df.get("deployment_name", pd.Series(dtype='str')).astype(str).str.strip(),
        "store_name": df.get("store_name", pd.Series(dtype='str')).astype(str).str.strip(),
        "user_name": df.get("user_name", pd.Series(dtype='str')).astype(str).str.strip(),
        "transaction_number": df.get("transaction_number", pd.Series(dtype='str')).astype(str).str.strip(),
        "item_code": df.get("item_code", pd.Series(dtype='str')).astype(str).str.strip(),
        "item_name": df.get("item_name", pd.Series(dtype='str')).astype(str).str[:390],
        "category": df.get("category", pd.Series(dtype='str')).astype(str).str.strip(),
        "super_category": df.get("super_category", pd.Series(dtype='str')).astype(str).str.strip(),
        "comment": df.get("comment", pd.Series(dtype='str')).astype(str).str[:490],
        "qty": df.get("qty"),
        "uom": df.get("unit", pd.Series(dtype='str')).astype(str).str.strip(),
        "unit_price": df.get("unit_price"),
        "amount": df.get("amount"),
        "source": file[:490]
    })

    # Filter out empty rows
    final = final[final["item_name"].notna() & (final["item_name"] != "nan")]

    print(f"🔍 Preview for {file}:")
    print(final[["wastage_date", "item_name", "qty", "uom", "unit_price", "amount"]].head())

    # Check for duplicates in SQL
    dup = pd.read_sql(f"SELECT COUNT(*) as c FROM warehouse_wastage WHERE source='{file}'", engine)
    if dup.iloc[0,0] > 0:
        print(f"⚠️ Already imported: {file}")
        return

    # Export to SQL
    final.to_sql("warehouse_wastage", engine, if_exists="append", index=False)
    print(f"✅ Imported {len(final)} rows\n")

# ================= LOOP =================
if __name__ == "__main__":
    for file in os.listdir(BASE_PATH):
        if file.endswith(".csv"):
            print(f"📂 Processing: {file}")
            import_wastage_file(file)
    print("🏁 ALL IMPORTS COMPLETE")