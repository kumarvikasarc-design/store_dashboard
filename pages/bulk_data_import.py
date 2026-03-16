import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from urllib.parse import quote_plus
from sqlalchemy import create_engine

# ==========================================================
# DATABASE CONNECTION
# ==========================================================

username = "postgres"
password = quote_plus("rds@12")   # auto-encodes special chars
host = "127.0.0.1"
port = 5432
db = "sales_dashboard"

DB_URL = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{db}"

engine = create_engine(DB_URL, pool_pre_ping=True)

# ==========================================================
# CONFIGURATION
# ==========================================================
DATE_COLUMN = "data_date"   # column that will be added if missing

IMPORT_MAP = {
    "dsr_sales": r"C:\Users\ACER\store_dashboard\sales_dashboard",
    "daypart_sales": r"C:\Users\ACER\store_dashboard\hourly_sales",
    "daily_sales": r"C:\Users\ACER\store_dashboard\daily_sales",
    "menu_mix": r"C:\Users\ACER\store_dashboard\item_source",
    "customer_feedback": r"C:\Users\ACER\store_dashboard\feedback",
    "warehouse_stock": r"C:\Users\ACER\store_dashboard\inventory",
}

SUPPORTED_EXTENSIONS = (".csv", ".xls", ".xlsx")

# ==========================================================
# ONE-TIME SETUP (IMPORT LOG TABLE)
# ==========================================================
def create_import_log():
    query = """
    CREATE TABLE IF NOT EXISTS import_log (
        table_name TEXT,
        file_path TEXT PRIMARY KEY,
        imported_at TIMESTAMP DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(query))

# ==========================================================
# DATE EXTRACTION FROM FILENAME
# ==========================================================
def extract_date_from_filename(filename):
    patterns = [
        r"\d{4}-\d{2}-\d{2}",   # 2024-01-15
        r"\d{2}-\d{2}-\d{4}",   # 15-01-2024
        r"\d{8}",               # 20240115
    ]

    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            try:
                return pd.to_datetime(match.group(), dayfirst=True)
            except Exception:
                pass
    return None

# ==========================================================
# FILE LOADER
# ==========================================================
def load_file(filepath):
    try:
        if filepath.endswith(".csv"):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)
    except Exception as e:
        print(f"     ❌ Failed to read file: {e}")
        return None

    if df.empty:
        return None

    # Auto add date column if missing
    if DATE_COLUMN not in df.columns:
        file_date = extract_date_from_filename(os.path.basename(filepath))
        if file_date:
            df[DATE_COLUMN] = file_date
        else:
            df[DATE_COLUMN] = pd.NaT

    return df

# ==========================================================
# IMPORT LOG CHECKS
# ==========================================================
def is_file_imported(table, filepath):
    query = """
        SELECT 1 FROM import_log
        WHERE table_name = :table AND file_path = :path
        LIMIT 1
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(query),
            {"table": table, "path": filepath}
        ).fetchone()
        return result is not None

def log_file_import(table, filepath):
    query = """
        INSERT INTO import_log (table_name, file_path)
        VALUES (:table, :path)
    """
    with engine.begin() as conn:
        conn.execute(
            text(query),
            {"table": table, "path": filepath}
        )

# ==========================================================
# MAIN IMPORT PROCESS
# ==========================================================
def run_import():
    create_import_log()

    for table, folder in IMPORT_MAP.items():
        print(f"\n📥 Processing table: {table}")

        if not os.path.exists(folder):
            print(f"   ❌ Folder not found: {folder}")
            continue

        for root, _, files in os.walk(folder):
            for file in files:
                if not file.lower().endswith(SUPPORTED_EXTENSIONS):
                    continue

                full_path = os.path.join(root, file)

                if is_file_imported(table, full_path):
                    print(f"   ⏭ Skipped (already imported): {file}")
                    continue

                print(f"   → Importing: {full_path}")

                df = load_file(full_path)
                if df is None:
                    print("     ⚠ Skipped (empty / unreadable)")
                    continue

                try:
                    df.to_sql(
                        table,
                        engine,
                        if_exists="append",
                        index=False,
                        method="multi"
                    )

                    log_file_import(table, full_path)
                    print("     ✅ Imported successfully")

                except Exception as e:
                    print(f"     ❌ Import failed: {e}")

    print("\n🎉 ALL NEW FILES PROCESSED")

# ==========================================================
# ENTRY POINT
# ==========================================================
if __name__ == "__main__":
    run_import()
