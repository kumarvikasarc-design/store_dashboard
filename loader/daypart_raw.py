import os, shutil, hashlib
import pandas as pd
import pyodbc
import re
from datetime import datetime

BASE = r"C:\Daypart_Bulkdata"
INCOMING, CLEAN, PROCESSED, ERROR, LOGS = [
    f"{BASE}\\{p}" for p in ["incoming","clean","processed","error","logs"]
]

for p in [INCOMING, CLEAN, PROCESSED, ERROR, LOGS]:
    os.makedirs(p, exist_ok=True)

SQL_CONN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=coffee_island_analytics;"
    "Trusted_Connection=yes;"
)

CANONICAL = {
    "outlet_name": "Outlet_Name",
    "date": "Sale_Date",
    "hour": "Sale_Hour",
    "tab": "Tab",
    
    "sale": "Sale",
    "discount": "Discount",
    "net_sale": "Net_Sale",
    "total_charges": "Total_Charges",
    "total_tax": "Total_Tax",
    "gross_sale": "Gross_Sale",
    "nob": "No_Of_Bills" 
}

def file_hash(p):
    h = hashlib.sha256()
    with open(p,"rb") as f:
        for c in iter(lambda: f.read(8192), b""):
            h.update(c)
    return h.hexdigest()

def log(msg):
    print(msg)
    with open(f"{LOGS}\\import.log","a",encoding="utf-8") as f:
        f.write(f"{datetime.now()} | {msg}\n")

def detect_header_row(csv_path, max_scan=30):
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f):
            l = line.lower()
            if (
                "outlet" in l and
                "date" in l and
                "hour" in l and
                "tab" in l and
                "sale" in l and
                "gross" in l
            ):
                return i
            if i >= max_scan:
                break
    return 0

def extract_date_from_filename(filename):
    """
    Extracts start date from:
    Enterprise_Daily_Sales_Tabwise_Report(2025.05.01--2025.05.31).csv
    """
    m = re.search(r"\((\d{4}\.\d{2}\.\d{2})--", filename)
    if m:
        return pd.to_datetime(m.group(1), format="%Y.%m.%d")
    return pd.NaT

conn = pyodbc.connect(SQL_CONN, autocommit=True)
cur = conn.cursor()

log("Auto-import service started")

for file in os.listdir(INCOMING):
    if not file.lower().endswith(".csv"):
        continue

    src = f"{INCOMING}\\{file}"
    try:
        log(f"Processing {file}")

        h = file_hash(src)
        if cur.execute(
            "SELECT 1 FROM dbo.file_import_log WHERE file_hash=? AND pipeline='DAYPART'",
            h
        ).fetchone():
            log("SKIPPED duplicate")
            shutil.move(src, f"{PROCESSED}\\{file}")
            continue

        header_row = detect_header_row(src)
        log(f"Detected header row at line {header_row + 1}")

        df = pd.read_csv(
            src,
            skiprows=header_row,
            engine="python",
            sep=None,          # 👈 THIS FILE IS TAB SEPARATED
            skip_blank_lines=True
        )
        
        df.columns = (
            df.columns
                .str.lower()
                .str.replace(r"[^a-z0-9]+", "_", regex=True)
                .str.strip("_")
        )

        # Rename only existing columns based on CANONICAL mapping
        out = df.rename(
            columns={src: dst for src, dst in CANONICAL.items() if src in df.columns}
        )
        
        # drop junk rows
        out = out[out["Outlet_Name"].notna()]
        
        #  Normalize Source values
        out["Tab"] = (
            out["Tab"]
            .astype(str)
            .str.upper()
            .replace({
                "DINE IN": "DINEIN",
                "COUNTER": "COUNTER",
                "TAKEAWAY": "TAKEAWAY"
            })
        )

        # Ensure No_Of_Bills ALWAYS exists first
        if "No_Of_Bills" not in out.columns:
            out["No_Of_Bills"] = 0

        # Now drop junk rows
        out = out[
            out["Outlet_Name"].notna() |
            out["Sale"].notna() |
            (out["No_Of_Bills"] > 0)
        ]
        # Dates
        out["Sale_Date"] = pd.to_datetime(out["Sale_Date"], errors="coerce")
        out["Sale_Hour"] = pd.to_numeric(out["Sale_Hour"], errors="coerce").fillna(0).astype(int)
        # Normalize numeric columns: convert to numbers, fill blanks with 0
        NUMERIC_COLS = [
            "Sale","Discount","Net_Sale",
            "Total_Charges","Total_Tax","Gross_Sale"
        ]

        out["No_Of_Bills"] = (
            pd.to_numeric(out["No_Of_Bills"], errors="coerce")
            .fillna(0)
            .astype(int)
        )

        # Force numeric type and replace blanks/NaN with 0
        for col in NUMERIC_COLS:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)
            out[col] = out[col].astype(float)   # enforce numeric type        # # --- FORCE ZERO TEXT FOR OPTIONAL NUMERIC COLUMNS (BULK INSERT SAFE) ---

        FINAL_ORDER = [
            "Outlet_Name",
            "Sale_Date",
            "Sale_Hour",
            "Tab",
            "Sale",
            "Discount",
            "Net_Sale",
            "Total_Charges",
            "Total_Tax",
            "Gross_Sale",
            "No_Of_Bills"
        ]
        out = out[FINAL_ORDER]

        # ---------------- WRITE CLEAN CSV ----------------
        clean_file = f"{CLEAN}\\clean_{file}"
        out.to_csv(
            clean_file,
            index=False,
            na_rep="0"
        )

        cur.execute("TRUNCATE TABLE dbo.daypart_stage")
        cur.execute(f"""
            BULK INSERT dbo.daypart_stage
            FROM '{clean_file}'
            WITH (
                FIRSTROW=2,
                FIELDTERMINATOR=',',
                ROWTERMINATOR='0x0d0a',
                CODEPAGE='65001'
            );
        """)

        cur.execute("""
        INSERT INTO dbo.daypart_raw (
            file_name, load_time,
            Outlet_Name, Sale_Date, Sale_Hour, Tab,
            Sale, Discount, Net_Sale, Total_Charges, Total_Tax, Gross_Sale, No_Of_Bills

        )
        SELECT
            ?, GETDATE(),
            Outlet_Name,
            CAST(Sale_Date AS DATE),
            CAST(Sale_Hour AS INT), 
            Tab,       
            TRY_CAST(Sale AS DECIMAL(18,2)),
            TRY_CAST(Discount AS DECIMAL(18,2)),
            TRY_CAST(Net_Sale AS DECIMAL(18,2)),
            TRY_CAST(Total_Charges AS DECIMAL(18,2)),
            TRY_CAST(Total_Tax AS DECIMAL(18,2)),
            TRY_CAST(Gross_Sale AS DECIMAL(18,2)),
            CAST(TRY_CAST(No_Of_Bills AS DECIMAL(18,2)) AS INT)
        FROM dbo.daypart_stage;
        """, file)

        cur.execute("""
            INSERT INTO dbo.file_import_log
            (file_name, file_hash, rows_loaded, status, pipeline)
            SELECT ?, ?, COUNT(*), 'SUCCESS', 'DAYPART'
            FROM dbo.daypart_stage
        """, file, h)

        shutil.move(src, f"{PROCESSED}\\{file}")
        log(f"SUCCESS {file}")

    except Exception as e:
        log(f"ERROR DETAIL: {repr(e)}")   # 👈 THIS LINE IS CRITICAL

        cur.execute("""
            INSERT INTO dbo.file_import_log
            (file_name, file_hash, status, error_message, pipeline)
            VALUES (?, ?, 'FAILED', ?, 'DAYPART')
        """, file, h, str(e)[:4000])

        shutil.move(src, f"{ERROR}\\{file}")
        log(f"FAILED {file}")


log("Cycle complete")
