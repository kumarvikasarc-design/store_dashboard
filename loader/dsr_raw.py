import os, shutil, hashlib
import pandas as pd
import pyodbc
import re
from datetime import datetime

BASE = r"C:\Dsr_Bulkdata"
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
    "building_type": "Building_Type",
    "area": "Area",
    "region": "Region",
    "outlet_name": "Outlet_Name",
    "source": "Source",
    "no_of_items": "No_Of_Items",
    "no_of_bills": "No_Of_Bills",
    "sale": "Sale",
    "discount": "Discount",
    "restaurant_packaging_charges": "Restaurant_Packaging_Charges",
    "packaging_charge_cart_swiggy": "Packaging_Charge_CART_SWIGGY",
    "restaurant_charge": "Restaurant_Charge",
    "staff_welfare_charge_5": "Staff_Welfare_Charge_5",
    "staff_welfare": "Staff_Welfare",
    "delivery_charge": "Delivery_Charge",
    "platform_fee_charge": "Platform_Fee_Charge",
    "smile_amount_charge": "Smile_Amount_Charge",
    "total_charges": "Total_Charges",
    "net_sale": "Net_Sale",
    "gst_5": "GST_5",
    "ecom_gst_5": "ECom_GST_5",
    "gst_18": "GST_18",
    "gst_40": "GST_40",
    "total_tax": "Total_Tax",
    "total_amount": "Total_Amount",
    "round_off": "Round_Off",
    "gross_amount": "Gross_Amount",
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
                "building" in l and
                "area" in l and
                "region" in l and
                "source" in l and
                "items" in l and
                "bills" in l
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
            "SELECT 1 FROM dbo.file_import_log WHERE file_hash=? AND pipeline='DSR'",
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
        #  Normalize Source values
        out["Source"] = (
            out["Source"]
            .str.upper()
            .replace({
                "SWIGGY INSTAMART": "SWIGGY",
                "ZOMATO GOLD": "ZOMATO"
            })
        )

        # 🚨 DROP NON-DATA ROWS (critical)
        out = out[
            out["Outlet_Name"].notna() |
            out["Sale"].notna() |
            out["No_Of_Items"].notna()
        ]
        # Add optional numeric charge columns with default 0
        NUMERIC_DEFAULT_ZERO = [
            "Restaurant_Packaging_Charges",
            "Packaging_Charge_CART_SWIGGY",
            "Restaurant_Charge",
            "Staff_Welfare_Charge_5",
            "Staff_Welfare",
            "Delivery_Charge",
            "Platform_Fee_Charge",
            "Smile_Amount_Charge",
            "ECom_GST_5",
            "GST_40"
        ]
        for c in NUMERIC_DEFAULT_ZERO:
            if c not in out.columns:
                out[c] = 0

        # 🚨 DROP ROWS WHERE EVERYTHING IS ZERO
        NUMERIC_CHECK = [
            c for c in [
                "No_Of_Items","No_Of_Bills","Sale","Net_Sale","Total_Amount"
            ]
            if c in out.columns
        ]
        if NUMERIC_CHECK:
            out = out[out[NUMERIC_CHECK].sum(axis=1) != 0]

        # --- FIX SALE_DATE (ROW LEVEL, PRESERVE CSV) ---
        if "sale_date" in df.columns:
            # preserve original values if parsing fails
            out["Sale_Date"] = pd.to_datetime(df["sale_date"], errors="coerce")
            out["Sale_Date"] = out["Sale_Date"].dt.strftime("%Y-%m-%d")
            # if parsing failed, keep original string
            out.loc[out["Sale_Date"].isna(), "Sale_Date"] = df["sale_date"].astype(str)
        elif "date" in df.columns:
            out["Sale_Date"] = pd.to_datetime(df["date"], errors="coerce")
            out["Sale_Date"] = out["Sale_Date"].dt.strftime("%Y-%m-%d")
            out.loc[out["Sale_Date"].isna(), "Sale_Date"] = df["date"].astype(str)
        else:
            # fallback only if no date column exists
            file_date = extract_date_from_filename(file)
            out["Sale_Date"] = file_date.strftime("%Y-%m-%d") if pd.notna(file_date) else ""
        # Define final order
        FINAL_ORDER = [
            "Building_Type","Area","Region","Outlet_Name","Sale_Date","Source",
            "No_Of_Items","No_Of_Bills","Sale","Discount",
            "Restaurant_Packaging_Charges","Packaging_Charge_CART_SWIGGY",
            "Restaurant_Charge","Staff_Welfare_Charge_5","Staff_Welfare",
            "Delivery_Charge","Platform_Fee_Charge","Smile_Amount_Charge",
            "Total_Charges","Net_Sale","ECom_GST_5","GST_5","GST_18","GST_40",
            "Total_Tax","Total_Amount","Round_Off","Gross_Amount"
        ]

        for col in FINAL_ORDER:
            if col not in out.columns:
                out[col] = "-" if col in [
                    "Building_Type","Area","Region","Outlet_Name","Source"
                ] else 0

        # Reorder
        out = out[FINAL_ORDER]

        TEXT_COLS = ["Building_Type","Area","Region","Outlet_Name","Source"]

        for c in TEXT_COLS:
            out[c] = (
                out[c]
                .astype(str)
                .str.strip()
                .replace({
                    "nan": "-",
                    "None": "-",
                    "": "-"
                })
            )

        # Now reorder
        out = out[FINAL_ORDER]

        # Normalize numeric columns: convert to numbers, fill blanks with 0
        NUMERIC_COLS = [
            "No_Of_Items","No_Of_Bills","Sale","Discount","Total_Charges","Net_Sale",
            "ECom_GST_5","GST_5","GST_18","GST_40",
            "Total_Tax","Total_Amount","Round_Off","Gross_Amount",
            "Restaurant_Packaging_Charges","Packaging_Charge_CART_SWIGGY",
            "Restaurant_Charge","Staff_Welfare_Charge_5","Staff_Welfare",
            "Delivery_Charge","Platform_Fee_Charge","Smile_Amount_Charge"
        ]

        # Force numeric type and replace blanks/NaN with 0
        for col in NUMERIC_COLS:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0)
            out[col] = out[col].astype(float)   # enforce numeric type        # # --- FORCE ZERO TEXT FOR OPTIONAL NUMERIC COLUMNS (BULK INSERT SAFE) ---

        # ---------------- WRITE CLEAN CSV ----------------
        clean_file = f"{CLEAN}\\clean_{file}"
        out.to_csv(clean_file, index=False)

        cur.execute("TRUNCATE TABLE dbo.dsr_stage")
        cur.execute(f"""
            BULK INSERT dbo.dsr_stage
            FROM '{clean_file}'
            WITH (
                FIRSTROW=2,
                FIELDTERMINATOR=',',
                ROWTERMINATOR='0x0d0a',
                CODEPAGE='65001'
            );
        """)

        cur.execute("""
        INSERT INTO dbo.dsr_raw (
            file_name, load_time,
            Building_Type, Area, Region, Outlet_Name, Sale_Date, Source,
            No_Of_Items, No_Of_Bills,
            Sale, Discount,

            Restaurant_Packaging_Charges,
            Packaging_Charge_CART_SWIGGY,
            Restaurant_Charge,
            Staff_Welfare_Charge_5,
            Staff_Welfare,
            Delivery_Charge,
            Platform_Fee_Charge,
            Smile_Amount_Charge,

            Total_Charges, Net_Sale,
            ECom_GST_5, GST_5, GST_18, GST_40,
            Total_Tax, Total_Amount, Round_Off, Gross_Amount
        )
        SELECT
            ?, GETDATE(),
            Building_Type, Area, Region, Outlet_Name,
            CAST(Sale_Date AS DATE), Source,

            CAST(TRY_CAST(No_Of_Items AS DECIMAL(18,2)) AS INT),
            CAST(TRY_CAST(No_Of_Bills AS DECIMAL(18,2)) AS INT),

            TRY_CAST(Sale AS DECIMAL(18,2)),
            TRY_CAST(Discount AS DECIMAL(18,2)),

            TRY_CAST(Restaurant_Packaging_Charges AS DECIMAL(18,2)),
            TRY_CAST(Packaging_Charge_CART_SWIGGY AS DECIMAL(18,2)),
            TRY_CAST(Restaurant_Charge AS DECIMAL(18,2)),
            TRY_CAST(Staff_Welfare_Charge_5 AS DECIMAL(18,2)),
            TRY_CAST(Staff_Welfare AS DECIMAL(18,2)),
            TRY_CAST(Delivery_Charge AS DECIMAL(18,2)),
            TRY_CAST(Platform_Fee_Charge AS DECIMAL(18,2)),
            TRY_CAST(Smile_Amount_Charge AS DECIMAL(18,2)),

            TRY_CAST(Total_Charges AS DECIMAL(18,2)),
            TRY_CAST(Net_Sale AS DECIMAL(18,2)),
            TRY_CAST(ECom_GST_5 AS DECIMAL(18,2)),
            TRY_CAST(GST_5 AS DECIMAL(18,2)),
            TRY_CAST(GST_18 AS DECIMAL(18,2)),
            TRY_CAST(GST_40 AS DECIMAL(18,2)),
            TRY_CAST(Total_Tax AS DECIMAL(18,2)),
            TRY_CAST(Total_Amount AS DECIMAL(18,2)),
            TRY_CAST(Round_Off AS DECIMAL(18,2)),
            TRY_CAST(Gross_Amount AS DECIMAL(18,2))
        FROM dbo.dsr_stage;
        """, file)

        cur.execute("""
            INSERT INTO dbo.file_import_log
            (file_name, file_hash, rows_loaded, status, pipeline)
            SELECT ?, ?, COUNT(*), 'SUCCESS', 'DSR'
            FROM dbo.dsr_stage
        """, file, h)

        shutil.move(src, f"{PROCESSED}\\{file}")
        log(f"SUCCESS {file}")

    except Exception as e:
        log(f"ERROR DETAIL: {repr(e)}")   # 👈 THIS LINE IS CRITICAL

        cur.execute("""
            INSERT INTO dbo.file_import_log
            (file_name, file_hash, status, error_message, pipeline)
            VALUES (?, ?, 'FAILED', ?, 'DSR')
        """, file, h, str(e)[:4000])

        shutil.move(src, f"{ERROR}\\{file}")
        log(f"FAILED {file}")


log("Cycle complete")
