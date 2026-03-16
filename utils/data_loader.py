# utils/data_loader.py — Updated to use your exact column mappings
# Paths are kept as your original. New loader helpers added:
#  - load_purchases()        -> reads latest GRN from inventory/entry_report
#  - load_opening_variance() -> reads latest opening/variance file from inventory/Varience_report
#  - load_item_sales()       -> reads item sales and normalises columns
# Existing functions retained and adapted to use these loaders.

import os
import glob
from functools import lru_cache
from datetime import date

import pandas as pd

# ---------------------------------------------------------------------
# CONFIG – YOUR PATHS
# ---------------------------------------------------------------------
SALES_DIR = r"C:\Users\ACER\store_dashboard\item_sales"
INVENTORY_DIR = r"C:\Users\ACER\store_dashboard\inventory"
RECIPES_FILE = r"C:\Users\ACER\store_dashboard\brand_recipes.csv"
EXPIRY_FILE = r"C:\Users\ACER\store_dashboard\inventory\expiryReportExport.xls"
STORES_DB = r"C:\Users\ACER\store_dashboard\stores_db.csv"
ENTRY_REPORT_DIR = r"C:\Users\ACER\store_dashboard\inventory\entry_report"
VARIENCE_DIR = r"C:\Users\ACER\store_dashboard\inventory\Varience_report"

# ---------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------

def _parse_date_series(series: pd.Series) -> pd.Series:
    """Auto-detect date formats like DD-MM, DD/MM, YYYY-MM-DD. Keep dayfirst behaviour."""
    return pd.to_datetime(series, errors="coerce", dayfirst=True)


def _safe_read_csv(path: str) -> pd.DataFrame:
    """Read CSV safely; on any error return empty DataFrame."""
    try:
        df = pd.read_csv(path, dtype=str)
        df.columns = df.columns.str.strip()
        return df
    except Exception:
        return pd.DataFrame()


def _latest_file_in(dir_path, pattern="*.csv"):
    candidates = glob.glob(os.path.join(dir_path, pattern))
    if not candidates:
        return None
    return max(candidates, key=os.path.getmtime)


# ---------------------------------------------------------------------
# PURCHASES (GRN) loader — uses column positions as provided
# Path: inventory/entry_report
# Mapping (1-based Excel columns):
#   A: Outlet Name (index 0)
#   D: Vendor       (index 3)
#   E: GRN date     (index 4)
#   G: Invoice no   (index 6)
#   M: Item name    (index 12)
#   Q: Qty received (index 16)
#   T: Rate         (index 19)
#   AF: Tax+Total   (index 31)
# ---------------------------------------------------------------------
@lru_cache(maxsize=1)
def load_purchases():
    path = _latest_file_in(ENTRY_REPORT_DIR, "*.csv")
    if not path:
        return pd.DataFrame()

    df = _safe_read_csv(path)
    if df.empty:
        return df

    # Work by position — protect against short files
    cols = list(df.columns)
    def col_at(idx):
        return cols[idx] if idx < len(cols) else None

    mapping = {}
    mapping[col_at(0)] = "Outlet Name"
    if col_at(3): mapping[col_at(3)] = "Vendor"
    if col_at(4): mapping[col_at(4)] = "GRN Date"
    if col_at(6): mapping[col_at(6)] = "Invoice No"
    if col_at(12): mapping[col_at(12)] = "Item Name"
    if col_at(16): mapping[col_at(16)] = "Qty Received"
    if col_at(19): mapping[col_at(19)] = "Rate"
    if col_at(31): mapping[col_at(31)] = "TaxTotal"

    df = df.rename(columns=mapping)

    # Force numeric where relevant
    for c in ["Qty Received", "Rate", "TaxTotal"]:
        # Safe numeric cleanup for all columns
            if c in df.columns:
                df[c] = (
                    df[c]
                    .astype(str)                 # convert to string safely
                    .str.replace(",", "", regex=False)
                    .str.strip()
                    .replace("", "0")
                )
    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Parse date
    if "GRN Date" in df.columns:
        df["GRN Date"] = _parse_date_series(df["GRN Date"])

    return df


# ---------------------------------------------------------------------
# OPENING & CLOSING INVENTORY loader
# Path: inventory/Varience_report
# Provided mapping (1-based):
# A Outlet Name (0)
# D Item (3)
# I Opening Qty (8)
# L Purchase Qty (11)
# N Stock In Qty (13)
# P Consumption Qty (15)
# R Yield Wastage (17)
# T Stock Out Qty (19)
# X Wastage Qty (23)
# Z Reuse Qty (25)
# T Returns Qty (re-uses 19?)  <-- user listed Returns Qty :- Column T (same as Stock Out). We'll keep Return Qty if present
# AH Physical Qty (33)
# AE Closing Qty (30)
# AJ Variance Qty (35)
# AM PhysicalGain/Loss Qty (38)
# AO Actual Consumption (39)
# AQ Ideal Closing Qty (41)
# ---------------------------------------------------------------------
@lru_cache(maxsize=1)
def load_opening_variance():
    """Safe loader for opening_variance CSV with robust numeric cleaning."""

    path = r"C:\Users\ACER\store_dashboard\inventory\opening_variance\*.csv"
    files = glob.glob(path)

    if not files:
        return pd.DataFrame()

    df_list = []
    for f in files:
        try:
            temp = pd.read_csv(f, dtype=str)
            temp["SourceFile"] = os.path.basename(f)
            df_list.append(temp)
        except Exception:
            continue

    if not df_list:
        return pd.DataFrame()

    df = pd.concat(df_list, ignore_index=True)

    # Identify numeric-like columns
    numeric_cols = [
        c for c in df.columns
        if any(k in c.lower() for k in [
            "qty", "amount", "amt", "price",
            "consumption", "variance", "opening", "closing", "physical"
        ])
    ]

    # Safe numeric conversion for each column
    for c in numeric_cols:
        if c not in df.columns:
            continue

        # Convert anything to string first to avoid .str errors
        df[c] = (
            df[c]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("–", "0", regex=False)  # handle dash
            .str.replace("-", "0", regex=False)
            .str.strip()
        )

        # Convert to numeric safely
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    return df

    cols = list(df.columns)
    def col_at(idx):
        return cols[idx] if idx < len(cols) else None

    mapping = {}
    if col_at(0): mapping[col_at(0)] = "Outlet Name"
    if col_at(3): mapping[col_at(3)] = "Item"
    if col_at(8): mapping[col_at(8)] = "Opening Qty"
    if col_at(11): mapping[col_at(11)] = "Purchase Qty"
    if col_at(13): mapping[col_at(13)] = "Stock In Qty"
    if col_at(15): mapping[col_at(15)] = "Consumption Qty"
    if col_at(17): mapping[col_at(17)] = "Yield Wastage"
    if col_at(19): mapping[col_at(19)] = "Stock Out Qty"
    if col_at(23): mapping[col_at(23)] = "Wastage Qty"
    if col_at(25): mapping[col_at(25)] = "Reuse Qty"
    # Return Qty: user said Column T -> index 19; map if present
    if col_at(19): mapping[col_at(19)] = mapping.get(col_at(19), "Return Qty")
    if col_at(30): mapping[col_at(30)] = "Closing Qty"
    if col_at(33): mapping[col_at(33)] = "Physical Qty"
    if col_at(35): mapping[col_at(35)] = "Variance Qty"
    if col_at(38): mapping[col_at(38)] = "PhysicalGainLoss Qty"
    if col_at(39): mapping[col_at(39)] = "Actual Consumption"
    if col_at(41): mapping[col_at(41)] = "Ideal Closing Qty"

    df = df.rename(columns=mapping)

    # Numeric conversions
    for c in ["Opening Qty","Purchase Qty","Stock In Qty","Consumption Qty","Yield Wastage",
              "Stock Out Qty","Wastage Qty","Reuse Qty","Return Qty","Physical Qty","Closing Qty",
              "Variance Qty","PhysicalGainLoss Qty","Actual Consumption","Ideal Closing Qty"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].str.replace(',',''), errors="coerce").fillna(0)

    # Compute derived fields (if missing)
    if "Ideal Closing Qty" not in df.columns and all(x in df.columns for x in ["Opening Qty","Purchase Qty","Stock In Qty","Reuse Qty","Consumption Qty","Stock Out Qty","Wastage Qty","Return Qty"]):
        df["Ideal Closing Qty"] = df["Opening Qty"] + df["Purchase Qty"] + df["Stock In Qty"] + df["Reuse Qty"] - df["Consumption Qty"] - df["Stock Out Qty"] - df["Wastage Qty"] - df["Return Qty"]

    if "Closing Qty" not in df.columns and all(x in df.columns for x in ["Opening Qty","Purchase Qty","Stock In Qty","Consumption Qty","Stock Out Qty","Wastage Qty","Reuse Qty","Return Qty"]):
        df["Closing Qty"] = df["Opening Qty"] + df["Purchase Qty"] + df["Stock In Qty"] - df["Consumption Qty"] - df["Stock Out Qty"] - df["Wastage Qty"] + df["Reuse Qty"] - df["Return Qty"]

    if "PhysicalGainLoss Qty" not in df.columns and "Physical Qty" in df.columns and "Ideal Closing Qty" in df.columns:
        df["PhysicalGainLoss Qty"] = df["Physical Qty"] - df["Ideal Closing Qty"]

    if "Variance Qty" not in df.columns and "Physical Qty" in df.columns and "Closing Qty" in df.columns:
        df["Variance Qty"] = df["Physical Qty"] - df["Closing Qty"]

    if "Actual Consumption" not in df.columns and "Consumption Qty" in df.columns and "PhysicalGainLoss Qty" in df.columns:
        df["Actual Consumption"] = df["Consumption Qty"] - df["PhysicalGainLoss Qty"]

    return df


# ---------------------------------------------------------------------
# ITEM SALES loader
# Path: item_sales
# Mapping provided (1-based):
# A: Outlet Name (0)
# B: Date (1)
# F: Item Name (5)
# G: Qty (6)
# H: Net Sale (7)
# Date format: DD-MM-YY (dayfirst)
# ---------------------------------------------------------------------
@lru_cache(maxsize=1)
def load_item_sales():
    path = _latest_file_in(SALES_DIR, "*.csv")
    if not path:
        return pd.DataFrame()
    df = _safe_read_csv(path)
    if df.empty:
        return df

    cols = list(df.columns)
    def col_at(idx):
        return cols[idx] if idx < len(cols) else None

    mapping = {}
    if col_at(0): mapping[col_at(0)] = "Outlet Name"
    if col_at(1): mapping[col_at(1)] = "Date"
    if col_at(5): mapping[col_at(5)] = "Item Name"
    if col_at(6): mapping[col_at(6)] = "Qty"
    if col_at(7): mapping[col_at(7)] = "Net Sale"

    df = df.rename(columns=mapping)

    # Parse date
    if "Date" in df.columns:
        df["Date"] = _parse_date_series(df["Date"])  # dayfirst True

    # Numeric cleanup
    for c in ["Qty","Net Sale"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].str.replace(',',''), errors="coerce").fillna(0)

    return df


# ---------------------------------------------------------------------
# Existing wrapper adapted to use load_item_sales
# ---------------------------------------------------------------------
@lru_cache(maxsize=1)
def load_item_and_summary_sales():
    """
    Return:
      - item_sales:  item-level sales
      - sales_summary: empty (kept for compatibility)
    """
    item_sales = load_item_sales()
    sales_summary = pd.DataFrame()
    return item_sales, sales_summary


# ---------------------------------------------------------------------
# VARIANCE / COGS (existing function kept but now prefers variance files in Varience_report)
# ---------------------------------------------------------------------
@lru_cache(maxsize=1)
def load_variance_cogs():
    # Prefer files matching *Variance*.csv anywhere under INVENTORY_DIR
    candidates = glob.glob(os.path.join(INVENTORY_DIR, "*Variance*.csv"))
    # fallback: try Varience_report folder latest csv
    if not candidates:
        path = _latest_file_in(VARIENCE_DIR, "*.csv")
    else:
        path = max(candidates, key=os.path.getmtime)

    if not path:
        return pd.DataFrame()

    df = _safe_read_csv(path)
    if df.empty:
        return df

    df.columns = df.columns.str.strip()
    # If this file already has the expected variance columns, try to normalise
    expected = ["Item Code", "Item Name", "Category Name", "Super Category Name", "Average Price", "Actual Consumption", "Amt .11"]
    if set(expected).issubset(set(df.columns)):
        # existing behaviour
        df["Actual Consumption"] = pd.to_numeric(df["Actual Consumption"], errors="coerce").fillna(0)
        df["COGS_Amount"] = pd.to_numeric(df.get("Amt .11", 0), errors="coerce").fillna(0)
        df["Average Price"] = pd.to_numeric(df.get("Average Price", 0), errors="coerce")
        agg = (
            df.groupby(["Item Code", "Item Name", "Category Name", "Super Category Name"], as_index=False)
              .agg({
                  "Actual Consumption": "sum",
                  "COGS_Amount": "sum",
                  "Average Price": "mean",
              })
        )
        return agg

    # Otherwise try to map columns by position (common in your Varience_report)
    cols = list(df.columns)
    def col_at(idx):
        return cols[idx] if idx < len(cols) else None

    # Heuristic mapping — prefer Item Name, Item Code, Outlet Name, COGS Amount, Actual Consumption, Category
    mapping = {}
    if col_at(3): mapping[col_at(3)] = "Item Name"
    if col_at(0): mapping[col_at(0)] = "Outlet Name"
    if col_at(2): mapping[col_at(2)] = "Item Code"
    if col_at(39): mapping[col_at(39)] = "Actual Consumption"
    if col_at(31): mapping[col_at(31)] = "COGS_Amount"
    if col_at(4): mapping[col_at(4)] = "Category Name"

    df = df.rename(columns=mapping)

    # Numeric cleanup
    for c in ["Actual Consumption","COGS_Amount"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].str.replace(',',''), errors="coerce").fillna(0)

    return df


# ---------------------------------------------------------------------
# EXPIRY REPORT loader (kept unchanged)
# ---------------------------------------------------------------------
@lru_cache(maxsize=1)
def load_expiry():
    if not os.path.exists(EXPIRY_FILE):
        return pd.DataFrame()
    try:
        df = pd.read_excel(EXPIRY_FILE)
    except Exception:
        try:
            df = pd.read_html(EXPIRY_FILE)[0]
        except Exception:
            return pd.DataFrame()
    df.columns = df.columns.str.strip()

    # Normalise column names
    rename_map = {}
    for col in df.columns:
        low = col.lower()
        if "item code" in low:
            rename_map[col] = "Item Code"
        elif "item name" in low:
            rename_map[col] = "Item Name"
        elif low.startswith("category"):
            rename_map[col] = "Category"
        elif "expiry" in low:
            rename_map[col] = "Expiry Date"
        elif "days left" in low:
            rename_map[col] = "Days Left"
        elif "status" in low:
            rename_map[col] = "Status"
        elif col.strip().lower() == "qty":
            rename_map[col] = "Qty"
        elif col.strip().lower() == "amount":
            rename_map[col] = "Amount"
        elif "transaction date" in low:
            rename_map[col] = "Transaction Date"

    df = df.rename(columns=rename_map)
    if "Transaction Date" in df.columns:
        df["Transaction Date"] = _parse_date_series(df["Transaction Date"])

    def parse_expiry(x):
        if isinstance(x, str) and "invalid" in x.lower():
            return pd.NaT
        return pd.to_datetime(x, errors="coerce")

    if "Expiry Date" in df.columns:
        df["Expiry Parsed"] = df["Expiry Date"].apply(parse_expiry)
    else:
        df["Expiry Parsed"] = pd.NaT

    today = pd.to_datetime(date.today())

    if "Days Left" in df.columns:
        df["Days Left"] = pd.to_numeric(df["Days Left"], errors="coerce")
    else:
        df["Days Left"] = (df["Expiry Parsed"] - today).dt.days

    def derive_status(row):
        if pd.isna(row["Expiry Parsed"]):
            s = str(row.get("Status", "")).strip()
            return s if s else "No Expiry / Invalid"
        days = row["Days Left"]
        if pd.isna(days):
            return "No Expiry / Invalid"
        if days < 0:
            return "Expired"
        elif days <= 7:
            return "Expiring ≤ 7 days"
        elif days <= 30:
            return "Expiring ≤ 30 days"
        else:
            return "OK"

    df["Expiry Status"] = df.apply(derive_status, axis=1)
    for c in ["Qty", "Amount"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


# ---------------------------------------------------------------------
# RECIPES & STORES
# ---------------------------------------------------------------------
@lru_cache(maxsize=1)
def load_recipes():
    if not os.path.exists(RECIPES_FILE):
        return pd.DataFrame()
    df = pd.read_csv(RECIPES_FILE)
    df.columns = df.columns.str.strip()
    return df


@lru_cache(maxsize=1)
def load_store_mapping():
    if not os.path.exists(STORES_DB):
        return pd.DataFrame()
    df = pd.read_csv(STORES_DB)
    df.columns = df.columns.str.strip()
    return df
