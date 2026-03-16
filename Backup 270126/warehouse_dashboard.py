from dash import html, dcc, Input, Output, State
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import glob, os, csv
import plotly.io as pio
from dash import dash_table
from dash.dash_table.Format import Format, Scheme
from dash import callback_context
import logging
import pandera.pandas as pa
from pandera.pandas import Column, DataFrameSchema
import plotly.graph_objects as go
import io
from dotenv import load_dotenv
import os
import numpy as np
from email.mime.base import MIMEBase
from email import encoders
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from apscheduler.schedulers.background import BackgroundScheduler
from dash.exceptions import PreventUpdate


scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
scheduler.start()

pio.templates.default = "plotly"

# ======================================================
# PATHS
# ======================================================
BASE = r"C:\Users\ACER\store_dashboard"
ENTRY_PATH = os.path.join(BASE, "inventory", "warehouse_stockentry")
INDENT_PATH = os.path.join(BASE, "inventory", "Indent_report")
BULK_RETURN_PATH = os.path.join(BASE, "inventory", "bulk_return")
ENV_PATH = r"C:\Users\ACER\store_dashboard\pages\Gmail_credentials.env"
EXPIRY_PATH = r"C:\Users\ACER\store_dashboard\inventory\expiryreport"
WASTAGE_PATH = os.path.join(BASE, "inventory", "warehouse_wastage")
CONSUMPTION_PATH = os.path.join(BASE, "inventory", "consumption")
ITEM_MASTER_PATH = os.path.join(
    BASE, "inventory", "stockitem_master.csv"
)
OPENING_STOCK_PATH = os.path.join(
    BASE, "inventory", "warehouse_opening_stock.csv"
)

OPENING_STOCK_DATE = pd.Timestamp("2025-05-01")

# ======================================================
# CONSTANTS
# ======================================================
COL_DATE = "date"
COL_WAREHOUSE = "Warehouse"
COL_OUTLET = "Outlet"
COL_ITEM = "Item"
COL_CATEGORY = "Category"
COL_UOM = "UOM"
COL_UNIT_PRICE = "Unit Price"
COL_TAX = "Tax"

LOOKBACK_DAYS = 14
STOCKOUT_DAYS = 7
ALERT_DEAD_DAYS = 30

load_dotenv(ENV_PATH)
# ================= LOGGING CONFIG =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ======================================================
# UOM NORMALIZER (SINGLE SOURCE OF TRUTH)
# ======================================================
def normalize_uom(u):
    if pd.isna(u):
        return ""

    u = str(u).upper().strip()

    if "KG" in u:
        return "KG"
    if "GM" in u:
        return "GM"
    if "LTR" in u or "LIT" in u:
        return "LTR"
    if "ML" in u:
        return "ML"
    if "PCS" in u or "PC" in u:
        return "PCS"
    if "BTL" in u:
        return "BTL"

    return u


# ==========================================================
# UOM PATCH FILE – Multi-UOM Conversion for Consumption Only
# Safe integration with existing warehouse_dashboard.py
# ==========================================================

import pandas as pd

# -------------------------------------------
# Ensure ITEM_CONV_MAP exists
# -------------------------------------------
try:
    master_uom = pd.read_csv(ITEM_MASTER_PATH)
    master_uom.columns = master_uom.columns.str.lower().str.strip()

    ITEM_CONV_MAP = {
    (str(r["item name"]).strip(), str(r["purchase uom"]).upper().strip()): { 
        # Removed .title() above
            "base": str(r["base uom"]).upper().strip(),
            "factor": float(r["conversion factor"])
        }
        for _, r in master_uom.iterrows()
    }

except Exception as e:
    print("❌ Failed to load ITEM_MASTER:", e)
    ITEM_CONV_MAP = {}


# ==========================================================
# Conversion Function (ONLY for consumption)
# ==========================================================
def convert_uom_for_consumption(item, qty, uom):
    """
    Converts outgoing qty to base UOM using item master.
    Purchase UOM is never modified.
    """
    if pd.isna(qty) or qty is None:
        return 0.0

    item = str(item).strip()#.title()
    u = str(uom).strip().upper()

    key = (item, u)

    if key in ITEM_CONV_MAP:
        factor = ITEM_CONV_MAP[key]["factor"]
        return qty * factor

    # fallback conversions
    if u == "GM":
        return qty
    if u == "KG":
        return qty * 1000
    if u == "ML":
        return qty
    if u == "LTR":
        return qty * 1000

    return qty  # default

def force_purchase_uom(df):
    for idx, row in df.iterrows():
        item = row[COL_ITEM]
        # get purchase uom from conversion map
        for (it, puom), x in ITEM_CONV_MAP.items():
            if it == item:
                df.at[idx, COL_UOM] = puom
    return df



# ==========================================================
# PATCH #4 – Fallback patch in apply_expiry_deduction()
# ==========================================================
def ensure_qty_out_base(tx_df):
    if "qty_out_base" not in tx_df.columns:
        print("ℹ️ qty_out_base missing – creating fallback")
        tx_df["qty_out_base"] = tx_df.get("qty_out", 0)
    return tx_df


# ==========================================================
# PATCH #5 – Fix get_available_stock_map()
# ==========================================================
def patch_get_available_stock_map(df):
    if "qty_out_base" not in df.columns:
        df["qty_out_base"] = df.get("qty_out", 0)
    return df


# ==========================================================
# END OF UOM PATCH FILE
# ==========================================================

def to_grams(qty, uom):
    if pd.isna(qty):
        return 0.0

    u = str(uom).upper()
    qty = float(qty)

    if u == "KG":
        return qty * 1000
    if u == "GM":
        return qty

    return qty

# ======================================================
# UNIT CONVERSION (LOW → BIG UNIT)
# ======================================================
def convert_to_base_unit(qty, uom):
    """
    Converts qty from lowest unit to base unit
    GM  → KG
    ML  → KG (assumed liquid weight)
    """
    if pd.isna(qty):
        return 0.0

    u = str(uom).upper().strip()
    qty = float(qty)

    if u == "GM":
        return qty / 1000, "KG"

    if u == "ML":
        return qty / 1000, "KG"   # business rule: liquid treated as KG

    return qty, u

#============================================
# Build a GLOBAL DISPLAY UOM MAP
#============================================
def build_display_uom_map(tx_df, opening_df):
    """
    Resolve display UOM per (Warehouse, Item)
    Priority:
    1. ENTRY
    2. INDENT
    3. OPENING
    """
    uom_map = {}

    # 1️⃣ STOCK ENTRY
    entry = tx_df[tx_df["source"] == "ENTRY"]
    for _, r in entry.iterrows():
        key = (r[COL_WAREHOUSE], r[COL_ITEM])
        uom_map.setdefault(key, r[COL_UOM])

    # 2️⃣ INDENT OUT
    indent = tx_df[(tx_df["source"] == "INDENT") & (tx_df["qty_out"] > 0)]
    for _, r in indent.iterrows():
        key = (r[COL_WAREHOUSE], r[COL_ITEM])
        uom_map.setdefault(key, r[COL_UOM])

    # 3️⃣ OPENING STOCK
    if opening_df is not None and not opening_df.empty:
        for _, r in opening_df.iterrows():
            key = (r[COL_WAREHOUSE], r[COL_ITEM])
            uom_map.setdefault(key, r[COL_UOM])

    return uom_map

# ==============================================================
# 🔥 FINAL UOM + LEDGER + AGING STABLE PATCH (DROP-IN MODULE)
# ==============================================================

# --------------------------------------------------------------
# Auto-detect column names from Item Master safely
# --------------------------------------------------------------
def find_col(df, name):
    target = name.lower().replace(" ", "").strip()
    for col in df.columns:
        clean = str(col).lower().replace(" ", "").strip()
        if clean == target:
            return col
    return None
# ==============================
# ITEM & UOM NORMALIZATION UTILITIES
# ==============================

# --------------------------------------------------------------
# Normalizer for item names
# --------------------------------------------------------------
def normalize_item_name(name):
    if pd.isna(name):
        return ""
    # Removed .title() and .replace() to keep original Excel formatting
    return str(name).strip()

# def normalize_uom(uom):
#     """Ensure UOM is uppercase and trimmed"""
#     if not isinstance(uom, str):
#         return ""
#     return uom.replace(" ", "").upper().strip()

# ================= SKIP CATEGORIES =================
SKIP_CATEGORIES = [
    "CROCKERY",
    "CUTLERY",
    "DISPOSABLE CUTLERY - WOODEN FORK",
    "BOH BEVERAGE EQUIPMENT",
    "BOH FOOD EQUIPMENT",
    "BOH OTHER EQUIPMENT",
    "CLEANING CONSUMABLES - GARBAGE BAGS/GLUE PADS",
    "CLEANING TOOLS - BROOMS/PANS/BINS/WIPERS/TROLLEY/BUCKETS/BRUSHES/DISPENSER",
    "DISPOSABLE CUTLERY - WOODEN FORK",
    "DISPOSABLE PACKAGING - ACCESSORIES - CARRY BAG",
    "GLASSWARE",
    "BOH DEAD FABRICATION EQUIPMENT",
    "BOH OTHER EQUIPMENT",
    "CLEANING CHEMICALS - LIQUID",
]

def apply_category_skip(df, col=COL_CATEGORY):
    return df[
        ~df[col]
        .astype(str)
        .str.strip()
        .str.upper()
        .isin(SKIP_CATEGORIES)
    ]

# =========================================
# LOAD STOCK ITEM MASTER
# =========================================
# Load CSV as DataFrame
ITEM_MASTER_PATH = "C:/Users/ACER/store_dashboard/inventory/stockitem_master.csv"

# Load as DataFrame
ITEM_MASTER = pd.read_csv(ITEM_MASTER_PATH)

# Clean column names lightly
ITEM_MASTER.columns = ITEM_MASTER.columns.str.strip()

# Detect correct column names dynamically
col_item  = find_col(ITEM_MASTER, "Item Name")
col_puom  = find_col(ITEM_MASTER, "Purchase UOM")
col_buom  = find_col(ITEM_MASTER, "Base UOM")
col_factor = find_col(ITEM_MASTER, "Conversion Factor")

# HARD CHECK
missing = [c for c in [col_item, col_puom, col_buom, col_factor] if c is None]
if missing:
    raise Exception(
        f"❌ ERROR: Could not detect required master columns.\n"
        f"Detected columns: {ITEM_MASTER.columns.tolist()}"
    )

ITEM_MASTER[col_item] = ITEM_MASTER[col_item].astype(str).str.strip()
ITEM_MASTER[col_puom] = ITEM_MASTER[col_puom].apply(normalize_uom)
ITEM_MASTER[col_buom] = ITEM_MASTER[col_buom].apply(normalize_uom)

# Build global conversion map
ITEM_CONV_MAP = {
    (
        row[col_item],
        row[col_puom],
    ): {
        "factor": float(row[col_factor]),
        "base_uom": row[col_buom],
    }
    for _, row in ITEM_MASTER.iterrows()
}
# ======================================================
# SAFE DROPDOWN OPTIONS (STRING-ONLY GUARANTEE)
# ======================================================
def dropdown_options(series):
    values = (
        series
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: (s != "") & (s.str.lower() != "nan")]
        .unique()
    )
    return [{"label": v, "value": v} for v in sorted(values)]

def reload_all_data():

    entry_df   = load_entry_data(ENTRY_PATH)
    indent_df  = load_indent_or_return_data(INDENT_PATH)
    bulk_df    = load_bulk_return_data(BULK_RETURN_PATH)
    wastage_df = load_wastage_data(WASTAGE_PATH)

    df = pd.concat(
        [entry_df, indent_df, bulk_df, wastage_df],
        ignore_index=True
    )

    # APPLY OPENING STOCK
    df = apply_opening_stock(df, opening_stock_df)

    # ================= NORMALIZATION =================
    df[COL_ITEM] = df[COL_ITEM].astype(str).str.strip()
    df[COL_CATEGORY] = (
        df[COL_CATEGORY]
        .astype(str)
        .str.strip()
        .str.upper()   # 🔥 IMPORTANT
    )

    # ================= PRICE BACKFILL =================
    price_map = entry_df.groupby(COL_ITEM)[COL_UNIT_PRICE].mean()

    df[COL_UNIT_PRICE] = (
        df[COL_UNIT_PRICE]
        .where(df[COL_UNIT_PRICE] > 0, df[COL_ITEM].map(price_map))
        .fillna(0)
    )

    # ================= MASTER MAPS =================
    df["Super Category"] = df[COL_ITEM].map(ITEM_SUPER_CAT_MAP).fillna("Unknown")
    df["Has Expiry"]     = df[COL_ITEM].map(ITEM_EXPIRY_MAP).fillna("No")
    df[COL_TAX]          = df[COL_ITEM].map(ITEM_TAX_MAP).fillna(0)

    return df


scheduler.add_job(
    lambda: send_expiry_alerts(load_expiry_data(EXPIRY_PATH), reload_all_data(), 60),
    trigger="cron", hour=9, minute=0
)

scheduler.add_job(
    lambda: send_expiry_alerts(load_expiry_data(EXPIRY_PATH), reload_all_data(), 30),
    trigger="cron", hour=9, minute=5
)

scheduler.add_job(
    lambda: send_expiry_alerts(load_expiry_data(EXPIRY_PATH), reload_all_data(), 15),
    trigger="cron", hour=9, minute=10
)

scheduler.add_job(
    lambda: send_negative_stock_mail(tx_df),
    trigger="cron",
    day="15,30",
    hour=9,
    minute=15
)
scheduler.add_job(
    lambda: send_available_stock_snapshot(tx_df),
    trigger="cron",
    day="last",
    hour=9,
    minute=20
)

def find_col_priority(columns, priority_keys, fallback_keys=None):
    cols = list(columns)

    # 1️⃣ Try priority keys first (EXACT match)
    for key in priority_keys:
        for c in cols:
            if key == c:
                return c

    # 2️⃣ Try contains match for priority keys
    for key in priority_keys:
        for c in cols:
            if key in c:
                return c

    # 3️⃣ Fallback (codes etc.)
    if fallback_keys:
        for key in fallback_keys:
            for c in cols:
                if key in c:
                    return c

    return None
# ===============================================================
# 🔽 CONVERSION LOGIC FOR CONSUMPTION ONLY
# ===============================================================
def convert_indent_to_purchase(item_name, qty, from_uom):
    """Convert qty_out UOM → Purchase UOM using master sheet"""
    item_name = normalize_item_name(item_name)
    from_uom = normalize_uom(from_uom)

    # find purchase uom for item
    puom = None
    for (item, u), v in ITEM_CONV_MAP.items():
        if item == item_name:
            puom = u
            factor = v["factor"]
            break

    if puom is None:
        return qty

    if from_uom == puom:
        return qty

    # Convert indent UOM → purchase UOM
    return qty / factor

# ======================================================
# ENTRY → STOCK IN
# ======================================================
def load_entry_data(path):
    rows = []

    for f in glob.glob(os.path.join(path, "*.csv")):
        df = pd.read_csv(f, engine="python", on_bad_lines="skip")
        df.columns = df.columns.str.strip()

        # ------------------------------------------
        # DATE
        # ------------------------------------------
        df[COL_DATE] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")

        # ------------------------------------------
        # SAFE WAREHOUSE DETECTION
        # ------------------------------------------
        c_wh = None
        for c in df.columns:
            c_low = c.lower().replace(" ", "")
            if c_low == "deploymentname":
                c_wh = c
                break
            if c_low == "warehousename":
                c_wh = c
                break
            if c_low == "warehouse":
                c_wh = c
                break

        df[COL_WAREHOUSE] = (
            df[c_wh].astype(str).str.strip()
            if c_wh else "__UNKNOWN_WAREHOUSE__"
        )

        df[COL_OUTLET] = "INTERNAL_WAREHOUSE"
        df[COL_ITEM] = df["Item Name"]
        df[COL_CATEGORY] = df["Category Name"]

        # UOM + PRICE detection unchanged
        price_col = next((c for c in df.columns if "unit price" in c.lower()), None)
        tax_col   = next((c for c in df.columns if "tax" in c.lower()), None)
        uom_col = next(
            (c for c in df.columns if "uom" in c.lower() or "unit" in c.lower()),
            None
        )
        df[COL_UOM] = (
            df[uom_col].astype(str).str.strip().str.upper()
            if uom_col else "UNKNOWN"
        )
        df[COL_UNIT_PRICE] = pd.to_numeric(df[price_col], errors="coerce").fillna(0) if price_col else 0

        df[COL_TAX] = 0
        df["qty_in"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).round(2)
        df["qty_out"] = 0.0
        df["source"] = "ENTRY"

        rows.append(df)

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

def get_conversion_factor(unit, conv_unit):
    u = str(unit).strip().upper()
    c = str(conv_unit).strip().upper()

    if u == c:
        return 1

    MAP = {
        ("BOX", "PCS"): 12,
        ("KG", "GM"): 1000,
        ("LTR", "ML"): 1000,
    }

    return MAP.get((u, c), 1)  # default safe = 1

def force_name_only(series, fallback="Unknown"):
    s = series.astype(str).str.strip()
    return s.where(~s.str.fullmatch(r"\d+"), fallback)

def load_opening_stock(path):
    if not os.path.exists(path):
        #logging.warning("Opening stock file not found")
        return pd.DataFrame(
            columns=[COL_WAREHOUSE, COL_CATEGORY, COL_ITEM, COL_UOM, "opening_qty"]
        )

    df = pd.read_csv(path)
    df.columns = df.columns.str.strip().str.lower()

    def find_col(keys):
        for c in df.columns:
            if all(k in c for k in keys):
                return c
        return None

    c_wh   = find_col(["warehouse"])
    c_cat  = find_col(["category"])
    c_item = find_col(["item"])
    c_uom  = find_col(["uom"]) or find_col(["unit"])
    c_qty  = (
        find_col(["opening", "stock"]) or
        find_col(["opening", "qty"]) or
        find_col(["qty"])
    )

    if not all([c_wh, c_item, c_uom, c_qty]):
        raise ValueError(
            f"Opening stock CSV missing required columns. Found: {df.columns.tolist()}"
        )

    out = pd.DataFrame()
    out[COL_WAREHOUSE] = df[c_wh].astype(str).str.strip()
    out[COL_CATEGORY]  = df[c_cat].astype(str).str.strip() if c_cat else "Opening Stock"
    out[COL_ITEM]      = df[c_item].astype(str).str.strip()
    out[COL_UOM]       = df[c_uom].apply(normalize_uom)
    out["opening_qty"] = pd.to_numeric(df[c_qty], errors="coerce").fillna(0).round(2)

    #logging.info("Opening stock loaded: %s rows", len(out))
    return out

def get_opening_stock(opening_df, warehouse, item, uom):

    if opening_df is None or opening_df.empty:
        return 0.0

    row = opening_df[
        (opening_df[COL_WAREHOUSE] == warehouse) &
        (opening_df[COL_ITEM] == item) &
        (opening_df[COL_UOM] == uom)
    ]

    if row.empty:
        return 0.0

    return float(row["opening_qty"].iloc[0])

# ======================================================
# INDENT / RETURN AUTO-DETECTION (AUDIT SAFE)
# ======================================================
def parse_indent_csv(path):
    header = None
    with open(path, encoding="utf-8", errors="ignore") as f:
        for i, row in enumerate(csv.reader(f)):
            row = [x.lower().strip() for x in row]
            if any("item" in x for x in row) and any("qty" in x for x in row):
                header = i
                break

    if header is None:
        return pd.DataFrame()

    df = pd.read_csv(path, skiprows=header, engine="python", on_bad_lines="skip")
    df.columns = df.columns.str.lower().str.strip()
    return df

def load_indent_or_return_data(path):
    rows = []

    for f in glob.glob(os.path.join(path, "*.csv")):

        df = parse_indent_csv(f)
        if df.empty:
            df = pd.read_csv(f, engine="python", on_bad_lines="skip")
            df.columns = df.columns.str.lower().str.strip()

        # ---------------------
        # SAFE COLUMN FINDERS
        # ---------------------
        def find_col(keys):
            for c in df.columns:
                if all(k in c for k in keys):
                    return c
            return None

        c_date = find_col(["date"])
        c_item = find_col_priority(
            df.columns,
            priority_keys=["item name"],
            fallback_keys=["item"]
        )
        c_qty  = find_col(["qty"])
        c_wh   = find_col(["warehouse"]) or find_col(["supplier"])
        c_cat  = find_col_priority(
            df.columns,
            priority_keys=["category name"],
            fallback_keys=["category"]
        )
        c_out  = (
            find_col(["outlet"]) or
            find_col(["receiver"]) or
            find_col(["store"]) or
            find_col(["location"]) or
            find_col(["kitchen"])
        )
        c_reuse_qty    = find_col(["reuse", "qty"])
        c_supplied_qty = find_col(["supplied", "qty"]) or c_qty
        c_price        = find_col(["unit", "price"])
        c_tax = (
            find_col(["tax"]) or
            find_col(["gst"]) or
            find_col(["igst"]) or
            find_col(["cgst"]) or
            find_col(["sgst"])
        )

        # ---------------------
        # CRITICAL REQUIRED FIELDS
        # ---------------------
        if not all([c_item, c_qty]):
            continue

        # ---------------------
        # DATE
        # ---------------------
        df[COL_DATE] = (
            pd.to_datetime(df[c_date], dayfirst=True, errors="coerce")
            if c_date else pd.NaT
        )

        # ---------------------
        # WAREHOUSE
        # ---------------------
        df[COL_WAREHOUSE] = (
            df[c_wh].astype(str).str.strip()
            if c_wh else "__UNKNOWN__"
        )

        # ---------------------
        # OUTLET / RECEIVER
        # ---------------------
        df[COL_OUTLET] = (
            df[c_out].astype(str).str.strip()
            if c_out else "UNKNOWN_OUTLET"
        )
        df[COL_OUTLET] = df[COL_OUTLET].replace(
            ["", "nan", "none", None],
            "UNKNOWN_OUTLET"
        )

        # ---------------------
        # ITEM & CATEGORY
        # ---------------------
        df[COL_ITEM] = force_name_only(df[c_item], "UNKNOWN ITEM")

        df[COL_CATEGORY] = (
            force_name_only(df[c_cat], "Uncategorized")
            if c_cat else "Uncategorized"
        )

        # ---------------------
        # DETECT UOM COLUMN
        # ---------------------
        c_uom = find_col(["unit"])
        df[COL_UOM] = (
            df[c_uom].apply(normalize_uom)
            if c_uom else None
        )

        # ---------------------
        # PRICE & TAX
        # ---------------------
        df[COL_UNIT_PRICE] = (
            pd.to_numeric(df[c_price], errors="coerce").fillna(0)
            if c_price else 0
        )
        df[COL_TAX] = 0

        # ---------------------
        # QUANTITY CALCULATION
        # supplied_qty - reuse_qty
        # ---------------------
        supplied_qty = (
            pd.to_numeric(df[c_supplied_qty], errors="coerce")
            .fillna(0).round(2)
        )

        reuse_qty = (
            pd.to_numeric(df[c_reuse_qty], errors="coerce")
            .fillna(0).round(2)
            if c_reuse_qty else 0
        )

        df["qty_in"] = 0.0
        df["qty_out"] = (supplied_qty - reuse_qty).clip(lower=0)

        # ---------------------
        # UOM CONVERSION PATCH
        # qty_out_base always required
        # ---------------------
        df["qty_out_base"] = df.apply(
            lambda r: convert_uom_for_consumption(
                r[COL_ITEM], r["qty_out"], r[COL_UOM]
            ),
            axis=1
        )

        # ---------------------
        # SOURCE TAG
        # ---------------------
        df["source"] = "INDENT"

        # ---------------------
        # FINAL CLEAN DF
        # ---------------------
        rows.append(
            df[
                [
                    COL_DATE,
                    COL_WAREHOUSE,
                    COL_OUTLET,
                    COL_ITEM,
                    COL_CATEGORY,
                    COL_UOM,
                    COL_UNIT_PRICE,
                    COL_TAX,
                    "qty_in",
                    "qty_out",
                    "qty_out_base",
                    "source",
                ]
            ]
        )

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def apply_opening_stock(tx_df, opening_df):

    if opening_df is None or opening_df.empty:
        return tx_df

    opening_txn = pd.DataFrame({
        COL_DATE: OPENING_STOCK_DATE,
        COL_WAREHOUSE: opening_df[COL_WAREHOUSE],
        COL_OUTLET: "OPENING_STOCK",
        COL_ITEM: opening_df[COL_ITEM],
        COL_CATEGORY: opening_df[COL_CATEGORY],
        COL_UOM: opening_df[COL_UOM],        # ✅ FROM OPENING FILE
        COL_UNIT_PRICE: 0,
        COL_TAX: 0,
        "qty_in": opening_df["opening_qty"],
        "qty_out": 0,
        "source": "OPENING",
    })

    return pd.concat([opening_txn, tx_df], ignore_index=True)

# ======================================================
# BULK RETURN → STOCK IN ONLY
# ======================================================
def load_bulk_return_data(path):
    rows = []

    for f in glob.glob(os.path.join(path, "*.csv")):
        df = pd.read_csv(f, engine="python", on_bad_lines="skip")
        df.columns = df.columns.str.lower().str.strip()

        def find_col(keys):
            for c in df.columns:
                if all(k in c for k in keys):
                    return c
            return None

        # ================= COLUMN DETECTION =================
        c_date = find_col(["date"])
        c_item_name = find_col(["item name"])   # STRICT ITEM NAME ONLY
        c_qty  = find_col(["qty"]) or find_col(["quantity"])
        c_wh   = find_col(["warehouse"]) or find_col(["supplier"])
        c_out  = find_col(["outlet"]) or find_col(["receiver"])
        c_cat  = find_col(["category"])
        c_uom  = find_col(["uom"]) or find_col(["unit"])

        # ❌ Item Code intentionally ignored
        if not all([c_item_name, c_qty]):
            continue

        # ================= DATE =================
        df[COL_DATE] = (
            pd.to_datetime(df[c_date], dayfirst=True, errors="coerce")
            if c_date else pd.NaT
        )

        # ================= WAREHOUSE / OUTLET =================
        df[COL_WAREHOUSE] = (
            df[c_wh].astype(str).str.strip()
            if c_wh else "__UNKNOWN__"
        )

        df[COL_OUTLET] = (
            df[c_out].astype(str).str.strip()
            if c_out else "__UNKNOWN__"
        )

        # ================= UOM NORMALIZATION =================
        df[COL_UOM] = (
            df[c_uom].apply(normalize_uom)
            if c_uom else "UNKNOWN"
        )

        # ================= ITEM NAME (FINAL SOURCE) =================
        df[COL_ITEM] = df[COL_ITEM].astype(str).str.strip()

        # ================= CATEGORY =================
        df[COL_CATEGORY] = (
            df[c_cat]
            .astype(str)
            .str.strip()
            .replace(["", "nan", "none"], "Uncategorized")
            if c_cat else "Uncategorized"
        )

        # ================= QUANTITY =================
        qty = pd.to_numeric(df[c_qty], errors="coerce").fillna(0).round(2)

        df["qty_in"] = qty.abs()
        df["qty_out"] = 0.0

        # ================= PATCH: qty_out_base ALWAYS REQUIRED
        df["qty_out_base"] = 0.0

        # ================= SOURCE =================
        df["source"] = "BULK_RETURN"

        # ================= FINAL CLEAN REDUCED DF =================
        rows.append(
            df[
                [
                    COL_DATE,
                    COL_WAREHOUSE,
                    COL_OUTLET,
                    COL_ITEM,
                    COL_CATEGORY,
                    COL_UOM,
                    "qty_in",
                    "qty_out",
                    "qty_out_base",
                    "source",
                ]
            ]
        )

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

# ======================================================
# POSIST ENTERPRISE WASTAGE REPORT → STOCK OUT
# ======================================================
# 🔧 Header mapping dictionary
def find_column(df, include_keywords, exclude_keywords=None):
    include_keywords = [k.lower() for k in include_keywords]
    exclude_keywords = [k.lower() for k in exclude_keywords] if exclude_keywords else []

    for col in df.columns:
        name = col.lower().strip()

        if all(k in name for k in include_keywords) and not any(
            k in name for k in exclude_keywords
        ):
            return col

    return None


def safe_col(df, col):
    if col and col in df:
        return pd.to_numeric(df[col], errors="coerce").fillna(0)
    return pd.Series([0] * len(df))

def col_or_empty(df, col):
    if col and col in df.columns:
        return df[col].astype(str).fillna("")
    return pd.Series([""] * len(df))

def clean_columns(df):
    # Remove spaces + lowercase
    df.columns = df.columns.str.strip().str.lower()

    # Deduplicate
    new_cols = []
    counter = {}
    for c in df.columns:
        if c not in counter:
            counter[c] = 0
            new_cols.append(c)
        else:
            counter[c] += 1
            new_cols.append(f"{c}_{counter[c]}")
    df.columns = new_cols
    return df


# ======================================================
# POSIST WASTAGE REPORT (CUSTOM FORMAT)
# ======================================================
def load_wastage_data(folder_path):
    """
    Supports files with columns:
    Deployment Name, Store/Kitchen Name, User Name, Date,
    Transaction Number, Item Code, Item Name, Category Name,
    Super Category, Comment, Quantity, Unit, Unit Price, Amount
    """

    rows = []
    files = [f for f in os.listdir(folder_path) if f.lower().endswith(".csv")]

    for fname in files:
        fpath = os.path.join(folder_path, fname)

        # Read CSV with flexible delimiter
        try:
            df = pd.read_csv(fpath, engine="python", on_bad_lines="skip")
        except Exception:
            continue

        # Normalize columns
        df.columns = df.columns.str.lower().str.strip()

        # Detect fields
        c_date  = next((c for c in df.columns if "date" in c), None)
        c_item  = next((c for c in df.columns if "item name" in c), None)
        c_cat   = next((c for c in df.columns if "category" in c), None)
        c_unit  = next((c for c in df.columns if "unit" in c), None)
        c_qty   = next((c for c in df.columns if c in ["qty","quantity"]), None)
        c_price = next((c for c in df.columns if "unit price" in c), None)
        c_wh    = next((c for c in df.columns if "wastage" in c), None)

        # Required minimum fields
        if not c_item or not c_qty:
            continue

        # Convert numbers safely
        qty = pd.to_numeric(df[c_qty], errors="coerce").fillna(0)
        price = pd.to_numeric(df.get(c_price, 0), errors="coerce").fillna(0)
        wastage_value = qty * price

        cleaned = pd.DataFrame({
            "date": pd.to_datetime(df[c_date], dayfirst=True, errors="coerce") if c_date else pd.NaT,
            "Warehouse": df[c_wh].astype(str) if c_wh else "Unknown Warehouse",
            "Outlet": "WASTAGE",
            "Item": df[c_item].astype(str).str.strip(),
            "Category": df[c_cat].astype(str) if c_cat else "Unknown",
            "UOM": df[c_unit].astype(str).str.upper(),
            "qty_in": 0,
            "qty_out": qty,
            "wastage_amount": wastage_value,
            "source": "WASTAGE"
        })
        # =============================================
        # PATCH: create qty_out_base for wastage
        # =============================================
        cleaned["qty_out_base"] = cleaned.apply(
            lambda r: convert_uom_for_consumption(
                r["Item"], r["qty_out"], r["UOM"]
            ),
            axis=1
        )

        rows.append(cleaned)

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

def build_wastage_report(df, sd=None, ed=None):

    df = df[df["source"] == "WASTAGE"].copy()
    df = apply_category_skip(df, COL_CATEGORY)
    if sd:
        df["date"] = pd.to_datetime(df["date"])
        df = df[df["date"] >= pd.to_datetime(sd)]

    if ed:
        df = df[df["date"] <= pd.to_datetime(ed)]

    # DO NOT FILTER ZERO QTY
    # df = df[df["qty_out"] > 0]  # ❌ remove this line

    if df.empty:
        return pd.DataFrame()

    report = (
        df.groupby(
            ["date", "Warehouse", "Category", "Item", "UOM"],
            as_index=False
        )
        .agg(
            Wastage_Qty=("qty_out", "sum"),
            Wastage_Value=("wastage_amount", "sum")
        )
    )
    report["date"] = pd.to_datetime(report["date"]).dt.strftime("%d-%m-%Y")
    # return entire report — even 0 qty rows
    report = force_display_uom(report)
    return report

def wastage_tab_layout():
    return dbc.Container([
        html.H3("Warehouse Wastage Report", className="mb-3 mt-3"),

        dbc.Row([
            dbc.Col([
                dcc.DatePickerRange(
                    id="wastage_date_range",
                    display_format="DD-MM-YYYY",
                    start_date_placeholder_text="Start Date",
                    end_date_placeholder_text="End Date",
                )
            ], width=4),

            dbc.Col([
                dbc.Button("Refresh", id="refresh_wastage", color="primary")
            ], width="auto"),
        ], className="mb-3"),

        dash_table.DataTable(
            id="wastage_table",
            columns=[
                {"name": "Date", "id": COL_DATE},
                {"name": "Warehouse", "id": COL_WAREHOUSE},
                {"name": "Category", "id": COL_CATEGORY},
                {"name": "Item Name", "id": COL_ITEM},
                {"name": "Unit", "id": COL_UOM},
                {
                    "name": "Wastage Qty",
                    "id": "Wastage_Qty",
                    "type": "numeric",
                    "format": Format(precision=3),
                },
                {
                    "name": "Wastage Value (₹)",
                    "id": "Wastage_Value",
                    "type": "numeric",
                    "format": Format(precision=2, group=True),
                },
            ],
            page_size=12,
            sort_action="native",
            filter_action="native",
            style_header={"fontWeight": "bold"},
            style_cell={"fontSize": "13px", "padding": "6px"},
            style_data_conditional=[
                {
                    "if": {"filter_query": "{Wastage_Qty} > 0"},
                    "backgroundColor": "#fff3cd",
                }
            ],
        ),
    ], fluid=True)

# ===============================================================
# 1️⃣  CLEAN SINGLE CSV FILE
# ===============================================================

FINAL_COLUMNS = [
    "outlet name",
    "storekitchen name",
    "item code",
    "item name",
    "category name",
    "super category name",
    "average price",
    "opening date",
    "opening qty",
    "unit",
    "purchase qty",
    "indent receive qty",
    "indent dispatch qty",
    "internalindent receive qty",
    "internalindent dispatch qty",
    "stock in qty",
    "consumption qty",
    "yield wastage",
    "stock out qty",
    "total stock out + consumption qty",
    "wastage qty",
    "reuse qty",
    "return qty",
    "closing date",
    "closing qty",
    "latest physical qty",
    "physical gain loss qty",
    "ideal closing qty",
    "physical adjusted closing qty"
]
skip_columns = [
    "storekitchen name",   # drop this
    "internalindent receive qty",  # example skip
    "internalindent dispatch qty",
    "super category name",
    "item code",
    "latest physical qty",
    "physical gain loss qty",
    "physical adjusted closing qty",
    
]

def load_consumption_csv(file_path):
    print("🔍 Loading:", file_path)

    # Step 1: Read raw file with no header
    df = pd.read_csv(file_path, engine="python", header=None)

    # Step 2: Extract header row and normalize
    raw_header = df.iloc[0].astype(str).str.lower().str.strip()
    raw_data = df.iloc[1:].reset_index(drop=True)

    # Step 3: Drop all columns where header contains "amt"
    keep_mask = ~raw_header.str.contains("amt")
    clean_header = raw_header[keep_mask].tolist()
    clean_data = raw_data.loc[:, keep_mask]
    clean_data.columns = clean_header

    # Step 4: Exact keyword mapping
    keyword_map = {
        "outlet name": "outlet name",
        "storekitchen name": "storekitchen name",
        "item code": "item code",
        "item name": "item name",
        "category name": "category name",
        "super category name": "super category name",
        "average price": "average price",
        "opening date": "opening date",
        "opening qty": "opening qty",
        "unit": "unit",
        "purchase qty": "purchase qty",
        "indent receive qty": "indent receive qty",
        "indent dispatch qty": "indent dispatch qty",
        "internalindent receive qty": "internalindent receive qty",
        "internalindent dispatch qty": "internalindent dispatch qty",
        "stock in qty": "stock in qty",
        "consumption qty": "consumption qty",
        "yield wastage": "yield wastage",
        "stock out qty": "stock out qty",
        "total stock out + consumption qty": "total stock out + consumption qty",
        "wastage qty": "wastage qty",
        "reuse qty": "reuse qty",
        "return qty": "return qty",
        "closing date": "closing date",
        "closing qty": "closing qty",
        "latest physical qty": "latest physical qty",
        "physical gain loss qty": "physical gain loss qty",
        "ideal closing qty": "ideal closing qty",
        "physical adjusted closing qty": "physical adjusted closing qty"
    }

    # Step 5: Build final structure with exact matches
    final = pd.DataFrame()
    for new_col, key in keyword_map.items():
        if key in clean_data.columns:
            final[new_col] = clean_data[key]
        else:
            final[new_col] = 0

    # Step 6: Drop unwanted columns
    final = final.drop(columns=[c for c in skip_columns if c in final.columns])

    return final


# ===============================================================
# 2️⃣  MERGE ALL FILES
# ===============================================================

def load_consumption_data():
    folder = r"C:\Users\ACER\store_dashboard\inventory\consumption\*"
    files = glob.glob(folder)

    frames = []

    for f in files:
        try:
            frames.append(load_consumption_csv(f))
        except Exception as e:
            print(f"❌ ERROR in {f}: {e}")

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


# ===============================================================
# 3️⃣  BUILD SIMPLE REPORT
# ===============================================================
hide_categories = ["GLASSWARE", "MUSIC SYSTEM", "FIRE FIGHTING SYSTEM",
                    "CCTV CAMERA & NETWORKING", "DIGITAL MENU BOARD",
                    "BOH OTHER EQUIPMENT", "BOH FOOD EQUIPMENT",
                    "POS HARDWARE", "BOH BEVERAGE EQUIPMENT",
                    "SMALLWARE", "BOH DEAD FABRICATION EQUIPMENT", 
                    "OTHER ELECTRONIC & ELECTRICAL ITEMS", "OUTLET EQUIPMENT",
                    "CROCKERY", "CUTLERY", "COOKING ACCESSORIES", 
                    "CLEANING CHEMICALS - LIQUID", "CLEANING CHEMICALS - POWDERS",
                    "CLEANING CONSUMABLES - GARBAGE BAGS/GLUE PADS", 
                    "CLEANING CONSUMABLES - TOILET PAPER/TISSUE PAPER/URINAL BALLS/URINAL PADS",
                    "CLEANING TOOLS - BROOMS/PANS/BINS/WIPERS/TROLLEY/BUCKETS/BRUSHES/DISPENSER",
                    "CLEANING TOOLS - WIPES/SCRUBBERS/PADS", "STATIONARY", 
                    "CLEANING CONSUMABLES - GARBAGE BAGS/GLUE PADS/GLOVES/CAPS", "UPS STABILIZER & PROTECTION SYSTEM",
                    "KITCHEN ACCESSORIES", "PACKAGING FOR PRODUCT DEVELOPMENT", "DISPOSABLE PACKAGING - ACCESSORIES - STICKERS",
                    "Electronic & IT Equipment", "DECOR - LIVE PLANTS", "FOOD DISPLAY ACCESSORIES",
                    "DISPENSING ACCESSORIES", "DISPOSABLE PACKAGING - TRAY MATS"]

def build_simple_consumption_report(df):

    df = df.copy()
    # 1. Hide unwanted rows first
    if hide_categories and "category name" in df.columns:
        df = df[~df["category name"].isin(hide_categories)]

    df = apply_category_skip(df, "category name")

    # 2. Drop unwanted columns
    if skip_columns:
        df = df.drop(columns=[c for c in skip_columns if c.lower() in df.columns.str.lower()])


    numeric = [
        "opening qty", "purchase qty", "indent dispatch qty",
        "indent receive qty", "consumption qty",
        "yield wastage", "wastage qty", "reuse qty", "return qty"
    ]

    for col in numeric:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["stock in"] = (
        df["opening qty"] +
        df["purchase qty"] +
        df["indent dispatch qty"] +
        df["reuse qty"]
    )

    df["stock out"] = (
        df["consumption qty"] +
        df["yield wastage"] +
        df["wastage qty"] +
        df["indent receive qty"] -
        df["return qty"]
    )

    df["closing"] = df["stock in"] - df["stock out"]

    return df

# ======================================================
# LOAD ENTRY FIRST
# ======================================================
entry_df = load_entry_data(ENTRY_PATH)
opening_stock_df = load_opening_stock(OPENING_STOCK_PATH)

# ======================================================
# LOAD ITEM MASTER
# ======================================================
item_master = pd.read_csv(ITEM_MASTER_PATH)
# ================================
# DETECT UOM COLUMN FROM ITEM MASTER
# ================================
item_master.columns = item_master.columns.str.lower().str.strip()

# Normalize master names
item_master["item name"] = item_master["item name"].astype(str).str.strip()

item_master["category name"] = (
    item_master["category name"]
    .astype(str)
    .str.strip()
)

item_master["super category"] = (
    item_master["super category"]
    .astype(str)
    .str.strip()
)

item_master["tax rate"] = (
    pd.to_numeric(item_master["tax rate"], errors="coerce")
    .fillna(0)
)

# ======================================================
# MASTER MAPS
# ======================================================
ITEM_TAX_MAP = item_master.set_index("item name")["tax rate"].to_dict()
ITEM_SUPER_CAT_MAP = item_master.set_index("item name")["super category"].to_dict()
ITEM_EXPIRY_MAP = item_master.set_index("item name")["has expiry"].to_dict()

# ======================================================
# LOAD OTHER SOURCES
# ======================================================
indent_df = load_indent_or_return_data(INDENT_PATH)
bulk_df   = load_bulk_return_data(BULK_RETURN_PATH)
wastage_df = load_wastage_data(WASTAGE_PATH)
# ======================================================
# LOAD EXPIRY DATA (GLOBAL)
# ======================================================
tx_df = pd.concat([entry_df, indent_df, bulk_df, wastage_df], ignore_index=True)

# ✅ APPLY OPENING STOCK (CRITICAL)P
tx_df = apply_opening_stock(tx_df, opening_stock_df)
tx_df = ensure_qty_out_base(tx_df)


# ======================================================
# OPENING UOM MAP (GLOBAL & FINAL — ALWAYS DEFINED)
# ======================================================
OPENING_UOM_MAP = (
    opening_stock_df
    .set_index([COL_WAREHOUSE, COL_ITEM])[COL_UOM]
    .to_dict()
)

# ======================================================
# NORMALIZE ITEM / CATEGORY NAMES FIRST (CRITICAL)
# ======================================================
tx_df[COL_ITEM] = (
    tx_df[COL_ITEM]
    .astype(str)
    .str.strip()
)

tx_df[COL_CATEGORY] = (
    tx_df[COL_CATEGORY]
    .astype(str)
    .str.strip()
)

# ======================================================
# BACKFILL UNIT PRICE FROM ENTRY
# ======================================================
price_map = entry_df.groupby(COL_ITEM)[COL_UNIT_PRICE].mean()

tx_df[COL_UNIT_PRICE] = (
    tx_df[COL_UNIT_PRICE]
    .where(tx_df[COL_UNIT_PRICE] > 0, tx_df[COL_ITEM].map(price_map))
    .fillna(0)
)

tx_df[COL_UNIT_PRICE] = pd.to_numeric(tx_df[COL_UNIT_PRICE], errors="coerce").fillna(0)

# ======================================================
# APPLY ITEM MASTER TAX (FINAL – SAFE)
# ======================================================
tx_df[COL_TAX] = (
    tx_df[COL_ITEM]
    .map(ITEM_TAX_MAP)
    .fillna(0)
)

# ======================================================
# APPLY SUPER CATEGORY & EXPIRY
# ======================================================
tx_df["Super Category"] = tx_df[COL_ITEM].map(ITEM_SUPER_CAT_MAP).fillna("Unknown")
tx_df["Has Expiry"] = tx_df[COL_ITEM].map(ITEM_EXPIRY_MAP).fillna("No")

# ======================================================
# QUANTITY SAFETY
# ======================================================
tx_df["qty_in"]  = pd.to_numeric(tx_df["qty_in"], errors="coerce").fillna(0)
tx_df["qty_out"] = pd.to_numeric(tx_df["qty_out"], errors="coerce").fillna(0)
TX_DF_GLOBAL = tx_df.copy()

DISPLAY_UOM_MAP = build_display_uom_map(tx_df, opening_stock_df)

uom_conflicts = (
    tx_df.groupby([COL_WAREHOUSE, COL_ITEM])[COL_UOM]
    .nunique()
    .reset_index()
    .query(f"{COL_UOM} > 1")
)

def force_display_uom(df):
    if df.empty or COL_UOM not in df.columns:
        return df

    df = df.copy()

    df[COL_UOM] = df.apply(
        lambda r: DISPLAY_UOM_MAP.get(
            (r.get(COL_WAREHOUSE), r.get(COL_ITEM)),
            r[COL_UOM]
        ),
        axis=1
    )

    return df


def get_display_uom_map():
    return build_display_uom_map(tx_df, opening_stock_df)

# ======================================================
#  FIX: GLOBAL qty_out_base ALWAYS AVAILABLE
# ======================================================

def compute_qty_out_base(row):
    """
    Convert qty_out into base UOM using ITEM_CONV_MAP
    Purchase UOM is NEVER changed. Only consumption/wastage is converted.
    """
    item = str(row[COL_ITEM]).strip()
    uom  = str(row[COL_UOM]).strip().upper()
    qty  = float(row["qty_out"])

    key = (item, uom)

    # If item conversion exists
    if key in ITEM_CONV_MAP:
        factor = ITEM_CONV_MAP[key]["factor"]
        return qty * factor

    # Fallback logic (if no map):
    # GM is base by default
    if uom == "GM":
        return qty

    # Convert common cases
    if uom == "KG":
        return qty * 1000

    if uom == "ML":
        return qty

    if uom == "LTR":
        return qty * 1000

    # PCS/NOS fallback to 1:1
    if uom in ["PCS", "NOS"]:
        return qty

    # Otherwise return qty as-is
    return qty

# Create qty_out_base if missing
if "qty_out_base" not in tx_df.columns:
    tx_df["qty_out_base"] = tx_df.apply(compute_qty_out_base, axis=1)
else:
    # Ensure recalculation in case previous runs were wrong/incomplete
    tx_df["qty_out_base"] = tx_df.apply(compute_qty_out_base, axis=1)

#=============================================
#    SUMMARY TABLE BUILDER (CORE LOGIC)
#=============================================
def build_summary_table(df):

    df = df.copy()

    # ================= SAFETY =================
    for c in ["qty_in", "qty_out", COL_UNIT_PRICE, COL_TAX]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0)

    # ================= BASE AMOUNTS =================
    df["base_in"]  = df["qty_in"]  * df[COL_UNIT_PRICE]
    df["base_out"] = df["qty_out"] * df[COL_UNIT_PRICE]

    df["stock_in_amount"] = df["base_in"] + (df["base_in"] * df[COL_TAX] / 100)
    df["stock_out_amount"] = df["base_out"] + (df["base_out"] * df[COL_TAX] / 100)

    # ================= GROUPING (FIXED) =================
    summary = (
        df.groupby(
            [
                COL_DATE,
                COL_WAREHOUSE,
                COL_OUTLET,
                COL_CATEGORY,
                COL_ITEM,
                COL_UOM,
                COL_UNIT_PRICE,
                COL_TAX,
            ],
            as_index=False
        )
        .agg(
            **{
                "Stock In": ("qty_in", "sum"),
                "Stock Out": ("qty_out", "sum"),
                "Stock In Amount": ("stock_in_amount", "sum"),
                "Stock Out Amount": ("stock_out_amount", "sum"),
            }
        )
    )

    # ================= AVAILABLE STOCK =================
    summary["Available Stock"] = summary["Stock In"] - summary["Stock Out"]

    # ================= DATE FORMAT =================
    summary[COL_DATE] = pd.to_datetime(
        summary[COL_DATE], errors="coerce"
    ).dt.strftime("%d-%m-%Y")

    # ================= RENAME FOR UI =================
    summary.rename(
        columns={
            COL_DATE: "Date",
            COL_WAREHOUSE: "Warehouse",
            COL_OUTLET: "Outlet",
            COL_CATEGORY: "Category Name",
            COL_ITEM: "Item Name",
            COL_UOM: "Unit",
            COL_UNIT_PRICE: "Unit Price",
            COL_TAX: "Tax",
        },
        inplace=True
    )

    # ================= SUB TOTAL =================
    subtotal = {
        "Date": "TOTAL",
        "Warehouse": "",
        "Outlet": "",
        "Category Name": "",
        "Item Name": "",
        "Unit": "",
        "Unit Price": "",
        "Tax": "",
        "Stock In": summary["Stock In"].sum(),
        "Stock Out": summary["Stock Out"].sum(),
        "Stock In Amount": summary["Stock In Amount"].sum(),
        "Stock Out Amount": summary["Stock Out Amount"].sum(),
        "Available Stock": summary["Available Stock"].sum(),
    }

    summary = pd.concat(
        [summary, pd.DataFrame([subtotal])],
        ignore_index=True
    )

    return summary.round(2)

def build_outlet_grand_totals(df):

    df = df.copy()
    df = df[(df["source"] == "INDENT") & (df["qty_out"] > 0)]

    if df.empty:
        return 0, 0

    df["base"] = df["qty_out"] * df[COL_UNIT_PRICE]
    df["amount"] = df["base"] + (df["base"] * df[COL_TAX] / 100)

    total_qty = df["qty_out"].sum()
    total_amt = df["amount"].sum()

    return round(total_qty, 2), round(total_amt, 2)

def build_outlet_consumption_chart(df):

    df = df.copy()
    df = df[(df["source"] == "INDENT") & (df["qty_out"] > 0)]

    if df.empty:
        return None

    df["amount"] = (
        df["qty_out"] * df[COL_UNIT_PRICE] *
        (1 + df[COL_TAX] / 100)
    )

    s = (
        df.groupby(COL_OUTLET, as_index=False)
        .agg(Total_Amount=("amount", "sum"))
        .sort_values("Total_Amount", ascending=False)
    )

    fig = px.bar(
        s,
        x=COL_OUTLET,
        y="Total_Amount",
        text_auto=".2s",
        title="Outlet-wise Consumption Amount"
    )

    fig.update_layout(
        xaxis_title="Outlet",
        yaxis_title="Amount",
        xaxis_tickangle=-30
    )

    return fig

def build_outlet_category_pivot(df):

    df = df.copy()
    df = df[(df["source"] == "INDENT") & (df["qty_out"] > 0)]

    if df.empty:
        return pd.DataFrame()

    df["amount"] = (
        df["qty_out"] * df[COL_UNIT_PRICE] *
        (1 + df[COL_TAX] / 100)
    )

    pivot = pd.pivot_table(
        df,
        index=COL_OUTLET,
        columns=COL_CATEGORY,
        values="amount",
        aggfunc="sum",
        fill_value=0,
        margins=True,
        margins_name="TOTAL"
    )

    pivot.reset_index(inplace=True)
    
    return pivot.round(2)

def build_outlet_consumption_table(df):

    df = df.copy()

    # ================= FILTER =================
    df = df[
        (df["source"] == "INDENT") &
        (df["qty_out"] > 0)
    ]

    if df.empty:
        return pd.DataFrame()

    # ================= SAFETY =================
    for c in ["qty_out", COL_UNIT_PRICE, COL_TAX]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0)

    # ================= AMOUNT =================
    df["Base Amount"] = df["qty_out"] * df[COL_UNIT_PRICE]
    df["Tax Amount"] = df["Base Amount"] * df[COL_TAX] / 100
    df["Total Amount"] = df["Base Amount"] + df["Tax Amount"]

    # ================= FORMAT DATE =================
    df["Date"] = pd.to_datetime(df[COL_DATE], errors="coerce").dt.strftime("%d-%m-%Y")

    # ================= SELECT =================
    summary = df[
        [
            "Date",
            COL_OUTLET,
            COL_CATEGORY,
            COL_ITEM,
            COL_UOM,
            "qty_out",
            COL_UNIT_PRICE,
            COL_TAX,
            "Total Amount",
        ]
    ].rename(columns={
        COL_OUTLET: "Outlet Name",
        COL_CATEGORY: "Category Name",
        COL_ITEM: "Item Name",
        COL_UOM: "Unit",
        "qty_out": "Stock Out",
        COL_UNIT_PRICE: "Unit Price",
        COL_TAX: "Tax",
    })

    # ================= GROUP =================
    summary = (
        summary
        .groupby(
            [
                "Date",
                "Outlet Name",
                "Category Name",
                "Item Name",
                "Unit",
                "Unit Price",
                "Tax",
            ],
            as_index=False
        )
        .agg(
            **{
                "Stock Out": ("Stock Out", "sum"),
                "Total Amount": ("Total Amount", "sum"),
            }
        )
    )

    # ================= SUB TOTAL =================
    subtotal = {
        "Date": "TOTAL",
        "Outlet Name": "",
        "Category Name": "",
        "Item Name": "",
        "Unit": "",
        "Unit Price": "",
        "Tax": "",
        "Stock Out": summary["Stock Out"].sum(),
        "Total Amount": summary["Total Amount"].sum(),
    }

    summary = pd.concat(
        [summary, pd.DataFrame([subtotal])],
        ignore_index=True
    )
    summary = force_display_uom(summary)
    return summary.round(2)

def get_global_opening_qty(opening_df: pd.DataFrame) -> float:
    if opening_df is None or opening_df.empty:
        return 0.0

    # BUSINESS RULE: first row = NCR opening
    return float(opening_df.iloc[0]["opening_qty"])

# ===============================================================
# 🔽 FIXED GLOBAL LEDGER — PURCHASE UOM ONLY
# ===============================================================
def build_global_ledger(tx_df, opening_df):

    df = tx_df.copy()

    if df.empty:
        return pd.DataFrame()

    df[COL_DATE] = pd.to_datetime(df[COL_DATE], errors="coerce")
    df["qty_in"] = pd.to_numeric(df["qty_in"], errors="coerce").fillna(0)
    df["qty_out"] = pd.to_numeric(df["qty_out"], errors="coerce").fillna(0)

    # Identify item & purchase UOM
    item = normalize_item_name(df[COL_ITEM].iloc[0])
    purchase_uom = None

    # detect purchase uom from master
    for (it, u), v in ITEM_CONV_MAP.items():
        if it == item:
            purchase_uom = u
            break

    if purchase_uom is None:
        purchase_uom = normalize_uom(df[COL_UOM].iloc[0])

    # Opening qty ALWAYS in purchase UOM
    opening_qty = (
        opening_df[
            (opening_df[COL_ITEM] == item)
            & (opening_df[COL_UOM] == purchase_uom)
        ]["opening_qty"]
        .sum()
        if not opening_df.empty
        else 0
    )

    # remove opening rows
    df = df[df["source"] != "OPENING"]

    # Convert qty_out to purchase uom only
    df["Stock_Out"] = df.apply(
        lambda r: convert_indent_to_purchase(
            r[COL_ITEM],
            r["qty_out"],
            r[COL_UOM],
        ),
        axis=1,
    )

    # group by date
    daily = (
        df.groupby(COL_DATE, as_index=False)
        .agg(
            Stock_In=("qty_in", "sum"),
            Stock_Out=("Stock_Out", "sum"),
            Warehouse=(COL_WAREHOUSE, "first"),
        )
        .sort_values(COL_DATE)
    )

    # running closing
    running = opening_qty
    closing_list = []

    for _, r in daily.iterrows():
        running += r["Stock_In"] - r["Stock_Out"]
        closing_list.append(round(running, 3))

    daily["Closing Stock"] = closing_list
    daily["Item"] = item
    daily["UOM"] = purchase_uom
    daily["Date"] = daily[COL_DATE].dt.strftime("%d-%m-%Y")

    return daily[
        [
            "Date",
            "Warehouse",
            "Item",
            "UOM",
            "Stock_In",
            "Stock_Out",
            "Closing Stock",
        ]
    ]
                
def find_item_column(cols):
    # 1️⃣ Exact "item name" first
    for c in cols:
        if c.strip() == "item name":
            return c
    # 2️⃣ Contains "item name"
    for c in cols:
        if "item name" in c:
            return c
    # 3️⃣ LAST fallback: generic item
    for c in cols:
        if "item" in c:
            return c
    return None

def load_expiry_data(path):

    rows = []

    files = glob.glob(os.path.join(path, "*.csv"))
    for f in files:

        # ==================================================
        # 1️⃣ FIND HEADER ROW
        # ==================================================
        header_row = None

        with open(f, encoding="utf-8", errors="ignore") as fh:
            for i, line in enumerate(fh):
                l = line.lower()
                if ("item" in l) and ("expiry" in l) and ("qty" in l):
                    header_row = i
                    break

        if header_row is None:
            continue

        # ==================================================
        # 2️⃣ READ CSV USING CORRECT HEADER
        # ==================================================
        df = pd.read_csv(
            f,
            header=header_row,
            engine="python",
            on_bad_lines="skip"
        )

        # ==================================================
        # 3️⃣ NORMALIZE COLUMNS
        # ==================================================
        df.columns = (
            df.columns
            .astype(str)
            .str.lower()
            .str.replace(".", "", regex=False)
            .str.replace("_", " ", regex=False)
            .str.strip()
        )

        cols = df.columns.tolist()

        def find_any(*keys):
            for c in cols:
                if any(k in c for k in keys):
                    return c
            return None

        c_entry = find_any("entry")
        c_txn   = find_any("transaction")
        c_item = find_item_column(cols)
        c_cat   = find_any("category")
        c_uom   = find_any("unit")
        c_qty   = find_any("qty", "quantity")
        c_exp   = find_any("expiry")

        if not c_item or not c_exp or not c_qty:
            continue

        # ==================================================
        # 4️⃣ BUILD CLEAN DATAFRAME
        # ==================================================
        c_wh = None
        for c in cols:
            if "warehouse" in c:
                c_wh = c
                break

        temp = pd.DataFrame({
            "Entry No": df[c_entry].astype(str).str.strip() if c_entry else os.path.basename(f),
            "Transaction Date": pd.to_datetime(
                df[c_txn], dayfirst=True, errors="coerce"
            ) if c_txn else pd.NaT,
            "Warehouse": (
                df[c_wh].astype(str).str.strip()
                if "warehouse" in df.columns
                else "UNKNOWN"
            ),
            "Category Name": df[c_cat].astype(str).str.strip() if c_cat else "Unknown",
            "Item Name": df[c_item].astype(str).str.strip(),
            "Unit": df[c_uom].astype(str).str.strip().str.upper() if c_uom else "UNKNOWN",
            "Qty": pd.to_numeric(df[c_qty], errors="coerce").fillna(0),
            "Expiry Date": pd.to_datetime(
                df[c_exp], dayfirst=True, errors="coerce"
            ),
        })

        # ==================================================
        # 5️⃣ FILTER VALID ROWS
        # ==================================================
        temp = temp[
            temp["Expiry Date"].notna() &
            (temp["Qty"] > 0)
        ]

        rows.append(temp)

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

def get_available_stock_map(tx_df):
    """
    Returns map: Item → Available Stock
    Stock math uses qty_out_base (base UOM)
    UOM is NOT part of stock identity
    """

    df = tx_df.copy()

    # ================= SAFETY =================
    if "qty_out_base" not in df.columns:
        df["qty_out_base"] = df.get("qty_out", 0)

    if "qty_in" not in df.columns:
        df["qty_in"] = 0

    # ================= GROUP BY ITEM ONLY =================
    grp = (
        df
        .groupby("Item", dropna=False)
        .agg(
            qty_in_sum=("qty_in", "sum"),
            qty_out_sum=("qty_out_base", "sum"),
        )
        .reset_index()
    )

    grp["available_stock"] = grp["qty_in_sum"] - grp["qty_out_sum"]

    # ================= ITEM → STOCK MAP =================
    return dict(zip(grp["Item"], grp["available_stock"]))


expiry_df = load_expiry_data(EXPIRY_PATH)

if not expiry_df.empty:
    expiry_df["Unit"] = expiry_df["Unit"].apply(normalize_uom)
    
def build_expiry_summary(expiry_df, tx_df):

    if expiry_df.empty:
        return pd.DataFrame()

    today = pd.Timestamp.today().normalize()
    df = expiry_df.copy()

    df["Days Left"] = (df["Expiry Date"] - today).dt.days

    def bucket(days):
        if days < 0:
            return "Expired"
        if days <= 15:
            return "Critical (≤15)"
        if days <= 30:
            return "Major (≤30)"
        if days <= 60:
            return "Minor (≤60)"
        return "OK"

    df["Status"] = df["Days Left"].apply(bucket)

    # ================= AVAILABLE STOCK =================
    stock_map = get_available_stock_map(tx_df)
    df["Available Stock"] = df["Item Name"].astype(str).str.strip().map(
        stock_map
    ).fillna(0)


    df = df[df["Qty"] > 0]
    if df.empty:
        return pd.DataFrame()

    # ================= DATE FORMAT =================
    df["Expiry Date"] = pd.to_datetime(df["Expiry Date"]).dt.strftime("%d-%m-%Y")
    df["Transaction Date"] = pd.to_datetime(df["Transaction Date"]).dt.strftime("%d-%m-%Y")

    return (
        df[
            [
                "Entry No",
                "Transaction Date",
                "Warehouse",
                "Category Name",
                "Item Name",
                "Unit",
                "Qty",
                "Available Stock",
                "Expiry Date",
                "Days Left",
                "Status",
            ]
        ]
        .sort_values("Days Left")
        .reset_index(drop=True)
    )

def apply_expiry_deduction(tx_df, expiry_df):
    """
    Deduct ONLY expired quantity
    FIFO-safe, GLOBAL stock (no warehouse split)
    """

    if expiry_df is None or expiry_df.empty:
        return tx_df

    today = pd.Timestamp.today().normalize()

    expired = expiry_df[
        expiry_df["Expiry Date"] < today
    ].copy()

    if expired.empty:
        return tx_df

    # ==============================
    # 🔑 GLOBAL AVAILABLE STOCK MAP
    # ==============================
    stock_map = get_available_stock_map(tx_df)   # (Item, UOM) → qty
    
    adjustments = []

    # FIFO: earliest expiry first
    for _, r in expired.sort_values("Expiry Date").iterrows():

        item = r["Item Name"].strip()
        uom  = normalize_uom(r["Unit"])
        key = (item, uom)

        available = stock_map.get(key, 0)

        if available <= 0:
            continue

        deduct_qty = min(float(r["Qty"]), float(available))

        adjustments.append({
            COL_DATE: today,
            COL_WAREHOUSE: r.get("Warehouse", "GLOBAL"),
            COL_OUTLET: "EXPIRY_FIFO",
            COL_ITEM: item,
            COL_CATEGORY: "Expiry Loss",
            COL_UOM: uom,
            COL_UNIT_PRICE: 0,
            COL_TAX: 0,
            "qty_in": 0,
            "qty_out": deduct_qty,
            "qty_out_base": convert_uom_for_consumption(item, deduct_qty, uom),
            "source": "EXPIRY_FIFO",
        })

        # 🔁 Reduce available stock
        stock_map[key] = available - deduct_qty

    if not adjustments:
        return tx_df

    return pd.concat([tx_df, pd.DataFrame(adjustments)], ignore_index=True)

tx_df = apply_expiry_deduction(tx_df, expiry_df)
expiry_summary = build_expiry_summary(expiry_df, tx_df)

# ===============================================
# FIX: Ensure qty_out_base exists before grouping
# ===============================================
if "qty_out_base" not in tx_df.columns:
    tx_df["qty_out_base"] = tx_df.get("qty_out", 0)

def build_expiry_alerts(expiry_df, tx_df, days=60):

    if expiry_df is None or expiry_df.empty:
        return pd.DataFrame()

    today = pd.Timestamp.today().normalize()
    df = expiry_df.copy()

    # ================= DATE & DAYS LEFT =================
    df["Expiry Date"] = pd.to_datetime(df["Expiry Date"], errors="coerce")
    df["Days Left"] = (df["Expiry Date"] - today).dt.days

    # ================= SKIP CATEGORIES =================
    df = apply_category_skip(df, "Category Name")

    # ================= VALID WINDOW (-10 to days) =================
    df = df[
        (df["Days Left"] >= -10) &
        (df["Days Left"] <= days)
    ]

    if df.empty:
        return pd.DataFrame()

    # ================= AVAILABLE STOCK =================
    stock_map = get_available_stock_map(tx_df)
    df["Available Stock"] = df["Item Name"].astype(str).str.strip().map(
        stock_map
    ).fillna(0)

    # ================= ALERT LEVEL =================
    def expiry_alert_level(d):
        if d <= 15:
            return "🔴 Critical (≤15 Days)"
        if d <= 30:
            return "🟠 Major (≤30 Days)"
        if d <= 60:
            return "🟡 Minor (≤60 Days)"
        return "OK"

    df["Status"] = df["Days Left"].apply(expiry_alert_level)
    df["Alert Level"] = df["Status"]  # 🔑 UI compatibility

    # ================= GROUP =================
    df = (
        df.groupby(
            [
                "Warehouse",
                "Category Name",
                "Item Name",
                "Unit",
                "Days Left",
                "Status",
            ],
            as_index=False
        )
        .agg(
            Qty=("Qty", "sum"),
            Available_Stock=("Available Stock", "max"),
            Expiry_Date=("Expiry Date", "min"),
        )
    )

    # ================= FORMAT =================
    df["Expiry Date"] = df["Expiry_Date"].dt.strftime("%d-%m-%Y")
    df.drop(columns=["Expiry_Date"], inplace=True)

    return df.sort_values("Days Left").reset_index(drop=True)

    
def build_expiry_aging_chart(expiry_df):

    if expiry_df is None or expiry_df.empty:
        return px.bar(title="No Expiry Data")

    # ✅ FIRST define df
    df = expiry_df.copy()

    # ✅ THEN apply category skip
    df = apply_category_skip(df, "Category Name")

    today = pd.Timestamp.today().normalize()

    df["Days Left"] = (df["Expiry Date"] - today).dt.days

    # keep only valid future / recent expiry
    df = df[df["Days Left"] >= 0]

    if df.empty:
        return px.bar(title="No Expiry Data")

    df["Expiry Bucket"] = pd.cut(
        df["Days Left"],
        bins=[-1, 10, 30, 60, 10_000],
        labels=["0–10", "11–30", "31–60", "60+"]
    )

    summary = (
        df.groupby("Expiry Bucket", observed=True)
        .agg(Items=("Item Name", "count"))
        .reset_index()
    )

    fig = px.bar(
        summary,
        x="Expiry Bucket",
        y="Items",
        title="📊 Expiry Aging Summary",
        text="Items"
    )

    return fig


def export_full_stock_excel(tx_df, opening_stock_df, sd=None, ed=None):

    df = apply_filters(tx_df, None, None, None, sd, ed)
    if df.empty:
        return None

    ledger_rows = []
    month_rows = []
    open_close_rows = []

    for item in sorted(df[COL_ITEM].dropna().unique()):

        item_df = df[df[COL_ITEM] == item]
        ledger = build_global_ledger(item_df, opening_stock_df)

        if ledger.empty:
            continue

        ledger = ledger.sort_values("Date")

        # =========================
        # 1️⃣ FULL LEDGER
        # =========================
        ledger_rows.append(ledger)

        # =========================
        # 2️⃣ MONTH END CLOSING
        # =========================
        tmp = ledger.copy()
        tmp["Month"] = pd.to_datetime(
            tmp["Date"], format="%d-%m-%Y"
        ).dt.to_period("M").astype(str)

        month_end = (
            tmp.groupby("Month", as_index=False)
            .last()
        )

        for _, r in month_end.iterrows():
            month_rows.append({
                "Item": r["Item"],
                "UOM": r["UOM"],
                "Month": r["Month"],
                "Closing Stock": round(r["Closing Stock"], 2),
            })

        # =========================
        # 3️⃣ OPENING VS CLOSING
        # =========================
        open_close_rows.append({
            "Item": ledger.iloc[0]["Item"],
            "UOM": ledger.iloc[0]["UOM"],
            "Opening Stock": round(ledger.iloc[0]["Closing Stock"], 2),
            "Closing Stock": round(ledger.iloc[-1]["Closing Stock"], 2),
            "Net Change": round(
                ledger.iloc[-1]["Closing Stock"]
                - ledger.iloc[0]["Closing Stock"], 2
            ),
        })

    # =========================
    # WRITE EXCEL
    # =========================
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:

        pd.concat(ledger_rows, ignore_index=True).to_excel(
            writer, sheet_name="Ledger_All_Items", index=False
        )

        pd.DataFrame(month_rows).to_excel(
            writer, sheet_name="Month_End_Closing", index=False
        )

        pd.DataFrame(open_close_rows).to_excel(
            writer, sheet_name="Opening_vs_Closing", index=False
        )

    out.seek(0)
    return out

# ==============================================================
# 🔥 FIXED AGING (NO BASE_UOM, NO qty_out_base)
# ==============================================================

def build_global_aging(tx_df, opening_df, as_on_date):

    df = tx_df.copy()
    df[COL_DATE] = pd.to_datetime(df[COL_DATE], errors="coerce")

    as_on = pd.to_datetime(as_on_date, errors="coerce")
    if pd.isna(as_on):
        as_on = pd.Timestamp.today().normalize()

    # Normalize
    df[COL_ITEM] = df[COL_ITEM].apply(normalize_item_name)
    df[COL_UOM] = df[COL_UOM].astype(str).str.strip().str.upper()


    # Build unique items
    items = df[[COL_ITEM]].drop_duplicates()

    rows = []

    for item in items[COL_ITEM]:

        # get purchase uom
        puom = None
        for (it, u), v in ITEM_CONV_MAP.items():
            if it == item:
                puom = u
                break

        if puom is None:
            continue

        # opening quantity (purchase uom)
        opening_qty = (
            opening_df[
                (opening_df[COL_ITEM] == item)
                & (opening_df[COL_UOM] == puom)
            ]["opening_qty"]
            .sum()
            if not opening_df.empty
            else 0
        )

        # transactions
        x = df[df[COL_ITEM] == item].copy()
        x = x[x["source"] != "OPENING"]

        x["Stock_Out"] = x.apply(
            lambda r: convert_indent_to_purchase(
                r[COL_ITEM], r["qty_out"], r[COL_UOM]
            ),
            axis=1,
        )

        stock_in = x["qty_in"].sum()
        stock_out = x["Stock_Out"].sum()

        closing = opening_qty + stock_in - stock_out

        last_move = (
            x[COL_DATE].max()
            if not x.empty
            else opening_stock_df["date"].min()
        )

        age_days = (as_on - last_move).days

        rows.append(
            {
                "Item": item,
                "UOM": puom,
                "Closing Stock": round(closing, 3),
                "Last Movement Date": last_move.strftime("%d-%m-%Y"),
                "Age Days": age_days,
                "Aging Bucket": (
                    "0–30"
                    if age_days <= 30
                    else "31–60"
                    if age_days <= 60
                    else "60+"
                ),
            }
        )
    return pd.DataFrame(rows).sort_values("Age Days", ascending=False)


def build_aging_bucket_chart(tx_df, opening_df, as_on_date):

    aging_df = build_global_aging(tx_df, opening_df, as_on_date)

    if aging_df.empty:
        return px.bar(title="No Aging Data")

    bucket_order = ["0–30", "31–60", "60+"]

    fig = px.bar(
        aging_df.groupby("Aging Bucket", as_index=False, observed=False)
                .agg(Items=(COL_ITEM, "nunique")),
        x="Aging Bucket",
        y="Items",
        category_orders={"Aging Bucket": bucket_order},
        title="📊 Aging Bucket Summary"
    )

    return fig

# ================= ORDER PLANNING CONFIG =================
LEAD_TIME_DAYS = 30          # supplier lead time
MIN_DAYS_HISTORY = 30       # consumption lookback
CRITICAL_DAYS = 10           # <= 3 days stock = critical
MIN_ORDER_QTY = 1
MAX_ORDER_DAYS = 15   # do not order more than 15 days cover
# ================= MOQ / PACK CONFIG =================
DEFAULT_PACK_SIZE = 1
DEFAULT_MOQ = 1

ITEM_MOQ_MAP = (
    item_master.set_index("item name")["moq"].to_dict()
    if "moq" in item_master.columns else {}
)

ITEM_PACK_MAP = (
    item_master.set_index("item name")["pack size"].to_dict()
    if "pack size" in item_master.columns else {}
)
import math

def apply_moq_pack(qty, pack_size, moq):
    if qty <= 0:
        return 0

    pack_size = max(pack_size, 1)
    moq = max(moq, 1)

    rounded = math.ceil(qty / pack_size) * pack_size
    return max(rounded, moq)

def build_order_planning(tx_df, as_on_date=None):

    if tx_df is None or tx_df.empty:
        return pd.DataFrame()

    df = tx_df.copy()
    df[COL_DATE] = pd.to_datetime(df[COL_DATE], errors="coerce")

    as_on_date = (
        pd.to_datetime(as_on_date)
        if as_on_date else pd.Timestamp.today().normalize()
    )

    lookback_start = as_on_date - pd.Timedelta(days=MIN_DAYS_HISTORY)

    # =====================================================
    # 1️⃣ CONSUMPTION (WAREHOUSE × ITEM) — BASE UOM ONLY
    # =====================================================
    cons = (
        df[
            (df["source"] == "INDENT") &
            (df["qty_out"] > 0) &
            (df[COL_DATE] >= lookback_start)
        ]
        .groupby([COL_WAREHOUSE, COL_ITEM], as_index=False)
        .agg(
            Total_Consumed=("qty_out_base", "sum"),
            Active_Days=(COL_DATE, "nunique")
        )
    )

    cons["Daily Consumption"] = (
        cons["Total_Consumed"] /
        cons["Active_Days"].replace(0, 1)
    )

    # =====================================================
    # 2️⃣ AVAILABLE STOCK — BASE UOM ONLY
    # =====================================================
    stock = (
        df.groupby([COL_WAREHOUSE, COL_ITEM], as_index=False)
        .agg(
            Stock_In=("qty_in", "sum"),
            Stock_Out=("qty_out_base", "sum"),
            Unit_Price=(COL_UNIT_PRICE, "mean"),
            Tax=(COL_TAX, "max"),
        )
    )

    stock["Available Stock"] = stock["Stock_In"] - stock["Stock_Out"]

    # =====================================================
    # 3️⃣ MERGE (❗ FIXED — NO UOM HERE)
    # =====================================================
    plan = cons.merge(
        stock,
        on=[COL_WAREHOUSE, COL_ITEM],
        how="left"
    )

    # ================= SAFETY DEFAULTS =================
    plan["Available Stock"] = plan["Available Stock"].fillna(0)
    plan["Daily Consumption"] = plan["Daily Consumption"].fillna(0)
    plan["Unit_Price"] = plan["Unit_Price"].fillna(0)
    plan["Tax"] = plan["Tax"].fillna(0)

    # =====================================================
    # 4️⃣ REORDER LOGIC
    # =====================================================
    zero_cons = plan["Daily Consumption"] <= 0

    plan.loc[zero_cons, "Days Left"] = float("inf")
    plan.loc[~zero_cons, "Days Left"] = (
        plan.loc[~zero_cons, "Available Stock"] /
        plan.loc[~zero_cons, "Daily Consumption"]
    )

    max_reorder = plan["Daily Consumption"] * MAX_ORDER_DAYS

    plan["Reorder Point"] = (
        plan["Daily Consumption"] * LEAD_TIME_DAYS
    ).clip(upper=max_reorder)

    raw_qty = pd.Series(0.0, index=plan.index)
    raw_qty.loc[~zero_cons] = (
        plan.loc[~zero_cons, "Reorder Point"]
        - plan.loc[~zero_cons, "Available Stock"]
    ).clip(lower=0)

    # =====================================================
    # 5️⃣ MOQ / PACK ROUNDING
    # =====================================================
    plan["Pack Size"] = plan[COL_ITEM].map(ITEM_PACK_MAP).fillna(DEFAULT_PACK_SIZE)
    plan["MOQ"] = plan[COL_ITEM].map(ITEM_MOQ_MAP).fillna(DEFAULT_MOQ)

    plan["Suggested Order Qty"] = plan.apply(
        lambda r: apply_moq_pack(
            raw_qty.loc[r.name],
            r["Pack Size"],
            r["MOQ"]
        ),
        axis=1
    )

    plan = plan[plan["Suggested Order Qty"] >= MIN_ORDER_QTY]

    # =====================================================
    # 🔑 DISPLAY UOM (ENTRY → INDENT → OPENING)
    # =====================================================
    display_uom_map = get_display_uom_map()

    plan["Unit"] = plan.apply(
        lambda r: display_uom_map.get(
            (r[COL_WAREHOUSE], r[COL_ITEM]),
            None
        ),
        axis=1
    )
    plan = plan[plan["Unit"].notna()]
    # =====================================================
    # 6️⃣ URGENCY
    # =====================================================
    def urgency(d):
        if not np.isfinite(d):
            return "OK"
        if d <= CRITICAL_DAYS:
            return "CRITICAL"
        if d <= LEAD_TIME_DAYS:
            return "WARNING"
        return "OK"

    plan["Urgency"] = plan["Days Left"].apply(urgency)

    # =====================================================
    # 7️⃣ ORDER VALUE
    # =====================================================
    plan["Order Value"] = (
        plan["Suggested Order Qty"]
        * plan["Unit_Price"]
        * (1 + plan["Tax"] / 100)
    )

    # =====================================================
    # 8️⃣ FINAL TABLE
    # =====================================================
    final = plan.rename(columns={
        COL_WAREHOUSE: "Warehouse",
        COL_ITEM: "Item Name",
    })[
        [
            "Warehouse",
            "Item Name",
            "Unit",
            "Daily Consumption",
            "Available Stock",
            "Days Left",
            "Reorder Point",
            "Suggested Order Qty",
            "Order Value",
            "Urgency",
            "Unit_Price",
            "Tax",
        ]
    ].round(2)

    return final.sort_values(
        ["Urgency", "Order Value"],
        ascending=[True, False]
    )


def export_purchase_order_excel(plan_df: pd.DataFrame):

    if plan_df is None or plan_df.empty:
        return None

    out = io.BytesIO()

    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:

        for wh, g in plan_df.groupby("Warehouse"):

            po = g.copy()

            po["Order Qty"] = po["Suggested Order Qty"]

            po = po[
                [
                    "Warehouse",
                    "Item Name",
                    "Unit",
                    "Order Qty",
                    "Unit_Price",
                    "Tax",
                    "Order Value",
                ]
            ].rename(columns={
                "Unit_Price": "Unit Price",
                "Tax": "Tax %",
            })

            po.to_excel(
                writer,
                sheet_name=str(wh)[:31],   # Excel sheet name limit
                index=False
            )

    out.seek(0)
    return out

# ======================================================
# UI
# ======================================================
def get_layout():
    return dbc.Container([

        dcc.Interval(
            id="auto_reload",
            interval=600 * 1000,
            n_intervals=0
        ),

        html.H3("🏬 Warehouse Inventory Dashboard"),

        # ================= FILTERS =================
        dbc.Card(
            dbc.CardBody(
                dbc.Row([
                    dbc.Col(
                    dcc.Dropdown(
                        id="f_warehouse",
                        options=dropdown_options(tx_df[COL_WAREHOUSE]),
                        placeholder="Select Warehouse",
                        multi=True
                    ),
                    md=3
                ),
                    dbc.Col(dcc.Dropdown(id="f_supercat", placeholder="Super Category")),
                    dbc.Col(dcc.Dropdown(id="f_cat", placeholder="Category")),
                    dbc.Col(dcc.Dropdown(id="f_item", placeholder="Item")),
                ], className="g-2")
            )
        ),

        # ================= EXPORT =================
        dbc.Button(
            "📤 Export Full Stock Ledger",
            id="export_full_stock_btn",
            color="dark",
            className="mb-2"
        ),
        dcc.Download(id="download_full_stock_excel"),

        # ================= DATE RANGE =================
        dcc.DatePickerRange(
            id="f_date",
            start_date=min(tx_df[COL_DATE].min(), OPENING_STOCK_DATE),
            end_date=tx_df[COL_DATE].max(),
            display_format="DD-MM-YYYY"
        ),

        html.Hr(),

        # ================= TABS =================
        dcc.Tabs([

            dcc.Tab(label="Summary", children=html.Div(id="summary")),

            dcc.Tab(label="Ledger", children=html.Div(id="ledger")),

            # ✅ AGING TAB (CORRECT PLACE)
            dcc.Tab(
                label="Aging",
                children=html.Div([

                    # 🔑 REQUIRED FOR DRILL-DOWN
                    dcc.Store(id="selected_aging_bucket"),

                    dcc.Graph(
                        id="aging_bucket_chart"
                    ),

                    html.Hr(),

                    dash_table.DataTable(
                        id="aging_table",
                        columns=[
                            {"name": "Item", "id": "Item"},
                            {"name": "UOM", "id": "UOM"},
                            {"name": "Last Movement Date", "id": "Last Movement Date"},
                            {"name": "Age Days", "id": "Age Days"},
                            {"name": "Aging Bucket", "id": "Aging Bucket"},
                            {"name": "Closing Stock", "id": "Closing Stock"},
                        ],

                        page_size=15,
                        sort_action="native",
                        filter_action="native",
                        style_header={"fontWeight": "bold"},
                        style_cell={"fontSize": "13px", "padding": "6px"},
                    )
                ])
            ),

            dcc.Tab(label="Outlet", children=html.Div(id="outlet")),
            dcc.Tab(
                label="Alerts",
                children=html.Div([

                    # ================= EXISTING ALERTS =================
                    dcc.Store(id="selected_expiry_bucket"),
                    html.Div(id="alerts"),

                    html.Hr(),

                    # ================= ORDER PLANNING =================
                    html.H5("📦 Item Ordering Planning (Warehouse-wise)"),

                    dash_table.DataTable(
                        id="order_planning_table",

                        columns=[
                            {"name": "Warehouse", "id": "Warehouse"},
                            {"name": "Item Name", "id": "Item Name"},
                            {"name": "Unit", "id": "Unit"},
                            {
                                "name": "Daily Consumption",
                                "id": "Daily Consumption",
                                "type": "numeric",
                                "format": Format(precision=2, scheme=Scheme.fixed),
                            },
                            {
                                "name": "Available Stock",
                                "id": "Available Stock",
                                "type": "numeric",
                                "format": Format(precision=2, scheme=Scheme.fixed),
                            },
                            {
                                "name": "Days Left",
                                "id": "Days Left",
                                "type": "numeric",
                                "format": Format(precision=1, scheme=Scheme.fixed),
                            },
                            {
                                "name": "Reorder Point",
                                "id": "Reorder Point",
                                "type": "numeric",
                                "format": Format(precision=2, scheme=Scheme.fixed),
                            },
                            {
                                "name": "Suggested Order Qty",
                                "id": "Suggested Order Qty",
                                "type": "numeric",
                                "format": Format(
                                    precision=0,
                                    scheme=Scheme.fixed,
                                    group=True
                                ),
                            },
                            {
                                "name": "Order Value (₹)",
                                "id": "Order Value",
                                "type": "numeric",
                                "format": Format(precision=2, scheme=Scheme.fixed),
                            },
                            {"name": "Urgency", "id": "Urgency"},
                        ],

                        page_size=12,
                        sort_action="native",
                        filter_action="native",

                        style_header={"fontWeight": "bold"},
                        style_cell={"fontSize": "13px", "padding": "6px"},

                        style_data_conditional=[
                            {
                                "if": {"filter_query": '{Urgency} = "CRITICAL"'},
                                "backgroundColor": "#f8d7da",
                                "fontWeight": "bold",
                            },
                            {
                                "if": {"filter_query": '{Urgency} = "WARNING"'},
                                "backgroundColor": "#fff3cd",
                            },
                        ],
                    ),

                    dbc.Button(
                        "📤 Download Purchase Order (Excel)",
                        id="export_po_btn",
                        color="success",
                        className="mt-2"
                    ),

                    dcc.Download(id="download_po_excel"),

                ])
            ),
            dcc.Tab(
                label="Wastage",
                children=html.Div(id="wastage_tab")
            ),
            dcc.Tab(label="Consumption", children=html.Div(id="consumption_tab")),
            ])
    ], fluid=True)

def apply_filters(df, w=None, c=None, i=None, sd=None, ed=None):

    df = df.copy()
    df[COL_DATE] = pd.to_datetime(df[COL_DATE], errors="coerce")

    if w:
        df = df[df[COL_WAREHOUSE] == w]

    if c:
        df = df[df[COL_CATEGORY].astype(str).str.strip() == str(c).strip()]

    if i:
        df = df[df[COL_ITEM] == i]

    if sd:
        sd = pd.to_datetime(sd, errors="coerce")
        df = df[df[COL_DATE] >= sd]

    if ed:
        ed = pd.to_datetime(ed, errors="coerce")
        df = df[df[COL_DATE] <= ed]

    return df

def build_negative_summary(df):

    s = (
        df.groupby([COL_CATEGORY, COL_ITEM, COL_UOM], as_index=False)
        .agg(
            stock_in=("qty_in", "sum"),
            stock_out=("qty_out", "sum"),
        )
    )

    s["Negative Qty"] = s["stock_in"] - s["stock_out"]
    s = s[s["Negative Qty"] < 0]

    return s[
        [COL_CATEGORY, COL_ITEM, COL_UOM, "Negative Qty"]
    ].round(2)

# ======================================================
# CALLBACKS
# ======================================================
def register_callbacks(app):

    # ========== CASCADE DROPDOWNS ==========
    @app.callback(
        Output("f_supercat", "options"),
        Output("f_cat", "options"),
        Output("f_item", "options"),
        Input("f_supercat", "value"),
        Input("f_cat", "value"),
        Input("f_item", "value"),
        Input("f_warehouse", "value"),
        Input("auto_reload", "n_intervals"),
    )
    def cascade_filters(sc, cat, item, wh, _):

        #global tx_df
        #tx_df = reload_all_data()   # ✅ THIS LINE WAS MISSING

        df = reload_all_data()

        if wh:
            df = df[df[COL_WAREHOUSE] == wh]
        if sc:
            df = df[df["Super Category"] == sc]
        if cat:
            df = df[df[COL_CATEGORY] == cat]
        if item:
            df = df[df[COL_ITEM] == item]

        return (
            dropdown_options(df["Super Category"]),
            dropdown_options(df[COL_CATEGORY]),
            dropdown_options(df[COL_ITEM]),
        )

    # ========== SUMMARY ==========
    @app.callback(
        Output("summary", "children"),
        Input("f_warehouse", "value"),
        Input("f_cat", "value"),
        Input("f_item", "value"),
        Input("f_date", "start_date"),
        Input("f_date", "end_date"),
        Input("auto_reload", "n_intervals"),
    )
    def summary(w, c, i, sd, ed, n_intervals):

        ctx = callback_context
        trigger = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else None

        # Allow first page load (no trigger yet)
        if trigger is None:
            trigger = "initial_load"

        valid_triggers = [
            "initial_load",
            "f_warehouse",
            "f_cat",
            "f_item",
            "f_date",
            "auto_reload"
        ]

        # If trigger is not in valid list, ignore update
        if trigger not in valid_triggers:
            raise PreventUpdate

        df = apply_filters(tx_df, w, c, i, sd, ed)
        if df.empty:
            return dbc.Alert("No data available", color="warning")

        # KPI calculations
        stock_in = df["qty_in"].sum()
        stock_out = df["qty_out"].sum()
        stock_bal = stock_in - stock_out

        df["_available_qty"] = df["qty_in"] - df["qty_out"]
        df["_available_value"] = (
            df["_available_qty"]
            * df[COL_UNIT_PRICE]
            * (1 + df[COL_TAX] / 100)
        )

        available_stock_value = df["_available_value"].sum()

        kpi_row = dbc.Row([
            dbc.Col(dbc.Alert(f"📥 Stock In: {stock_in:,.2f}", color="success",
                            className="text-center fw-bold"), md=3),
            dbc.Col(dbc.Alert(f"📤 Stock Out: {stock_out:,.2f}", color="danger",
                            className="text-center fw-bold"), md=3),
            dbc.Col(dbc.Alert(f"📦 Available Qty: {stock_bal:,.2f}", color="info",
                            className="text-center fw-bold"), md=3),
            dbc.Col(dbc.Alert(f"💰 Available Stock Value: ₹ {available_stock_value:,.2f}",
                            color="primary", className="text-center fw-bold"), md=3)
        ], className="mb-3")

        summary_df = build_summary_table(df)

        table = dash_table.DataTable(
            data=summary_df.to_dict("records"),
            columns=[
                {"name": c, "id": c, "type": "numeric",
                "format": Format(precision=2, scheme=Scheme.fixed)}
                if summary_df[c].dtype != "object"
                else {"name": c, "id": c}
                for c in summary_df.columns
            ],
            page_size=15,
            sort_action="native",
            filter_action="native",
            style_table={"overflowX": "auto"},
            style_cell={
                "textAlign": "left",
                "padding": "6px",
                "fontSize": "13px",
                "whiteSpace": "normal",
                "height": "auto"
            },
            style_header={
                "backgroundColor": "#f8f9fa",
                "fontWeight": "bold",
                "border": "1px solid #dee2e6"
            },
            style_data_conditional=[
                {"if": {"filter_query": "{Available Stock} < 0"},
                "backgroundColor": "#f8d7da", "color": "black"}
            ],
        )

        return dbc.Container([kpi_row, html.Hr(), table], fluid=True)

    @app.callback(
        Output("selected_aging_bucket", "data"),
        Input("aging_bucket_chart", "clickData"),
    )
    def set_bucket(clickData):
        if not clickData:
            return None
        return clickData["points"][0]["x"]
    @app.callback(
        Output("aging_bucket_chart", "figure"),
        Input("f_date", "end_date"),
        Input("auto_reload", "n_intervals"),
    )
    def update_aging_chart(ed, _):

        if not ed:
            raise PreventUpdate

        return build_aging_bucket_chart(
            TX_DF_GLOBAL,        # ✅ NOT tx_df
            opening_stock_df,
            ed
        )

    # ========== AGING ==========
    @app.callback(
        Output("aging_table", "data"),
        Input("f_cat", "value"),
        Input("f_item", "value"),
        Input("f_date", "end_date"),
        Input("selected_aging_bucket", "data"),  # ✅ MISSING INPUT
        Input("auto_reload", "n_intervals"),
    )
    def update_aging_table(c, i, ed, bucket, _):

        if not ed:
            raise PreventUpdate

        # =============================
        # 1️⃣ BUILD AGING (GLOBAL ONLY)
        # =============================
        aging_df = build_global_aging(
            TX_DF_GLOBAL,        # 🔒 always global
            opening_stock_df,
            ed
        )

        if aging_df.empty:
            return []

        # =============================
        # 2️⃣ AGING BUCKET FILTER (🔥 FIX)
        # =============================
        if bucket:
            aging_df = aging_df[aging_df["Aging Bucket"] == bucket]

        # =============================
        # 3️⃣ CATEGORY FILTER
        # =============================
        if c:
            valid_items = (
                TX_DF_GLOBAL.loc[
                    TX_DF_GLOBAL[COL_CATEGORY] == c,
                    COL_ITEM
                ]
                .dropna()
                .unique()
            )
            aging_df = aging_df[aging_df["Item"].isin(valid_items)]

        # =============================
        # 4️⃣ ITEM FILTER
        # =============================
        if i:
            aging_df = aging_df[aging_df["Item"] == i]

        return aging_df.to_dict("records")

    # ========== OUTLET ==========
    @app.callback(
        Output("outlet", "children"),
        Input("f_warehouse", "value"),
        Input("f_cat", "value"),
        Input("f_item", "value"),
        Input("f_date", "start_date"),
        Input("f_date", "end_date"),
        Input("auto_reload", "n_intervals"),
    )
    def outlet(w, c, i, sd, ed, _):

        df = apply_filters(tx_df, w, c, i, sd, ed)

        # ================= EMPTY SAFE =================
        if df.empty:
            return dbc.Alert("No outlet consumption data", color="warning")

        # =================================================
        # 🧾 GRAND TOTAL KPI
        # =================================================
        total_qty, total_amt = build_outlet_grand_totals(df)

        kpis = dbc.Row([
            dbc.Col(
                dbc.Alert(
                    f"🧾 Total Stock Out: {total_qty:,.2f}",
                    color="primary",
                    className="text-center fw-bold"
                ),
                md=6
            ),
            dbc.Col(
                dbc.Alert(
                    f"💰 Total Amount: ₹ {total_amt:,.2f}",
                    color="success",
                    className="text-center fw-bold"
                ),
                md=6
            ),
        ], className="mb-3")

        # =================================================
        # 📊 OUTLET CONSUMPTION CHART
        # =================================================
        fig = build_outlet_consumption_chart(df)
        chart = (
            dcc.Graph(figure=fig)
            if fig else
            dbc.Alert("No outlet consumption chart data", color="warning")
        )

        # =================================================
        # 📊 OUTLET × CATEGORY PIVOT
        # =================================================
        pivot_df = build_outlet_category_pivot(df)

        pivot_table = (
            dash_table.DataTable(
                data=pivot_df.to_dict("records"),
                columns=[{"name": c, "id": c} for c in pivot_df.columns],
                page_size=10,
                sort_action="native",
                filter_action="native",
                style_table={"overflowX": "auto"},
                style_header={"fontWeight": "bold"},
                style_cell={"fontSize": "13px", "padding": "6px"},
            )
            if not pivot_df.empty
            else dbc.Alert("No pivot data available", color="warning")
        )

        # =================================================
        # 📋 OUTLET CONSUMPTION SUMMARY TABLE
        # =================================================
        summary_df = build_outlet_consumption_table(df)

        summary_table = (
            dash_table.DataTable(
                data=summary_df.to_dict("records"),
                columns=[
                    {"name": col, "id": col,
                    "type": "numeric",
                    "format": Format(precision=2, scheme=Scheme.fixed)}
                    if summary_df[col].dtype != "object"
                    else {"name": col, "id": col}
                    for col in summary_df.columns
                ],
                page_size=15,
                page_action="native",
                sort_action="native",
                filter_action="native",
                style_table={"overflowX": "auto"},
                style_header={
                    "fontWeight": "bold",
                    "backgroundColor": "#f8f9fa",
                    "border": "1px solid #dee2e6",
                },
                style_cell={
                    "fontSize": "13px",
                    "padding": "6px",
                    "textAlign": "left",
                },
                style_data_conditional=[
                    {
                        "if": {"filter_query": "{Date} = 'TOTAL'"},
                        "backgroundColor": "#e9ecef",
                        "fontWeight": "bold",
                    }
                ],
            )
            if not summary_df.empty
            else dbc.Alert("No summary data available", color="warning")
        )

        # =================================================
        # ✅ FINAL OUTLET TAB LAYOUT
        # =================================================
        return dbc.Container(
            [
                kpis,
                html.Hr(),
                chart,
                html.Hr(),
                html.H5("📊 Outlet × Category Consumption"),
                pivot_table,
                html.Hr(),
                html.H5("📋 Outlet Consumption Summary"),
                summary_table,
            ],
            fluid=True
        )

    @app.callback(
        Output("ledger", "children"),
        Input("f_item", "value"),
        Input("f_date", "start_date"),
        Input("f_date", "end_date"),
        Input("auto_reload", "n_intervals"),
    )
    def ledger(item, sd, ed, _):

        # 🔴 HARD RULE — item is mandatory
        if not item:
            return dbc.Alert(
                "⚠️ Please select an Item to view Global Ledger",
                color="warning"
            )

        # ✅ FILTER STRICTLY BY ITEM
        df = apply_filters(tx_df, None, None, item, sd, ed)

        if df.empty:
            return dbc.Alert("No ledger data available", color="warning")

        ledger_df = build_global_ledger(df, opening_stock_df)

        # ================= CHART =================
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=pd.to_datetime(ledger_df["Date"], format="%d-%m-%Y"),
            y=ledger_df["Closing Stock"],
            mode="lines+markers",
            name="Global Closing Stock"
        ))

        fig.update_layout(
            title=f"🌍 Global Ledger — {item}",
            xaxis_title="Date",
            yaxis_title="Closing Stock",
            height=400,
            template="plotly_white"
        )

        table = dash_table.DataTable(
            data=ledger_df.to_dict("records"),
            columns=[{"name": c, "id": c} for c in ledger_df.columns],
            page_size=15,
            sort_action="native",
            filter_action="native",
            style_header={"fontWeight": "bold"},
            style_cell={"fontSize": "13px", "padding": "6px"},
        )

        return dbc.Container(
            [
                html.H5("🌍 Global Ledger Trend"),
                dcc.Graph(figure=fig),
                html.Hr(),
                html.H5("📒 Global Ledger Details"),
                table,
            ],
            fluid=True
        )

    @app.callback(
        Output("alerts", "children"),
        Input("auto_reload", "n_intervals"),
    )
    def alerts_combined(_):

        global expiry_df
        expiry_df = load_expiry_data(EXPIRY_PATH)

        blocks = []

        # =====================================================
        # 🔴 NEGATIVE STOCK ALERT
        # =====================================================
        neg = build_negative_summary(tx_df)

        if not neg.empty:
            blocks.append(html.H5("🔴 Negative Stock Items"))
            blocks.append(
                dash_table.DataTable(
                    data=neg.to_dict("records"),
                    columns=[{"name": c, "id": c} for c in neg.columns],
                    page_size=10,
                    style_header={"fontWeight": "bold"},
                    style_data_conditional=[
                        {
                            "if": {"filter_query": "{Negative Qty} < 0"},
                            "backgroundColor": "#f8d7da",
                            "color": "black",
                        }
                    ],
                )
            )
        else:
            blocks.append(dbc.Alert("✅ No negative stock items", color="success"))

        blocks.append(html.Hr())

        # =====================================================
        # 🧊 EXPIRY AGING CHART (ALWAYS SHOW)
        # =====================================================
        blocks.append(html.H5("📊 Expiry Aging Summary"))
        blocks.append(
            dcc.Graph(
                id="expiry_aging_chart",
                figure=build_expiry_aging_chart(expiry_df)
            )
        )

        blocks.append(html.Hr())

        # =====================================================
        # 🧊 EXPIRY ALERT TABLE (≤ 60 DAYS)
        # =====================================================
        # expiry_alert_df = build_expiry_summary(expiry_df, tx_df)
        # expiry_alert_df = expiry_alert_df[expiry_alert_df["Days Left"] <= 60]
        expiry_alert_df = build_expiry_alerts(
            expiry_df,
            tx_df,
            days=60
        )

        if not expiry_alert_df.empty:
            blocks.append(html.H5("🧊 Items Nearing Expiry (≤ 60 days)"))
            blocks.append(
                dash_table.DataTable(
                    id="expiry_table",
                    data=expiry_alert_df.to_dict("records"),
                    columns=[{"name": c, "id": c} for c in expiry_alert_df.columns],
                    page_size=10,
                    sort_action="native",
                    filter_action="native",
                    style_header={"fontWeight": "bold"},
                    style_cell={"fontSize": "13px", "padding": "6px"},
                    style_data_conditional=[

                        # 🔴 EXPIRED
                        {
                            "if": {"filter_query": '{Status} = "Expired"'},
                            "backgroundColor": "#f8d7da",
                            "color": "black",
                            "fontWeight": "bold",
                        },

                        # 🔴 CRITICAL (≤15)
                        {
                            "if": {"filter_query": '{Status} contains "Critical"'},
                            "backgroundColor": "#f8d7da",
                            "color": "black",
                            "fontWeight": "bold",
                        },

                        # 🟠 MAJOR (≤30)
                        {
                            "if": {"filter_query": '{Status} contains "Major"'},
                            "backgroundColor": "#fff3cd",
                            "color": "black",
                        },

                        # 🟡 MINOR (≤60)
                        {
                            "if": {"filter_query": '{Status} contains "Minor"'},
                            "backgroundColor": "#cff4fc",
                            "color": "black",
                        },

                        # ✅ OK
                        {
                            "if": {"filter_query": '{Status} = "OK"'},
                            "backgroundColor": "#e9ecef",
                            "color": "black",
                        },
                    ],
                )
            )
        else:
            blocks.append(dbc.Alert("✅ No items nearing expiry", color="success"))
        return dbc.Container(blocks, fluid=True)
    
    @app.callback(
        Output("order_planning_table", "data"),
        Input("auto_reload", "n_intervals"),
    )
    def update_order_planning_table(_):

        plan_df = build_order_planning(tx_df)

        if plan_df.empty:
            return []

        # Hide internal columns from UI
        return plan_df.drop(
            columns=["Unit_Price", "Tax"],
            errors="ignore"
        ).to_dict("records")

    @app.callback(
        Output("selected_expiry_bucket", "data"),
        Input("expiry_aging_chart", "clickData")
    )
    def set_expiry_bucket(clickData):
        if not clickData:
            return None
        return clickData["points"][0]["x"]

    @app.callback(
        Output("expiry_table", "data"),
        Input("selected_expiry_bucket", "data"),
        Input("auto_reload", "n_intervals"),
    )
    def update_expiry_table(bucket, _):

        df = build_expiry_alerts(
            expiry_df,
            tx_df,
            days=60
        )

        if df.empty:
            return []

        if bucket:
            limits = {
                "0–10": (0, 10),
                "11–30": (11, 30),
                "31–60": (31, 60),
                "60+": (61, 10_000),
            }
            lo, hi = limits[bucket]
            df = df[(df["Days Left"] >= lo) & (df["Days Left"] <= hi)]

        return df.to_dict("records")


    @app.callback(
        Output("download_po_excel", "data"),
        Input("export_po_btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_purchase_order(n):

        plan_df = build_order_planning(tx_df)

        if plan_df.empty:
            raise PreventUpdate

        out = export_purchase_order_excel(plan_df)

        return dcc.send_bytes(
            out.read(),
            "Purchase_Order_Warehouse_Wise.xlsx"
        )

    @app.callback(
        Output("download_full_stock_excel", "data"),
        Input("export_full_stock_btn", "n_clicks"),
        State("f_date", "start_date"),
        State("f_date", "end_date"),
        prevent_initial_call=True,
    )
    def export_full_stock(n, sd, ed):

        out = export_full_stock_excel(tx_df, opening_stock_df, sd, ed)
        if out is None:
            raise PreventUpdate

        return dcc.send_bytes(
            out.read(),
            "Global_Stock_Ledger_All_Items.xlsx"
        )
    @app.callback(
        Output("wastage_tab", "children"),
        Input("f_warehouse", "value"),
        Input("f_date", "start_date"),
        Input("f_date", "end_date"),
        Input("auto_reload", "n_intervals"),
    )
    def wastage_tab(wh, sd, ed, _):

        df = reload_all_data()

        if wh:
            df = df[df["Warehouse"] == wh]

        report = build_wastage_report(df, sd, ed)

        if report.empty:
            return dbc.Alert("No wastage > 0 found. Showing items with zero wastage.", color="warning")

        table = dash_table.DataTable(
            data=report.to_dict("records"),
            columns=[{"name": c, "id": c} for c in report.columns],
            page_size=12,
            sort_action="native",
            filter_action="native",
            style_header={"fontWeight": "bold"},
            style_cell={"fontSize": "13px", "padding": "6px"},
        )

        total_loss = report["Wastage_Value"].sum()

        return dbc.Container([
            dbc.Alert(
                f"💸 Total Wastage Loss: ₹ {total_loss:,.2f}",
                color="danger",
                className="fw-bold"
            ),
            html.Hr(),
            table
        ], fluid=True)
    @app.callback(
        Output("wastage_table", "data"),
        Input("refresh_wastage", "n_clicks"),
        State("wastage_date_range", "start_date"),
        State("wastage_date_range", "end_date"),
    )
    def update_wastage_table(_, sd, ed):
        df = reload_all_data()
        report = build_wastage_report(df, sd, ed)
        return report.to_dict("records")

    @app.callback(
        Output("consumption_tab", "children"),
        [
            Input("f_warehouse", "value"),
            Input("auto_reload", "n_intervals")
        ]
    )
    def update_consumption_tab(outlet_filter, _):
        cons_df = load_consumption_data()

        if cons_df.empty:
            return dbc.Alert("No consumption data found.", color="warning")

        report = build_simple_consumption_report(cons_df)

        if outlet_filter:
            report = report[report["outlet name"] == outlet_filter]

        table = dash_table.DataTable(
            id="consumption_table",
            data=report.to_dict("records"),
            columns=[{"name": str(c), "id": str(c)} for c in report.columns],
            page_size=25,
            sort_action="native",
            filter_action="native",
            style_table={"overflowX": "auto"},
            style_cell={'textAlign': 'left'},
            style_header={'fontWeight': 'bold'},
        )

        return table



def attach_excel(msg, df, filename):

    buffer = io.BytesIO()
    df.to_excel(buffer, index=False)
    buffer.seek(0)

    part = MIMEBase("application", "octet-stream")
    part.set_payload(buffer.read())
    encoders.encode_base64(part)

    part.add_header(
        "Content-Disposition",
        f'attachment; filename="{filename}"'
    )

    msg.attach(part)

def resolve_cc_by_warehouse(df):

    cc = set()

    if df["Warehouse"].str.contains("NCR", case=False).any():
        cc.add(os.getenv("ALERT_CC_NCR"))

    if df["Warehouse"].str.contains("Mumbai", case=False).any():
        cc.add(os.getenv("ALERT_CC_MUM"))

    return ",".join(filter(None, cc))

    
def send_mail_with_excel(subject, html_body, df, filename):

    msg = MIMEMultipart()
    msg["From"] = os.getenv("SMTP_USER")
    msg["To"] = os.getenv("ALERT_TO")

    cc = resolve_cc_by_warehouse(df)
    if cc:
        msg["Cc"] = cc

    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))

    attach_excel(msg, df, filename)

    recipients = msg["To"].split(",") + (cc.split(",") if cc else [])

    with smtplib.SMTP(os.getenv("SMTP_HOST"), int(os.getenv("SMTP_PORT"))) as server:
        server.starttls()
        server.login(os.getenv("SMTP_USER"), os.getenv("SMTP_PASS"))
        server.sendmail(msg["From"], recipients, msg.as_string())

def send_expiry_alerts(expiry_df, tx_df, days):

    if expiry_df is None or expiry_df.empty:
        return

    today = pd.Timestamp.today().normalize()
    df = expiry_df.copy()

    # ================= DATE SAFETY =================
    df["Expiry Date"] = pd.to_datetime(df["Expiry Date"], errors="coerce")

    # ================= DAYS LEFT =================
    df["Days Left"] = (df["Expiry Date"] - today).dt.days

    # ================= WINDOW =================
    df = df[
        (df["Days Left"] >= -10) &
        (df["Days Left"] <= days)
    ]
    df = apply_category_skip(df, "Category Name")

    if df.empty:
        return

    # ================= AVAILABLE STOCK (✅ FIXED) =================
    stock_map = get_available_stock_map(tx_df)

    df["Available Stock"] = df["Item Name"].astype(str).str.strip().map(
        stock_map
    ).fillna(0)



    # ================= ALERT LEVEL =================
    def alert_level(d):
        if d <= 15:
            return "🔴 Critical (≤15 Days)"
        if d <= 30:
            return "🟠 Major (≤30 Days)"
        if d <= 60:
            return "🟡 Minor (≤60 Days)"
        return "OK"

    df["Alert Level"] = df["Days Left"].apply(alert_level)

    # ================= GROUP (REMOVE DUPLICATES) =================
    df = (
        df
        .groupby(
            [
                "Warehouse",
                "Category Name",
                "Item Name",
                "Unit",
                "Days Left",
                "Alert Level",
            ],
            as_index=False
        )
        .agg(
            Qty=("Qty", "sum"),
            Available_Stock=("Available Stock", "max"),
            Expiry_Date=("Expiry Date", "min")
        )
    )

    # ================= FORMAT DATE =================
    df["Expiry Date"] = df["Expiry_Date"].dt.strftime("%d-%m-%Y")
    df.drop(columns=["Expiry_Date"], inplace=True)

    df = df.sort_values("Days Left")

    # ================= EMAIL BODY =================
    html = df[
        [
            "Warehouse",
            "Item Name",
            "Unit",
            "Qty",
            "Available_Stock",
            "Expiry Date",
            "Days Left",
            "Alert Level",
        ]
    ].to_html(index=False)

    send_mail_with_excel(
        subject=f"⚠️ Expiry Alert ≤ {days} Days",
        html_body=html,
        df=df,
        filename=f"Expiry_Alert_{days}_Days.xlsx"
    )

def send_negative_stock_mail(tx_df):

    neg = build_negative_summary(tx_df)
    if neg.empty:
        return

    html = neg.to_html(index=False)

    send_mail_with_excel(
        subject="🔴 Negative Stock Alert",
        html_body=html,
        df=neg,
        filename="Negative_Stock_Items.xlsx"
    )

def send_available_stock_snapshot(tx_df):

    stock = (
        tx_df.groupby([COL_ITEM, COL_CATEGORY], as_index=False)
        .agg(
            Stock_In=("qty_in","sum"),
            Stock_Out=("qty_out_base","sum"),
        )
    )

    stock["Available Stock"] = stock["Stock_In"] - stock["Stock_Out"]
    stock = stock[stock["Available Stock"] > 0]

    if stock.empty:
        return

    html = stock.to_html(index=False)

    send_mail_with_excel(
        subject="📦 Month End Available Stock",
        html_body=html,
        df=stock,
        filename="Month_End_Available_Stock.xlsx"
    )