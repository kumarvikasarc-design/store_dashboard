# ================= STANDARD LIB =================
import os
import io
import math
import logging
import smtplib
from datetime import datetime
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders

# ================= THIRD PARTY =================
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler

# ================= DASH =================
from dash import Dash, html, dcc, Input, Output, State, callback_context
from dash.exceptions import PreventUpdate
from dash import dash_table
from dash.dash_table.Format import Format, Scheme
import dash_bootstrap_components as dbc
#from db_connection import engine
from sqlalchemy import create_engine
import urllib

scheduler = BackgroundScheduler(timezone="Asia/Kolkata")

if not scheduler.running:
    scheduler.start()
    
pio.templates.default = "plotly"

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

# ==========================================================
# 🔥 MASTER DATA LOAD (FILTERS)
# ==========================================================
def load_ordering_stock():

    query = """
    SELECT 
        i.item_id,
        i.item_name,
        w.warehouse_name,

        ISNULL(os.opening_qty,0)
        + ISNULL(se.total_entry,0)
        - ISNULL(ind.total_indent,0)
        - ISNULL(ws.total_wastage,0)
        - ISNULL(ex.total_expiry,0)
        + (ISNULL(ind.total_indent,0) - ISNULL(cons.total_consumption,0))
        AS ordering_stock

    FROM dbo.stockitem_master i
    CROSS JOIN dbo.warehouse_master w

    LEFT JOIN (
        SELECT warehouse_id,item_id,SUM(opening_qty) opening_qty
        FROM dbo.warehouse_opening_stock
        GROUP BY warehouse_id,item_id
    ) os ON os.item_id=i.item_id AND os.warehouse_id=w.warehouse_id

    LEFT JOIN (
        SELECT warehouse_id,item_id,SUM(qty) total_entry
        FROM dbo.warehouse_stockentry
        GROUP BY warehouse_id,item_id
    ) se ON se.item_id=i.item_id AND se.warehouse_id=w.warehouse_id

    LEFT JOIN (
        SELECT warehouse_id,item_id,SUM(qty) total_indent
        FROM dbo.warehouse_indent
        GROUP BY warehouse_id,item_id
    ) ind ON ind.item_id=i.item_id AND ind.warehouse_id=w.warehouse_id

    LEFT JOIN (
        SELECT item_id,SUM(qty) total_consumption
        FROM dbo.outlet_consumption
        GROUP BY item_id
    ) cons ON cons.item_id=i.item_id

    LEFT JOIN (
        SELECT warehouse_id,item_id,SUM(qty) total_wastage
        FROM dbo.warehouse_wastage
        GROUP BY warehouse_id,item_id
    ) ws ON ws.item_id=i.item_id AND ws.warehouse_id=w.warehouse_id

    LEFT JOIN (
        SELECT warehouse_id,item_id,SUM(qty) total_expiry
        FROM dbo.warehouse_item_expiry
        GROUP BY warehouse_id,item_id
    ) ex ON ex.item_id=i.item_id AND ex.warehouse_id=w.warehouse_id
    """

    df = pd.read_sql(query, engine)
    return df

def load_sql_data():

    query = "SELECT * FROM vw_dashboard_stock"
    df = pd.read_sql(query, engine)

    # Clean column names
    df.columns = df.columns.str.strip()

    # Rename EXACTLY to dashboard standard
    df.columns = [
        "date",
        "Warehouse",
        "Outlet",
        "Item",
        "Category",
        "UOM",
        "qty_in",
        "qty_out",
        "Unit Price",
        "Tax",
        "source"
    ]

    # Type conversion
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["qty_in"] = pd.to_numeric(df["qty_in"], errors="coerce").fillna(0)
    df["qty_out"] = pd.to_numeric(df["qty_out"], errors="coerce").fillna(0)
    df["Unit Price"] = pd.to_numeric(df["Unit Price"], errors="coerce").fillna(0)
    df["Tax"] = pd.to_numeric(df["Tax"], errors="coerce").fillna(0)

    print("✅ SQL loaded rows:", len(df))
    print("📊 Columns:", df.columns.tolist())

    return df
# ==========================================================
# GLOBAL SQL DATA RELOAD (MASTER)
# ==========================================================
def reload_all_data():
    global tx_df

    try:
        df = load_sql_data()

        df.columns = df.columns.str.strip()

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["qty_in"] = pd.to_numeric(df["qty_in"], errors="coerce").fillna(0)
        df["qty_out"] = pd.to_numeric(df["qty_out"], errors="coerce").fillna(0)
        df["Unit Price"] = pd.to_numeric(df["Unit Price"], errors="coerce").fillna(0)
        df["Tax"] = pd.to_numeric(df["Tax"], errors="coerce").fillna(0)

        df.rename(columns={
            "Unit Price": "unit_price",
            "Tax": "tax",
        }, inplace=True)

        df["qty_out_base"] = df["qty_out"]

        if "wastage_amount" not in df.columns:
            df["wastage_amount"] = 0

        # TEXT CLEAN
        for col in ["Warehouse","Outlet","Item","Category","UOM"]:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .str.replace(r"\s+"," ",regex=True)
                )

        tx_df = df.copy()

        print("✅ SQL data refreshed:", len(df))
        print("📊 Final columns:", df.columns.tolist())

        return df

    except Exception as e:
        print("❌ SQL reload error:", e)
        return pd.DataFrame()
    
# ======================================================
# 🔥 SINGLE ENTERPRISE ITEM MASTER (SQL ONLY)
# ======================================================
def load_item_master():
    try:
        df = pd.read_sql("""
            SELECT 
                Item_Name AS item_name,
                Category_Name AS category_name,
                Super_Category AS super_category,
                Tax_Rate AS tax_rate,
                Has_Expiry AS has_expiry,
                Purchase_UOM AS purchase_uom,
                Base_UOM AS base_uom,
                Conversion_Factor AS conversion_factor
            FROM stockitem_master
        """, engine)

        df.columns = df.columns.str.lower().str.strip()

        # 🔥 ADD DEFAULTS (since SQL doesn't have)
        df["moq"] = 1
        df["pack_size"] = 1

        print("✅ SQL item master loaded:", len(df))
        return df

    except Exception as e:
        print("❌ item master load error:", e)
        return pd.DataFrame()

# ===== LOAD ONCE =====
ITEM_MASTER = load_item_master()

# ======================================================
# 🔁 CONVERSION MAP
# ======================================================
ITEM_CONV_MAP = {
    (
        str(r["item_name"]).strip(),
        str(r["purchase_uom"]).upper().strip()
    ): {
        "factor": float(r.get("conversion_factor", 1)),
        "base_uom": str(r.get("base_uom","")).upper().strip(),
    }
    for _, r in ITEM_MASTER.iterrows()
}

# ======================================================
# 📊 MASTER MAPS
# ======================================================
ITEM_TAX_MAP = ITEM_MASTER.set_index("item_name")["tax_rate"].to_dict() if not ITEM_MASTER.empty else {}
ITEM_SUPER_CAT_MAP = ITEM_MASTER.set_index("item_name")["super_category"].to_dict() if not ITEM_MASTER.empty else {}
ITEM_EXPIRY_MAP = ITEM_MASTER.set_index("item_name")["has_expiry"].to_dict() if not ITEM_MASTER.empty else {}

# ======================================================
# 📦 MOQ + PACK MAP
# ======================================================
ITEM_MOQ_MAP = ITEM_MASTER.set_index("item_name")["moq"].to_dict() if "moq" in ITEM_MASTER else {}
ITEM_PACK_MAP = ITEM_MASTER.set_index("item_name")["pack_size"].to_dict() if "pack_size" in ITEM_MASTER else {}

def load_master_filters():

    df = pd.read_sql("""
        SELECT DISTINCT 
            s.Brand,
            s.State,
            s.Region,
            s.City,
            s.Store_Type,
            s.Outlet_Name
        FROM stores_master s
    """, engine)

    return df

def build_master_global_df():
    global TX_DF_GLOBAL

    df = tx_df.copy()

    # ================= STORE MASTER =================
    try:
        store_df = load_master_filters()
        store_df.columns = store_df.columns.str.strip()

        def clean_text(s):
            return (
                s.astype(str)
                .str.lower()
                .str.replace(r"[^a-z0-9 ]", "", regex=True)
                .str.replace(r"\s+", " ", regex=True)
                .str.strip()
            )

        df["Outlet_clean"] = clean_text(df["Outlet"])
        store_df["Outlet_clean"] = clean_text(store_df["Outlet_Name"])

        df = df.merge(store_df, on="Outlet_clean", how="left")
        # REMOVE rows where store mapping not found
        df = df[~df["Brand"].isna()].copy()

        # optional: fill unknown instead of drop
        # df["Brand"] = df["Brand"].fillna("Unknown")
        # df["State"] = df["State"].fillna("Unknown")
        # df["Region"] = df["Region"].fillna("Unknown")
        # df["City"] = df["City"].fillna("Unknown")

        print("✅ Store master merged")

        if "Brand" in df.columns:
            print("Brand null:", df["Brand"].isna().sum())
        else:
            print("⚠ Brand column missing after merge")

    except Exception as e:
        print("❌ store merge error:", e)

    # ================= MONTH =================
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["Month"] = df["date"].dt.strftime("%b-%y")
    df["Month_Sort"] = df["date"].dt.to_period("M").astype(str)
    df = df.sort_values("Month_Sort")

    # ================= FY =================
    df["Financial Year"] = df["date"].apply(
        lambda d: f"{d.year}-{str(d.year+1)[-2:]}"
        if pd.notna(d) and d.month >= 4
        else f"{d.year-1}-{str(d.year)[-2:]}"
        if pd.notna(d)
        else None
    )

    TX_DF_GLOBAL = df
        
tx_df = reload_all_data()

if tx_df is None or tx_df.empty:
    print("❌ SQL FAILED — DASHBOARD STOPPED")
    TX_DF_GLOBAL = pd.DataFrame()

else:
    build_master_global_df()

    if not TX_DF_GLOBAL.empty:

        TX_DF_GLOBAL["Super Category"] = (
            TX_DF_GLOBAL["Item"]
            .map(ITEM_SUPER_CAT_MAP)
            .fillna("Unknown")
        )
        # PERFECT MONTH SORTING
        if "Month" in TX_DF_GLOBAL.columns:
            TX_DF_GLOBAL["Month"] = pd.Categorical(
                TX_DF_GLOBAL["Month"],
                ordered=True,
                categories=sorted(
                    TX_DF_GLOBAL["Month"].dropna().unique(),
                    key=lambda x: pd.to_datetime(x, format="%b-%y")
                )
            )
        # 🔥 SPEED BOOST HERE
        for col in [
            "Brand","State","Region","City",
            "Store_Type","Outlet",
            "Warehouse","Category","Item"
        ]:
            if col in TX_DF_GLOBAL.columns:
                TX_DF_GLOBAL[col] = TX_DF_GLOBAL[col].astype("category")
        TX_DF_GLOBAL.sort_values(
            ["Brand","State","Region","City","Outlet"],
            inplace=True
        )
        TX_DF_GLOBAL.reset_index(drop=True, inplace=True)
        print("⚡ Dashboard optimized (category dtype enabled)")

def load_items():
    return pd.read_sql("""
        SELECT DISTINCT
            item_id,
            Item_Name,
            Category_Name,
            Super_Category
        FROM stockitem_master
    """, engine)

# ======================================================
# CONSTANTS
# ======================================================
COL_DATE = "date"
COL_WAREHOUSE = "Warehouse"
COL_OUTLET = "Outlet"
COL_ITEM = "Item"
COL_CATEGORY = "Category"
COL_UOM = "UOM"
COL_UNIT_PRICE = "unit_price"   # 🔥 FIXED
COL_TAX = "tax"                 # 🔥 FIXED

LOOKBACK_DAYS = 14
STOCKOUT_DAYS = 7
ALERT_DEAD_DAYS = 30

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

# --------------------------------------------------------------
# Normalizer for item names
# --------------------------------------------------------------
def normalize_item_name(name):
    if pd.isna(name):
        return ""
    # Removed .title() and .replace() to keep original Excel formatting
    return str(name).strip()

# ================= SKIP CATEGORIES =================
SKIP_CATEGORIES = [
    "GLASSWARE", "MUSIC SYSTEM", "FIRE FIGHTING SYSTEM",
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

def apply_category_skip(df, col=COL_CATEGORY):
    return df[
        ~df[col]
        .astype(str)
        .str.strip()
        .str.upper()
        .isin(SKIP_CATEGORIES)
    ]

def load_opening_stock_sql():
    try:
        df = pd.read_sql("""
            SELECT
                Warehouse AS warehouse,
                Item_Name AS item_name,
                UOM AS uom,
                Opening_Stock AS opening_qty,
                Opening_Date AS opening_date
            FROM warehouse_opening_stock
        """, engine)

        df.columns = df.columns.str.lower().str.strip()
        df["opening_date"] = pd.to_datetime(df["opening_date"], errors="coerce")

        print("✅ Opening stock loaded:", len(df))
        return df

    except Exception as e:
        print("❌ Opening stock load error:", e)
        return pd.DataFrame()

opening_stock_df = load_opening_stock_sql()
OPENING_DF_GLOBAL = opening_stock_df.copy()

# ======================================================
# SAFE DROPDOWN OPTIONS (STRING-ONLY GUARANTEE)
# ======================================================
def dropdown_options(series, sort_series=None, remove_outlets=False):

    if series is None:
        return []

    s = pd.Series(series)

    s = s.dropna().astype(str).str.strip()

    # remove blank + nan text
    s = s[~s.str.lower().isin(["", "nan", "none", "null"])]
    s = s[~s.str.fullmatch(r"\s*")]

    if remove_outlets:
        BAD = [
            "central store",
            "warehouse",
            "warehouse ncr",
            "warehouse mumbai"
        ]
        s = s[~s.str.lower().isin(BAD)]

    if sort_series is not None:
        temp = pd.DataFrame({"val": s})
        temp["sort"] = pd.Series(sort_series)
        temp = temp.sort_values("sort")
        s = temp["val"]

    unique_vals = s.drop_duplicates().tolist()

    return [{"label": v, "value": v} for v in unique_vals]

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

def build_wastage_report(df, sd=None, ed=None):

    if df is None or df.empty:
        return pd.DataFrame()

    df = df[df.get("source","") == "WASTAGE"].copy()
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

# ======================================================
# BACKFILL UNIT PRICE FROM EXISTING DATA (SQL SAFE)
# ======================================================
price_map = (
    tx_df[tx_df["qty_in"] > 0]
    .groupby(COL_ITEM)[COL_UNIT_PRICE]
    .mean()
)

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

DISPLAY_UOM_MAP = build_display_uom_map(tx_df, None)

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
    return build_display_uom_map(tx_df, None)

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

    summary[COL_UOM] = summary.apply(
        lambda r: DISPLAY_UOM_MAP.get(
            (r[COL_WAREHOUSE], r[COL_ITEM]),
            None
        ),
        axis=1
    )
    
    summary = summary[summary[COL_UOM].notna()].copy()
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
    BASE_UOMS = {"GM", "KG", "ML", "LTR"}

    bad = summary[summary["Unit"].isin(BASE_UOMS)]
    if not bad.empty:
        print("⚠️ BASE UOM FOUND IN SUMMARY:")
        print(bad[["Item Name", "Unit"]].drop_duplicates().head(10))

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

# ===============================================================
# 🔽 FIXED GLOBAL LEDGER — PURCHASE UOM ONLY
# ===============================================================
def build_global_ledger(tx_df, opening_df=None):

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
    # Opening already included in SQL view
    opening_qty = 0

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


# ======================================================
# LOAD EXPIRY FROM SQL (ENTERPRISE)
# ======================================================
def load_expiry_sql():
    try:
        df = pd.read_sql("""
            SELECT 
                e.entry_no,
                e.transaction_date,
                e.warehouse,
                i.Category_Name AS category_name,
                e.item_name,
                e.unit,
                e.qty,
                e.expiry_date
            FROM warehouse_item_expiry e
            LEFT JOIN stockitem_master i
                ON i.Item_Name = e.item_name
            WHERE e.qty > 0
            AND e.expiry_date IS NOT NULL
        """, engine)

        if df.empty:
            return pd.DataFrame()

        df.columns = df.columns.str.strip()

        df.rename(columns={
            "entry_no": "Entry No",
            "transaction_date": "Transaction Date",
            "warehouse": "Warehouse",
            "category_name": "Category Name",
            "item_name": "Item Name",
            "unit": "Unit",
            "qty": "Qty",
            "expiry_date": "Expiry Date"
        }, inplace=True)

        df["Expiry Date"] = pd.to_datetime(df["Expiry Date"], errors="coerce")
        df["Transaction Date"] = pd.to_datetime(df["Transaction Date"], errors="coerce")

        df["Unit"] = df["Unit"].apply(normalize_uom)

        print("✅ Expiry loaded:", len(df))
        return df

    except Exception as e:
        print("❌ expiry load error:", e)
        return pd.DataFrame()

expiry_df = load_expiry_sql()
   
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
        
        available = stock_map.get(item, 0)
        
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
        stock_map[item] = available - deduct_qty
    if not adjustments:
        return tx_df

    return pd.concat([tx_df, pd.DataFrame(adjustments)], ignore_index=True)

if not tx_df.empty:
    if "EXPIRY_FIFO" not in tx_df.get("source","").astype(str).unique():
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

def export_full_stock_excel(tx_df, sd=None, ed=None):

    df = apply_filters(tx_df, None, None, None, sd, ed)
    if df.empty:
        return None

    ledger_rows = []
    month_rows = []
    open_close_rows = []

    for item in sorted(df[COL_ITEM].dropna().unique()):

        item_df = df[df[COL_ITEM] == item]
        ledger = build_global_ledger(item_df)

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

def build_global_aging(tx_df, as_on_date):

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

        # Opening already included in SQL view
        opening_qty = 0

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

        last_move = x[COL_DATE].max() if not x.empty else as_on

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

def build_aging_bucket_chart(tx_df, as_on_date):

    aging_df = build_global_aging(tx_df, as_on_date)

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
    ITEM_MASTER.set_index("item_name")["moq"].to_dict()
    if "moq" in ITEM_MASTER.columns else {}
)

ITEM_PACK_MAP = (
    ITEM_MASTER.set_index("item_name")["pack_size"].to_dict()
    if "pack_size" in ITEM_MASTER.columns else {}
)

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
    # 1️⃣ CONSUMPTION (WAREHOUSE × ITEM)
    # =====================================================
    cons = (
        df[
            (df.get("source","") == "INDENT") &
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
    # 2️⃣ AVAILABLE STOCK
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
    # 3️⃣ MERGE
    # =====================================================
    plan = cons.merge(
        stock,
        on=[COL_WAREHOUSE, COL_ITEM],
        how="left"
    )

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

    # 🔥 AI SAFETY BUFFER
    SAFETY_DAYS = 3

    max_reorder = plan["Daily Consumption"] * MAX_ORDER_DAYS

    plan["Reorder Point"] = (
        plan["Daily Consumption"] * (LEAD_TIME_DAYS + SAFETY_DAYS)
    ).clip(upper=max_reorder)

    raw_qty = pd.Series(0.0, index=plan.index)
    raw_qty.loc[~zero_cons] = (
        plan.loc[~zero_cons, "Reorder Point"]
        - plan.loc[~zero_cons, "Available Stock"]
    ).clip(lower=0)

    # =====================================================
    # 5️⃣ MOQ + PACK
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
    # DISPLAY UOM
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
    # URGENCY
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
    # ORDER VALUE
    # =====================================================
    plan["Order Value"] = (
        plan["Suggested Order Qty"]
        * plan["Unit_Price"]
        * (1 + plan["Tax"] / 100)
    )

    # =====================================================
    # FINAL TABLE
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
        ]
    ].round(2)

    # 🔥 SORT: CRITICAL FIRST
    urgency_order = {"CRITICAL": 0, "WARNING": 1, "OK": 2}
    final["sort"] = final["Urgency"].map(urgency_order)

    final = final.sort_values(
        ["sort", "Order Value"],
        ascending=[True, False]
    ).drop(columns="sort")

    return final

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

def apply_filters(df, w=None, c=None, i=None, sd=None, ed=None):

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    # normalize text columns
    df[COL_WAREHOUSE] = df[COL_WAREHOUSE].astype(str).str.strip()
    df[COL_CATEGORY] = df[COL_CATEGORY].astype(str).str.strip()
    df[COL_ITEM] = df[COL_ITEM].astype(str).str.strip()

    df[COL_DATE] = pd.to_datetime(df[COL_DATE], errors="coerce")

    # ================= WAREHOUSE =================
    if w:
        df = df[
            df[COL_WAREHOUSE]
            .str.lower()
            .str.strip()
            == str(w).lower().strip()
        ]

    # ================= CATEGORY =================
    if c:
        df = df[
            df[COL_CATEGORY]
            .str.lower()
            .str.strip()
            == str(c).lower().strip()
        ]

    # ================= ITEM =================
    if i:
        df = df[
            df[COL_ITEM]
            .str.lower()
            .str.strip()
            == str(i).lower().strip()
        ]

    # ================= DATE =================
    if sd:
        sd = pd.to_datetime(sd, errors="coerce")
        if pd.notna(sd):
            df = df[df[COL_DATE] >= sd]

    if ed:
        ed = pd.to_datetime(ed, errors="coerce")
        if pd.notna(ed):
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
             # 🔵 FILTER ROW 1
        dbc.Card(
            dbc.CardBody([   # 🔥 FIX HERE

                dbc.Row([
                    dbc.Col(dcc.Dropdown(id="wh_brand_filter", placeholder="Brand")),
                    dbc.Col(dcc.Dropdown(id="wh_state_filter", placeholder="State")),
                    dbc.Col(dcc.Dropdown(id="wh_region_filter", placeholder="Region")),
                    dbc.Col(dcc.Dropdown(id="wh_city_filter", placeholder="City")),
                    dbc.Col(dcc.Dropdown(id="wh_type_filter", placeholder="Store Type")),
                    dbc.Col(dcc.Dropdown(id="wh_outlet_filter", placeholder="Outlet")),
                ], className="mb-2"),

                dbc.Row([
                    dcc.DatePickerRange(
                        id="f_date",
                        start_date=tx_df[COL_DATE].min() if not tx_df.empty else None,
                        end_date=tx_df[COL_DATE].max() if not tx_df.empty else None,
                        display_format="DD-MM-YYYY"
                    ),
                    dbc.Col(dcc.Dropdown(id="wh_fy_filter", placeholder="Financial Year")),
                    dbc.Col(dcc.Dropdown(id="wh_month_filter", placeholder="Month")),
                    dbc.Col(dcc.Dropdown(id="wh_warehouse", placeholder="Warehouse")),
                    dbc.Col(dcc.Dropdown(id="wh_supercat_filter", placeholder="Super Category")),
                    dbc.Col(dcc.Dropdown(id="wh_category_filter", placeholder="Category")),
                    dbc.Col(dcc.Dropdown(id="wh_item_filter", placeholder="Item")),
                ], className="mb-3"),

            ])   # 🔥 closing list
        ),
        # dbc.Row([
        #     dbc.Col(dbc.Card(dbc.CardBody([
        #         html.H6("Total Stock Value"),
        #         html.H4(id="kpi_stock_value")
        #     ]))),
        #     dbc.Col(dbc.Card(dbc.CardBody([
        #         html.H6("Monthly Consumption"),
        #         html.H4(id="kpi_consumption")
        #     ]))),
        #     dbc.Col(dbc.Card(dbc.CardBody([
        #         html.H6("Dead Stock Value"),
        #         html.H4(id="kpi_dead")
        #     ]))),
        #     dbc.Col(dbc.Card(dbc.CardBody([
        #         html.H6("Urgent Orders"),
        #         html.H4(id="kpi_urgent")
        #     ]))),
        # ], className="mb-3"),

        # ================= EXPORT =================
        dbc.Button(
            "📤 Export Full Stock Ledger",
            id="export_full_stock_btn",
            color="dark",
            className="mb-2"
        ),
        dcc.Download(id="download_full_stock_excel"),


        html.Hr(),

        # ================= TABS =================
        dcc.Tabs([

            dcc.Tab(label="Warehouse Summary", children=html.Div(id="summary")),

            dcc.Tab(label="Warehouse Ledger", children=html.Div(id="ledger")),

            # ===== AGING TAB =====
            dcc.Tab(
                label="Warehouse Aging",
                children=html.Div([

                    dcc.Store(id="selected_aging_bucket"),

                    dcc.Graph(id="aging_bucket_chart"),

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

            dcc.Tab(label="Outlet Consumption", children=html.Div(id="outlet")),

            dcc.Tab(
                label="Alerts",
                children=html.Div([
                    dcc.Store(id="selected_expiry_bucket"),
                    html.Div(id="alerts"),
                    html.Hr(),
                ])
            ),

            dcc.Tab(label="Wastage", children=html.Div(id="wastage_tab")),
            dcc.Tab(label="Consumption", children=html.Div(id="consumption_tab")),

            # ================= ORDERING TAB (NOW INSIDE TABS) =================
            dcc.Tab(
                label="Ordering Planning",
                children=html.Div([

                    html.H5("📦 Item Ordering Planning (Warehouse-wise)"),

                    dash_table.DataTable(
                        id="order_planning_table",
                        columns=[
                            {"name": "Warehouse", "id": "Warehouse"},
                            {"name": "Item Name", "id": "Item Name"},
                            {"name": "Unit", "id": "Unit"},
                            {"name": "Daily Consumption", "id": "Daily Consumption"},
                            {"name": "Available Stock", "id": "Available Stock"},
                            {"name": "Days Left", "id": "Days Left"},
                            {"name": "Reorder Point", "id": "Reorder Point"},
                            {"name": "Suggested Order Qty", "id": "Suggested Order Qty"},
                            {"name": "Order Value", "id": "Order Value"},
                            {"name": "Urgency", "id": "Urgency"},
                        ],
                        page_size=12,
                        style_header={"fontWeight": "bold"},
                        style_cell={"fontSize": "13px", "padding": "6px"},
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

        ])
    ], fluid=True)

# ======================================================
# CALLBACKS
# ======================================================
def register_callbacks(app):

    @app.callback(
        Output("wh_brand_filter", "options"),
        Output("wh_state_filter", "options"),
        Output("wh_region_filter", "options"),
        Output("wh_city_filter", "options"),
        Output("wh_type_filter", "options"),
        Output("wh_outlet_filter", "options"),
        Output("wh_fy_filter", "options"),
        Output("wh_month_filter", "options"),
        Output("wh_warehouse", "options"),
        Output("wh_supercat_filter", "options"),
        Output("wh_category_filter", "options"),
        Output("wh_item_filter", "options"),

        Input("wh_brand_filter", "value"),
        Input("wh_state_filter", "value"),
        Input("wh_region_filter", "value"),
        Input("wh_city_filter", "value"),
        Input("wh_type_filter", "value"),
        Input("wh_outlet_filter", "value"),
        Input("wh_fy_filter", "value"),
        Input("wh_month_filter", "value"),
        Input("wh_warehouse", "value"),
        Input("wh_supercat_filter", "value"),
        Input("wh_category_filter", "value"),
        Input("wh_item_filter", "value"),
        Input("auto_reload", "n_intervals"),
    )
    def cascade_filters(
        brand, state, region, city,
        store_type, outlet, fy, month,
        warehouse, supercat, category, item, _
    ):

        df = TX_DF_GLOBAL.copy()

        if df.empty:
            empty = []
            return (empty,)*12

        # ================= CLEAN TEXT =================
        text_cols = [
            "Brand","State","Region","City","Store_Type","Outlet",
            "Financial Year","Month","Super Category",
            COL_WAREHOUSE, COL_CATEGORY, COL_ITEM
        ]

        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # ================= CASCADE FILTERING =================
        if brand:
            df = df[df["Brand"] == brand]

        if state:
            df = df[df["State"] == state]

        if region:
            df = df[df["Region"] == region]

        if city:
            df = df[df["City"] == city]

        if store_type:
            df = df[df["Store_Type"] == store_type]

        if outlet:
            df = df[df["Outlet"] == outlet]

        if fy:
            df = df[df["Financial Year"] == fy]

        if month:
            df = df[df["Month"] == month]

        if warehouse:
            df = df[df[COL_WAREHOUSE] == warehouse]

        if supercat:
            df = df[df["Super Category"] == supercat]

        if category:
            df = df[df[COL_CATEGORY] == category]

        if item:
            df = df[df[COL_ITEM] == item]

        # ================= OPTIONS BUILD =================
        brand_opt   = dropdown_options(df.get("Brand", pd.Series()))
        state_opt   = dropdown_options(df.get("State", pd.Series()))
        region_opt  = dropdown_options(df.get("Region", pd.Series()))
        city_opt    = dropdown_options(df.get("City", pd.Series()))
        type_opt    = dropdown_options(df.get("Store_Type", pd.Series()))
        outlet_opt = dropdown_options(
            df.get("Outlet", pd.Series()),
            remove_outlets=True
        )

        fy_opt      = dropdown_options(df.get("Financial Year", pd.Series()))
        month_series = df.get("Month", pd.Series())
        month_sort = df.loc[month_series.index, "Month_Sort"] if "Month_Sort" in df.columns else None

        month_opt = dropdown_options(month_series, month_sort)

        wh_opt      = dropdown_options(df.get(COL_WAREHOUSE, pd.Series()))
        sc_opt      = dropdown_options(df.get("Super Category", pd.Series()))
        cat_opt     = dropdown_options(df.get(COL_CATEGORY, pd.Series()))
        item_opt    = dropdown_options(df.get(COL_ITEM, pd.Series()))

        return (
            brand_opt,
            state_opt,
            region_opt,
            city_opt,
            type_opt,
            outlet_opt,
            fy_opt,
            month_opt,
            wh_opt,
            sc_opt,
            cat_opt,
            item_opt,
        )
    @app.callback(
        Output("summary", "children"),
        Input("wh_warehouse", "value"),
        Input("wh_category_filter", "value"),
        Input("wh_item_filter", "value"),
        Input("f_date", "start_date"),
        Input("f_date", "end_date"),
        Input("auto_reload", "n_intervals"),
    )
    def summary(w, c, i, sd, ed, _):

        df = apply_filters(TX_DF_GLOBAL, w, c, i, sd, ed)

        if df.empty:
            return dbc.Alert("No data available", color="warning")

        # ===== KPI CALCULATIONS =====
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
            dbc.Col(dbc.Alert(f"📥 Stock In: {stock_in:,.2f}", color="success", className="text-center fw-bold"), md=3),
            dbc.Col(dbc.Alert(f"📤 Stock Out: {stock_out:,.2f}", color="danger", className="text-center fw-bold"), md=3),
            dbc.Col(dbc.Alert(f"📦 Available Qty: {stock_bal:,.2f}", color="info", className="text-center fw-bold"), md=3),
            dbc.Col(dbc.Alert(f"💰 Available Stock Value: ₹ {available_stock_value:,.2f}", color="primary", className="text-center fw-bold"), md=3),
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
            style_header={"fontWeight": "bold"},
            style_data_conditional=[
                {
                    "if": {"filter_query": "{Available Stock} < 0"},
                    "backgroundColor": "#f8d7da",
                    "color": "black"
                }
            ],
        )
        return dbc.Container([kpi_row, html.Hr(), table], fluid=True)

    @app.callback(
        Output("kpi_stock_value","children"),
        Output("kpi_consumption","children"),
        Output("kpi_dead","children"),
        Output("kpi_urgent","children"),
        Input("auto_reload","n_intervals")
    )
    def update_top_kpis(_):

        df = TX_DF_GLOBAL.copy()

        if df.empty:
            return "₹ 0","0","₹ 0","0"

        # ===============================
        # 1️⃣ AVAILABLE STOCK VALUE
        # ===============================
        grp = (
            df.groupby(COL_ITEM, as_index=False)
            .agg(
                qty_in=("qty_in","sum"),
                qty_out=("qty_out_base","sum"),
                price=(COL_UNIT_PRICE,"mean"),
                tax=(COL_TAX,"max")
            )
        )

        grp["available"] = grp["qty_in"] - grp["qty_out"]

        grp["stock_value"] = (
            grp["available"]
            * grp["price"]
            * (1 + grp["tax"]/100)
        )

        stock_val = grp["stock_value"].sum()

        # ===============================
        # 2️⃣ MONTHLY CONSUMPTION
        # ===============================
        cons = df[df["source"]=="INDENT"]["qty_out_base"].sum()

        # ===============================
        # 3️⃣ DEAD STOCK VALUE (60+ days)
        # ===============================
        aging = build_global_aging(
            df,
            pd.Timestamp.today()
        )


        if not aging.empty:
            dead_items = aging[aging["Age Days"] > 60]["Item"].unique()
            dead_df = grp[grp[COL_ITEM].isin(dead_items)]
            dead_val = dead_df["stock_value"].sum()
        else:
            dead_val = 0

        # ===============================
        # 4️⃣ URGENT ORDERS
        # ===============================
        plan = build_order_planning(df)

        urgent = 0
        if not plan.empty:
            urgent = (plan["Urgency"]=="CRITICAL").sum()

        # ===============================
        return (
            f"₹ {stock_val:,.0f}",
            f"{cons:,.0f}",
            f"₹ {dead_val:,.0f}",
            f"{urgent}"
        )

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
            TX_DF_GLOBAL,
            ed
        )


    # ========== AGING ==========
    @app.callback(
        Output("aging_table", "data"),
        Input("wh_category_filter", "value"),
        Input("wh_item_filter", "value"),
        Input("f_date", "end_date"),
        Input("selected_aging_bucket", "data"),
        Input("auto_reload", "n_intervals"),
    )
    def update_aging_table(c, i, ed, bucket, _):

        if not ed:
            raise PreventUpdate

        # =============================
        # 1️⃣ BUILD AGING (GLOBAL ONLY)
        # =============================
        aging_df = build_global_aging(
            TX_DF_GLOBAL,
            ed
        )

        if aging_df.empty:
            return []

        # =============================
        # 2️⃣ AGING BUCKET FILTER
        # =============================
        if bucket:
            aging_df = aging_df[
                aging_df["Aging Bucket"] == bucket
            ]

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

            aging_df = aging_df[
                aging_df["Item"].isin(valid_items)
            ]

        # =============================
        # 4️⃣ ITEM FILTER
        # =============================
        if i:
            aging_df = aging_df[
                aging_df["Item"] == i
            ]

        return aging_df.to_dict("records")

    # ========== OUTLET ==========
    @app.callback(
        Output("outlet", "children"),
        Input("wh_warehouse", "value"),
        Input("wh_category_filter", "value"),
        Input("wh_item_filter", "value"),
        Input("f_date", "start_date"),
        Input("f_date", "end_date"),
        Input("auto_reload", "n_intervals"),
    )
    def outlet(w, c, i, sd, ed, _):

        df = apply_filters(TX_DF_GLOBAL, w, c, i, sd, ed)

        # ================= EMPTY SAFE =================
        if df is None or df.empty:
            return dbc.Alert(
                "No outlet consumption data for selected filters",
                color="warning"
            )
        df = df.copy()
        df["qty_out"] = pd.to_numeric(df["qty_out"], errors="coerce").fillna(0)
        df = df[df["qty_out"] > 0]
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
        Input("wh_item_filter", "value"),
        Input("f_date", "start_date"),
        Input("f_date", "end_date"),
        Input("auto_reload", "n_intervals"),
    )
    def ledger(item, sd, ed, _):

        # 🔴 HARD RULE — item mandatory
        if not item:
            return dbc.Alert(
                "⚠️ Please select an Item to view Global Ledger",
                color="warning"
            )

        # ✅ ALWAYS USE GLOBAL DATA
        df = apply_filters(TX_DF_GLOBAL, None, None, item, sd, ed)

        if df is None or df.empty:
            return dbc.Alert("No ledger data available", color="warning")

        # ✅ USE GLOBAL OPENING
        ledger_df = build_global_ledger(
            df,
            OPENING_DF_GLOBAL
        )

        if ledger_df.empty:
            return dbc.Alert("No ledger data available", color="warning")

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

        # ================= TABLE =================
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

        # ✅ ALWAYS USE GLOBAL SQL DATA
        expiry_df = load_expiry_sql()
        tx_df = TX_DF_GLOBAL.copy()

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
        # 📊 EXPIRY AGING CHART
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
        # 🧊 EXPIRY ALERT TABLE
        # =====================================================
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
                            "fontWeight": "bold",
                        },

                        # 🔴 CRITICAL
                        {
                            "if": {"filter_query": '{Status} contains "Critical"'},
                            "backgroundColor": "#f8d7da",
                            "fontWeight": "bold",
                        },

                        # 🟠 MAJOR
                        {
                            "if": {"filter_query": '{Status} contains "Major"'},
                            "backgroundColor": "#fff3cd",
                        },

                        # 🟡 MINOR
                        {
                            "if": {"filter_query": '{Status} contains "Minor"'},
                            "backgroundColor": "#cff4fc",
                        },

                        # OK
                        {
                            "if": {"filter_query": '{Status} = "OK"'},
                            "backgroundColor": "#e9ecef",
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

        # ✅ ALWAYS USE GLOBAL DATA
        df = TX_DF_GLOBAL.copy()

        if df.empty:
            return []

        # 🔥 BUILD ORDER PLAN
        global ORDER_PLAN_CACHE

        if "ORDER_PLAN_CACHE" not in globals():
            ORDER_PLAN_CACHE = build_order_planning(df)

        plan_df = ORDER_PLAN_CACHE

        if plan_df.empty:
            return []

        # Hide internal calculation columns
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

        # ✅ ALWAYS USE GLOBAL SQL DATA
        expiry_df = load_expiry_sql()
        tx_df = TX_DF_GLOBAL.copy()

        df = build_expiry_alerts(
            expiry_df,
            tx_df,
            days=60
        )

        if df.empty:
            return []

        # ================= BUCKET FILTER =================
        if bucket:
            limits = {
                "0–10": (0, 10),
                "11–30": (11, 30),
                "31–60": (31, 60),
                "60+": (61, 10000),
            }
            lo, hi = limits.get(bucket, (None, None))

            if lo is not None:
                df = df[(df["Days Left"] >= lo) & (df["Days Left"] <= hi)]

        return df.to_dict("records")


    @app.callback(
        Output("download_po_excel", "data"),
        Input("export_po_btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_purchase_order(n):

        # 🔴 ALWAYS USE GLOBAL SQL DATA
        tx_df = TX_DF_GLOBAL.copy()

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

        # 🔴 ALWAYS use GLOBAL SQL DATA
        tx_df = TX_DF_GLOBAL.copy()

        # 🔴 Ensure opening stock loaded
        global opening_stock_df
        if "opening_stock_df" not in globals() or opening_stock_df is None:
            opening_stock_df = load_opening_stock_sql()

        if opening_stock_df is None or opening_stock_df.empty:
            raise PreventUpdate

        out = export_full_stock_excel(tx_df, opening_stock_df, sd, ed)

        if out is None:
            raise PreventUpdate

        return dcc.send_bytes(
            out.read(),
            "Global_Stock_Ledger_All_Items.xlsx"
        )

    @app.callback(
        Output("wastage_tab", "children"),
        Input("wh_warehouse", "value"),
        Input("f_date", "start_date"),
        Input("f_date", "end_date"),
        Input("auto_reload", "n_intervals"),
    )
    def wastage_tab(wh, sd, ed, _):

        # 🔴 ALWAYS USE GLOBAL SQL DATA
        df = TX_DF_GLOBAL.copy()

        if df.empty:
            return dbc.Alert("No wastage data available", color="warning")

        # ================= FILTER =================
        if wh:
            df = df[df[COL_WAREHOUSE] == wh]

        if sd:
            df = df[df[COL_DATE] >= pd.to_datetime(sd)]

        if ed:
            df = df[df[COL_DATE] <= pd.to_datetime(ed)]

        # ================= BUILD REPORT =================
        report = build_wastage_report(df, sd, ed)

        if report.empty:
            return dbc.Alert("No wastage found in selected period", color="success")

        # ================= TABLE =================
        table = dash_table.DataTable(
            data=report.to_dict("records"),
            columns=[{"name": c, "id": c} for c in report.columns],
            page_size=12,
            sort_action="native",
            filter_action="native",
            style_header={"fontWeight": "bold"},
            style_cell={"fontSize": "13px", "padding": "6px"},
            style_data_conditional=[
                {
                    "if": {"filter_query": "{Wastage_Value} > 0"},
                    "backgroundColor": "#f8d7da",
                    "color": "black",
                }
            ]
        )

        total_loss = report["Wastage_Value"].sum()

        # ================= UI =================
        return dbc.Container([
            dbc.Alert(
                f"💸 Total Wastage Loss: ₹ {total_loss:,.2f}",
                color="danger",
                className="fw-bold text-center"
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

        # 🔴 NEVER reload CSV again
        df = TX_DF_GLOBAL.copy()

        if df.empty:
            return []

        # date filter
        if sd:
            df = df[df[COL_DATE] >= pd.to_datetime(sd)]
        if ed:
            df = df[df[COL_DATE] <= pd.to_datetime(ed)]

        report = build_wastage_report(df, sd, ed)

        if report.empty:
            return []

        return report.to_dict("records")

    @app.callback(
        Output("consumption_tab", "children"),
        Input("f_warehouse", "value"),
        Input("auto_reload", "n_intervals")
    )
    def update_consumption_tab(warehouse, _):

        df = TX_DF_GLOBAL.copy()

        if df.empty:
            return dbc.Alert("No consumption data found.", color="warning")

        # ================= CONSUMPTION ONLY =================
        cons = df[df.get("source","") == "INDENT"].copy()

        if warehouse:
            cons = cons[cons[COL_WAREHOUSE] == warehouse]

        if cons.empty:
            return dbc.Alert("No consumption records found", color="warning")

        # ================= BUILD SIMPLE REPORT =================
        report = (
            cons.groupby(
                [COL_WAREHOUSE, COL_OUTLET, COL_ITEM],
                as_index=False
            )
            .agg(
                Total_Qty=("qty_out_base","sum"),
                Total_Value=(COL_UNIT_PRICE,"sum")
            )
        )

        report["Total_Value"] = (
            report["Total_Qty"] *
            report["Total_Value"]
        )

        report = report.rename(columns={
            COL_WAREHOUSE: "Warehouse",
            COL_OUTLET: "Outlet",
            COL_ITEM: "Item"
        }).sort_values("Total_Value", ascending=False)

        # ================= TABLE =================
        table = dash_table.DataTable(
            id="consumption_table",
            data=report.round(2).to_dict("records"),
            columns=[{"name": c, "id": c} for c in report.columns],
            page_size=25,
            sort_action="native",
            filter_action="native",
            style_table={"overflowX": "auto"},
            style_header={"fontWeight": "bold"},
            style_cell={"fontSize":"13px","padding":"6px"},
        )

        total = report["Total_Value"].sum()

        return dbc.Container([
            dbc.Alert(
                f"💰 Total Consumption Value: ₹ {total:,.0f}",
                color="primary",
                className="fw-bold text-center"
            ),
            html.Hr(),
            table
        ], fluid=True)


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

    if df is None or df.empty:
        return

    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = os.getenv("SMTP_PORT")
    alert_to  = os.getenv("ALERT_TO")

    if not all([smtp_user, smtp_pass, smtp_host, smtp_port, alert_to]):
        print("❌ SMTP configuration missing")
        return

    try:
        msg = MIMEMultipart()
        msg["From"] = smtp_user
        msg["To"] = alert_to

        cc = resolve_cc_by_warehouse(df)
        if cc:
            msg["Cc"] = cc

        msg["Subject"] = subject
        msg.attach(MIMEText(html_body, "html"))

        attach_excel(msg, df, filename)

        recipients = alert_to.split(",") + (cc.split(",") if cc else [])

        with smtplib.SMTP(smtp_host, int(smtp_port)) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(msg["From"], recipients, msg.as_string())

        print(f"✅ Email sent: {subject}")

    except Exception as e:
        print("❌ Mail send error:", e)

def send_expiry_alerts(days=60):

    expiry_df = load_expiry_sql()
    tx_df = TX_DF_GLOBAL.copy()

    if expiry_df.empty:
        return

    today = pd.Timestamp.today().normalize()

    df = expiry_df.copy()
    df["Expiry Date"] = pd.to_datetime(df["Expiry Date"], errors="coerce")
    df["Days Left"] = (df["Expiry Date"] - today).dt.days

    df = df[
        (df["Days Left"] >= -10) &
        (df["Days Left"] <= days)
    ]

    if df.empty:
        return

    stock_map = get_available_stock_map(tx_df)

    df["Available Stock"] = df["Item Name"].astype(str).str.strip().map(
        stock_map
    ).fillna(0)

    def alert_level(d):
        if d <= 15:
            return "🔴 Critical"
        if d <= 30:
            return "🟠 Major"
        if d <= 60:
            return "🟡 Minor"
        return "OK"

    df["Alert Level"] = df["Days Left"].apply(alert_level)

    html = df.to_html(index=False)

    send_mail_with_excel(
        subject=f"⚠️ Expiry Alert ≤ {days} Days",
        html_body=html,
        df=df,
        filename="Expiry_Alert.xlsx"
    )

def send_negative_stock_mail():

    tx_df = TX_DF_GLOBAL.copy()

    neg = build_negative_summary(tx_df)

    if neg.empty:
        return

    send_mail_with_excel(
        subject="🔴 Negative Stock Alert",
        html_body=neg.to_html(index=False),
        df=neg,
        filename="Negative_Stock.xlsx"
    )

def send_available_stock_snapshot():

    tx_df = TX_DF_GLOBAL.copy()

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

    send_mail_with_excel(
        subject="📦 Month End Available Stock",
        html_body=stock.to_html(index=False),
        df=stock,
        filename="Month_End_Stock.xlsx"
    )

for days, minute in [(60,0), (30,5), (15,10)]:
# ================= ENTERPRISE AUTO MAIL =================

# ================= AUTO MAIL SCHEDULER =================

    if not scheduler.get_jobs():

        scheduler.add_job(
            send_expiry_alerts,
            "cron",
            hour=9,
            minute=0
        )

        scheduler.add_job(
            send_negative_stock_mail,
            "cron",
            hour=9,
            minute=5
        )

        scheduler.add_job(
            send_available_stock_snapshot,
            "cron",
            day="last",
            hour=9,
            minute=10
        )
        print("✅ Scheduler started (no duplicate jobs)")