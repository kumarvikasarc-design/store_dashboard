# expiry_cogs_page.py
# Inventory & COGS Dashboard — Full Revised File (patched)
# - Smart CSV loaders
# - Date detection fix (safe filtering)
# - Deployment Name -> Outlet Name normalization (aliases)
# - Advanced caching + filtering
# - KPI summary
# - Heatmap builder

import os
import glob
import numpy as np
import pandas as pd
from functools import lru_cache
from datetime import datetime
from dash import html, dcc, dash_table

FONT_FAMILY = "Times New Roman"

BASE_DIR = r"C:\Users\ACER\store_dashboard"

# CSV PATHS (updated to match your provided paths)
PATH_ENTRY_REPORT    = os.path.join(BASE_DIR, "inventory", "entry_report", "*.csv")
PATH_EXPIRY_REPORT   = os.path.join(BASE_DIR, "inventory", "expiryreport", "*.csv")
PATH_VARIANCE_REPORT = os.path.join(BASE_DIR, "inventory", "varience_report", "*.csv")
PATH_RECIPES         = os.path.join(BASE_DIR, "inventory", "recipe", "*.csv")
PATH_WASTAGE         = os.path.join(BASE_DIR, "inventory", "wastage", "*.csv")
PATH_PHYSICAL        = os.path.join(BASE_DIR, "inventory", "physicalstock", "*.csv")
PATH_CONSUMPTION     = os.path.join(BASE_DIR, "inventory", "consumption", "*.csv")
PATH_ITEM_SALES      = os.path.join(BASE_DIR, "item_sales", "*.csv")

STORES_FILE = os.path.join(BASE_DIR, "stores_db.csv")
pd.set_option('future.no_silent_downcasting', True)

# -----------------------------------------------------------
# SAFE READERS
# -----------------------------------------------------------

def safe_read_list(pattern, dtype=None, usecols=None):
    # recursive glob + csv / CSV
    files = glob.glob(pattern, recursive=True)
    files += glob.glob(pattern.replace("*.csv", "*.CSV"), recursive=True)

    print("LOADING:", pattern, "→", len(files), "files")

    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        try:
            tmp = pd.read_csv(f, low_memory=False, encoding="utf-8")
        except Exception:
            try:
                tmp = pd.read_csv(f, low_memory=False, engine="python")
            except Exception as e:
                print("FAILED:", f, e)
                continue

        tmp["_source_file"] = os.path.basename(f)
        dfs.append(tmp)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def safe_read_csv(path, dtype=None, usecols=None):
    if not path or not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=dtype, usecols=usecols, engine="python", low_memory=False)
    except Exception:
        try:
            return pd.read_csv(path, engine="python", low_memory=False)
        except Exception:
            return pd.DataFrame()

# -----------------------------------------------------------
# BASIC HELPERS
# -----------------------------------------------------------

def clean_colnames(cols):
    return [" ".join(str(c).strip().split()) for c in cols]


def safe_num(df, col):
    if df is None or df.empty or col not in df.columns:
        return pd.Series([0] * (len(df) if df is not None else 0), dtype="float64")
    s = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
    s = s.replace({"": "0", "nan": "0", "None": "0"})
    return pd.to_numeric(s, errors="coerce").fillna(0)


def normalize_unit_qty(df, unit_candidates=None, qty_candidates=None):
    if df is None or df.empty:
        return df
    df = df.copy()
    unit_candidates = unit_candidates or ["Unit", "Units", "Unit "]
    qty_candidates = qty_candidates or ["Quantity", "Qty", "Quantity "]
    unit_col = next((c for c in unit_candidates if c in df.columns), None)
    qty_col = next((c for c in qty_candidates if c in df.columns), None)
    df["Unit"] = df[unit_col] if unit_col else ""
    df["Quantity"] = safe_num(df, qty_col) if qty_col else 0
    return df


def fmt_inr(v):
    try:
        return "₹ {:,.2f}".format(float(v))
    except Exception:
        return str(v)

# -----------------------------------------------------------
# OUTLET NORMALIZATION HELPERS
# -----------------------------------------------------------

def normalize_outlet_name(name):
    """
    Normalize many common variations to canonical outlet names.
    Currently handles GK2 / GK II variants and similar patterns.
    You can expand this function to add more alias rules.
    """
    try:
        if not isinstance(name, str):
            return name
        n = name.strip()
        if n == "":
            return ""
        # uppercase for pattern checks
        nu = n.upper().replace(".", "").replace("-", " ").replace("_", " ").strip()
        # remove double spaces
        while "  " in nu:
            nu = nu.replace("  ", " ")

        # GK variants -> canonical name
        gk_variants = ["GK2", "GK 2", "GKII", "GK II", "G K 2", "G K II", "GK-2", "GK_II", "GKII."]
        if any(v in nu for v in gk_variants):
            # preserve full prefix if it's already like 'Coffee Island GK 2' or 'Coffee Island GK II'
            # If original contains 'COFFEE' likely already full name; otherwise return canonical full name
            if "COFFEE" in nu and "GK" in nu:
                # try to preserve the beginning of the string
                # but still normalize GK part
                # Example: "COFFEE ISLAND GK2" -> "Coffee Island GK II"
                return "Coffee Island GK II"
            return "Coffee Island GK II"

        # Generic digit-to-roman/spacing normalization examples (expand as needed)
        # e.g., "STORE 5" vs "STORE V" not implemented by default
        return n
    except Exception:
        return name

def normalize_outlet(df):
    """
    STRICT outlet normalization:
    ONLY Deployment Name is used as Outlet Name.
    StoreKitchen Name is completely ignored.
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    df.columns = clean_colnames(df.columns)

    if "Deployment Name" in df.columns:
        df["Outlet Name"] = df["Deployment Name"].astype(str).str.strip()
    elif "Outlet Name" in df.columns:
        df["Outlet Name"] = df["Outlet Name"].astype(str).str.strip()
    else:
        df["Outlet Name"] = ""

    df["Outlet Name"] = df["Outlet Name"].apply(normalize_outlet_name)
    return df

# -----------------------------------------------------------
# DATE DETECTOR — NEW CRITICAL FIX
# -----------------------------------------------------------

def detect_date_column(df):
    """
    Detect the most likely date column.
    Returns COLUMN NAME or None.
    """
    if df is None or df.empty:
        return None

    candidates = [
        "Date", "Opening Date", "Closing Date",
        "Invoice Date", "Expiry Date", "Final Expiry",
        "CreatedOn", "Posted Date", "Tran Date",
        "Stock Date", "Bill Date", "GRN Date", "Transaction Date"
    ]

    # 1️⃣ Exact match
    for c in df.columns:
        if str(c).strip().lower() in [x.lower() for x in candidates]:
            return c

    # 2️⃣ Keyword match
    for c in df.columns:
        low = str(c).lower()
        if any(k in low for k in ("date", "expiry", "opening", "closing", "invoice", "tran", "posted")):
            return c

    # 3️⃣ Best parsable column
    best_col = None
    best_count = 0

    for c in df.columns:
        try:
            parsed = pd.to_datetime(df[c], dayfirst=True, errors="coerce")
            cnt = parsed.notna().sum()
            if cnt > best_count:
                best_count = cnt
                best_col = c
        except Exception:
            continue

    return best_col
    
# -----------------------------------------------------------
# CACHED LOADERS
# -----------------------------------------------------------

@lru_cache(maxsize=1)
def load_stores_cached():
    df = safe_read_csv(STORES_FILE)
    if df is None or df.empty:
        # return empty with expected columns
        return pd.DataFrame(columns=["Brand","Region","City","State","Type","Outlet Name"])
    df = df.copy()
    df.columns = clean_colnames(df.columns)
    # pick a column to be "Outlet Name"
    if "Outlet Name" in df.columns:
        df["Outlet Name"] = df["Outlet Name"].astype(str).str.strip()
    elif "Deployment Name" in df.columns:
        df["Outlet Name"] = df["Deployment Name"].astype(str).str.strip()
    else:
        df["Outlet Name"] = ""
    # normalize outlet names
    df["Outlet Name"] = df["Outlet Name"].apply(normalize_outlet_name)

    for c in ["Brand", "Region", "City", "State", "Type"]:
        df[c] = df[c].astype(str).fillna("").str.strip() if c in df.columns else ""

    return df.fillna("")


@lru_cache(maxsize=1)
def load_entry_cached():
    df = safe_read_list(PATH_ENTRY_REPORT)
    if df.empty:
        return df
    df = df.copy()
    df.columns = clean_colnames(df.columns)
    df = normalize_outlet(df)
    df = normalize_unit_qty(df)

    for n in ["Unit Price", "Amount", "GST/IGST Value", "Total", "Quantity"]:
        if n in df.columns:
            df[n] = safe_num(df, n)

    col = detect_date_column(df)
    if col:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    return df


@lru_cache(maxsize=1)
def load_variance_cached():
    df = safe_read_list(PATH_VARIANCE_REPORT)
    if df.empty:
        return df
    df = df.copy()
    df.columns = clean_colnames(df.columns)
    df = normalize_outlet(df)

    for n in ["Opening Qty","Consumption Qty","Closing Qty","Variance Qty",
              "Average Price","COGS_Amount","Variance Percent"]:
        if n in df.columns:
            df[n] = safe_num(df, n)

    col = detect_date_column(df)
    if col:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    return df


@lru_cache(maxsize=1)
def load_wastage_cached():
    df = safe_read_list(PATH_WASTAGE)
    if df.empty:
        return df
    df = df.copy()
    df.columns = clean_colnames(df.columns)
    df = normalize_outlet(df)
    df = normalize_unit_qty(df)

    for n in ["Unit Price", "Amount", "Quantity"]:
        if n in df.columns:
            df[n] = safe_num(df, n)

    col = detect_date_column(df)
    if col:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    return df


@lru_cache(maxsize=1)
def load_physical_cached():
    df = safe_read_list(PATH_PHYSICAL)
    if df.empty:
        return df
    df = df.copy()
    df.columns = clean_colnames(df.columns)
    df = normalize_outlet(df)

    if "Physical Qty" in df.columns:
        df["Physical Qty"] = safe_num(df, "Physical Qty")

    df = normalize_unit_qty(df)

    if "Unit Price" in df.columns:
        df["Unit Price"] = safe_num(df, "Unit Price")

    col = detect_date_column(df)
    if col:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    return df


@lru_cache(maxsize=1)
def load_consumption_cached():
    df = safe_read_list(PATH_CONSUMPTION)
    if df.empty:
        return df
    df = df.copy()
    df.columns = clean_colnames(df.columns)
    df = normalize_outlet(df)

    rename_cols = {
        "Total (Stock Out + Consumption Qty)": "Total Qty",
        "Latest Physical Qty": "Physical Qty",
    }
    df.rename(columns={k:v for k,v in rename_cols.items() if k in df.columns}, inplace=True)

    for n in ["Opening Qty","Purchase Qty","Consumption Qty","Closing Qty",
              "Average Price","Total Qty","Wastage Qty"]:
        if n in df.columns:
            df[n] = safe_num(df, n)

    if "Consumption Qty" in df.columns and "Average Price" in df.columns:
        df["Consumption Value"] = df["Consumption Qty"] * df["Average Price"]

    col = detect_date_column(df)
    if col:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    return df


@lru_cache(maxsize=1)
def load_item_sales_cached():
    df = safe_read_list(PATH_ITEM_SALES)
    if df.empty:
        return df
    df = df.copy()
    df.columns = clean_colnames(df.columns)

    if "Deployment Name" in df.columns and "Outlet Name" not in df.columns:
        df["Outlet Name"] = df["Deployment Name"].astype(str).str.strip()

    # normalize outlet names
    if "Outlet Name" in df.columns:
        df["Outlet Name"] = df["Outlet Name"].apply(normalize_outlet_name)

    for n in ["Rate","Item Qty","Constituent Qty","Discount","Total Qty"]:
        if n in df.columns:
            df[n] = safe_num(df, n)

    if "Item Qty" in df.columns and "Constituent Qty" in df.columns:
        df["Total Qty"] = df["Item Qty"] + df["Constituent Qty"]

    if "Rate" in df.columns and "Item Qty" in df.columns:
        df["Amount"] = df["Rate"] * df["Item Qty"]

    if "Discount" in df.columns:
        df["Net Amount"] = df["Amount"] - df["Discount"]
    else:
        df["Net Amount"] = df.get("Amount", 0)

    col = detect_date_column(df)
    if col:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    return df


@lru_cache(maxsize=1)
def load_expiry_cached():
    df = safe_read_list(PATH_EXPIRY_REPORT)
    if df.empty:
        return df
    df = df.copy()
    df.columns = clean_colnames(df.columns)
    df = normalize_outlet(df)

    if "Qty" in df.columns:
        df["Qty"] = safe_num(df, "Qty")

    col = detect_date_column(df)
    if col:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

    return df


@lru_cache(maxsize=1)
def load_recipes_cached():
    df = safe_read_list(PATH_RECIPES)
    if df.empty:
        return df
    df.columns = clean_colnames(df.columns)
    return df

# ----------------------------------------------------------------
# CACHE HEALTH CHECKER
# ----------------------------------------------------------------

def _files_exist_for_pattern(pattern):
    return len(glob.glob(pattern)) > 0


def ensure_caches_are_fresh():
    need_clear = False
    loaders = [
        load_stores_cached, load_entry_cached, load_variance_cached, load_wastage_cached,
        load_physical_cached, load_consumption_cached, load_item_sales_cached, load_expiry_cached
    ]

    for loader in loaders:
        try:
            df = loader()
            # if file exists but df empty → clear cache
            # check underlying filesystem for presence of files
            # We cannot know the pattern from loader here reliably, so be conservative:
            if isinstance(df, pd.DataFrame) and df.empty:
                need_clear = True
        except Exception:
            need_clear = True

    if need_clear:
        for loader in loaders:
            try:
                loader.cache_clear()
            except Exception:
                pass

# ----------------------------------------------------------------
# FILTER ENGINE — Includes fixed date logic
# ----------------------------------------------------------------

def cached_filter(
    start_date,
    end_date,
    stores_df,
    brands=None,
    regions=None,
    states=None,
    cities=None,
    types=None,
    outlets=None,
    items=None,
    cats=None,
    supers=None,
    debug=False,
):
    """
    FINAL FILTER ENGINE — Inventory & COGS
    ✔ Correct Outlet Name handling
    ✔ Safe date filtering (won't drop data)
    ✔ Deployment Name = Outlet Name
    ✔ Works with negative stock
    """

    # ----------------------------
    # Load cached datasets
    # ----------------------------
    data = {
        "variance": load_variance_cached(),
        "consumption": load_consumption_cached(),
        "wastage": load_wastage_cached(),
        "purchases": load_entry_cached(),
        "physical": load_physical_cached(),
        "item_sales": load_item_sales_cached(),
        "expiry": load_expiry_cached(),
    }

    if debug:
        return data

    # ----------------------------
    # Date handling (SAFE)
    # ----------------------------
    sd = pd.to_datetime(start_date, errors="coerce")
    ed = pd.to_datetime(end_date, errors="coerce")

    def fdate(df):
        if df is None or df.empty:
            return df

        col = detect_date_column(df)
        if not col or col not in df.columns:
            return df

        parsed = pd.to_datetime(df[col], dayfirst=True, errors="coerce")

        # 🚑 CRITICAL FIX — do NOT drop if no valid dates
        if parsed.notna().sum() == 0:
            return df

        if sd is not pd.NaT:
            df = df.loc[parsed >= sd]
        if ed is not pd.NaT:
            df = df.loc[parsed <= ed]

        return df

    # ----------------------------
    # Generic column filter
    # ----------------------------
    def f(df, col, vals):
        if df is None or df.empty or not vals or col not in df.columns:
            return df
        vals = {str(v) for v in vals}
        return df[df[col].astype(str).isin(vals)]

    # ----------------------------
    # Store filtering (MASTER FIX)
    # ----------------------------
    st = stores_df.copy() if isinstance(stores_df, pd.DataFrame) else pd.DataFrame()

    st = f(st, "Brand", brands)
    st = f(st, "Region", regions)
    st = f(st, "State", states)
    st = f(st, "City", cities)
    st = f(st, "Type", types)
    st = f(st, "Outlet Name", outlets)

    allowed_outlets = (
        {x for x in st["Outlet Name"].astype(str).unique() if x.strip()}
        if not st.empty and "Outlet Name" in st.columns
        else None
    )

    # ----------------------------
    # Outlet filter — ALWAYS Outlet Name
    # ----------------------------
    def fout(df):
        if df is None or df.empty or not allowed_outlets:
            return df
        if "Outlet Name" not in df.columns:
            return df
        return df[
            df["Outlet Name"]
            .astype(str)
            .apply(normalize_outlet_name)
            .isin(allowed_outlets)
        ]

    # ----------------------------
    # Apply filters
    # ----------------------------
    out = {}

    for key, df in data.items():
        if df is None or df.empty:
            out[key] = df
            continue

        df2 = fdate(df)
        df2 = fout(df2)

        # Item / category filters
        if key in ("variance", "consumption", "item_sales", "expiry"):
            df2 = f(df2, "Super Category Name", supers)
            df2 = f(df2, "Category Name", cats)
            df2 = f(df2, "Item Name", items)

        out[key] = df2

    return out

# -----------------------------------------------------------
# TABLE + KPI UI HELPERS
# -----------------------------------------------------------

def safe_table(df, page_size=20, height="520px"):
    if df is None or df.empty:
        return html.Div("No data available.", style={"padding":"10px","color":"#777"})
    df2 = df.copy()
    df2.columns = clean_colnames(df2.columns)
    df2 = df2.replace([None, np.nan, float("inf"), -float("inf")], "").fillna("")

    # convert datetime to string
    for c in df2.columns:
        try:
            if pd.api.types.is_datetime64_any_dtype(df2[c]):
                df2[c] = df2[c].dt.strftime("%d-%b-%Y")
        except Exception:
            pass

    # ensure string
    for c in df2.columns:
        try:
            df2[c] = df2[c].astype(str)
        except Exception:
            df2[c] = df2[c].astype(str)

    columns = [{"name": c, "id": c} for c in df2.columns]

    return dash_table.DataTable(
        data=df2.to_dict("records"),
        columns=columns,
        page_size=page_size,
        filter_action="native",
        sort_action="native",
        style_table={"overflowX": "auto", "maxHeight": height},
        style_cell={"fontFamily":FONT_FAMILY, "fontSize":"12px", "whiteSpace":"nowrap", "padding":"6px"},
        style_header={"fontWeight":"700", "backgroundColor":"#f7f7f7"},
    )


def kpi_card(title, value, subtitle="", icon="📊"):
    # include data-value attribute to allow number animator to find numeric values
    return html.Div(
        style={"flex":"1","padding":"12px","borderRadius":"10px","border":"1px solid #e6e6e6","background":"#fff"},
        children=[
            html.Div([
                html.Span(icon, style={"fontSize":"18px","marginRight":"6px"}),
                html.Span(title, style={"fontSize":"14px","fontWeight":"600"}),
            ]),
            html.Div(str(value), style={"fontSize":"18px","fontWeight":"700","marginTop":"6px"}, **{"data-value": str(value), "className": "kpi-animate-number"}),
            html.Div(subtitle, style={"fontSize":"11px","color":"#666","marginTop":"4px"}),
        ]
    )
# -----------------------------
# PART 3 — Tabs, Heatmap, Layout, Callbacks
# -----------------------------

import plotly.express as px
from dash import Input, Output, State, callback_context
import dash

# -----------------------------
# Tab builders (continued)
# -----------------------------

def _make_wastage_tab(staged):
    df = staged.get("wastage", pd.DataFrame())
    if df is None or df.empty:
        return html.Div("No wastage data available.", style={"padding":"10px","color":"#777"})
    df = df.copy()
    if "Quantity" in df.columns:
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    if "Unit Price" in df.columns:
        df["Unit Price"] = pd.to_numeric(df["Unit Price"], errors="coerce").fillna(0)
    if "Amount" not in df.columns and "Quantity" in df.columns and "Unit Price" in df.columns:
        df["Amount"] = df["Quantity"] * df["Unit Price"]
    df_sorted = df.sort_values("Quantity", ascending=False).reset_index(drop=True)
    total_qty = df_sorted["Quantity"].sum() if "Quantity" in df_sorted.columns else 0
    total_amt = df_sorted["Amount"].sum() if "Amount" in df_sorted.columns else 0
    kpis = html.Div([kpi_card("Wastage QTY", f"{total_qty:,.2f}", icon="🗑️"), kpi_card("Wastage Value", fmt_inr(total_amt), icon="💸")], style={"display":"flex","gap":"12px","flexWrap":"wrap"})
    cols = [c for c in ["Outlet Name","Date","Transaction Number","Item Code","Item Name","Category Name","Super Category Name","Quantity","Unit","Unit Price","Amount","Comment"] if c in df_sorted.columns]
    table = safe_table(df_sorted[cols], page_size=15)
    return html.Div([kpis, html.H5("Wastage Details"), table], style={"display":"flex","flexDirection":"column","gap":"12px"})


def _make_grn_tab(staged):
    df = staged.get("purchases", pd.DataFrame())
    if df is None or df.empty:
        return html.Div("No GRN / Purchase data for the selected filters.", style={"color":"#777"})
    df = df.copy()
    if "Deployment Name" in df.columns and "Outlet Name" not in df.columns:
        df["Outlet Name"] = df["Deployment Name"].astype(str)
    for n in ["Quantity","Unit Price","Amount","GST/IGST Value","Total"]:
        if n in df.columns:
            df[n] = pd.to_numeric(df[n], errors="coerce").fillna(0)
    if "Amount" not in df.columns and "Quantity" in df.columns and "Unit Price" in df.columns:
        df["Amount"] = df["Quantity"] * df["Unit Price"]
    if "Total" not in df.columns and "Amount" in df.columns:
        gst_col = "GST/IGST Value" if "GST/IGST Value" in df.columns else None
        df["Total"] = df["Amount"] + df[gst_col].fillna(0) if gst_col else df["Amount"]
    total_grn = df["Total"].sum() if "Total" in df.columns else df["Amount"].sum() if "Amount" in df.columns else 0
    kpi = html.Div([kpi_card("GRN Value", fmt_inr(total_grn), icon="📥")], style={"display":"flex","gap":"12px"})
    cols = [c for c in ["Outlet Name","Vendor Name","Date","Transaction Number","Invoice Number","PO Number","Invoice Date","Item Code","Item Name","Quantity","Unit","Unit Price","Amount","GST/IGST Rate","GST/IGST Value","Total"] if c in df.columns]
    table = safe_table(df[cols], page_size=15)
    return html.Div([kpi, html.H5("GRN / Purchases"), table], style={"display":"flex","flexDirection":"column","gap":"12px"})


def _make_consumption_tab(staged):
    df = staged.get("consumption", pd.DataFrame())
    if df is None or df.empty:
        return html.Div("No consumption data available.", style={"padding":"10px","color":"#777"})
    df = df.copy()
    for n in ["Opening Qty","Purchase Qty","Consumption Qty","Closing Qty","Average Price","Total Qty","Wastage Qty","Stock Out Qty"]:
        if n in df.columns:
            df[n] = pd.to_numeric(df[n], errors="coerce").fillna(0)
    if "Consumption Qty" in df.columns and "Average Price" in df.columns:
        df["Consumption Value"] = df["Consumption Qty"] * df["Average Price"]
    display_df = df.copy()
    if "Consumption Qty" in display_df.columns:
        display_df = display_df[display_df["Consumption Qty"] > 0]
    total_consumption_qty = df["Consumption Qty"].sum() if "Consumption Qty" in df.columns else 0
    total_consumption_value = df["Consumption Value"].sum() if "Consumption Value" in df.columns else 0
    avg_price = (total_consumption_value / total_consumption_qty) if total_consumption_qty else 0
    total_stock_in = df["Purchase Qty"].sum() if "Purchase Qty" in df.columns else 0
    total_stock_out = df["Stock Out Qty"].sum() if "Stock Out Qty" in df.columns else 0
    kpis = html.Div([
        kpi_card("Total Consumption Qty", f"{total_consumption_qty:,.2f}", icon="📦"),
        kpi_card("Total Consumption Value", fmt_inr(total_consumption_value), icon="💰"),
        kpi_card("Avg Price", fmt_inr(avg_price), icon="⚖️"),
        kpi_card("Total Stock In", f"{total_stock_in:,.2f}", icon="⬆️"),
        kpi_card("Total Stock Out", f"{total_stock_out:,.2f}", icon="⬇️"),
    ], style={"display":"flex","gap":"12px","flexWrap":"wrap"})
    cols = [c for c in ["Outlet Name","Item Code","Item Name","Opening Qty","Purchase Qty","Consumption Qty","Average Price","Consumption Value","Closing Qty"] if c in display_df.columns]
    table = safe_table(display_df[cols], page_size=15)
    return html.Div([kpis, html.H5("Consumption Details"), table], style={"display":"flex","flexDirection":"column","gap":"12px"})


def _make_variance_tab(staged):
    df = staged.get("variance", pd.DataFrame())
    if df is None or df.empty:
        return html.Div("No variance data available.", style={"padding":"10px","color":"#777"})
    df = df.copy()
    for n in ["Variance Qty","Average Price","COGS_Amount","Variance Percent"]:
        if n in df.columns:
            df[n] = pd.to_numeric(df[n], errors="coerce").fillna(0)
    if "Variance Value" not in df.columns and "Variance Qty" in df.columns and "Average Price" in df.columns:
        df["Variance Value"] = df["Variance Qty"] * df["Average Price"]
    total_variance_value = df["Variance Value"].sum() if "Variance Value" in df.columns else 0
    kpis = html.Div([kpi_card("Total Variance Value", fmt_inr(total_variance_value), icon="⚖️")], style={"display":"flex","gap":"12px"})
    cols = [c for c in ["Outlet Name","Item Code","Item Name","Opening Qty","Consumption Qty","Closing Qty","Variance Qty","Variance Percent","Average Price","Variance Value"] if c in df.columns]
    table = safe_table(df[cols], page_size=15)
    return html.Div([kpis, html.H5("Variance Details"), table], style={"display":"flex","flexDirection":"column","gap":"12px"})


def _make_physical_tab(staged):
    df = staged.get("physical", pd.DataFrame())
    if df is None or df.empty:
        return html.Div("No physical stock data available.", style={"padding":"10px","color":"#777"})
    df = df.copy()
    if "Physical Qty" in df.columns:
        df["Physical Qty"] = pd.to_numeric(df["Physical Qty"], errors="coerce").fillna(0)
    if "Unit Price" in df.columns:
        df["Unit Price"] = pd.to_numeric(df["Unit Price"], errors="coerce").fillna(0)
    if "Physical Amt" not in df.columns and "Physical Qty" in df.columns and "Unit Price" in df.columns:
        df["Physical Amt"] = df["Physical Qty"] * df["Unit Price"]
    kpis = html.Div([kpi_card("Physical QTY", f"{df['Physical Qty'].sum():,.2f}" if "Physical Qty" in df.columns else "0")], style={"display":"flex","gap":"12px"})
    cols = [c for c in ["Outlet Name","Date","Item Code","Item Name","Physical Qty","Unit","Unit Price","Physical Amt"] if c in df.columns]
    table = safe_table(df[cols], page_size=15)
    return html.Div([kpis, html.H5("Physical Stock Details"), table], style={"display":"flex","flexDirection":"column","gap":"12px"})


def _make_item_sales_tab(staged):
    df = staged.get("item_sales", pd.DataFrame())
    if df is None or df.empty:
        return html.Div("No Item Sales data available.", style={"padding":"10px","color":"#777"})
    df = df.copy()
    for n in ["Rate","Item Qty","Constituent Qty","Discount","Total Qty"]:
        if n in df.columns:
            df[n] = pd.to_numeric(df[n], errors="coerce").fillna(0)
    if "Item Qty" in df.columns and "Constituent Qty" in df.columns:
        df["Total Qty"] = df["Item Qty"] + df["Constituent Qty"]
    if "Rate" in df.columns and "Item Qty" in df.columns:
        df["Amount"] = df["Rate"] * df["Item Qty"]
    if "Discount" in df.columns and "Amount" in df.columns:
        df["Net Amount"] = df["Amount"] - df["Discount"]
    elif "Amount" in df.columns:
        df["Net Amount"] = df["Amount"]
    kpis = html.Div([kpi_card("Total Qty", f"{df['Total Qty'].sum():,.2f}" if "Total Qty" in df.columns else "0"), kpi_card("Net Amount", fmt_inr(df["Net Amount"].sum() if "Net Amount" in df.columns else 0))], style={"display":"flex","gap":"12px"})
    cols = [c for c in ["Outlet Name","Item Code","Item Name","Rate","Item Qty","Constituent Qty","Total Qty","Amount","Discount","Net Amount"] if c in df.columns]
    table = safe_table(df[cols], page_size=15)
    return html.Div([kpis, html.H5("Item Sales"), table], style={"display":"flex","flexDirection":"column","gap":"12px"})


def _make_expiry_tab(staged):
    df = staged.get("expiry", pd.DataFrame())
    if df is None or df.empty:
        return html.Div("No expiry report available.", style={"padding":"10px","color":"#777"})
    df = df.copy()
    if "Qty" in df.columns:
        df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0)
    total_qty = df["Qty"].sum() if "Qty" in df.columns else 0
    kpi = html.Div([kpi_card("Total Qty", f"{total_qty:,.0f}")], style={"display":"flex","gap":"12px"})
    cols = [c for c in ["Outlet Name","Item Code","Item Name","Unit","Qty","Final Expiry"] if c in df.columns]
    table = safe_table(df[cols], page_size=15)
    return html.Div([kpi, html.H5("Expiry Report"), table], style={"display":"flex","flexDirection":"column","gap":"12px"})


def _make_multistore_tab(staged):
    df = staged.get("variance", pd.DataFrame())
    if df is None or df.empty:
        return html.Div("No COGS / variance data available.", style={"padding":"10px","color":"#777"})
    df = df.copy()
    if "COGS_Amount" in df.columns:
        df["COGS_Amount"] = pd.to_numeric(df["COGS_Amount"], errors="coerce").fillna(0)
        summary = df.groupby("Outlet Name", as_index=False)["COGS_Amount"].sum().sort_values("COGS_Amount", ascending=False)
    else:
        summary = pd.DataFrame(columns=["Outlet Name","COGS_Amount"])
    table = safe_table(summary, page_size=20)
    return html.Div([html.H5("Multi-Store COGS Summary"), table], style={"display":"flex","flexDirection":"column","gap":"12px"})

# -----------------------------
# Heatmap builder
# -----------------------------

def build_variance_heatmap_fig(df, top_n_items=40):
    if df is None or df.empty:
        return None
    df = df.copy()
    if "Variance Percent" not in df.columns and "Variance_Pct" in df.columns:
        df["Variance Percent"] = df["Variance_Pct"]
    if "Variance Percent" not in df.columns:
        return None
    if "Item Name" not in df.columns or "Outlet Name" not in df.columns:
        return None
    try:
        pivot = df.pivot_table(values="Variance Percent", index="Item Name", columns="Outlet Name", aggfunc="mean", fill_value=0)
        if pivot.empty:
            return None
        pivot["abs_mean"] = pivot.abs().mean(axis=1)
        pivot_top = pivot.sort_values("abs_mean", ascending=False).head(top_n_items).drop(columns=["abs_mean"])
        fig = px.imshow(pivot_top.fillna(0).values, x=pivot_top.columns, y=pivot_top.index, labels=dict(color="Variance %"), title="Variance % Heatmap — Item vs Store")
        fig.update_layout(height=700, margin=dict(l=10,r=10,t=60,b=10), font_family=FONT_FAMILY)
        return fig
    except Exception:
        return None

# -----------------------------
# Layout + Callbacks
# -----------------------------

def get_layout():
    return html.Div(
        style={"padding": "14px", "fontFamily": FONT_FAMILY},
        children=[
            html.H3("Inventory & COGS Dashboard", style={"marginBottom": "10px"}),
            html.Div([
                html.Div("Date Range:", style={"fontSize": "14px", "marginRight": "6px"}),
                    dcc.DatePickerRange(
                        id="dt-range",
                        display_format="DD-MMM-YYYY",
                        minimum_nights=0,
                        clearable=True,
                        start_date=(datetime.today() - pd.Timedelta(days=30)).strftime("%Y-%m-%d"),
                        end_date=datetime.today().strftime("%Y-%m-%d"),
                    ),
                html.Button("Reset", id="dt-reset", n_clicks=0, style={"padding":"6px 10px"}),
            ], style={"display": "flex", "alignItems": "center", "gap": "12px", "marginBottom": "14px", "flexWrap": "wrap"}),
            html.Div([
                html.Div([html.Span("⚙️ Advanced Filters", style={"fontWeight":"600","fontSize":"14px"}), html.Span("▼", id="filter-arrow", style={"float":"right","cursor":"pointer","fontSize":"16px"})], id="filter-header", n_clicks=0, style={"padding":"10px 14px","background":"#f1f1f1","borderRadius":"6px","cursor":"pointer","userSelect":"none"}),
                html.Div([
                    html.Div([
                        dcc.Dropdown(id="f-brand", placeholder="Brand", multi=True, style={"width": "160px"}),
                        dcc.Dropdown(id="f-region", placeholder="Region", multi=True, style={"width": "160px"}),
                        dcc.Dropdown(id="f-state", placeholder="State", multi=True, style={"width": "160px"}),
                        dcc.Dropdown(id="f-city", placeholder="City", multi=True, style={"width": "160px"}),
                        dcc.Dropdown(id="f-type", placeholder="Type", multi=True, style={"width": "160px"}),
                        dcc.Dropdown(id="f-outlet", placeholder="Outlet", multi=True, style={"width": "220px"}),
                    ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap"}),
                    html.Div([
                        dcc.Dropdown(id="f-super", placeholder="Super Category", multi=True, style={"width": "200px"}),
                        dcc.Dropdown(id="f-category", placeholder="Category", multi=True, style={"width": "200px"}),
                        dcc.Dropdown(id="f-item", placeholder="Item", multi=True, style={"width": "260px"}),
                    ], style={"display": "flex", "gap": "10px", "flexWrap": "wrap", "marginTop": "8px"}),
                ], id="filter-body", style={"padding":"12px","background":"white","border":"1px solid #e1e1e1","marginTop":"4px","borderRadius":"6px","display":"none"}),
            ], style={"marginBottom": "14px"}),
            dcc.Tabs(id="tabs", value="tab-wastage", children=[
                dcc.Tab(label="Wastage", value="tab-wastage"),
                dcc.Tab(label="GRN / Purchases", value="tab-grn"),
                dcc.Tab(label="Consumption", value="tab-consumption"),
                dcc.Tab(label="Variance", value="tab-variance"),
                dcc.Tab(label="Physical Stock", value="tab-physical"),
                dcc.Tab(label="Item Sales", value="tab-sales"),
                dcc.Tab(label="COGS Multi-Store", value="tab-multi"),
                dcc.Tab(label="Variance Heatmap", value="tab-heatmap"),
                dcc.Tab(label="Expiry Report", value="tab-expiry"),
            ]),
            html.Div(id="tab-content", style={"marginTop": "14px"}),
            html.Div(id="heatmap-container", style={"marginTop": "12px"}),
            html.Div(id="dummy-kpi", style={"display": "none"}),
        ],
    )

_callbacks_registered = False

def register_callbacks(app):
    global _callbacks_registered
    if _callbacks_registered:
        return
    _callbacks_registered = True

    @app.callback(Output("filter-body", "style"), Output("filter-arrow", "children"), Input("filter-header", "n_clicks"), State("filter-body", "style"), prevent_initial_call=True)
    def toggle_filters(n, style):
        if not isinstance(style, dict):
            style = {"display": "none"}
        new_style = dict(style)
        if style.get("display") == "none":
            new_style["display"] = "block"
            return new_style, "▲"
        else:
            new_style["display"] = "none"
            return new_style, "▼"

    @app.callback(
        Output("dt-range", "start_date"),
        Output("dt-range", "end_date"),
        Input("dt-reset", "n_clicks"),
        prevent_initial_call=True
    )
    def reset_date(n_reset):
        ensure_caches_are_fresh()

        dfs = [
            load_entry_cached(),
            load_wastage_cached(),
            load_variance_cached(),
            load_consumption_cached(),
            load_physical_cached(),
            load_item_sales_cached(),
            load_expiry_cached()
        ]

        all_dates = []
        for df in dfs:
            if df is None or df.empty:
                continue
            col = detect_date_column(df)
            if not col:
                continue
            ser = pd.to_datetime(df[col], dayfirst=True, errors="coerce").dropna()
            if not ser.empty:
                all_dates.append(ser)

        if not all_dates:
            today = pd.Timestamp.today()
            return (
                (today - pd.Timedelta(days=30)).strftime("%Y-%m-%d"),
                today.strftime("%Y-%m-%d")
            )

        dates = pd.concat(all_dates)
        return dates.min().strftime("%Y-%m-%d"), dates.max().strftime("%Y-%m-%d")

    
    @app.callback(Output("f-brand", "options"), Output("f-region", "options"), Output("f-state", "options"), Output("f-city", "options"), Output("f-type", "options"), Output("f-outlet", "options"), Output("f-super", "options"), Output("f-category", "options"), Output("f-item", "options"), Input("filter-header", "n_clicks"), prevent_initial_call=False)
    def load_filter_options(_):
        ensure_caches_are_fresh()
        stores = load_stores_cached()
        if stores.empty:
            stores = pd.DataFrame(columns=["Brand","Region","State","City","Type","Outlet Name"])
        else:
            stores = stores.copy()
        def opts(col):
            if col not in stores.columns:
                return []
            vals = sorted({str(x).strip() for x in stores[col].dropna().unique() if str(x).strip() != ""})
            return [{"label": x, "value": x} for x in vals]
        v = load_consumption_cached()
        if v.empty:
            v = load_entry_cached()
        supers = sorted(v["Super Category Name"].dropna().unique()) if "Super Category Name" in v.columns else []
        cats   = sorted(v["Category Name"].dropna().unique())       if "Category Name" in v.columns else []
        items  = sorted(v["Item Name"].dropna().unique())            if "Item Name" in v.columns else []
        return (opts("Brand"), opts("Region"), opts("State"), opts("City"), opts("Type"), opts("Outlet Name"), [{"label": x, "value": x} for x in supers], [{"label": x, "value": x} for x in cats], [{"label": x, "value": x} for x in items])

    @app.callback(Output("tab-content", "children"), Output("heatmap-container", "children", allow_duplicate=True), Input("tabs", "value"), Input("dt-range", "start_date"), Input("dt-range", "end_date"), Input("f-brand", "value"), Input("f-region", "value"), Input("f-state", "value"), Input("f-city", "value"), Input("f-type", "value"), Input("f-outlet", "value"), Input("f-super", "value"), Input("f-category", "value"), Input("f-item", "value"))
    def render_tab(tab, start_date, end_date, brand, region, state, city, type_, outlet, super_cat, category, item_name):
        ensure_caches_are_fresh()
        print("FILTERS:", brand, region, state, city, type_, outlet)
        def to_tuple(v):
            if v is None:
                return ()
            if isinstance(v, (list, tuple)):
                return tuple(x for x in v if str(x).strip() != "")
            if str(v).strip() == "":
                return ()
            return (v,)
        stores_df = load_stores_cached()
        staged= cached_filter(start_date or "", end_date or "", stores_df, brands=to_tuple(brand), regions=to_tuple(region), states=to_tuple(state), cities=to_tuple(city), types=to_tuple(type_), outlets=to_tuple(outlet), supers=to_tuple(super_cat), cats=to_tuple(category), items=to_tuple(item_name),)
        heatmap_div = html.Div()
        if tab == "tab-wastage":     return _make_wastage_tab(staged), heatmap_div
        if tab == "tab-grn":         return _make_grn_tab(staged), heatmap_div
        if tab == "tab-consumption": return _make_consumption_tab(staged), heatmap_div
        if tab == "tab-variance":    return _make_variance_tab(staged), heatmap_div
        if tab == "tab-physical":    return _make_physical_tab(staged), heatmap_div
        if tab == "tab-sales":       return _make_item_sales_tab(staged), heatmap_div
        if tab == "tab-multi":       return _make_multistore_tab(staged), heatmap_div
        if tab == "tab-expiry":      return _make_expiry_tab(staged), heatmap_div
        if tab == "tab-heatmap":
            btn = html.Button("Load Variance Heatmap", id="btn-load-heatmap", style={"padding":"8px 16px","marginBottom":"12px"})
            return btn, html.Div()
        return html.Div("Invalid tab selected.", style={"color":"#b00"}), heatmap_div
        
    @app.callback(Output("heatmap-container", "children", allow_duplicate=True), Input("btn-load-heatmap", "n_clicks"), State("dt-range", "start_date"), State("dt-range", "end_date"), State("f-brand", "value"), State("f-region", "value"), State("f-state", "value"), State("f-city", "value"), State("f-type", "value"), State("f-outlet", "value"), State("f-super", "value"), State("f-category", "value"), State("f-item", "value"), prevent_initial_call=True)
    def load_heatmap(btn, start_date, end_date, brand, region, state, city, type_, outlet, super_cat, category, item_name):
        ensure_caches_are_fresh()
        def to_tuple(v):
            if v is None:
                return ()
            if isinstance(v, (list, tuple)):
                return tuple(x for x in v if str(x).strip() != "")
            if str(v).strip() == "":
                return ()
            return (v,)
        staged = cached_filter(start_date or "", end_date or "", to_tuple(brand), to_tuple(region), to_tuple(state), to_tuple(city), to_tuple(type_), to_tuple(outlet), to_tuple(super_cat), to_tuple(category), to_tuple(item_name))
        df = staged.get("variance", pd.DataFrame())
        fig = build_variance_heatmap_fig(df)
        if fig is None:
            return html.Div("No heatmap data available.", style={"color":"#777"})
        return dcc.Graph(figure=fig)

    # clientside KPI animator (non-critical)
    app.clientside_callback("""
        function(children) {
            try {
                const els = document.querySelectorAll('.kpi-animate-number');
                els.forEach(el => {
                    let finalVal = parseFloat((el.dataset.value||"0").replace(/[^0-9.-]/g,""));
                    if (isNaN(finalVal)) return;
                    let cur = 0; let step = finalVal/40;
                    function tick(){ cur += step; if (cur >= finalVal){ el.textContent = Intl.NumberFormat('en-IN').format(finalVal);} else { el.textContent = Intl.NumberFormat('en-IN').format(Math.round(cur)); requestAnimationFrame(tick);} }
                    requestAnimationFrame(tick);
                });
            } catch(e) {}
            return "";
        }
    """, Output("dummy-kpi", "children"), Input("tab-content", "children"))
def debug_file_counts():
    paths = {
        "ENTRY": PATH_ENTRY_REPORT,
        "WASTAGE": PATH_WASTAGE,
        "CONSUMPTION": PATH_CONSUMPTION,
        "VARIANCE": PATH_VARIANCE_REPORT,
        "PHYSICAL": PATH_PHYSICAL,
        "EXPIRY": PATH_EXPIRY_REPORT,
        "ITEM_SALES": PATH_ITEM_SALES,
    }
    for k, p in paths.items():
        print(k, "→", len(glob.glob(p)), "files")    
# End of file
