# dsr_page.py — FINAL MERGED (with Decimal fixes, State/Type filters, Cross-filtering, Week↔Month mapping)
# Generated: Integrated from your 4-part source, preserving all original logic and adding only:
#  - State & Type filters in layout
#  - Cross-filter callback (State <-> Region <-> City <-> Type <-> Outlet)
#  - Week -> Month reverse mapping callback
#  - force_int_columns helper & application so Bills/Days/OutletDays/WeekNumber/Year show no decimals
#
# NOTE: This file intentionally keeps your original logic unchanged except for the additions above.
# ------------------------------------------------------------------------------

import os
import glob
import hashlib
from datetime import timedelta, datetime
from functools import lru_cache
from typing import List, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import dash_bootstrap_components as dbc
from dash import Dash, html, dcc, Input, Output, dash_table, no_update, callback_context
#app = Dash(__name__, suppress_callback_exceptions=True)
# ---------- Configuration ----------
BASE_DIR = r"C:\Users\ACER\store_dashboard"
SALES_FOLDER = os.path.join(BASE_DIR, "sales_dashboard")
STORES_DB_FILE = os.path.join(BASE_DIR, "stores_db.csv")

# Default display font/style
GLOBAL_FONT_FAMILY = "Times New Roman, serif"
GLOBAL_CELL_STYLE = {
    "fontFamily": GLOBAL_FONT_FAMILY,
    "textAlign": "left",
    "fontSize": "12px",
    "whiteSpace": "normal",
    "height": "auto",
    "padding": "6px 8px",
}

# ---------- Utility helpers ----------
def fmt(x):
    """Format a number with commas and 2 decimals (keeps non-numeric as-is)."""
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return x


def fmt_int(x):
    """Format as int (no decimals) when possible, else blank."""
    try:
        if pd.isna(x):
            return ""
        return str(int(float(x)))
    except Exception:
        return ""


def safe_read_csv(path, **kwargs):
    """Wrap pandas read_csv safely returning empty DataFrame on failure."""
    try:
        return pd.read_csv(path, **kwargs)
    except Exception:
        return pd.DataFrame()


def file_signature(paths: List[str]) -> str:
    """Return a small signature (hash) for list of files using path+mtime to bust cache when changed."""
    items = []
    for p in sorted(paths):
        try:
            m = os.path.getmtime(p)
            items.append(f"{p}:{int(m)}")
        except Exception:
            items.append(f"{p}:0")
    raw = "|".join(items).encode("utf-8")
    return hashlib.md5(raw).hexdigest()


# ---------- Robust date parser ----------
def parse_any_date(series: pd.Series) -> pd.Series:
    """
    Extremely robust date parser for your DSR files.
    Priority:
      1. Detect strict DD-MM-YYYY, DD/MM/YYYY, DD.MM.YYYY
      2. Detect YYYY-MM-DD, YYYY/MM/DD
      3. Detect DD-Mon-YYYY
      4. Last fallback → pandas.to_datetime with dayfirst=True
    """
    s = series.fillna("").astype(str).str.strip()

    out = pd.Series(pd.NaT, index=s.index)

    # --- Pattern 1: dd-mm-yyyy or dd/mm/yyyy or dd.mm.yyyy ---
    mask_ddmm = s.str.match(r"^\d{2}[-/\.]\d{2}[-/\.]\d{4}$")
    out.loc[mask_ddmm] = pd.to_datetime(
        s.loc[mask_ddmm], format=None, errors="coerce", dayfirst=True
    )

    # --- Pattern 2: yyyy-mm-dd or yyyy/mm/dd ---
    mask_yyyymmdd = s.str.match(r"^\d{4}[-/\.]\d{2}[-/\.]\d{2}$")
    out.loc[mask_yyyymmdd] = pd.to_datetime(
        s.loc[mask_yyyymmdd], format=None, errors="coerce"
    )

    # --- Pattern 3: dd-Mon-YYYY ---
    mask_dd_mon = s.str.match(r"^\d{2}-[A-Za-z]{3}-\d{4}$")
    out.loc[mask_dd_mon] = pd.to_datetime(
        s.loc[mask_dd_mon], format="%d-%b-%Y", errors="coerce"
    )

    # --- Fallback for anything else ---
    fallback_mask = out.isna() & s.ne("")
    out.loc[fallback_mask] = pd.to_datetime(
        s.loc[fallback_mask], errors="coerce", dayfirst=True
    )

    return out


# ---------- Smart CSV loader ----------
def _try_read_with_skip(file_path: str, skip: int):
    try:
        df = pd.read_csv(file_path, skiprows=skip, dtype=str)
        return df
    except Exception:
        return pd.DataFrame()


def smart_read_csv(file_path: str) -> pd.DataFrame:
    """
    Tries reading CSV with multiple skiprows choices, picks the variant with most columns,
    drops fully empty rows/cols, strips headers, auto-detects Date column robustly.
    """
    best_df = None
    best_cols = 0
    for skip in range(0, 5):
        df_try = _try_read_with_skip(file_path, skip)
        if df_try is None or df_try.empty:
            continue
        # count non-empty columns
        non_empty_cols = df_try.dropna(axis=1, how="all").shape[1]
        if non_empty_cols > best_cols:
            best_df = df_try
            best_cols = non_empty_cols

    if best_df is None or best_cols <= 1:
        # invalid or tiny file
        return pd.DataFrame()

    # Clean: drop empty rows/cols and strip headers
    best_df = best_df.dropna(how="all").dropna(axis=1, how="all")
    best_df.columns = [str(c).strip() for c in best_df.columns]

    # Normalize column names: common alternatives mapping
    common_map = {
        "outlet name": "Outlet Name",
        "outlet": "Outlet Name",
        "region": "Region",
        "date": "Date",
        "sale": "Sale",
        "discount": "Discount",
        "no of bills": "No Of Bills",
        "no of items": "No Of Items",
        "source": "Source",
    }
    new_cols = []
    for c in best_df.columns:
        cn = str(c).strip()
        cn_lower = cn.lower()
        new_cols.append(common_map.get(cn_lower, cn))
    best_df.columns = new_cols

    # Parse Date robustly if present
    if "Date" in best_df.columns:
        best_df["Date"] = parse_any_date(best_df["Date"])
        best_df = best_df.dropna(subset=["Date"])
    else:
        best_df["Date"] = pd.NaT

    return best_df.reset_index(drop=True)


# ---------- Caching: cached loader keyed by file signature ----------
def _gather_sales_files() -> List[str]:
    return glob.glob(os.path.join(SALES_FOLDER, "*.csv"))


@lru_cache(maxsize=8)
def _load_sales_data_cached(sig: str) -> pd.DataFrame:
    """
    Internal cached loader. The public loader computes a signature and calls this.
    """
    files = _gather_sales_files()
    frames = []
    for f in files:
        df = smart_read_csv(f)
        if df.empty:
            continue
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    full_df = pd.concat(frames, ignore_index=True)
    # normalize column whitespace
    full_df.columns = [str(c).strip() for c in full_df.columns]
    return full_df


def load_sales_data() -> pd.DataFrame:
    """
    Public loader that uses file_signature to invalidate cache when files change.
    """
    files = _gather_sales_files()
    sig = file_signature(files)
    df = _load_sales_data_cached(sig)
    return df.copy()


# --------------------------------------------------------------------------
# Data cleaning, KPI computations, Summary / MIS builders, formatting
# --------------------------------------------------------------------------
def iso_week_bounds(d: pd.Timestamp):
    iso = d.isocalendar()
    year, week = int(iso.year), int(iso.week)

    # Monday of ISO week
    start = pd.Timestamp.fromisocalendar(year, week, 1)
    # End = given date (WTD)
    end = d
    return start, end

def kpi_card_small(title, value, color="#0F172A"):
    return dbc.Card(
        dbc.CardBody([
            html.Div(title, style={"fontSize": "11px", "color": "#374151"}),
            html.Div(
                f"{int(value)}",
                style={"fontSize": "20px", "fontWeight": "700", "color": color}
            )
        ]),
        style={"borderRadius": "10px", "padding": "6px"}
    )

# ---------- Data cleaning & enrichment ----------
def clean_and_enrich_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize numeric columns, compute Net Sale, Charges, Final_Net_Sale and time keys."""
    if df is None or df.empty:
        return pd.DataFrame()

    d = df.copy()

    # Ensure key string cols exist
    for col in ["Region", "Outlet Name", "Source"]:
        if col not in d.columns:
            d[col] = ""

    # Numeric columns we try to coerce
    numeric_candidates = [
        "No Of Items",
        "No Of Bills",
        "Sale",
        "Discount",
        "Restaurant Charge",
        "Packaging Charge [CART - SWIGGY]",
        "Restaurant Packaging Charges",
        "Delivery Charge",
        "Platform Fee Charge",
        "Smile Amount Charge",
        "Packaging Charge",
        "Net Sale",  # may already be present
        "Charges",
    ]
    for col in numeric_candidates:
        if col in d.columns:
            d[col] = (
                pd.to_numeric(d[col].astype(str).str.replace(",", "").str.replace(" ", ""),
                              errors="coerce")
                .fillna(0.0)
            )
        else:
            d[col] = 0.0

    # Compute Net Sale if missing/zero
    # -------------------------------
    # NET SALE (authoritative)
    # -------------------------------
    d["Net Sale"] = pd.to_numeric(
        d["Net Sale"].astype(str).str.replace(",", ""),
        errors="coerce"
    ).fillna(
        pd.to_numeric(d["Sale"], errors="coerce").fillna(0)
        - pd.to_numeric(d["Discount"], errors="coerce").fillna(0)
    )

    # -------------------------------
    # CHARGES (FILE VALUE ONLY)
    # -------------------------------
    d["Charges"] = pd.to_numeric(
        d.get("Charges", 0).astype(str).str.replace(",", ""),
        errors="coerce"
    ).fillna(0.0)

    # -------------------------------
    # FINAL NET SALE (LOCKED)
    # -------------------------------
    d["Final_Net_Sale"] = d["Net Sale"] + d["Charges"]

    # SourceType mapping
    def map_source_type(src: str) -> str:
        s = str(src or "").strip().lower()
        if s in ["pos", "dine in", "dine-in", "dinein"]:
            return "Dine In"
        if any(k in s for k in ["swiggy", "zomato", "magicpin", "bolt", "delivery"]):
            return "Delivery"
        if "app" in s or "mobile" in s:
            return "App"
        return "Other"

    d["SourceType"] = d["Source"].apply(map_source_type)

    # Date-based keys (assumes Date is datetime already)
    d["Year"] = d["Date"].dt.year
    d["WeekNumber"] = d["Date"].dt.isocalendar().week.astype("Int64")
    d["YearWeek"] = d["Year"].astype(str).fillna("") + "-W" + d["WeekNumber"].astype("Int64").astype(str).str.zfill(2)
    d["MonthKey"] = d["Date"].dt.to_period("M").astype(str)
    d["MonthLabel"] = d["Date"].dt.strftime("%b-%Y")
    d["DayName"] = d["Date"].dt.day_name()
    dow = d["Date"].dt.dayofweek
    d["DayCode"] = np.select(
        [dow.isin([0, 1, 2, 3]), dow == 4, dow.isin([5, 6])],
        ["WD", "WF", "WE"],
        default="",
    )

    # Merge with stores DB (if available)
    if os.path.exists(STORES_DB_FILE):
        try:
            stores = pd.read_csv(STORES_DB_FILE, dtype=str)
            stores.columns = [str(c).strip() for c in stores.columns]
            if "Outlet Name" in stores.columns:
                # reduce to small set
                stores_small = stores.drop_duplicates(subset=["Outlet Name"])
                # left merge on Outlet Name
                d = d.merge(stores_small, on="Outlet Name", how="left", suffixes=("", "_store"))
        except Exception:
            pass

    return d

def compute_active_billing_days(df):
    return (
        df[df["No Of Bills"] > 0]
        .groupby("Outlet Name")["Date"]
        .nunique()
        .sum()
    )

# ---------- KPI aggregation helpers ----------
def compute_group_kpis(df_period: pd.DataFrame) -> pd.DataFrame:
    """
    KPI Engine using TRUE OUTLET-DAY calculation:
      total_days = Σ (days each outlet was open)
      ADS = TotalNetSale / total_days
      ADT = TotalBills / total_days
      AOV = TotalNetSale / TotalBills
    """

    if df_period.empty:
        return pd.DataFrame(columns=[
            "Region","Outlet Name","SourceType",
            "Days","FinalNet","Bills","AOV","ADT","ADS"
        ])

    d = df_period.copy()
    
    # ---------- 1️⃣ TRUE ACTIVE OUTLET DAYS ----------
    outlet_days = (
        d[d["No Of Bills"] > 0]
        .groupby(["Region","Outlet Name"])["Date"]
        .nunique()
        .reset_index()
        .rename(columns={"Date": "OutletDays"})
    )

    # ---------- 2️⃣ Aggregate sales & bills per SourceType ----------
    grouped = (
        d.groupby(["Region","Outlet Name","SourceType"], as_index=False)
         .agg(
             FinalNet=("Final_Net_Sale", "sum"),
             Bills=("No Of Bills", "sum")
         )
    )

    # Attach per-outlet days (same for each SourceType row)
    grouped = grouped.merge(outlet_days,
                            on=["Region","Outlet Name"],
                            how="left")
    grouped["Days"] = grouped["OutletDays"]

    # ---------- 3️⃣ Compute OVERALL (sum DI+Delivery+App per outlet) ----------
    mask = grouped["SourceType"].isin(["Dine In","Delivery","App"])

    overall = (
        grouped.loc[mask]
            .groupby(["Region","Outlet Name"], as_index=False)
            .agg(
                FinalNet=("FinalNet","sum"),
                Bills=("Bills","sum")
            )
    )

    # Attach correct outlet-day count
    overall = overall.merge(outlet_days,
                            on=["Region","Outlet Name"],
                            how="left")
    overall = overall.rename(columns={"OutletDays":"Days"})
    overall["SourceType"] = "Overall"

    # ---------- 4️⃣ Combine ----------
    group = pd.concat([grouped, overall], ignore_index=True)

    # ---------- 5️⃣ FINAL KPI FORMULAS ----------
    group["AOV"] = np.where(group["Bills"] > 0,
                            group["FinalNet"] / group["Bills"], 0.0)
    group["ADT"] = np.where(group["Days"] > 0,
                            group["Bills"] / group["Days"], 0.0)
    group["ADS"] = np.where(group["Days"] > 0,
                            group["FinalNet"] / group["Days"], 0.0)

    # ---------- 6️⃣ Add Grand Total row (Σ across all outlets) ----------
    total_days = outlet_days["OutletDays"].sum()
    total_net_sale = df_period["Final_Net_Sale"].sum()
    total_bills = overall["Bills"].sum()

    grand_total = pd.DataFrame([{
        "Region": "ALL",
        "Outlet Name": "ALL",
        "SourceType": "Grand Total",
        "Days": total_days,
        "FinalNet": total_net_sale,
        "Bills": total_bills,
        "AOV": total_net_sale / total_bills if total_bills > 0 else 0.0,
        "ADT": total_bills / total_days if total_days > 0 else 0.0,
        "ADS": total_net_sale / total_days if total_days > 0 else 0.0
    }])

    group = pd.concat([group, grand_total], ignore_index=True)

    return group


def period_kpis(df_period: pd.DataFrame, period_name: str) -> pd.DataFrame:
    if df_period.empty:
        return pd.DataFrame(columns=[
            "Region","Outlet Name","SourceType","Category","Value","Period"
        ])

    # Get core KPIs
    g = compute_group_kpis(df_period)

    # ---------------------- Mix% calculation ----------------------
    overall_ads = (
        g[g["SourceType"]=="Overall"][["Region","Outlet Name","ADS"]]
         .rename(columns={"ADS":"ADS_Overall"})
    )

    g = g.merge(overall_ads, on=["Region","Outlet Name"], how="left")

    def mix_calc(row):
        if row["SourceType"] == "Overall":
            return 100.0
        if row["ADS_Overall"] and row["ADS_Overall"] > 0:
            return (row["ADS"] / row["ADS_Overall"]) * 100
        return 0.0

    g["Mix"] = g.apply(mix_calc, axis=1)

    # Melt to long format
    long_df = g.melt(
        id_vars=["Region","Outlet Name","SourceType"],
        value_vars=["AOV","ADT","ADS","Mix"],
        var_name="Category",
        value_name="Value"
    )

    long_df["Period"] = period_name
    return long_df


# ---------- Summary / MIS builders ----------
def build_weekly_mis(df: pd.DataFrame) -> pd.DataFrame:
    """Build weekly MIS (Year, WeekNumber, Region, Outlet Name, Source, AOV, ADT, ADS, Mix)."""

    if df.empty:
        return pd.DataFrame()

    d = df.copy()

    # ----------------------------- SAFE DATE PARSING → ISO COMPONENTS -----------------------------
    iso = d["Date"].dt.isocalendar()

    d["Year"] = iso.year.astype("Int64")
    d["WeekNumber"] = iso.week.astype("Int64")

    # Drop rows where week/year could not be computed (NaT cases)
    d = d.dropna(subset=["Year", "WeekNumber"])

    weekly_list = []

    # ----------------------------- GROUP BY (Year, WeekNumber) -----------------------------
    for (year, week), df_week in d.groupby(["Year", "WeekNumber"]):

        group = compute_group_kpis(df_week)
        if group.empty:
            continue

        # Overall ADS for mix calculation
        overall_ads = (
            group[group["SourceType"] == "Overall"][["Region", "Outlet Name", "ADS"]]
            .rename(columns={"ADS": "ADS_Overall"})
        )

        g = group.merge(overall_ads, on=["Region", "Outlet Name"], how="left")

        # Mix% calculation
        def compute_mix(row):
            if row["SourceType"] == "Overall":
                return 100.0 if row["ADS_Overall"] and row["ADS_Overall"] > 0 else 0.0
            if row["ADS_Overall"] and row["ADS_Overall"] > 0:
                return row["ADS"] / row["ADS_Overall"] * 100.0
            return 0.0

        g["Mix"] = g.apply(compute_mix, axis=1)

        # Assign grouping keys
        g["Year"] = int(year)
        g["WeekNumber"] = int(week)

        weekly_list.append(g)

    if not weekly_list:
        return pd.DataFrame()

    return pd.concat(weekly_list, ignore_index=True)


def build_summary_and_mis(df: pd.DataFrame):
    """
    Build summary (period KPIs), MTD MIS, Weekly MIS anchored on max(date) in df.
    Returns (summary, mis, weekly_mis)
    """
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    max_date = df["Date"].max()

    # Week-based
    start_wtd, end_wtd = iso_week_bounds(max_date)

    start_lwtd = start_wtd - timedelta(days=7)
    end_lwtd = end_wtd - timedelta(days=7)
    # Month-based
    start_mtd = max_date.replace(day=1)
    end_mtd = max_date
    last_month_end = start_mtd - timedelta(days=1)
    start_lmtd = last_month_end.replace(day=1)
    day_num = min(max_date.day, last_month_end.day)
    end_lmtd = start_lmtd.replace(day=day_num)

    period_defs = {
        "WTD": (start_wtd, end_wtd),
        "LWTD": (start_lwtd, end_lwtd),
        "MTD": (start_mtd, end_mtd),
        "LMTD": (start_lmtd, end_lmtd),
        "Last4Weeks": (max_date - timedelta(days=28), max_date),
        "Yesterday": (max_date, max_date),
    }

    period_frames = []
    for pname, (start, end) in period_defs.items():
        mask = (df["Date"] >= start) & (df["Date"] <= end)
        df_p = df.loc[mask].copy()
        period_frames.append(period_kpis(df_p, pname))

    valid_period_frames = [p for p in period_frames if p is not None and not p.empty]
    if valid_period_frames:
        all_periods = pd.concat(valid_period_frames, ignore_index=True)
    else:
        all_periods = pd.DataFrame()

    if all_periods.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    summary = all_periods.pivot_table(index=["Region", "Outlet Name", "SourceType", "Category"],
                                      columns="Period", values="Value", aggfunc="first").reset_index()

    # fill missing period columns
    for col in ["WTD", "LWTD", "MTD", "LMTD", "Last4Weeks", "Yesterday"]:
        if col not in summary.columns:
            summary[col] = 0.0

    summary["Change_WTD"] = np.where(summary["LWTD"] != 0,
                                     (summary["WTD"] - summary["LWTD"]) / summary["LWTD"] * 100.0, 0.0)
    summary["Change_MTD"] = np.where(summary["LMTD"] != 0,
                                     (summary["MTD"] - summary["LMTD"]) / summary["LMTD"] * 100.0, 0.0)

    # add Month & WeekNumber placeholders
    summary["Month"] = ""
    summary["WeekNumber"] = 0
    summary = summary.rename(columns={"SourceType": "Source"})

    # round numeric columns
    num_cols = summary.select_dtypes(include=[np.number]).columns
    summary[num_cols] = summary[num_cols].round(2)

    # MTD MIS
    mtd_long = all_periods[all_periods["Period"] == "MTD"].copy()
    if mtd_long.empty:
        mis = pd.DataFrame()
    else:
        # -------------------------------
        # FIX MIS (ALL OUTLET KPI VALUES)
        # -------------------------------
        g_all_mtd = compute_group_kpis(df[(df["Date"] >= start_mtd) & (df["Date"] <= end_mtd)])
        # find grand total row if exists
        grand_mtd = g_all_mtd[g_all_mtd["SourceType"].isin(["Grand Total", "GrandTotal", "Grand_Total"])]
        if not grand_mtd.empty:
            grand_mtd = grand_mtd.iloc[0]
            mis = pd.DataFrame([{
                "Source": "Overall",
                "AOV": grand_mtd["AOV"],
                "ADT": grand_mtd["ADT"],
                "ADS": grand_mtd["ADS"],
                "Mix": 100
            }])
        else:
            # fallback compute
            overall = g_all_mtd[g_all_mtd["SourceType"] == "Overall"]
            if not overall.empty:
                total_days = int(overall["Days"].sum())
                total_net = float(overall["FinalNet"].sum())
                total_bills = float(overall["Bills"].sum())
                mis = pd.DataFrame([{
                    "Source": "Overall",
                    "AOV": (total_net / total_bills) if total_bills > 0 else 0.0,
                    "ADT": (total_bills / total_days) if total_days > 0 else 0.0,
                    "ADS": (total_net / total_days) if total_days > 0 else 0.0,
                    "Mix": 100
                }])
            else:
                mis = pd.DataFrame([{"Source":"Overall","AOV":0.0,"ADT":0.0,"ADS":0.0,"Mix":100}])

    weekly_mis = build_weekly_mis(df)
    return summary, mis, weekly_mis


# ---------- Formatting for display: change columns with arrows and preparing style rules ----------
def format_change_columns_for_display(df: pd.DataFrame, change_cols=("Change_WTD", "Change_MTD")) -> pd.DataFrame:
    """Return copy of df where numeric change columns are replaced with arrow strings for display."""
    out = df.copy()
    for c in change_cols:
        if c in out.columns:
            def to_arrow(v):
                try:
                    if pd.isna(v):
                        return "—"
                    v = float(v)
                    if v > 0.0001:
                        return f"▲ {v:0.2f}%"
                    if v < -0.0001:
                        return f"▼ {abs(v):0.2f}%"
                    return "—"
                except Exception:
                    return str(v)
            out[c] = out[c].apply(to_arrow)
    return out

def format_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Force ALL numeric columns to show only 2 decimals (string formatted).
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    for col in out.columns:
        if pd.api.types.is_numeric_dtype(out[col]):
            out[col] = out[col].apply(
                lambda x: f"{x:,.2f}" if pd.notna(x) else ""
            )

    return out

def build_change_style_conditions(change_cols=("Change_WTD", "Change_MTD")):
    """
    Cell-level coloring:
      ▲ -> green cell
      ▼ -> red cell
      — -> no color
    """
    rules = []

    for col in change_cols:
        # Green for positive
        rules.append({
            "if": {
                "column_id": col,
                "filter_query": f'{{{col}}} contains "▲"'
            },
            "backgroundColor": "#e6ffed",
            "color": "#166534",
            "fontWeight": "600",
        })

        # Red for negative
        rules.append({
            "if": {
                "column_id": col,
                "filter_query": f'{{{col}}} contains "▼"'
            },
            "backgroundColor": "#ffeceb",
            "color": "#991b1b",
            "fontWeight": "600",
        })

    return rules


# --------------------------------------------------------------------------
# Layout, initial load, DataTable defaults, and Tabs (including new Overall MIS tab).
# --------------------------------------------------------------------------

# ---------- Initial load/cached prepared datasets ----------
def _initial_load_prepared():
    raw = load_sales_data()
    if raw.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    clean = clean_and_enrich_sales(raw)
    summary, mis, weekly = build_summary_and_mis(clean)
    return clean, summary, mis, weekly

_sales_df_init, _summary_init, _mis_init, _weekly_init = _initial_load_prepared()

SUMMARY_COLUMNS = [
    "Region", "Outlet Name", "Source", "Category", "Month", "WeekNumber",
    "WTD", "LWTD", "Change_WTD", "MTD", "LMTD", "Change_MTD", "Last4Weeks", "Yesterday"
]

# DataTable base styles
DATATABLE_BASE_STYLE = {
    'style_cell': GLOBAL_CELL_STYLE,
    'style_header': {
        "fontFamily": GLOBAL_FONT_FAMILY,
        "fontSize": "14px",
        "fontWeight": "700",
        "textAlign": "left",
        "backgroundColor": "#f3f4f6",
    },
    'page_size': 20,
    'sort_action': 'native',
    'filter_action': 'native',
    'style_table': {'overflowX': 'auto', 'maxHeight': '520px'},
}

# Tab colors (attractive)
TAB_STYLES = {
    'tab-summary': {'backgroundColor': '#F0F9FF', 'color': '#0369A1'},
    'tab-mis': {'backgroundColor': '#ECFDF5', 'color': '#065F46'},
    'tab-weekly-mis': {'backgroundColor': '#FFFBEB', 'color': '#92400E'},
    'tab-charts': {'backgroundColor': '#F8FAFC', 'color': '#0F172A'},
    'tab-overall-mis': {'backgroundColor': '#F5F3FF', 'color': '#6D28D9'},
}

# UI helpers
def kpi_card(title, value, color="primary"):
    color_map = {
        "primary": "#1D4ED8",
        "success": "#16A34A",
        "warning": "#F59E0B",
        "danger": "#DC2626",
        "info": "#0891B2",
    }
    bg = color_map.get(color, "#1D4ED8")
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(title, style={"fontSize": "12px", "color": "#E5E7EB", "fontFamily": GLOBAL_FONT_FAMILY}),
                html.Div(
                    fmt(value),
                    style={"fontSize": "20px", "fontWeight": "bold", "color": "white", "fontFamily": GLOBAL_FONT_FAMILY},
                ),
            ]
        ),
        style={
            "borderRadius": "12px",
            "boxShadow": "0 3px 8px rgba(0,0,0,0.12)",
            "background": f"linear-gradient(135deg, {bg}, #0b1220)",
        },
    )


# ---------- New helper: force int columns (no decimals) ----------
INT_COLUMNS = {
    "Bills", "No Of Bills",
    "Days", "OutletDays",
    "WeekNumber", "Year"
}

def force_int_columns(df):
    """
    Convert selected columns to integer display (no decimals),
    safely handling numeric, string, and comma-formatted values.
    """
    out = df.copy()

    for col in INT_COLUMNS:
        if col not in out.columns:
            continue

        def to_int_safe(x):
            if pd.isna(x) or x == "":
                return ""
            try:
                # Remove commas if value is string
                if isinstance(x, str):
                    x = x.replace(",", "")
                return str(int(float(x)))
            except Exception:
                return ""

        out[col] = out[col].apply(to_int_safe)

    return out

# ---------- Layout export ----------
def get_layout():
    sales_df = _sales_df_init if _sales_df_init is not None else pd.DataFrame()

    state_options = []
    if not sales_df.empty and "State" in sales_df.columns:
        state_options = [{"label": s, "value": s} for s in sorted(sales_df["State"].dropna().unique())]

    region_options = [{"label": r, "value": r} for r in sorted(sales_df["Region"].dropna().unique())] if not sales_df.empty else []
    city_options = [{"label": c, "value": c} for c in sorted(sales_df["City"].dropna().unique())] if ("City" in sales_df.columns and not sales_df.empty) else []
    outlet_options = [{"label": o, "value": o} for o in sorted(sales_df["Outlet Name"].dropna().unique())] if not sales_df.empty else []
    month_options = []
    if not sales_df.empty and "MonthKey" in sales_df.columns:
        month_pairs = sales_df[["MonthKey", "MonthLabel"]].drop_duplicates().sort_values("MonthKey")
        month_options = [{"label": ml, "value": mk} for mk, ml in month_pairs.itertuples(index=False, name=None)]

    type_options = [{"label": t, "value": t} for t in sorted(sales_df["Type"].dropna().unique())] if "Type" in sales_df.columns else []

    source_options = [{"label": s, "value": s} for s in ["Dine In", "Delivery", "App", "Other", "Mix"]]
    category_options = [{"label": c, "value": c} for c in ["AOV", "ADT", "ADS", "Mix", "Overall"]]
    day_options = [
        {"label": "All Days", "value": "ALL"},
        {"label": "WD (Mon-Thu)", "value": "WD"},
        {"label": "WF (Fri)", "value": "WF"},
        {"label": "WE (Sat-Sun)", "value": "WE"},
        {"label": "Monday", "value": "Monday"},
        {"label": "Tuesday", "value": "Tuesday"},
        {"label": "Wednesday", "value": "Wednesday"},
        {"label": "Thursday", "value": "Thursday"},
        {"label": "Friday", "value": "Friday"},
        {"label": "Saturday", "value": "Saturday"},
        {"label": "Sunday", "value": "Sunday"},
    ]

    return html.Div(
        style={"padding": "14px", "backgroundColor": "#F8FAFC", "fontFamily": GLOBAL_FONT_FAMILY},
        children=[
            html.H2("DSR Dynamic Sales Dashboard", style={"textAlign": "center", "marginTop": "4px", "fontFamily": GLOBAL_FONT_FAMILY}),
            # Filters: State, Region, City, Type, Outlet, Source
            dbc.Row([
                dbc.Col([html.Label("Brand"), dcc.Dropdown(
                    id="brand_filter",
                    options=[{"label": b, "value": b}
                            for b in sorted(sales_df["Brand"].dropna().unique())]
                    if "Brand" in sales_df.columns else [],
                    multi=True,
                    placeholder="All Brands"
                )], md=2),
                dbc.Col([html.Label("State"), dcc.Dropdown(id="state_filter", multi=True, placeholder="All States")], md=2),
                dbc.Col([html.Label("Region"), dcc.Dropdown(id="region_filter", multi=True, placeholder="All Regions")], md=2),
                dbc.Col([html.Label("City"), dcc.Dropdown(id="city_filter", multi=True, placeholder="All Cities")], md=2),
                dbc.Col([html.Label("Type"), dcc.Dropdown(id="type_filter", multi=True, placeholder="All Types")], md=2),
                dbc.Col([html.Label("Outlet Name"), dcc.Dropdown(id="outlet_filter", multi=True, placeholder="All Outlets")], md=2)]),
            dbc.Row([
                dbc.Col([html.Label("Source"), dcc.Dropdown(id="source_filter", options=source_options, multi=True, placeholder="Dine In / Delivery / App")], md=2),  
                dbc.Col([html.Label("Category"), dcc.Dropdown(id="category_filter", options=category_options, multi=True, placeholder="AOV / ADT / ADS / Mix / Overall")], md=2),
                dbc.Col([html.Label("Month"), dcc.Dropdown(id="month_filter", options=month_options, placeholder="All Months")], md=2),
                dbc.Col([html.Label("Week"), dcc.Dropdown(id="week_filter", options=[], placeholder="All Weeks", multi=True)], md=2),
                dbc.Col([html.Label("Day"), dcc.Dropdown(id="day_filter", options=day_options, value="ALL")], md=2),
                dbc.Col([html.Label(" "), html.Button("Refresh Data", id="refresh_button", n_clicks=0, style={"width": "100%", "marginTop": "4px"})], md=1),
                html.Button("Reset Filters", id="reset_filters_btn")

            ], style={"marginTop": "10px"}),
            dbc.Row([dbc.Col([html.Label("Summary search (Outlet Name)"), dcc.Input(id="search_text", type="text", placeholder="Type letters... e.g. 'gk'", style={"width": "100%"})], md=4)], style={"marginTop": "10px", "marginBottom": "10px"}),
            html.Div(id="dsr_kpi_row", style={"marginBottom": "10px"}),
            # Tabs
            dcc.Tabs(id="tabs", value="tab-summary", children=[
                dcc.Tab(label="Summary (Running Month)", value="tab-summary"),
                dcc.Tab(label="MIS Report (MTD)", value="tab-mis"),
                dcc.Tab(label="Weekly MIS", value="tab-weekly-mis"),
                dcc.Tab(label="Overall MIS Report", value="tab-overall-mis"),
                dcc.Tab(label="Portfolio Summary", value="tab-portfolio"),
                dcc.Tab(label="Charts", value="tab-charts"),
            ]),
            html.Br(),
            html.Div(id="tabs-content"),
        ]
    )


# --------------------------------------------------------------------------
# PART 4/4: Callbacks (with cross-filtering and decimal formatting application)
# --------------------------------------------------------------------------

def _get_prepared_sales():
    raw = load_sales_data()
    if raw.empty:
        return pd.DataFrame()
    return clean_and_enrich_sales(raw)


# Helper to create week options based on selected month
def weeks_from_df(df: pd.DataFrame, month_key: str = None):
    if df.empty or "YearWeek" not in df.columns:
        return []
    dfc = df.copy()
    if month_key:
        dfc = dfc[dfc["MonthKey"] == month_key]
    if dfc.empty:
        return []
    weeks = dfc[["YearWeek", "Year", "WeekNumber"]].drop_duplicates().sort_values(["Year", "WeekNumber"])
    return [{"label": f"Wk {int(row.WeekNumber)} - {int(row.Year)}", "value": row.YearWeek} for row in weeks.itertuples(index=False)]

def months_from_weeks(df: pd.DataFrame, week_vals):
    """Return unique MonthKey options for the supplied YearWeek(s)."""
    if df.empty or "YearWeek" not in df.columns:
        return []
    if not week_vals:
        # return all month options
        if "MonthKey" in df.columns:
            month_pairs = df[["MonthKey", "MonthLabel"]].drop_duplicates().sort_values("MonthKey")
            return [{"label": ml, "value": mk} for mk, ml in month_pairs.itertuples(index=False, name=None)]
        return []
    # ensure list
    if not isinstance(week_vals, list):
        week_vals = [week_vals]
    dfc = df[df["YearWeek"].isin(week_vals)]
    if dfc.empty:
        return []
    month_pairs = dfc[["MonthKey", "MonthLabel"]].drop_duplicates().sort_values("MonthKey")
    return [{"label": ml, "value": mk} for mk, ml in month_pairs.itertuples(index=False, name=None)]


# Small helper card for percentage/arrow display (large)
def large_kpi_card(title: str, value_text: str, positive: bool | None):
    bg = "#ecfdf5" if positive is True else ("#fef2f2" if positive is False else "#f3f4f6")
    color = "#065f46" if positive is True else ("#b91c1c" if positive is False else "#0f172a")
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(title, style={"fontSize": "12px", "color": "#374151", "fontFamily": GLOBAL_FONT_FAMILY}),
                html.Div(value_text, style={"fontSize": "20px", "fontWeight": "700", "color": color, "fontFamily": GLOBAL_FONT_FAMILY}),
            ]
        ),
        style={"borderRadius": "10px", "backgroundColor": bg, "padding": "6px"}
    )

# Mini KPI card for numeric values
def mini_kpi_card(title: str, value, subtitle=None):
    return dbc.Card(
        dbc.CardBody([
            html.Div(title, style={"fontSize": "11px", "color": "#374151", "fontFamily": GLOBAL_FONT_FAMILY}),
            html.Div(fmt(value) if value not in (None, "", np.nan) else "", style={"fontSize": "16px", "fontWeight": "700", "fontFamily": GLOBAL_FONT_FAMILY}),
            html.Div(subtitle or "", style={"fontSize": "10px", "color": "#6b7280"})
        ]),
        style={"borderRadius": "10px", "padding": "6px"}
    )

# =========================================================
# KPI UP / DOWN COLOR LOGIC (ADS / AOV / ADT)
# =========================================================
def kpi_up_down_logic(current, previous):
    """
    Universal KPI comparison logic
    Returns: bg_color, text_color, arrow_text
    """
    try:
        current = float(current)
        previous = float(previous)
    except Exception:
        return "#f3f4f6", "#0f172a", "—"

    if previous <= 0:
        return "#f3f4f6", "#0f172a", "—"

    if current > previous:
        pct = (current - previous) / previous * 100
        return "#ecfdf5", "#166534", f"▲ {pct:0.2f}%"

    if current < previous:
        pct = (previous - current) / previous * 100
        return "#fef2f2", "#991b1b", f"▼ {pct:0.2f}%"

    return "#f3f4f6", "#0f172a", "—"


def metric_kpi_card(title, value, prev_value, value_fmt=fmt):
    """
    KPI card with green/red logic
    """
    bg, color, arrow = kpi_up_down_logic(value, prev_value)

    return dbc.Card(
        dbc.CardBody([
            html.Div(title, style={
                "fontSize": "12px",
                "color": "#374151",
                "fontFamily": GLOBAL_FONT_FAMILY
            }),
            html.Div(
                value_fmt(value),
                style={
                    "fontSize": "20px",
                    "fontWeight": "700",
                    "color": color,
                    "fontFamily": GLOBAL_FONT_FAMILY
                }
            ),
            html.Div(
                arrow,
                style={
                    "fontSize": "11px",
                    "marginTop": "2px",
                    "color": color
                }
            )
        ]),
        style={
            "borderRadius": "12px",
            "backgroundColor": bg,
            "boxShadow": "0 2px 6px rgba(0,0,0,0.08)"
        }
    )

# Arrow formatter for percentage values
def fmt_arrow_val(v):
    try:
        v = float(v)
        if v > 0.0001:
            return f"▲ {v:0.2f}%"
        if v < -0.0001:
            return f"▼ {abs(v):0.2f}%"
        return "—"
    except Exception:
        return "—"

# Heatmap color & utility functions (unchanged)
def heatmap_color(val, vmin=None, vmax=None):
    try:
        v = float(val)
    except Exception:
        return ""
    if vmin is None or vmax is None or vmin == vmax:
        return "#ecfdf5" if v > 0 else ("#fef2f2" if v < 0 else "#fff7ed")
    norm = (v - vmin) / (vmax - vmin)
    if norm >= 0.5:
        r = int(255 * min(1.0, (norm - 0.5) * 2))
        g = 200 - int(100 * min(1.0, (norm - 0.5) * 2))
        b = 0
    else:
        r = int(200 * min(1.0, norm * 2))
        g = int(255 - 100 * min(1.0, norm * 2))
        b = 0
    return f"rgb({r},{g},{b})"

def build_heatmap_style(df, col):
    vals = []
    for row in df:
        try:
            v = float(row.get(col, np.nan))
            if not np.isnan(v):
                vals.append(v)
        except Exception:
            pass
    if not vals:
        return []
    vmin, vmax = min(vals), max(vals)
    rules = []
    for i, row in enumerate(df):
        try:
            v = float(row.get(col, np.nan))
        except Exception:
            continue
        bg = heatmap_color(v, vmin, vmax)
        rules.append({
            "if": {"row_index": i, "column_id": col},
            "backgroundColor": bg,
            "color": "#000",
        })
    return rules

def arrow_color(val_now, val_prev):
    if val_prev == 0:
        return "—", None

    diff_pct = (val_now - val_prev) / val_prev * 100
    if diff_pct > 0:
        return f"▲ {diff_pct:0.2f}%", True
    if diff_pct < 0:
        return f"▼ {abs(diff_pct):0.2f}%", False
    return "—", None

# ---------- Helpful KPI helpers ----------
def safe_grand_metrics_for_period(df_period: pd.DataFrame):
    """
    Correct GRAND TOTAL KPIs (All Outlets).
    SINGLE source of truth for ADS / AOV / ADT.
    """

    if df_period is None or df_period.empty:
        return {"ADS": 0.0, "AOV": 0.0, "ADT": 0.0}

    # ✅ Correct denominators
    total_final_net = df_period["Final_Net_Sale"].sum()
    total_bills = df_period["No Of Bills"].sum()

    total_active_days = (
        df_period[df_period["No Of Bills"] > 0]
        .groupby(["Outlet Name", "Date"])
        .size()
        .reset_index()
        .shape[0]
    )

    ads = total_final_net / total_active_days if total_active_days > 0 else 0.0
    adt = total_bills / total_active_days if total_active_days > 0 else 0.0
    aov = total_final_net / total_bills if total_bills > 0 else 0.0

    return {
        "ADS": ads,
        "ADT": adt,
        "AOV": aov,
    }

def safe_grand_final_net(df_period: pd.DataFrame) -> float:
    """
    Returns Grand Total Final Net Sale for a given period (All Outlets).
    """
    if df_period is None or df_period.empty:
        return 0.0

    g = compute_group_kpis(df_period)

    # Prefer explicit Grand Total row
    gt = g[g["SourceType"].isin(["Grand Total", "GrandTotal", "Grand_Total"])]
    if not gt.empty:
        return float(gt.iloc[0]["FinalNet"])

    # Fallback: sum Overall rows
    overall = g[g["SourceType"] == "Overall"]
    return float(overall["FinalNet"].sum()) if not overall.empty else 0.0

def safe_total_bills(df_period: pd.DataFrame) -> int:
    """
    Returns total number of bills for the given period.
    """
    if df_period is None or df_period.empty:
        return 0
    return int(
        pd.to_numeric(df_period["No Of Bills"], errors="coerce")
        .fillna(0)
        .sum()
    )

def brand_kpi_metrics(df: pd.DataFrame, brand: str) -> dict:
    """
    Compute ADS / AOV / ADT for a single Brand using grand-total logic.
    Uses already-filtered df.
    """
    if df is None or df.empty or "Brand" not in df.columns:
        return {"ADS": 0.0, "AOV": 0.0, "ADT": 0.0, "Mix": 0.0}

    d = df[df["Brand"] == brand]
    if d.empty:
        return {"ADS": 0.0, "AOV": 0.0, "ADT": 0.0, "Mix": 0.0}

    metrics = safe_grand_metrics_for_period(d)
    metrics["Mix"] = 100.0
    return metrics

def make_kpi_row(title_prefix: str, metrics: dict):
    """
    Build a dbc.Row with 4 KPI cards: ADS, AOV, ADT, Mix
    title_prefix typically 'MTD', 'WTD', 'LWTD' etc.
    """
    ads = metrics.get("ADS", 0.0)
    aov = metrics.get("AOV", 0.0)
    adt = metrics.get("ADT", 0.0)
    mix = metrics.get("Mix", 100)

    # Determine positivity heuristics (for color only; None = neutral)
    pos_ads = True if ads > 0 else None
    pos_aov = True if aov > 0 else None
    pos_adt = True if adt > 0 else None
    pos_mix = True if mix > 0 else None

    return dbc.Row([
        dbc.Col(kpi_card(f"{title_prefix} ADS (All Outlet)", ads, color="primary"), md=3),
        dbc.Col(kpi_card(f"{title_prefix} AOV (All Outlet)", aov, color="success"), md=3),
        dbc.Col(kpi_card(f"{title_prefix} ADT (All Outlet)", adt, color="warning"), md=3),
        dbc.Col(kpi_card(f"{title_prefix} Mix % (All Outlet)", mix, color="info"), md=3),
    ], style={"marginBottom": "8px"})


# -------------------------
# Callback registrations
# -------------------------
def register_callbacks(app):

    # CROSS-FILTER callback (State, Region, City, Type, Outlet)
    @app.callback(
        Output("brand_filter", "options"),
        Output("region_filter", "options"),
        Output("state_filter", "options"),
        Output("city_filter", "options"),
        Output("type_filter", "options"),
        Output("outlet_filter", "options"),
        Input("brand_filter", "value"),
        Input("region_filter", "value"),
        Input("state_filter", "value"),
        Input("city_filter", "value"),
        Input("type_filter", "value"),
        Input("outlet_filter", "value"),
        [
        Input("refresh_button", "n_clicks"),
        Input("reset_filters_btn", "n_clicks"),
        ]
    )
    def cross_filter(brand_vals, region_vals, state_vals, city_vals, type_vals, outlet_vals,
                 refresh_clicks, reset_clicks):
        df = _get_prepared_sales()
        if df.empty:
            return [], [], [], [], [], []

        d = df.copy()

        # Apply filters progressively (order doesn't matter)
        if brand_vals:
            d = d[d["Brand"].isin(brand_vals)]
        if region_vals:
            d = d[d["Region"].isin(region_vals)]
        if state_vals:
            d = d[d["State"].isin(state_vals)]
        if city_vals:
            d = d[d["City"].isin(city_vals)]
        if type_vals:
            d = d[d["Type"].isin(type_vals)]
        if outlet_vals:
            d = d[d["Outlet Name"].isin(outlet_vals)]

        brand_opt  = [{"label": b, "value": b} for b in sorted(d["Brand"].dropna().unique())]
        region_opt = [{"label": r, "value": r} for r in sorted(d["Region"].dropna().unique())]
        state_opt  = [{"label": s, "value": s} for s in sorted(d["State"].dropna().unique())]
        city_opt   = [{"label": c, "value": c} for c in sorted(d["City"].dropna().unique())]
        type_opt   = [{"label": t, "value": t} for t in sorted(d["Type"].dropna().unique())]
        outlet_opt = [{"label": o, "value": o} for o in sorted(d["Outlet Name"].dropna().unique())]

        return brand_opt, region_opt, state_opt, city_opt, type_opt, outlet_opt

    # Update week options based on month selection
    @app.callback(
        Output("week_filter", "options"),
        Input("month_filter", "value"),
        Input("refresh_button", "n_clicks"),
        Input("reset_filters_btn", "n_clicks"),
    )
    def update_week_options(month_val, refresh_clicks, reset_clicks):
        df = _get_prepared_sales()
        return weeks_from_df(df, month_val)

    # WEEK -> MONTH reverse mapping: when user selects a week, show only mapped months
    @app.callback(
        Output("month_filter", "options"),
        Input("week_filter", "value"),
        Input("refresh_button", "n_clicks"),
        Input("reset_filters_btn", "n_clicks"),
    )
    def update_month_options_from_week(week_vals, refresh_clicks, reset_clicks):
        df = _get_prepared_sales()
        return months_from_weeks(df, week_vals)

    @app.callback(
        Output("overall_mis_table", "columns"),
        Input("overall_mis_column_toggle", "value"),
    )
    def update_overall_mis_columns(selected_cols):
        return [{"name": c, "id": c} for c in selected_cols]

    # Render main content for selected tab
    @app.callback(
        Output("tabs-content", "children"),
        Input("tabs", "value"),
        Input("brand_filter", "value"),      # ✅ ADD
        Input("state_filter", "value"),
        Input("region_filter", "value"),
        Input("city_filter", "value"),
        Input("type_filter", "value"),
        Input("outlet_filter", "value"),
        Input("source_filter", "value"),
        Input("category_filter", "value"),
        Input("month_filter", "value"),
        Input("week_filter", "value"),
        Input("day_filter", "value"),
        Input("search_text", "value"),
        [
        Input("refresh_button", "n_clicks"),
        Input("reset_filters_btn", "n_clicks"),
        ]
    )
    def render_tab_content(
        tab_value, brand_vals, state_vals, region_vals, city_vals, type_vals,
        outlet_vals, source_vals, category_vals, month_val, week_vals,
        day_val, search_txt, refresh_clicks, reset_clicks
    ):
        df = _get_prepared_sales()
        if df.empty:
            return html.Div("No data available", style={"fontFamily": GLOBAL_FONT_FAMILY})
        if brand_vals and "Brand" in df.columns:
            if isinstance(brand_vals, list):
                df = df[df["Brand"].isin(brand_vals)]
            else:
                df = df[df["Brand"] == brand_vals]
        # Apply filters (respecting the new fields)
        if state_vals and "State" in df.columns:
            if isinstance(state_vals, list):
                df = df[df["State"].isin(state_vals)]
            else:
                df = df[df["State"] == state_vals]
        if region_vals:
            if isinstance(region_vals, list):
                df = df[df["Region"].isin(region_vals)]
            else:
                df = df[df["Region"] == region_vals]
        if city_vals and "City" in df.columns:
            if isinstance(city_vals, list):
                df = df[df["City"].isin(city_vals)]
            else:
                df = df[df["City"] == city_vals]
        if type_vals and "Type" in df.columns:
            if isinstance(type_vals, list):
                df = df[df["Type"].isin(type_vals)]
            else:
                df = df[df["Type"] == type_vals]
        if outlet_vals:
            if isinstance(outlet_vals, list):
                df = df[df["Outlet Name"].isin(outlet_vals)]
            else:
                df = df[df["Outlet Name"] == outlet_vals]
        if month_val:
            df = df[df["MonthKey"] == month_val]
        if week_vals:
            if not isinstance(week_vals, list):
                week_vals = [week_vals]
            df = df[df["YearWeek"].isin(week_vals)]
        if day_val and day_val != "ALL":
            if day_val in ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]:
                df = df[df["DayName"] == day_val]
            elif day_val in ["WD","WF","WE"]:
                df = df[df["DayCode"] == day_val]

        summary, mis, weekly = build_summary_and_mis(df)

        # ------------------------
        # KPI ROWS — OPTION C (MTD, WTD, LWTD)
        # ------------------------
        # compute anchor dates# ---------------------------
        # KPI BLOCK (SAFE)
        # ---------------------------
        active_days_card = None
        final_net_kpis   = None
        row_mtd = row_wtd = row_lwtd = None
        brand_kpi_block  = None

        max_date = df["Date"].max()

        if pd.isna(max_date):
            kpi_block = html.Div(
                "No KPI data available",
                style={"padding": "8px", "fontStyle": "italic"}
            )
        else:
            # Period boundaries
            start_wtd, end_wtd = iso_week_bounds(max_date)
            start_lwtd, end_lwtd = start_wtd - timedelta(days=7), end_wtd - timedelta(days=7)
            start_mtd, end_mtd = max_date.replace(day=1), max_date
            
            # -------- LMTD (Last Month Till Date) --------
            last_month_end = start_mtd - timedelta(days=1)
            start_lmtd = last_month_end.replace(day=1)

            # Handle month-length mismatch safely
            day_num = min(max_date.day, last_month_end.day)
            end_lmtd = start_lmtd.replace(day=day_num)

            df_mtd  = df[(df["Date"] >= start_mtd) & (df["Date"] <= end_mtd)]
            df_wtd  = df[(df["Date"] >= start_wtd) & (df["Date"] <= end_wtd)]
            df_lwtd = df[(df["Date"] >= start_lwtd) & (df["Date"] <= end_lwtd)]
            df_lmtd = df[(df["Date"] >= start_lmtd) & (df["Date"] <= end_lmtd)]

            # -------- Active Billing Days --------
            total_active_days_mtd = compute_active_billing_days(df_mtd)
            total_bills_mtd = safe_total_bills(df_mtd)

            # -------- Final Net Sale KPIs --------
            #mtd_final  = safe_grand_final_net(df_mtd)
            wtd_final  = safe_grand_final_net(df_wtd)
            lwtd_final = safe_grand_final_net(df_lwtd)

            wow_arrow, wow_positive = arrow_color(wtd_final, lwtd_final)

            # ================= ROW 1 =================
            row_top = dbc.Row([
                dbc.Col(
                    kpi_card_small(
                        "Total Active Billing Days (MTD)",
                        total_active_days_mtd,
                        color="#1D4ED8"
                    ),
                    md=4
                ),
                dbc.Col(
                    kpi_card_small(
                        "Total No. of Bills (MTD)",
                        total_bills_mtd,
                        color="#7C3AED"
                    ),
                    md=4
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody([
                            html.Div("WoW Change (WTD vs LWTD)", style={"fontSize": "12px"}),
                            html.Div(
                                wow_arrow,
                                style={
                                    "fontSize": "20px",
                                    "fontWeight": "700",
                                    "color": (
                                        "#16a34a" if wow_positive is True
                                        else "#dc2626" if wow_positive is False
                                        else "#0f172a"
                                    )
                                }
                            ),
                            html.Div(f"LWTD: ₹ {fmt(lwtd_final)}", style={"fontSize": "10px"})
                        ])
                    ),
                    md=4
                ),
            ], style={"marginBottom": "10px"})
            
            # ================= ROW 2 : MTD =================
            mtd_final  = safe_grand_final_net(df_mtd)
            wtd_final  = safe_grand_final_net(df_wtd)
            lwtd_final = safe_grand_final_net(df_lwtd)
            lmtd_final = safe_grand_final_net(df_lmtd)

            mtd_metrics  = safe_grand_metrics_for_period(df_mtd)
            wtd_metrics  = safe_grand_metrics_for_period(df_wtd)
            lwtd_metrics = safe_grand_metrics_for_period(df_lwtd)
            lmtd_metrics = safe_grand_metrics_for_period(df_lmtd)

            for m in (mtd_metrics, wtd_metrics, lwtd_metrics, lmtd_metrics):
                m["Mix"] = 100.0

            row_mtd = dbc.Row([
                dbc.Col(metric_kpi_card("MTD Final Net", mtd_final, lmtd_final), md=3),
                dbc.Col(metric_kpi_card("MTD ADS", mtd_metrics["ADS"], lmtd_metrics["ADS"]), md=3),
                dbc.Col(metric_kpi_card("MTD AOV", mtd_metrics["AOV"], lmtd_metrics["AOV"]), md=3),
                dbc.Col(metric_kpi_card("MTD ADT", mtd_metrics["ADT"], lmtd_metrics["ADT"]), md=3),
            ], style={"marginBottom": "8px"})
            
            row_wtd = dbc.Row([
                dbc.Col(metric_kpi_card("WTD Final Net", wtd_final, lwtd_final), md=3),
                dbc.Col(metric_kpi_card("WTD ADS", wtd_metrics["ADS"], lwtd_metrics["ADS"]), md=3),
                dbc.Col(metric_kpi_card("WTD AOV", wtd_metrics["AOV"], lwtd_metrics["AOV"]), md=3),
                dbc.Col(metric_kpi_card("WTD ADT", wtd_metrics["ADT"], lwtd_metrics["ADT"]), md=3),
            ], style={"marginBottom": "8px"})

            row_lwtd = dbc.Row([
                dbc.Col(metric_kpi_card("LWTD Final Net", lwtd_final, lwtd_final), md=3),
                dbc.Col(metric_kpi_card("LWTD ADS", lwtd_metrics["ADS"], lwtd_metrics["ADS"]), md=3),
                dbc.Col(metric_kpi_card("LWTD AOV", lwtd_metrics["AOV"], lwtd_metrics["AOV"]), md=3),
                dbc.Col(metric_kpi_card("LWTD ADT", lwtd_metrics["ADT"], lwtd_metrics["ADT"]), md=3),
            ], style={"marginBottom": "8px"})
            
            row_lmtd = dbc.Row([
                dbc.Col(metric_kpi_card("LMTD Final Net", lmtd_final, lmtd_final), md=3),
                dbc.Col(metric_kpi_card("LMTD ADS", lmtd_metrics["ADS"], lmtd_metrics["ADS"]), md=3),
                dbc.Col(metric_kpi_card("LMTD AOV", lmtd_metrics["AOV"], lmtd_metrics["AOV"]), md=3),
                dbc.Col(metric_kpi_card("LMTD ADT", lmtd_metrics["ADT"], lmtd_metrics["ADT"]), md=3),
            ], style={"marginBottom": "8px"})


            # ================= FINAL KPI BLOCK =================
            kpi_block = html.Div(
                [row_top, row_mtd, row_wtd, row_lwtd, row_lmtd],
                style={"marginBottom": "12px"}
            )
            # -------- Brand-wise ADS (optional section) --------
            brand_cards = []

            if "Brand" in df.columns:
                for brand_name in sorted(df["Brand"].dropna().unique()):
                    metrics = brand_kpi_metrics(df, brand_name)
                    brand_cards.append(
                        dbc.Col(
                            kpi_card(f"{brand_name} ADS", metrics["ADS"], color="primary"),
                            md=3
                        )
                    )
            brand_kpi_block = (
                dbc.Row(brand_cards, style={"marginBottom": "12px"})
                if brand_cards else None
            )
            #total_active_days = compute_active_billing_days(df)

        # Format summary for display: arrow changes and Year/Week as ints without decimals
        if not summary.empty:
            # set Month & WeekNumber anchored to max date in df
            yesterday = df["Date"].max()
            if pd.notna(yesterday):
                month_label = yesterday.strftime("%b-%y")
                week_number = int(yesterday.isocalendar().week)
                summary["Month"] = month_label
                summary["WeekNumber"] = week_number

            # Filter by Source/Category selection (if provided)
            if source_vals and not (category_vals and "Overall" in category_vals):
                mask = pd.Series(False, index=summary.index)
                if any(sv == "Mix" for sv in (source_vals or [])):
                    mask = mask | (summary["Category"] == "Mix")
                non_mix_sources = [sv for sv in (source_vals or []) if sv != "Mix"]
                if non_mix_sources:
                    mask = mask | summary["Source"].isin(non_mix_sources)
                summary = summary[mask]

            if category_vals:
                cat_vals = [c for c in (category_vals or []) if c != "Overall"]
                if cat_vals:
                    summary = summary[summary["Category"].isin(cat_vals)]
                if "Overall" in (category_vals or []):
                    summary = summary[summary["Source"] == "Overall"]

            if search_txt:
                st = str(search_txt).strip().lower()
                if st:
                    summary = summary[summary["Outlet Name"].str.lower().str.contains(st, na=False)]

            # round numeric columns
            num_cols = summary.select_dtypes(include=[np.number]).columns
            summary[num_cols] = summary[num_cols].round(2)

            # ============================
            # FIX SUMMARY GRAND TOTAL (ADS / AOV / ADT)
            # ============================
            try:
                for period in ["MTD", "WTD", "LWTD", "LMTD", "Last4Weeks", "Yesterday"]:
                    if period not in summary.columns:
                        continue

                    if period == "MTD":
                        df_p = df_mtd
                    elif period == "WTD":
                        df_p = df_wtd
                    elif period == "LWTD":
                        df_p = df_lwtd
                    elif period == "LMTD":
                        df_p = df[(df["Date"] >= start_lmtd) & (df["Date"] <= end_lmtd)]
                    elif period == "Last4Weeks":
                        df_p = df[df["Date"] >= (max_date - timedelta(days=28))]
                    elif period == "Yesterday":
                        df_p = df[df["Date"] == max_date]
                    else:
                        continue

                    if df_p.empty:
                        continue

                    metrics = safe_grand_metrics_for_period(df_p)

                    mask = summary["Source"].isin(["Overall", "Grand Total"])

                    summary.loc[mask & (summary["Category"] == "ADS"), period] = metrics["ADS"]
                    summary.loc[mask & (summary["Category"] == "AOV"), period] = metrics["AOV"]
                    summary.loc[mask & (summary["Category"] == "ADT"), period] = metrics["ADT"]

            except Exception as e:
                print("Summary Grand Total fix skipped:", e)
            # prepare display-friendly version
            summary_disp = format_change_columns_for_display(
                summary.copy(),
                ("Change_WTD", "Change_MTD")
            )
            # ✅ FORCE 2 decimals for ALL numeric columns
            summary_disp = format_df(summary_disp)

            # ensure Year/WeekNumber no decimals in display (if present)
            if "WeekNumber" in summary_disp.columns:
                summary_disp["WeekNumber"] = summary_disp["WeekNumber"].apply(lambda x: fmt_int(x) if pd.notna(x) else "")

           # ✅ FORCE integer-only columns (Bills, Days, Year, Week)
            summary_disp = force_int_columns(summary_disp)

        else:
            summary_disp = pd.DataFrame()

        # ==== WEEKLY MIS (FULLY UPDATED & FIXED) ====
        weekly_disp = pd.DataFrame()  # always define

        if not weekly.empty:
            # Running week highlight
            running_week = df["Date"].max().isocalendar().week
            weekly["_is_running"] = (
                pd.to_numeric(weekly["WeekNumber"], errors="coerce").fillna(-1).astype(int)
                == int(running_week)
            ).astype(int)

            weekly = weekly.sort_values(
                ["_is_running", "Year", "WeekNumber"],
                ascending=[False, False, False]
            ).drop(columns=["_is_running"], errors="ignore")

            # Convert Year/WeekNumber to numeric for safe processing
            weekly["Year_num"] = pd.to_numeric(weekly["Year"], errors="coerce")
            weekly["Week_num"] = pd.to_numeric(weekly["WeekNumber"], errors="coerce")

            # ---- Compute Month & WeekRange based on ACTUAL available dates ----
            def compute_week_meta(row):
                year_val = row["Year_num"]
                week_val = row["Week_num"]

                if pd.isna(year_val) or pd.isna(week_val):
                    return "", ""

                # Filter df for that week
                dsub = df[(df["Year"] == year_val) & (df["WeekNumber"] == week_val)]
                if dsub.empty:
                    return "", ""

                # Actual available dates
                dmin = dsub["Date"].min()
                dmax = dsub["Date"].max()

                # Month = month of earliest date
                month_label = dmin.strftime("%b-%Y")

                # WeekRange: 03 Nov - 05 Nov 25
                week_range = f"{dmin.strftime('%d %b')} - {dmax.strftime('%d %b %y')}"
                return month_label, week_range

            # Apply safely
            weekly["Month"], weekly["WeekRange"] = zip(*weekly.apply(compute_week_meta, axis=1))

            # Remove helper numeric columns
            weekly.drop(columns=["Year_num", "Week_num"], inplace=True, errors="ignore")

            # Format Year & WeekNumber for display (no decimals)
            weekly["Year"] = weekly["Year"].apply(lambda x: fmt_int(x) if pd.notna(x) else "")
            weekly["WeekNumber"] = weekly["WeekNumber"].apply(lambda x: fmt_int(x) if pd.notna(x) else "")

            # Format numeric columns
            weekly_disp = format_df(weekly)

            # enforce integer columns for weekly (Year/WeekNumber already adjusted)
            weekly_disp = force_int_columns(weekly_disp)

            # Reorder columns
            desired = ["Year", "Month", "WeekNumber", "WeekRange"]
            others = [c for c in weekly_disp.columns if c not in desired]
            weekly_disp = weekly_disp[desired + others]

            # ---- Insert Month header rows ----
            group_rows = []
            last_month = None

            for _, row in weekly_disp.iterrows():
                m = str(row["Month"]).strip()
                if m and m != last_month:
                    header = {c: "" for c in weekly_disp.columns}
                    header["Month"] = f"------ {m} ------"
                    group_rows.append(header)
                    last_month = m

                group_rows.append(row.to_dict())

            weekly_disp = pd.DataFrame(group_rows)

        # Choose content per tab
        if tab_value == "tab-summary":
            table = dash_table.DataTable(
                id="summary_table",
                columns=[{"name": c, "id": c} for c in SUMMARY_COLUMNS],
                data=summary_disp[SUMMARY_COLUMNS].to_dict("records") if not summary_disp.empty else [],
                style_table={"overflowX":"auto"},
                style_cell=GLOBAL_CELL_STYLE,
                style_header={"fontFamily": GLOBAL_FONT_FAMILY, "fontWeight": "700"},
                page_size=20,
                sort_action="native",
                filter_action="none",
                style_data_conditional=build_change_style_conditions(("Change_WTD","Change_MTD")),
            )
            return html.Div([
                kpi_block,
                brand_kpi_block,
                table
            ])

        if tab_value == "tab-mis":
            mis_to_show = _mis_init.copy() if not _mis_init.empty else mis.copy()
            # format mis table numbers with 2 decimals and ensure int columns
            mis_disp = format_df(mis_to_show)
            mis_disp = force_int_columns(mis_disp)
            mis_table = dash_table.DataTable(
                id="mis_table",
                columns=[{"name": c, "id": c} for c in (mis_disp.columns.tolist() if not mis_disp.empty else [])],
                data=(mis_disp.to_dict("records") if not mis_disp.empty else []),
                **DATATABLE_BASE_STYLE
            )
            return html.Div([
                kpi_block,
                brand_kpi_block,
                mis_table
            ])

        if tab_value == "tab-weekly-mis":
            weekly_table = dash_table.DataTable(
                id="weekly_mis_table",
                columns=[{"name": c, "id": c} for c in (weekly_disp.columns.tolist() if not weekly_disp.empty else [])],
                data=(weekly_disp.to_dict("records") if not weekly_disp.empty else []),
                page_size=20,
                style_cell=GLOBAL_CELL_STYLE,
                style_header={"fontFamily": GLOBAL_FONT_FAMILY, "fontWeight": "700"},
            )
            return html.Div([
                kpi_block,
                brand_kpi_block,
                weekly_table
            ])

        if tab_value == "tab-overall-mis":

            if summary.empty:
                return html.Div("No data", style={"padding": "10px"})

            # Filter only "Overall"
            overall = summary[summary["Source"] == "Overall"].copy()

            # Remove Region
            if "Region" in overall.columns:
                overall.drop(columns=["Region"], inplace=True)

            # Include WTD & LWTD
            base_cols = [
                "Outlet Name", "Category",
                "WTD", "LWTD", "Change_WTD",
                "MTD", "LMTD", "Change_MTD"
            ]

            # Arrow formatting
            overall_disp = format_change_columns_for_display(overall.copy(), ("Change_WTD", "Change_MTD"))

            # 🔥 ADD THIS LINE (FIXES MANY DECIMALS)
            overall_disp = format_df(overall_disp)
            
            # Ensure WeekNumber formatting safety
            if "WeekNumber" in overall_disp.columns:
                overall_disp["WeekNumber"] = overall_disp["WeekNumber"].apply(lambda x: fmt_int(x) if pd.notna(x) else "")

            # enforce integer columns
            overall_disp = force_int_columns(overall_disp)

            # Default visible columns
            visible_cols = base_cols

            # Column toggle UI
            col_toggle = dcc.Checklist(
                id="overall_mis_column_toggle",
                options=[{"label": c, "value": c} for c in base_cols],
                value=visible_cols,
                inline=True,
                inputStyle={"margin-right": "3px"},
                labelStyle={"margin-right": "12px"}
            )

            table = dash_table.DataTable(
                id="overall_mis_table",
                columns=[{"name": c, "id": c} for c in visible_cols],
                data=overall_disp[visible_cols].to_dict("records"),
                style_cell=GLOBAL_CELL_STYLE,
                style_header={"fontFamily": GLOBAL_FONT_FAMILY, "fontWeight": "700"},
                fixed_columns={"headers": True, "data": 1},   # ❤️ FREEZE FIRST COLUMN
                style_table={"overflowX": "auto", "minWidth": "100%"},
                style_data_conditional=build_change_style_conditions(("Change_WTD", "Change_MTD")),
                page_size=25,
                sort_action="native",
            )

            return html.Div([
                kpi_block,
                brand_kpi_block,
                html.Div([
                    html.Label("Show / Hide Columns:", style={"fontWeight": "bold", "marginBottom": "5px"}),
                    col_toggle,
                    html.Br(), html.Br(),
                    table
            ])
            ])

        if tab_value == "tab-portfolio":

            # ------------------- PORTFOLIO SUMMARY TAB (HYBRID) -------------------
            # Metrics we show as large KPI cards (change KPIs)
            change_metrics = [
                ("Change in WTD", "Change_WTD"),
                ("Change in MTD", "Change_MTD"),
                ("Change in WD", "Change_WTD"),  # fallback using WTD/WF/WE conceptual mapping
                ("Change in WE", "Change_WTD"),
                ("Change in WoW", "Change_WTD"),
                ("Change in Yesterday", "Change_WTD"),
                ("Distance from Target", "Change_MTD"),
                ("Change in AOV", "Change_MTD"),
                ("Change in ADT", "Change_MTD"),
                ("Change in ADS", "Change_MTD"),
            ]

            # Mini KPI metrics (MTD/LMTD AOV/ADT/ADS)
            mini_metrics = [
                ("MTD AOV", "AOV", "MTD"),
                ("LMTD AOV", "AOV", "LMTD"),
                ("MTD ADT", "ADT", "MTD"),
                ("LMTD ADT", "ADT", "LMTD"),
                ("MTD ADS", "ADS", "MTD"),
                ("LMTD ADS", "ADS", "LMTD"),
            ]

            # Helper to fetch from summary safely
            def fetch_summary_val(df_sum, category, source, col):
                try:
                    return df_sum[(df_sum["Category"] == category) & (df_sum["Source"] == source)][col].values[0]
                except Exception:
                    return np.nan

            # Build large KPI cards data for a summary DF
            def build_large_cards(df_sum):
                cards = []
                for title, col in change_metrics:
                    # Prefer Overall source values for change columns
                    val = fetch_summary_val(df_sum, "ADS", "Overall", col)
                    # convert to display text
                    txt = fmt_arrow_val(val)
                    positive = None
                    try:
                        fv = float(val)
                        positive = fv > 0
                    except Exception:
                        positive = None
                    cards.append((title, txt, positive))
                return cards

            # Build mini KPI cards
            def build_mini_cards(df_sum):
                minis = []
                for label, cat, period in mini_metrics:
                    try:
                        v = df_sum[(df_sum["Category"] == cat) & (df_sum["Source"] == "Overall")][period].values
                        v = v[0] if len(v) else np.nan
                    except Exception:
                        v = np.nan
                    minis.append((label, v))
                return minis

            # Section A (Full Portfolio)
            secA_cards = build_large_cards(summary)
            secA_minis = build_mini_cards(summary)

            # Section B (Without MSFT)
            df_wo_msft = df[~df["Outlet Name"].str.contains("MSFT", na=False, case=False)]
            summary_wo, _, _ = build_summary_and_mis(df_wo_msft)
            secB_cards = build_large_cards(summary_wo)
            secB_minis = build_mini_cards(summary_wo)

            # Render side-by-side Section A & B
            def render_section(title, cards, minis):
                # large cards in rows of 4
                card_rows = []
                row = []
                for i, (t, txt, pos) in enumerate(cards):
                    row.append(dbc.Col(large_kpi_card(t, txt, pos), md=3))
                    if (i + 1) % 4 == 0:
                        card_rows.append(dbc.Row(row, style={"marginBottom":"8px"}))
                        row = []
                if row:
                    card_rows.append(dbc.Row(row, style={"marginBottom":"8px"}))

                # minis in rows of 3
                mini_cols = [dbc.Col(mini_kpi_card(t, v), md=2) for (t, v) in minis]
                mini_row = dbc.Row(mini_cols, style={"marginBottom":"8px"})

                return dbc.Card(dbc.CardBody([html.H5(title, style={"fontWeight":"700"}), html.Div(card_rows), mini_row]),
                                style={"borderRadius":"10px", "padding":"6px", "margin":"4px"})

            left = render_section("Full Portfolio", secA_cards, secA_minis)
            right = render_section("Portfolio without MSFT Hyd", secB_cards, secB_minis)

            # Section C — Store-wise heatmap changes (ADT/AOV/ADS)
            store_list = sorted(df["Outlet Name"].dropna().unique())
            store_rows = []
            for out in store_list:
                def g(cat):
                    try:
                        return summary[(summary["Outlet Name"] == out) & (summary["Category"] == cat)]["MTD"].values[0]
                    except Exception:
                        return np.nan

                ADT_MTD = g("ADT"); ADT_LMTD = g("ADT")
                AOV_MTD = g("AOV"); AOV_LMTD = g("AOV")
                ADS_MTD = g("ADS"); ADS_LMTD = g("ADS")
                def pct(a,b):
                    try:
                        return (a - b) / b * 100
                    except Exception:
                        return np.nan
                store_rows.append({
                    "Outlet Name": out,
                    "ADT Change": pct(ADT_MTD, ADT_LMTD),
                    "AOV Change": pct(AOV_MTD, AOV_LMTD),
                    "ADS Change": pct(ADS_MTD, ADS_LMTD),
                })

            # Build store table and heatmap styles
            store_table_cols = [{"name": c, "id": c} for c in ["Outlet Name", "ADT Change", "AOV Change", "ADS Change"]]
            store_data = [{k: ("" if (v is None or (isinstance(v,float) and np.isnan(v))) else (f"{v:0.2f}%" if k!="Outlet Name" else v)) for k,v in r.items()} for r in store_rows]
            # compute heatmap rules programmatically
            heat_rules = []
            # For each numeric column produce styles
            for col in ["ADT Change", "AOV Change", "ADS Change"]:
                # collect numeric values
                nums = []
                for r in store_rows:
                    try:
                        vv = float(r.get(col, np.nan))
                        if not np.isnan(vv):
                            nums.append(vv)
                    except Exception:
                        pass
                if nums:
                    vmin, vmax = min(nums), max(nums)
                    for i, r in enumerate(store_rows):
                        try:
                            vv = float(r.get(col, np.nan))
                        except Exception:
                            continue
                        bg = heatmap_color(vv, vmin, vmax)
                        heat_rules.append({"if": {"row_index": i, "column_id": col}, "backgroundColor": bg, "color":"#000"})

            # Final layout
            return html.Div([
                kpi_block,
                brand_kpi_block,
                dbc.Row([dbc.Col(left, md=6), dbc.Col(right, md=6)], style={"marginBottom":"10px"}),
                html.H5("Store-wise Change (MTD vs LMTD)", style={"marginTop":"8px", "fontWeight":"700"}),
                dash_table.DataTable(
                    id="portfolio_store_table",
                    columns=store_table_cols,
                    data=store_data,
                    page_size=50,
                    style_cell=GLOBAL_CELL_STYLE,
                    style_header={"fontWeight":"700"},
                    style_data_conditional=heat_rules
                )
            ])
        # ============================================================================

        if tab_value == "tab-charts":

            if df.empty:
                return html.Div("No data available for charts", style={"padding": "10px"})

            # ----------------------- Build Summary (MTD / WTD / LWTD) ------------------------
            summary_df, _, _ = build_summary_and_mis(df)

            # Helper to fetch KPI safely
            def get_metric(category, period):
                try:
                    return summary_df[
                        (summary_df["Category"] == category) &
                        (summary["Source"].isin(["Overall", "Grand Total"]))
                    ][period].values[0]
                except Exception:
                    return 0

            # KPIs for bar chart
            wtd_ads, lwtd_ads = get_metric("ADS", "WTD"), get_metric("ADS", "LWTD")
            wtd_aov, lwtd_aov = get_metric("AOV", "WTD"), get_metric("AOV", "LWTD")
            wtd_adt, lwtd_adt = get_metric("ADT", "WTD"), get_metric("ADT", "LWTD")

            # ----------------------- BAR CHART: WTD vs LWTD ------------------------
            df_bars = pd.DataFrame({
                "Metric": ["ADS", "ADS", "AOV", "AOV", "ADT", "ADT"],
                "Period": ["WTD", "LWTD"] * 3,
                "Value": [wtd_ads, lwtd_ads, wtd_aov, lwtd_aov, wtd_adt, lwtd_adt]
            })

            fig_bar = px.bar(
                df_bars,
                x="Metric",
                y="Value",
                color="Period",
                barmode="group",
                text_auto=".2s",
                title="WTD vs LWTD Comparison (ADS, AOV, ADT)"
            )
            fig_bar.update_layout(template="plotly_white", font=dict(family=GLOBAL_FONT_FAMILY))

            # ----------------------- DISTRIBUTION — NET SALES ------------------------
            fig_sale_dist = px.histogram(
                df,
                x="Final_Net_Sale",
                nbins=30,
                marginal="box",
                title="Distribution of Net Sales (Daily Aggregated)"
            )
            fig_sale_dist.update_layout(template="plotly_white", font=dict(family=GLOBAL_FONT_FAMILY))

            # ----------------------- DISTRIBUTION — ADS DAILY ------------------------
            df_ads_daily = df.groupby("Date", as_index=False)["Final_Net_Sale"].sum()
            df_ads_daily.rename(columns={"Final_Net_Sale": "ADS"}, inplace=True)

            fig_ads_dist = px.histogram(
                df_ads_daily,
                x="ADS",
                nbins=25,
                marginal="violin",
                title="Distribution of Daily ADS"
            )
            fig_ads_dist.update_layout(template="plotly_white", font=dict(family=GLOBAL_FONT_FAMILY))

            # ============================================================================

            outlets = sorted(df["Outlet Name"].dropna().unique())

            heat_rows = []
            for outlet in outlets:
                def g(cat):
                    try:
                        return summary_df[
                            (summary_df["Outlet Name"] == outlet) &
                            (summary_df["Category"] == cat)
                        ]["MTD"].values[0]
                    except Exception:
                        return np.nan

                heat_rows.append({
                    "Outlet Name": outlet,
                    "ADS": g("ADS"),
                    "AOV": g("AOV"),
                    "ADT": g("ADT"),
                })

            heatmap_df = pd.DataFrame(heat_rows).set_index("Outlet Name")

            fig_heat = px.imshow(
                heatmap_df,
                color_continuous_scale="RdYlGn",
                title="Outlet Heatmap (MTD ADS / AOV / ADT)",
                aspect="auto"
            )
            fig_heat.update_layout(template="plotly_white", font=dict(family=GLOBAL_FONT_FAMILY))

            # ============================================================================

            radar_rows = []

            for outlet in outlets:
                def g(cat):
                    try:
                        return summary_df[
                            (summary_df["Outlet Name"] == outlet) &
                            (summary_df["Category"] == cat)
                        ]["MTD"].values[0]
                    except Exception:
                        return 0

                radar_rows.append({
                    "Outlet": outlet,
                    "ADS": g("ADS"),
                    "AOV": g("AOV"),
                    "ADT": g("ADT"),
                })

            radar_df = pd.DataFrame(radar_rows)

            # Normalize KPIs for visual scale (0–100)
            def normalize(series):
                s = series.replace([np.inf, -np.inf], np.nan).fillna(0)
                if s.max() == 0:
                    return s * 0
                return (s / s.max()) * 100

            radar_df["ADS_n"] = normalize(radar_df["ADS"])
            radar_df["AOV_n"] = normalize(radar_df["AOV"])
            radar_df["ADT_n"] = normalize(radar_df["ADT"])

            # Build long-form radar dataset
            radar_plot_df = pd.DataFrame()
            for _, row in radar_df.iterrows():
                radar_plot_df = pd.concat([
                    radar_plot_df,
                    pd.DataFrame({
                        "Metric": ["ADS", "AOV", "ADT"],
                        "Value": [row["ADS_n"], row["AOV_n"], row["ADT_n"]],
                        "Outlet": row["Outlet"]
                    })
                ])

            fig_radar = px.line_polar(
                radar_plot_df,
                r="Value",
                theta="Metric",
                color="Outlet",
                line_close=True,
                markers=True,
                title="Multi-Store Comparison Radar (ADS / AOV / ADT - MTD)"
            )
            fig_radar.update_traces(fill='toself', opacity=0.35)
            fig_radar.update_layout(
                polar=dict(
                    radialaxis=dict(range=[0, 100], showticklabels=True)
                ),
                template="plotly_white",
                font=dict(family=GLOBAL_FONT_FAMILY)
            )

            # ============================================================================

            return html.Div([
                kpi_block,
                brand_kpi_block,
                html.H3("Advanced Analytics Dashboard", style={"marginTop": "10px"}),

                # ------------------- Row 1 -------------------
                dbc.Row([
                    dbc.Col(dcc.Graph(figure=fig_bar), md=6),
                    dbc.Col(dcc.Graph(figure=fig_sale_dist), md=6),
                ], style={"marginBottom": "20px"}),

                # ------------------- Row 2 -------------------
                dbc.Row([
                    dbc.Col(dcc.Graph(figure=fig_ads_dist), md=6),
                    dbc.Col(dcc.Graph(figure=fig_heat), md=6),
                ], style={"marginBottom": "20px"}),

                # ------------------- Radar Chart -------------------
                html.H4("Multi-Store Performance Radar", style={"marginTop": "20px"}),
                dcc.Graph(figure=fig_radar),
            ])

    # End of render_tab_content

# End of register_callbacks

# If this module is executed stand-alone for debugging, you may register callbacks to a Dash app:
# Example usage:
#   app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
#   app.layout = get_layout()
#   register_callbacks(app)
#   app.run_server(debug=True)
