# pages/sales_page.py
# Coffee Island – Sales Dashboard
#
# Data sources:
#   - Main sales data:      C:\Users\ACER\store_dashboard\daily_sales\*.csv
#   - City & Type mapping:  C:\Users\ACER\store_dashboard\stores_db.csv
#
# Features:
#   - Auto date parsing (handles DD-MM vs MM-DD etc.)
#   - Running-month auto-selected date range
#   - Quick buttons: Yesterday, Last 7 Days, This Month, This Year
#   - RESET button: resets date range + all filters to default
#   - Filters: Region, City, Type, Outlet, Tabs, Month, Week, Day
#   - KPIs: Total Sale, Discount, Final Net Sale, Total Tax, Total Amount,
#           Total Covers, Total Bills, Total Days, AOV, ADS, ADT
#   - Detailed table (NO Source column)
#   - Charge summary, Outlet summary, Top 5 outlets
#   - Charts: trends, outlet trend, area, region bar, tab-wise metrics,
#             charge breakdown

import os
import glob
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd
import plotly.express as px

from dash import html, dcc, dash_table, Input, Output, callback_context
import dash_bootstrap_components as dbc

# -----------------------------------------------------------
# Paths
# -----------------------------------------------------------

BASE_DIR = r"C:\Users\ACER\store_dashboard"
DAILY_SALES_FOLDER = os.path.join(BASE_DIR, "daily_sales")
STORES_DB_FILE = os.path.join(BASE_DIR, "stores_db.csv")


# -----------------------------------------------------------
# Formatting helpers
# -----------------------------------------------------------

def fmt_float(x):
    """Format number with commas and 2 decimals."""
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return x


def fmt_int(x):
    """Format integer with commas (no decimals)."""
    try:
        return f"{int(round(float(x))):,}"
    except Exception:
        return x


def format_table_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply formatting to table:
    - All numeric columns with 2 decimals,
    - Except 'No Of Bills' → integer with commas.
    """
    if df is None or df.empty:
        return df

    out = df.copy()
    num_cols = out.select_dtypes(include=["float", "int"]).columns

    for col in num_cols:
        if col == "No Of Bills":
            out[col] = out[col].map(fmt_int)
        else:
            out[col] = out[col].map(fmt_float)

    return out


# -----------------------------------------------------------
# CSV + date helpers
# -----------------------------------------------------------

def smart_read_csv(path: str) -> pd.DataFrame:
    """
    Smart CSV loader:
    - Try skiprows 0..4, keep version with most columns
    - Drop fully empty rows/cols
    - Strip headers
    """
    best_df = None
    best_cols = 0

    for skip in range(0, 5):
        try:
            df_try = pd.read_csv(path, skiprows=skip)
        except Exception:
            continue

        if df_try is None or df_try.empty:
            continue

        if df_try.shape[1] > best_cols:
            best_df = df_try
            best_cols = df_try.shape[1]

    if best_df is None or best_df.shape[1] <= 1:
        print(f"Skipping unreadable/invalid CSV: {path}")
        return pd.DataFrame()

    df = best_df.dropna(how="all").dropna(axis=1, how="all")
    df.columns = df.columns.astype(str).str.strip()
    return df.reset_index(drop=True)


def parse_dates_auto(series: pd.Series) -> pd.Series:
    """
    Smart date parser:
    - Try DD-MM first (Indian format)
    - Try MM-DD next
    - Pick the version with more valid dates
    - Extra safeguard to avoid Dec→Jan mis-parsing.
    """
    s = series.astype(str).str.strip()

    # Try day-first
    d1 = pd.to_datetime(s, errors="coerce", dayfirst=True)

    # Try month-first
    d2 = pd.to_datetime(s, errors="coerce", dayfirst=False)

    # Choose the parse with more valid dates
    if d1.notna().sum() > d2.notna().sum():
        return d1

    # Extra safeguard: check December vs January bias
    try:
        d1_month_counts = d1.dt.month.value_counts()
        d2_month_counts = d2.dt.month.value_counts()
        if (
            d2_month_counts.get(1, 0) > d1_month_counts.get(1, 0)
            and d1_month_counts.get(12, 0) > d2_month_counts.get(12, 0)
        ):
            return d1
    except Exception:
        pass

    return d2


# -----------------------------------------------------------
# Normalize daily_sales CSVs into canonical schema
# -----------------------------------------------------------

def normalize_daily_csv(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize one daily_sales CSV into canonical columns:

    Date, Region, Outlet Name, Tabs,
    No Of Items, No Of Bills, Sale, Discount,
    Net Sale, Charges, Final_Net_Sale, Total Tax,
    Total Amount, Covers
    (NO Source column in this version)
    """
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()

    df = df_raw.copy()
    lower_to_orig = {c.lower(): c for c in df.columns}

    def find(candidates):
        for cand in candidates:
            if cand in lower_to_orig:
                return lower_to_orig[cand]
        for cand in candidates:
            for lc, orig in lower_to_orig.items():
                if cand in lc:
                    return orig
        return None

    rename_map = {}

    # --- Core dimension columns ---
    date_col = find(["date", "txn date", "bill date"])
    if date_col:
        rename_map[date_col] = "Date"

    region_col = find(["region"])
    if region_col:
        rename_map[region_col] = "Region"

    outlet_col = find(["outlet name", "outlet", "store", "restaurant"])
    if outlet_col:
        rename_map[outlet_col] = "Outlet Name"

    tabs_col = find(["tabs", "tab type", "channel"])
    if tabs_col:
        rename_map[tabs_col] = "Tabs"

    # --- Measures ---
    no_items_col = find(["no of items", "quantity", "items"])
    if no_items_col:
        rename_map[no_items_col] = "No Of Items"

    no_bills_col = find(["no of bills", "bills"])
    if no_bills_col:
        rename_map[no_bills_col] = "No Of Bills"

    sale_col = find(["sale", "gross amount", "gross"])
    if sale_col:
        rename_map[sale_col] = "Sale"

    discount_col = find(["discount"])
    if discount_col:
        rename_map[discount_col] = "Discount"

    net_sale_col = find(["net sale"])
    if net_sale_col:
        rename_map[net_sale_col] = "Net Sale"

    charges_col = find(["charges", "total charges"])
    if charges_col:
        rename_map[charges_col] = "Charges"

    final_net_col = find(["final_net_sale", "final net sale", "final net"])
    if final_net_col:
        rename_map[final_net_col] = "Final_Net_Sale"

    total_tax_col = find(["total tax", "tax"])
    if total_tax_col:
        rename_map[total_tax_col] = "Total Tax"

    total_amount_col = find(["total amount", "grand total"])
    if total_amount_col:
        rename_map[total_amount_col] = "Total Amount"

    covers_col = find(["covers"])
    if covers_col:
        rename_map[covers_col] = "Covers"

    # --- Charge breakdown components ---
    rc_col = find(["restaurant charge"])
    if rc_col:
        rename_map[rc_col] = "Restaurant Charge"

    cart_pkg_col = find(["packaging charge [cart - swiggy]", "cart - swiggy"])
    if cart_pkg_col:
        rename_map[cart_pkg_col] = "Packaging Charge [CART - SWIGGY]"

    rest_pkg_col = find(["restaurant packaging charges", "restaurant packaging"])
    if rest_pkg_col:
        rename_map[rest_pkg_col] = "Restaurant Packaging Charges"

    delv_col = find(["delivery charge"])
    if delv_col:
        rename_map[delv_col] = "Delivery Charge"

    plat_col = find(["platform fee charge", "platform fee"])
    if plat_col:
        rename_map[plat_col] = "Platform Fee Charge"

    smile_col = find(["smile amount charge", "smile amount"])
    if smile_col:
        rename_map[smile_col] = "Smile Amount Charge"

    pkg_col = find(["packaging charge"])
    if pkg_col:
        rename_map[pkg_col] = "Packaging Charge"

    # --- Apply renames ---
    df = df.rename(columns=rename_map)

    # --- Date parsing ---
    if "Date" not in df.columns:
        print("CSV missing Date column after normalization, skipping file.")
        return pd.DataFrame()

    df["Date"] = parse_dates_auto(df["Date"])
    df = df.dropna(subset=["Date"])

    # --- Ensure numeric fields ---
    numeric_cols = [
        "No Of Items",
        "No Of Bills",
        "Sale",
        "Discount",
        "Net Sale",
        "Charges",
        "Final_Net_Sale",
        "Total Tax",
        "Total Amount",
        "Covers",
        "Restaurant Charge",
        "Packaging Charge [CART - SWIGGY]",
        "Restaurant Packaging Charges",
        "Delivery Charge",
        "Platform Fee Charge",
        "Smile Amount Charge",
        "Packaging Charge",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                pd.to_numeric(
                    df[col].astype(str).str.replace(",", "", regex=False),
                    errors="coerce",
                ).fillna(0.0)
            )

    # --- Derive core fields if missing ---
    if "Sale" not in df.columns:
        df["Sale"] = df.get("Net Sale", 0.0) + df.get("Discount", 0.0)

    if "Net Sale" not in df.columns:
        df["Net Sale"] = df.get("Sale", 0.0) - df.get("Discount", 0.0)

    # Charges from components if not present
    if "Charges" not in df.columns:
        comp = [
            "Restaurant Charge",
            "Packaging Charge [CART - SWIGGY]",
            "Restaurant Packaging Charges",
            "Delivery Charge",
            "Platform Fee Charge",
            "Smile Amount Charge",
            "Packaging Charge",
        ]
        use_cols = [c for c in comp if c in df.columns]
        if use_cols:
            df["Charges"] = df[use_cols].sum(axis=1)
        else:
            df["Charges"] = 0.0

    # Final Net Sale
    if "Final_Net_Sale" not in df.columns:
        df["Final_Net_Sale"] = df["Net Sale"] + df["Charges"]

    # Text fields
    for col in ["Region", "Outlet Name", "Tabs"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
        else:
            df[col] = ""

    return df


# -----------------------------------------------------------
# Load daily sales + merge City & Type from stores_db
# -----------------------------------------------------------

def load_daily_sales() -> pd.DataFrame:
    """Load all daily_sales CSVs, normalize, merge City & Type, add time keys."""
    if not os.path.exists(DAILY_SALES_FOLDER):
        print("Daily sales folder not found:", DAILY_SALES_FOLDER)
        return pd.DataFrame()

    files = glob.glob(os.path.join(DAILY_SALES_FOLDER, "*.csv"))
    if not files:
        print("No CSV files found in daily_sales.")
        return pd.DataFrame()

    frames = []
    for f in files:
        raw = smart_read_csv(f)
        if raw.empty:
            continue
        norm = normalize_daily_csv(raw)
        if not norm.empty:
            frames.append(norm)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # --- Merge City & Type from stores_db ---
    if os.path.exists(STORES_DB_FILE):
        try:
            stores = pd.read_csv(STORES_DB_FILE)
            stores.columns = stores.columns.astype(str).str.strip()
            lower_to_orig = {c.lower(): c for c in stores.columns}

            def store_pick(target):
                key = target.lower()
                if key in lower_to_orig:
                    return lower_to_orig[key]
                for lc, orig in lower_to_orig.items():
                    if key in lc:
                        return orig
                return None

            outlet_col = store_pick("outlet name") or store_pick("outlet")
            city_col = store_pick("city")
            type_col = store_pick("type")

            rename_map = {}
            if outlet_col:
                rename_map[outlet_col] = "Outlet Name"
            if city_col:
                rename_map[city_col] = "City"
            if type_col:
                rename_map[type_col] = "Type"

            stores = stores.rename(columns=rename_map)

            cols_to_use = ["Outlet Name"]
            if "City" in stores.columns:
                cols_to_use.append("City")
            if "Type" in stores.columns:
                cols_to_use.append("Type")

            stores = stores[cols_to_use].drop_duplicates()
            df = df.merge(stores, how="left", on="Outlet Name", suffixes=("", "_store"))
        except Exception as e:
            print("Warning merging stores_db.csv:", e)

    # Ensure City/Type exist
    if "City" not in df.columns:
        df["City"] = ""
    if "Type" not in df.columns:
        df["Type"] = ""

    # --- Time keys ---
    df["Year"] = df["Date"].dt.year
    df["WeekNumber"] = df["Date"].dt.isocalendar().week.astype(int)
    df["YearWeek"] = (
        df["Year"].astype(str)
        + "-W"
        + df["WeekNumber"].astype(str).str.zfill(2)
    )
    df["MonthKey"] = df["Date"].dt.to_period("M").astype(str)
    df["MonthLabel"] = df["Date"].dt.strftime("%b-%Y")
    df["DayName"] = df["Date"].dt.day_name()
    dow = df["Date"].dt.dayofweek
    df["DayCode"] = np.select(
        [dow.isin([0, 1, 2, 3]), dow == 4, dow.isin([5, 6])],
        ["WD", "WF", "WE"],
        default="",
    )

    return df.reset_index(drop=True)


# -----------------------------------------------------------
# KPI computation
# -----------------------------------------------------------

def compute_kpis(df: pd.DataFrame) -> dict:
    """Compute KPIs on filtered data."""
    if df is None or df.empty:
        return {
            "total_sale": 0.0,
            "discount": 0.0,
            "final_net": 0.0,
            "total_tax": 0.0,
            "total_amount": 0.0,
            "covers": 0.0,
            "bills": 0.0,
            "days": 0.0,
            "aov": 0.0,
            "ads": 0.0,
            "adt": 0.0,
        }

    total_sale = df["Sale"].sum()
    discount = df["Discount"].sum()
    final_net = df["Final_Net_Sale"].sum()
    total_tax = df["Total Tax"].sum() if "Total Tax" in df.columns else 0.0
    total_amount = df["Total Amount"].sum() if "Total Amount" in df.columns else 0.0
    bills = df["No Of Bills"].sum()
    covers = df["Covers"].sum() if "Covers" in df.columns else bills
    days = df["Date"].dt.normalize().nunique()

    aov = final_net / bills if bills else 0.0
    ads = final_net / days if days else 0.0
    adt = bills / days if days else 0.0

    return {
        "total_sale": total_sale,
        "discount": discount,
        "final_net": final_net,
        "total_tax": total_tax,
        "total_amount": total_amount,
        "covers": covers,
        "bills": bills,
        "days": days,
        "aov": aov,
        "ads": ads,
        "adt": adt,
    }


def kpi_card(title, value, integer=False):
    """Simple KPI card (used only in empty-state)."""
    if integer:
        text = fmt_int(value)
    else:
        text = fmt_float(value)
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(title, style={"fontSize": "12px", "color": "#6B7280"}),
                html.Div(
                    text,
                    style={"fontSize": "18px", "fontWeight": "bold", "color": "#111827"},
                ),
            ]
        ),
        style={
            "borderRadius": "10px",
            "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
            "backgroundColor": "white",
        },
    )


def empty_fig(message="No data"):
    """Blank placeholder figure with centered message."""
    fig = px.scatter()
    fig.update_layout(
        annotations=[dict(text=message, x=0.5, y=0.5, showarrow=False)],
        xaxis={"visible": False},
        yaxis={"visible": False},
        margin=dict(l=0, r=0, t=0, b=0),
    )
    return fig


# -----------------------------------------------------------
# Layout factory
# -----------------------------------------------------------

def get_layout():
    df_init = load_daily_sales()

    if df_init is None or df_init.empty:
        today = datetime.today().date()
        start_date = today.replace(day=1)
        end_date = today
        month_options = []
        week_options = []
        region_options = []
        city_options = []
        type_options = []
        outlet_options = []
        tabs_options = []
    else:
        # --- Default date: running month of latest data ---
        latest_date = df_init["Date"].max()
        start_date = latest_date.replace(day=1).date()
        end_date = latest_date.date()

        # --- Month dropdown (full, will be refined in callback) ---
        month_options = [
            {"label": ml, "value": mk}
            for mk, ml in (
                df_init[["MonthKey", "MonthLabel"]]
                .dropna()
                .drop_duplicates()
                .sort_values("MonthKey")
                .itertuples(index=False, name=None)
            )
        ]

        # --- Week dropdown (full, will be refined in callback) ---
        weeks = (
            df_init[["YearWeek", "Year", "WeekNumber"]]
            .drop_duplicates()
            .sort_values(["Year", "WeekNumber"])
        )
        week_options = [
            {
                "label": f"Wk {int(row.WeekNumber)} - {int(row.Year)}",
                "value": row.YearWeek,
            }
            for _, row in weeks.iterrows()
        ]

        # --- Filter dropdown options ---
        region_options = [
            {"label": r, "value": r}
            for r in sorted(df_init["Region"].dropna().unique())
        ]
        city_options = [
            {"label": c, "value": c}
            for c in sorted(df_init["City"].dropna().unique())
        ]
        type_options = [
            {"label": t, "value": t}
            for t in sorted(df_init["Type"].dropna().unique())
        ]
        outlet_options = [
            {"label": o, "value": o}
            for o in sorted(df_init["Outlet Name"].dropna().unique())
        ]
        tabs_options = [
            {"label": t, "value": t}
            for t in sorted(df_init["Tabs"].dropna().unique())
        ]

    # --- Day filter options ---
    day_options = [
        {"label": "All Days", "value": "ALL"},
        {"label": "Monday", "value": "Monday"},
        {"label": "Tuesday", "value": "Tuesday"},
        {"label": "Wednesday", "value": "Wednesday"},
        {"label": "Thursday", "value": "Thursday"},
        {"label": "Friday", "value": "Friday"},
        {"label": "Saturday", "value": "Saturday"},
        {"label": "Sunday", "value": "Sunday"},
        {"label": "WD (Mon-Thu)", "value": "WD"},
        {"label": "WF (Fri)", "value": "WF"},
        {"label": "WE (Sat-Sun)", "value": "WE"},
    ]

    # --- Detailed table columns definition (NO Source) ---
    detailed_table_columns = [
        {"name": "Date", "id": "Date"},
        {"name": "Region", "id": "Region"},
        {"name": "City", "id": "City"},
        {"name": "Type", "id": "Type"},
        {"name": "Outlet Name", "id": "Outlet Name"},
        {"name": "Tabs", "id": "Tabs"},
        {"name": "No Of Items", "id": "No Of Items"},
        {"name": "No Of Bills", "id": "No Of Bills"},
        {"name": "Sale", "id": "Sale"},
        {"name": "Discount", "id": "Discount"},
        {"name": "Net Sale", "id": "Net Sale"},
        {"name": "Charges", "id": "Charges"},
        {"name": "Final Net Sale", "id": "Final_Net_Sale"},
    ]

    # --- Charge summary table columns ---
    charge_columns = [
        {"name": "Region", "id": "Region"},
        {"name": "Outlet Name", "id": "Outlet Name"},
        {"name": "Restaurant Charge", "id": "Restaurant Charge"},
        {"name": "Packaging Charge [CART - SWIGGY]", "id": "Packaging Charge [CART - SWIGGY]"},
        {"name": "Restaurant Packaging Charges", "id": "Restaurant Packaging Charges"},
        {"name": "Delivery Charge", "id": "Delivery Charge"},
        {"name": "Platform Fee Charge", "id": "Platform Fee Charge"},
        {"name": "Smile Amount Charge", "id": "Smile Amount Charge"},
        {"name": "Charges", "id": "Charges"},
    ]

    # --- Layout ---
    return html.Div(
        style={
            "backgroundColor": "#F3F4F6",
            "minHeight": "100vh",
            "padding": "12px",
        },
        children=[
            html.Div(
                style={
                    "backgroundColor": "white",
                    "borderRadius": "10px",
                    "padding": "16px",
                    "boxShadow": "0 4px 10px rgba(0,0,0,0.08)",
                },
                children=[
                    html.H2(
                        "📊 Sales Dashboard",
                        style={"textAlign": "center", "marginBottom": "12px"},
                    ),

                    # --- Date + Month + Week row ---
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Label("Date Range"),
                                    dcc.DatePickerRange(
                                        id="date-range",
                                        start_date=start_date,
                                        end_date=end_date,
                                        display_format="DD-MM-YYYY",
                                    ),
                                ],
                                md=4,
                            ),
                            dbc.Col(
                                [
                                    html.Label("Month"),
                                    dcc.Dropdown(
                                        id="sales_month_filter",
                                        options=month_options,
                                        multi=False,
                                        placeholder="All Months",
                                    ),
                                ],
                                md=4,
                            ),
                            dbc.Col(
                                [
                                    html.Label("Week"),
                                    dcc.Dropdown(
                                        id="sales_week_filter",
                                        options=week_options,
                                        multi=True,
                                        placeholder="All Weeks",
                                    ),
                                ],
                                md=4,
                            ),
                        ],
                        style={"marginBottom": "10px"},
                    ),

                    # --- Filters row 1: Region / City / Type / Outlet ---
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Label("Region"),
                                    dcc.Dropdown(
                                        id="sales_region_filter",
                                        options=region_options,
                                        multi=True,
                                        placeholder="All Regions",
                                    ),
                                ],
                                md=3,
                            ),
                            dbc.Col(
                                [
                                    html.Label("City"),
                                    dcc.Dropdown(
                                        id="sales_city_filter",
                                        options=city_options,
                                        multi=True,
                                        placeholder="All Cities",
                                    ),
                                ],
                                md=3,
                            ),
                            dbc.Col(
                                [
                                    html.Label("Type"),
                                    dcc.Dropdown(
                                        id="sales_type_filter",
                                        options=type_options,
                                        multi=True,
                                        placeholder="All Types",
                                    ),
                                ],
                                md=3,
                            ),
                            dbc.Col(
                                [
                                    html.Label("Outlet Name"),
                                    dcc.Dropdown(
                                        id="sales_outlet_filter",
                                        options=outlet_options,
                                        multi=True,
                                        placeholder="All Outlets",
                                    ),
                                ],
                                md=3,
                            ),
                        ],
                        style={"marginBottom": "10px"},
                    ),

                    # --- Filters row 2: Tabs / Day / Search ---
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Label("Tabs"),
                                    dcc.Dropdown(
                                        id="sales_tabs_filter",
                                        options=tabs_options,
                                        multi=True,
                                        placeholder="All Tabs",
                                    ),
                                ],
                                md=4,
                            ),
                            dbc.Col(
                                [
                                    html.Label("Day"),
                                    dcc.Dropdown(
                                        id="sales_day_filter",
                                        options=day_options,
                                        multi=False,
                                        value="ALL",
                                    ),
                                ],
                                md=4,
                            ),
                            dbc.Col(
                                [
                                    html.Label("Search Outlet (letters)"),
                                    dcc.Input(
                                        id="sales_search_text",
                                        type="text",
                                        placeholder="Type letters... e.g. 'gk'",
                                        style={"width": "100%"},
                                    ),
                                ],
                                md=4,
                            ),
                        ],
                        style={"marginBottom": "10px"},
                    ),

                    # --- Refresh + Reset buttons ---
                    html.Div(
                        [
                            html.Button(
                                "Refresh Data",
                                id="sales_refresh_button",
                                n_clicks=0,
                                style={"width": "150px", "marginRight": "8px"},
                            ),
                            html.Button(
                                "Reset Filters",
                                id="sales_reset_button",
                                n_clicks=0,
                                style={"width": "150px", "backgroundColor": "#ef4444", "color": "white"},
                            ),
                        ],
                        style={"textAlign": "right", "marginBottom": "10px"},
                    ),

                    # --- Quick date buttons ---
                    html.Div(
                        [
                            html.Button(
                                "Yesterday",
                                id="btn_yesterday",
                                n_clicks=0,
                                style={"marginRight": "8px"},
                            ),
                            html.Button(
                                "Last 7 Days",
                                id="btn_last7",
                                n_clicks=0,
                                style={"marginRight": "8px"},
                            ),
                            html.Button(
                                "This Month",
                                id="btn_month",
                                n_clicks=0,
                                style={"marginRight": "8px"},
                            ),
                            html.Button(
                                "This Year",
                                id="btn_year",
                                n_clicks=0,
                                style={"marginRight": "8px"},
                            ),
                        ],
                        style={"marginBottom": "14px", "textAlign": "left"},
                    ),

                    # --- KPIs + Range summary badge ---
                    html.Div(id="sales_kpi_row", style={"marginBottom": "12px"}),
                    html.Div(
                        id="range-summary",
                        style={"textAlign": "center", "marginBottom": "15px"},
                    ),

                    # --- Charge summary table ---
                    html.H4(
                        "💰 Charge Summary (Region & Outlet)",
                        style={"marginTop": "8px", "marginBottom": "4px"},
                    ),
                    dash_table.DataTable(
                        id="sales_charge_summary",
                        columns=charge_columns,
                        page_size=10,
                        style_table={"overflowX": "auto"},
                        style_cell={
                            "textAlign": "center",
                            "padding": "6px",
                            "fontSize": "12px",
                            "fontFamily": "Arial",
                        },
                        style_header={
                            "backgroundColor": "#0f766e",
                            "color": "white",
                            "fontWeight": "bold",
                        },
                    ),

                    # --- Outlet summary table ---
                    html.H4(
                        "📄 Outlet Summary (Aggregated)",
                        style={"marginTop": "12px", "marginBottom": "4px"},
                    ),
                    dash_table.DataTable(
                        id="sales_outlet_summary_table",
                        columns=[
                            {"name": "Outlet Name", "id": "Outlet Name"},
                            {"name": "Net Sale", "id": "Net Sale"},
                            {"name": "Charges", "id": "Charges"},
                            {"name": "Discount", "id": "Discount"},
                            {"name": "Total Tax", "id": "Total Tax"},
                            {"name": "Total Final Sale", "id": "Total Final Sale"},
                        ],
                        page_size=10,
                        style_table={"overflowX": "auto"},
                        style_cell={
                            "textAlign": "center",
                            "padding": "6px",
                            "fontSize": "12px",
                            "fontFamily": "Arial",
                        },
                        style_header={
                            "backgroundColor": "#1d4ed8",
                            "color": "white",
                            "fontWeight": "bold",
                        },
                    ),

                    # --- Top 5 outlets table ---
                    html.H4(
                        "⭐ Top 5 Outlet Sale (by Total Final Sale)",
                        style={"marginTop": "12px", "marginBottom": "4px"},
                    ),
                    dash_table.DataTable(
                        id="sales_top5_outlet_table",
                        columns=[
                            {"name": "Outlet Name", "id": "Outlet Name"},
                            {"name": "Date", "id": "Date"},
                            {"name": "Total Final Sale", "id": "Total Final Sale"},
                            {"name": "No Of Bills", "id": "No Of Bills"},
                            {"name": "Avg Sale/Bill", "id": "Avg Sale/Bill"},
                        ],
                        page_size=5,
                        style_table={"overflowX": "auto"},
                        style_cell={
                            "textAlign": "center",
                            "padding": "6px",
                            "fontSize": "12px",
                            "fontFamily": "Arial",
                        },
                        style_header={
                            "backgroundColor": "#7c2d12",
                            "color": "white",
                            "fontWeight": "bold",
                        },
                    ),

                    # --- Detailed sales table (NO Source column) ---
                    html.H4(
                        "📄 Detailed Sales Records",
                        style={"marginTop": "12px", "marginBottom": "4px"},
                    ),
                    dash_table.DataTable(
                        id="sales_table",
                        columns=detailed_table_columns,
                        data=[],
                        style_table={"overflowX": "auto"},
                        style_header={
                            "fontSize": "13px",
                            "fontWeight": "bold",
                            "backgroundColor": "#E5E7EB",
                            "textAlign": "left",
                        },
                        style_cell={
                            "fontSize": "12px",
                            "fontFamily": "Arial",
                            "textAlign": "left",
                            "whiteSpace": "normal",
                            "height": "auto",
                            "padding": "4px",
                        },
                        page_size=25,
                    ),

                    # --- Charts ---
                    html.H4(
                        "📈 Final Net Sale Trend (Total)",
                        style={"marginTop": "12px", "marginBottom": "4px"},
                    ),
                    dcc.Graph(
                        id="sales_trend_graph",
                        figure=empty_fig("No data"),
                        style={"height": "360px"},
                    ),

                    html.H4(
                        "📈 Daily Final Net Sale Trend (Outlet Wise)",
                        style={"marginTop": "12px", "marginBottom": "4px"},
                    ),
                    dcc.Graph(
                        id="sales_outlet_trend",
                        figure=empty_fig("No data"),
                        style={"height": "360px"},
                    ),

                    html.H4(
                        "🌊 Total Final Sale Over Time (Area)",
                        style={"marginTop": "12px", "marginBottom": "4px"},
                    ),
                    dcc.Graph(
                        id="sales_final_sale_area",
                        figure=empty_fig("No data"),
                        style={"height": "360px"},
                    ),

                    html.H4(
                        "📊 Total Final Sale by Region",
                        style={"marginTop": "12px", "marginBottom": "4px"},
                    ),
                    dcc.Graph(
                        id="sales_final_sale_region_bar",
                        figure=empty_fig("No data"),
                        style={"height": "360px"},
                    ),

                    html.H4(
                        "📊 Tab-wise AOV / ADS / ADT",
                        style={"marginTop": "12px", "marginBottom": "4px"},
                    ),
                    dcc.Graph(
                        id="sales_tabwise_aov_ads_adt",
                        figure=empty_fig("No data"),
                        style={"height": "360px"},
                    ),

                    html.H4(
                        "📊 Charge Breakdown by Outlet (Stacked)",
                        style={"marginTop": "12px", "marginBottom": "4px"},
                    ),
                    dcc.Graph(
                        id="sales_charge_breakdown_outlet",
                        figure=empty_fig("No data"),
                        style={"height": "360px"},
                    ),
                ],
            )
        ],
    )


# -----------------------------------------------------------
# Callback registration
# -----------------------------------------------------------

def register_callbacks(app):

    @app.callback(
        Output("sales_kpi_row", "children"),
        Output("range-summary", "children"),
        Output("sales_table", "data"),
        Output("sales_charge_summary", "data"),
        Output("sales_trend_graph", "figure"),
        Output("sales_outlet_trend", "figure"),
        Output("sales_outlet_summary_table", "data"),
        Output("sales_top5_outlet_table", "data"),
        Output("sales_final_sale_area", "figure"),
        Output("sales_final_sale_region_bar", "figure"),
        Output("sales_tabwise_aov_ads_adt", "figure"),
        Output("sales_charge_breakdown_outlet", "figure"),
        Output("sales_month_filter", "options"),
        Output("sales_week_filter", "options"),
        Input("sales_region_filter", "value"),
        Input("sales_city_filter", "value"),
        Input("sales_type_filter", "value"),
        Input("sales_outlet_filter", "value"),
        Input("sales_tabs_filter", "value"),
        Input("sales_month_filter", "value"),
        Input("sales_week_filter", "value"),
        Input("sales_day_filter", "value"),
        Input("sales_search_text", "value"),
        Input("date-range", "start_date"),
        Input("date-range", "end_date"),
        Input("sales_refresh_button", "n_clicks"),
        Input("sales_reset_button", "n_clicks"),
        Input("btn_yesterday", "n_clicks"),
        Input("btn_last7", "n_clicks"),
        Input("btn_month", "n_clicks"),
        Input("btn_year", "n_clicks"),
    )
    def update_sales_dashboard(
        region_vals,
        city_vals,
        type_vals,
        outlet_vals,
        tabs_vals,
        month_val,
        week_vals,
        day_val,
        search_txt,
        start_date,
        end_date,
        n_clicks,
        n_reset,
        n_yesterday,
        n_last7,
        n_month,
        n_year,
    ):
        df_all = load_daily_sales()

        # --- Helper: build consistent empty result (14 outputs) ---
        def make_empty_result(message, month_opts=None, week_opts=None):
            fig = empty_fig(message)
            empty_kpi_row = dbc.Row(
                [
                    dbc.Col(kpi_card("Total Sale", 0.0), md=3),
                    dbc.Col(kpi_card("Final Net Sale", 0.0), md=3),
                    dbc.Col(kpi_card("AOV", 0.0), md=3),
                    dbc.Col(kpi_card("ADS", 0.0), md=3),
                ]
            )
            range_badge = html.Div(
                "No data available for selected filters",
                style={
                    "display": "inline-block",
                    "padding": "6px 12px",
                    "backgroundColor": "#6b7280",
                    "color": "white",
                    "borderRadius": "999px",
                    "fontSize": "12px",
                },
            )
            return (
                empty_kpi_row,  # KPI row
                range_badge,    # range summary
                [],             # detailed table
                [],             # charge summary table
                fig,            # main trend
                fig,            # outlet trend
                [],             # outlet summary table
                [],             # top 5 table
                fig,            # area chart
                fig,            # region bar
                fig,            # tabwise
                fig,            # charge breakdown
                month_opts or [],  # month options
                week_opts or [],   # week options
            )

        # --- No data at all ---
        if df_all is None or df_all.empty:
            return make_empty_result("No data")

        df = df_all.copy()

        # ---------------------------------------------------
        # BASE DATES + DEFAULTS
        # ---------------------------------------------------
        min_date = df_all["Date"].min().normalize()
        max_date = df_all["Date"].max().normalize()
        default_start = max_date.replace(day=1)
        default_end = max_date

        # Current picker values
        picker_start = pd.to_datetime(start_date).normalize() if start_date else default_start
        picker_end = pd.to_datetime(end_date).normalize() if end_date else default_end

        effective_start = picker_start
        effective_end = picker_end

        # ---------------------------------------------------
        # FIND WHICH CONTROL TRIGGERED
        # ---------------------------------------------------
        ctx = callback_context
        trig = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else None

        # ---------------------------------------------------
        # 0️⃣ RESET → reset everything to default (running month)
        # ---------------------------------------------------
        if trig == "sales_reset_button":
            # Ignore all filters
            region_vals = None
            city_vals = None
            type_vals = None
            outlet_vals = None
            tabs_vals = None
            month_val = None
            week_vals = None
            day_val = "ALL"
            search_txt = None

            effective_start = None
            effective_end = None

        # ---------------------------------------------------
        # 1️⃣ QUICK BUTTONS (override date range)
        # ---------------------------------------------------
        elif trig == "btn_yesterday":
            d = max_date - timedelta(days=1)
            effective_start = d
            effective_end = d

        elif trig == "btn_last7":
            effective_start = max_date - timedelta(days=6)
            effective_end = max_date

        elif trig == "btn_month":
            effective_start = default_start
            effective_end = default_end

        elif trig == "btn_year":
            effective_start = date(max_date.year, 1, 1)
            effective_end = max_date

        # ---------------------------------------------------
        # 2️⃣ MONTH DROPDOWN OVERRIDES DATE RANGE
        # ---------------------------------------------------
        if month_val:
            try:
                y, m = map(int, month_val.split("-"))
                month_start = date(y, m, 1)
                next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
                month_end = next_month - timedelta(days=1)
                effective_start = month_start
                effective_end = month_end
                df = df[df["MonthKey"] == month_val]
            except Exception:
                pass

        # ---------------------------------------------------
        # 3️⃣ WEEK DROPDOWN OVERRIDES BOTH MONTH & DATE RANGE
        # ---------------------------------------------------
        if week_vals:
            if not isinstance(week_vals, list):
                week_vals = [week_vals]

            df = df[df["YearWeek"].isin(week_vals)]

            wdf = df_all[df_all["YearWeek"].isin(week_vals)]
            if not wdf.empty:
                effective_start = wdf["Date"].min().normalize()
                effective_end = wdf["Date"].max().normalize()

        # ---------------------------------------------------
        # 4️⃣ DATE RANGE finally filters visible data
        #     (fix for TypeError: datetime64 vs date)
        # ---------------------------------------------------
        effective_start_ts = pd.Timestamp(effective_start)
        effective_end_ts = pd.Timestamp(effective_end)

        # For options (month/week) we use df_all in this range
        df_range_all = df_all[
            (df_all["Date"] >= effective_start_ts) & (df_all["Date"] <= effective_end_ts)
        ]

        # Month options mapped to date range
        month_opts = [
            {"label": ml, "value": mk}
            for mk, ml in (
                df_range_all[["MonthKey", "MonthLabel"]]
                .dropna()
                .drop_duplicates()
                .sort_values("MonthKey")
                .itertuples(index=False, name=None)
            )
        ]

        # Week options mapped to date range
        week_df = (
            df_range_all[["YearWeek", "Year", "WeekNumber"]]
            .drop_duplicates()
            .sort_values(["Year", "WeekNumber"])
        )
        week_opts = [
            {
                "label": f"Wk {int(row.WeekNumber)} - {int(row.Year)}",
                "value": row.YearWeek,
            }
            for _, row in week_df.iterrows()
        ]

        # Now apply date range filter to working df
        df = df[(df["Date"] >= effective_start_ts) & (df["Date"] <= effective_end_ts)]

        # ---------------------------------------------------
        # DIMENSION FILTERS
        # ---------------------------------------------------

        # Region
        if region_vals:
            df = df[df["Region"].isin(region_vals)]

        # City
        if city_vals:
            if isinstance(city_vals, list):
                df = df[df["City"].isin(city_vals)]
            else:
                df = df[df["City"] == city_vals]

        # Type
        if type_vals:
            if isinstance(type_vals, list):
                df = df[df["Type"].isin(type_vals)]
            else:
                df = df[df["Type"] == type_vals]

        # Outlet
        if outlet_vals:
            if not isinstance(outlet_vals, list):
                outlet_vals = [outlet_vals]
            df = df[df["Outlet Name"].isin(outlet_vals)]

        # Tabs
        if tabs_vals:
            if not isinstance(tabs_vals, list):
                tabs_vals = [tabs_vals]
            df = df[df["Tabs"].isin(tabs_vals)]

        # Day filter
        if day_val and day_val != "ALL":
            if day_val in [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ]:
                df = df[df["DayName"] == day_val]
            elif day_val in ["WD", "WF", "WE"]:
                df = df[df["DayCode"] == day_val]

        # Search by outlet substring
        if search_txt:
            st = str(search_txt).strip().lower()
            if st:
                df = df[df["Outlet Name"].str.lower().str.contains(st, na=False)]

        # --- No data after filters ---
        if df.empty:
            return make_empty_result("No data (filters)", month_opts, week_opts)

        # ---------------------------------------------------
        # KPI section (colored cards)
        # ---------------------------------------------------
        k = compute_kpis(df)

        kpi_colors = {
            "Total Sale": "#17a2b8",
            "Discount": "#ffc107",
            "Final Net Sale": "#22c55e",
            "Total Tax": "#6c757d",
            "Total Amount": "#0ea5e9",
            "Total Covers": "#6f42c1",
            "Total Bills": "#0dcaf0",
            "Total Days": "#20c997",
            "AOV": "#0d6efd",
            "ADS": "#198754",
            "ADT": "#6610f2",
        }

        def colored_card(title, value):
            return html.Div(
                [
                    html.Div(title, style={"fontSize": "12px", "color": "white"}),
                    html.Div(
                        fmt_float(value),
                        style={"fontSize": "18px", "fontWeight": "bold", "color": "white"},
                    ),
                ],
                style={
                    "backgroundColor": kpi_colors.get(title, "#0f766e"),
                    "padding": "10px",
                    "borderRadius": "10px",
                    "textAlign": "center",
                },
            )

        kpi_row = dbc.Row(
            [
                dbc.Col(colored_card("Total Sale", k["total_sale"]), md=3),
                dbc.Col(colored_card("Discount", k["discount"]), md=3),
                dbc.Col(colored_card("Final Net Sale", k["final_net"]), md=3),
                dbc.Col(colored_card("Total Tax", k["total_tax"]), md=3),
                dbc.Col(colored_card("Total Amount", k["total_amount"]), md=3),
                dbc.Col(colored_card("Total Covers", k["covers"]), md=3),
                dbc.Col(colored_card("Total Bills", k["bills"]), md=3),
                dbc.Col(colored_card("Total Days", k["days"]), md=3),
                dbc.Col(colored_card("AOV", k["aov"]), md=3),
                dbc.Col(colored_card("ADS", k["ads"]), md=3),
                dbc.Col(colored_card("ADT", k["adt"]), md=3),
            ],
            style={"rowGap": "8px", "marginBottom": "14px"},
        )

        # ---------------------------------------------------
        # Detailed table data
        # ---------------------------------------------------
        base_columns = [
            "Date",
            "Region",
            "City",
            "Type",
            "Outlet Name",
            "Tabs",
            "No Of Items",
            "No Of Bills",
            "Sale",
            "Discount",
            "Net Sale",
            "Charges",
            "Final_Net_Sale",
        ]

        for c in base_columns:
            if c not in df.columns:
                df[c] = np.nan

        df_table = df[base_columns].copy()

        if "Date" in df_table.columns:
            df_table["Date"] = df_table["Date"].dt.strftime("%d-%m-%Y")

        df_table = format_table_df(df_table)
        table_data = df_table.to_dict("records")

        # ---------------------------------------------------
        # Charge summary (Region + Outlet)
        # ---------------------------------------------------
        charge_cols = [
            "Restaurant Charge",
            "Packaging Charge [CART - SWIGGY]",
            "Restaurant Packaging Charges",
            "Delivery Charge",
            "Platform Fee Charge",
            "Smile Amount Charge",
        ]
        group_cols = ["Region", "Outlet Name"]
        for c in group_cols:
            if c not in df.columns:
                charge_summary_data = []
                break
        else:
            agg_dict = {c: "sum" for c in charge_cols if c in df.columns}
            if "Charges" in df.columns:
                agg_dict["Charges"] = "sum"
            if agg_dict:
                ch = df.groupby(group_cols).agg(agg_dict).reset_index()
                ch = format_table_df(ch)
                charge_summary_data = ch.to_dict("records")
            else:
                charge_summary_data = []

        # ---------------------------------------------------
        # Final Net Sale Trend (Total)
        # ---------------------------------------------------
        trend_group = df.groupby("Date", as_index=False)["Final_Net_Sale"].sum()
        if trend_group.empty:
            trend_fig = empty_fig("No trend data")
        else:
            trend_fig = px.line(
                trend_group,
                x="Date",
                y="Final_Net_Sale",
                markers=True,
            )
            trend_fig.update_traces(
                hovertemplate="Final Net: %{y:,.2f}<br>Date: %{x|%d-%b-%Y}<extra></extra>"
            )
            trend_fig.update_layout(
                xaxis_title="Date",
                yaxis_title="Final Net Sale",
                margin=dict(l=40, r=10, t=40, b=40),
            )

        # ---------------------------------------------------
        # Outlet-wise Daily Final Net Sale Trend
        # ---------------------------------------------------
        outlet_trend_df = (
            df.groupby(["Date", "Outlet Name"], as_index=False)["Final_Net_Sale"].sum()
        )
        if outlet_trend_df.empty:
            outlet_trend_fig = empty_fig("No outlet trend data")
        else:
            outlet_trend_fig = px.line(
                outlet_trend_df,
                x="Date",
                y="Final_Net_Sale",
                color="Outlet Name",
                markers=True,
            )
            outlet_trend_fig.update_layout(
                xaxis_title="Date",
                yaxis_title="Final Net Sale",
                legend_title="Outlet",
                margin=dict(l=40, r=10, t=40, b=40),
            )

        # ---------------------------------------------------
        # Outlet Summary Table
        # ---------------------------------------------------
        outlet_agg = df.groupby("Outlet Name", as_index=False).agg(
            **{
                "Net Sale": ("Net Sale", "sum"),
                "Charges": ("Charges", "sum"),
                "Discount": ("Discount", "sum"),
                "Total Tax": ("Total Tax", "sum")
                if "Total Tax" in df.columns
                else ("Final_Net_Sale", "sum"),
                "Total Final Sale": ("Final_Net_Sale", "sum"),
            }
        )
        outlet_agg_fmt = format_table_df(outlet_agg)
        outlet_summary_data = outlet_agg_fmt.to_dict("records")

        # ---------------------------------------------------
        # Top 5 Outlet Sale Table
        # ---------------------------------------------------
        daily_outlet = (
            df.groupby(["Outlet Name", "Date"], as_index=False)
            .agg(
                Total_Final_Sale=("Final_Net_Sale", "sum"),
                Bills=("No Of Bills", "sum"),
            )
        )
        daily_outlet["Avg_Sale_Bill"] = np.where(
            daily_outlet["Bills"] > 0,
            daily_outlet["Total_Final_Sale"] / daily_outlet["Bills"],
            0.0,
        )
        if not daily_outlet.empty:
            top5 = daily_outlet.sort_values(
                "Total_Final_Sale", ascending=False
            ).head(5)
            top5["Date"] = top5["Date"].dt.strftime("%d-%m-%Y")
            top5_table = pd.DataFrame(
                {
                    "Outlet Name": top5["Outlet Name"],
                    "Date": top5["Date"],
                    "Total Final Sale": top5["Total_Final_Sale"],
                    "No Of Bills": top5["Bills"],
                    "Avg Sale/Bill": top5["Avg_Sale_Bill"],
                }
            )
            top5_table_fmt = format_table_df(top5_table)
            top5_data = top5_table_fmt.to_dict("records")
        else:
            top5_data = []

        # ---------------------------------------------------
        # Total Final Sale Over Time (Area)
        # ---------------------------------------------------
        if not trend_group.empty:
            final_area_fig = px.area(
                trend_group,
                x="Date",
                y="Final_Net_Sale",
            )
            final_area_fig.update_layout(
                xaxis_title="Date",
                yaxis_title="Total Final Net Sale",
                margin=dict(l=40, r=10, t=40, b=40),
            )
        else:
            final_area_fig = empty_fig("No Final Sale Over Time data")

        # ---------------------------------------------------
        # Total Final Sale by Region (Bar)
        # ---------------------------------------------------
        if "Region" in df.columns:
            reg_df = df.groupby("Region", as_index=False)["Final_Net_Sale"].sum()
            if not reg_df.empty:
                region_bar_fig = px.bar(
                    reg_df,
                    x="Region",
                    y="Final_Net_Sale",
                )
                region_bar_fig.update_layout(
                    xaxis_title="Region",
                    yaxis_title="Total Final Net Sale",
                    margin=dict(l=40, r=10, t=40, b=40),
                )
            else:
                region_bar_fig = empty_fig("No Region data")
        else:
            region_bar_fig = empty_fig("Region column missing")

        # ---------------------------------------------------
        # Tab-wise AOV / ADS / ADT (Grouped Bar)
        # ---------------------------------------------------
        if "Tabs" in df.columns:
            tab_group = (
                df.groupby("Tabs")
                .agg(
                    Final_Net_Sale=("Final_Net_Sale", "sum"),
                    Bills=("No Of Bills", "sum"),
                    Days=("Date", lambda x: x.dt.normalize().nunique()),
                )
                .reset_index()
            )
            tab_group["AOV"] = np.where(
                tab_group["Bills"] > 0,
                tab_group["Final_Net_Sale"] / tab_group["Bills"],
                0.0,
            )
            tab_group["ADS"] = np.where(
                tab_group["Days"] > 0,
                tab_group["Final_Net_Sale"] / tab_group["Days"],
                0.0,
            )
            tab_group["ADT"] = np.where(
                tab_group["Days"] > 0,
                tab_group["Bills"] / tab_group["Days"],
                0.0,
            )
            if not tab_group.empty:
                tab_long = tab_group.melt(
                    id_vars=["Tabs"],
                    value_vars=["AOV", "ADS", "ADT"],
                    var_name="Metric",
                    value_name="Value",
                )
                tabwise_fig = px.bar(
                    tab_long,
                    x="Tabs",
                    y="Value",
                    color="Metric",
                    barmode="group",
                )
                tabwise_fig.update_layout(
                    xaxis_title="Tabs",
                    yaxis_title="Value",
                    margin=dict(l=40, r=10, t=40, b=40),
                )
            else:
                tabwise_fig = empty_fig("No Tab-wise data")
        else:
            tabwise_fig = empty_fig("Tabs column missing")

        # ---------------------------------------------------
        # Charge Breakdown by Outlet (Stacked Bar)
        # ---------------------------------------------------
        breakdown_cols = [
            "Restaurant Charge",
            "Packaging Charge [CART - SWIGGY]",
            "Restaurant Packaging Charges",
            "Delivery Charge",
            "Platform Fee Charge",
            "Smile Amount Charge",
            "Packaging Charge",
        ]
        present_breakdown = [c for c in breakdown_cols if c in df.columns]
        if present_breakdown:
            ch_outlet = (
                df.groupby("Outlet Name")[present_breakdown].sum().reset_index()
            )
            ch_long = ch_outlet.melt(
                id_vars=["Outlet Name"],
                value_vars=present_breakdown,
                var_name="ChargeType",
                value_name="Value",
            )
            if not ch_long.empty:
                charge_break_fig = px.bar(
                    ch_long,
                    x="Outlet Name",
                    y="Value",
                    color="ChargeType",
                    barmode="stack",
                )
                charge_break_fig.update_layout(
                    xaxis_title="Outlet Name",
                    yaxis_title="Charge Amount",
                    margin=dict(l=40, r=10, t=40, b=40),
                )
            else:
                charge_break_fig = empty_fig("No charge breakdown data")
        else:
            charge_break_fig = empty_fig("Charge components not available")

        # ---------------------------------------------------
        # Green Badge Range Summary
        # ---------------------------------------------------
        range_text = f"{effective_start_ts:%d-%m-%Y} → {effective_end_ts:%d-%m-%Y}"
        range_badge = html.Div(
            [
                html.Span("📅 Showing: ", style={"fontWeight": "bold"}),
                html.Span(range_text),
            ],
            style={
                "display": "inline-block",
                "padding": "6px 14px",
                "backgroundColor": "#16a34a",
                "color": "white",
                "borderRadius": "999px",
                "fontSize": "12px",
                "boxShadow": "0 2px 6px rgba(0,0,0,0.15)",
            },
        )

        # ---------------------------------------------------
        # Final return (14 outputs)
        # ---------------------------------------------------
        return (
            kpi_row,              # 1: sales_kpi_row.children
            range_badge,          # 2: range-summary.children
            table_data,           # 3: sales_table.data
            charge_summary_data,  # 4: sales_charge_summary.data
            trend_fig,            # 5: sales_trend_graph.figure
            outlet_trend_fig,     # 6: sales_outlet_trend.figure
            outlet_summary_data,  # 7: sales_outlet_summary_table.data
            top5_data,            # 8: sales_top5_outlet_table.data
            final_area_fig,       # 9: sales_final_sale_area.figure
            region_bar_fig,       # 10: sales_final_sale_region_bar.figure
            tabwise_fig,          # 11: sales_tabwise_aov_ads_adt.figure
            charge_break_fig,     # 12: sales_charge_breakdown_outlet.figure
            month_opts,           # 13: sales_month_filter.options
            week_opts,            # 14: sales_week_filter.options
        )
