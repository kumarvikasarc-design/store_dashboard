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
import pyodbc
from sqlalchemy import create_engine
import urllib
import numpy as np
import pandas as pd
import plotly.express as px
import dash
from dash import html, dcc, dash_table, Input, Output, callback_context
import dash_bootstrap_components as dbc

# -----------------------------------------------------------
# Paths
# -----------------------------------------------------------

params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=coffee_island_analytics;"
    "Trusted_Connection=yes;"
)

ENGINE = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


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
def load_sales_from_sql():
    query = """
    SELECT
        s.Sale_Date AS [Date],

        m.Brand,
        m.Region,
        m.City,
        m.State,
        m.Store_Type AS Type,
        m.Outlet_Name AS Outlet_Name,

        s.Tabs,
        s.No_Of_Items AS No_Of_Items,
        s.No_Of_Bills AS No_Of_Bills,
        s.Sale,
        s.Discount,
        s.Net_Sale AS Net_Sale,

        s.Restaurant_Charge,
        s.Packaging_Charge_CART_SWIGGY AS Packaging_Charge_CART_SWIGGY,
        s.Restaurant_Packaging_Charges,
        s.Delivery_Charge,
        s.Platform_Fee_Charge,
        s.Smile_Amount_Charge,

        (
            ISNULL(s.Restaurant_Charge,0) +
            ISNULL(s.Packaging_Charge_CART_SWIGGY,0) +
            ISNULL(s.Restaurant_Packaging_Charges,0) +
            ISNULL(s.Delivery_Charge,0) +
            ISNULL(s.Platform_Fee_Charge,0) +
            ISNULL(s.Smile_Amount_Charge,0)
        ) AS Charges,

        s.Net_Sale +
        (
            ISNULL(s.Restaurant_Charge,0) +
            ISNULL(s.Packaging_Charge_CART_SWIGGY,0) +
            ISNULL(s.Restaurant_Packaging_Charges,0) +
            ISNULL(s.Delivery_Charge,0) +
            ISNULL(s.Platform_Fee_Charge,0) +
            ISNULL(s.Smile_Amount_Charge,0)
        ) AS Final_Net_Sale,

        s.Total_Tax,
        s.Total_Amount,
        s.Covers
    FROM dbo.sales_raw s
    LEFT JOIN dbo.stores_master m
        ON LTRIM(RTRIM(s.Outlet_Name)) = LTRIM(RTRIM(m.Outlet_Name))
    WHERE s.Sale_Date IS NOT NULL
    """

    df = pd.read_sql(query, ENGINE)

    # ---- Friendly dashboard column names ----
    df = df.rename(columns={
        "Outlet_Name": "Outlet Name",
        "Net_Sale": "Net Sale",
        "No_Of_Items": "No Of Items",
        "No_Of_Bills": "No Of Bills",

        # 🔑 FIXED
        "Restaurant_Charge": "Restaurant Charge",
        "Packaging_Charge_CART_SWIGGY": "Packaging Charge [CART - SWIGGY]",
        "Restaurant_Packaging_Charges": "Restaurant Packaging Charges",
        "Delivery_Charge": "Delivery Charge",
        "Platform_Fee_Charge": "Platform Fee Charge",
        "Smile_Amount_Charge": "Smile Amount Charge",

        "Total_Tax": "Total Tax",
        "Total_Amount": "Total Amount",
    })


    df.columns = df.columns.str.strip()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    # 🔒 GUARANTEE critical numeric columns (FIX)
    for col in ["Total Tax", "Total Amount"]:
        if col not in df.columns:
            df[col] = 0.0

    return df

def parse_dates_auto(series: pd.Series) -> pd.Series:
    """
    Parse dates assuming DD-MM-YYYY input.
    Safely handles accidental YYYY-MM-DD rows without warnings.
    """

    s = series.astype(str).str.strip()

    # Explicit ISO format (YYYY-MM-DD) — avoid warning
    iso_mask = s.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    out = pd.Series(pd.NaT, index=s.index)

    if iso_mask.any():
        out.loc[iso_mask] = pd.to_datetime(
            s[iso_mask],
            format="%Y-%m-%d",
            errors="coerce",
        )

    # All remaining rows → DD-MM-YYYY
    rem = ~iso_mask
    if rem.any():
        out.loc[rem] = pd.to_datetime(
            s[rem],
            dayfirst=True,
            errors="coerce",
        )

    return out

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
    
    brand_col = find(["brand"])
    if brand_col: rename_map[brand_col] = "Brand"
    
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
    df = load_sales_from_sql()

    if df.empty:
        return df

    # ---- GUARANTEE dashboard columns ----
    for col in ["Brand", "Region", "City", "State", "Type", "Outlet Name", "Tabs"]:
        if col not in df.columns:
            df[col] = ""

    # ---- Time keys (same logic you already use) ----
    df["MonthKey"] = df["Date"].dt.to_period("M").astype(str)
    df["MonthLabel"] = df["Date"].dt.strftime("%b-%Y")

    iso = df["Date"].dt.isocalendar()
    df["ISO_Year"] = iso.year.astype(int)
    df["WeekNumber"] = iso.week.astype(int)
    df["YearWeek"] = (
        df["ISO_Year"].astype(str)
        + "-W"
        + df["WeekNumber"].astype(str).str.zfill(2)
    )

    df["DayName"] = df["Date"].dt.day_name()
    df["DayCode"] = np.select(
        [
            df["Date"].dt.dayofweek.isin([0, 1, 2, 3]),
            df["Date"].dt.dayofweek == 4,
            df["Date"].dt.dayofweek.isin([5, 6]),
        ],
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

def build_fy_options(df):
    if df.empty or "Date" not in df.columns:
        return []

    # Ensure pandas timestamps
    dmin = pd.to_datetime(df["Date"].min()).normalize()
    dmax = pd.to_datetime(df["Date"].max()).normalize()

    # Determine FY start year range (India FY Apr–Mar)
    start_fy = dmin.year if dmin.month >= 4 else dmin.year - 1
    end_fy = dmax.year if dmax.month >= 4 else dmax.year - 1

    fy_options = []

    for y in range(start_fy, end_fy + 1):
        fy_start = pd.Timestamp(year=y, month=4, day=1)
        fy_end = pd.Timestamp(year=y + 1, month=3, day=31)

        # ✅ SAFE TIMESTAMP COMPARISON
        if fy_start <= dmax and fy_end >= dmin:
            fy_options.append(
                {
                    "label": f"FY {y}-{str(y + 1)[-2:]}",
                    "value": f"{y}-{y + 1}",
                }
            )

    return fy_options




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
    
    # safety ensure df_init dataframe and required columns exist
    if df_init is None or not isinstance(df_init, pd.DataFrame):
        df_init = pd.DataFrame()

    for col in ["Date", "Brand", "Region", "City", "State", "Type", "Outlet Name", "Tabs"]:
        if col not in df_init.columns:
            df_init[col] = ""
    
        # regenerate time keys if dates exist
    try:
        if df_init["Date"].notna().sum() > 0:
            df_init["MonthKey"] = df_init["Date"].dt.to_period("M").astype(str)
            df_init["MonthLabel"] = df_init["Date"].dt.strftime("%b-%Y")
            df_init["Year"] = df_init["Date"].dt.year
            df_init["WeekNumber"] = df_init["Date"].dt.isocalendar().week.astype(int)
            df_init["YearWeek"] = df_init["Year"].astype(str) + "-W" + df_init["WeekNumber"].astype(str).str.zfill(2)
            df_init["DayName"] = df_init["Date"].dt.day_name()
            df_init["DayCode"] = np.select(
                [df_init["Date"].dt.dayofweek.isin([0,1,2,3]), df_init["Date"].dt.dayofweek == 4, df_init["Date"].dt.dayofweek.isin([5,6])],
                ["WD", "WF", "WE"], default=""
            )
    except Exception:
        pass

    # default date range = running month of latest data
    if df_init.empty:
        today = datetime.today().date()
        start_date = today.replace(day=1)
        end_date = today
    else:
        latest = df_init["Date"].max().date()
        start_date = latest.replace(day=1)
        end_date = latest
        
    if df_init is None or df_init.empty:
        today = datetime.today().date()
        start_date = today.replace(day=1)
        end_date = today

        # Always define options to avoid UnboundLocalError
        
# --- Always define options safely ---
    brand_options = []
    state_options = []
    region_options = []
    city_options = []
    type_options = []
    outlet_options = []
    tabs_options = []
    month_options = []
    week_options = []

    if not df_init.empty:
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

        weeks = (
            df_init[["YearWeek", "ISO_Year", "WeekNumber"]]
            .drop_duplicates()
            .sort_values(["ISO_Year", "WeekNumber"])
        )

        week_options = [
            {
                "label": f"Wk {int(r.WeekNumber)} - {int(r.ISO_Year)}",
                "value": r.YearWeek,
            }
            for _, r in weeks.iterrows()
        ]

        brand_options  = build_options(df_init, "Brand")
        state_options  = build_options(df_init, "State")
        region_options = build_options(df_init, "Region")
        city_options   = build_options(df_init, "City")
        type_options   = build_options(df_init, "Type")
        outlet_options = build_options(df_init, "Outlet Name")
        tabs_options   = build_options(df_init, "Tabs")

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
                    # Cascade filter row
                    dbc.Row(
                        [
                            dbc.Col(
                                [html.Label("Brand"), dcc.Dropdown(id="sales_brand_filter", options=brand_options, multi=False, placeholder="Brand")],
                                md=2
                            ),
                            dbc.Col(
                                [html.Label("State"), dcc.Dropdown(id="sales_state_filter", options=state_options, multi=False, placeholder="All States")],
                                md=2
                            ),
                            dbc.Col(
                                [html.Label("Region"), dcc.Dropdown(id="sales_region_filter", options=region_options, multi=False, placeholder="All Regions")],
                                md=2
                            ),
                            dbc.Col(
                                [html.Label("City"), dcc.Dropdown(id="sales_city_filter", options=city_options, multi=False, placeholder="All Cities")],
                                md=2
                            ),
                            dbc.Col(
                                [html.Label("Type"), dcc.Dropdown(id="sales_type_filter", options=type_options, multi=False, placeholder="All Types")],
                                md=2
                            ),
                            dbc.Col(
                                [html.Label("Outlet Name"), dcc.Dropdown(id="sales_outlet_filter", options=outlet_options, multi=False, placeholder="All Outlets")],
                                md=2
                            ),
                        ],
                        style={"marginBottom": "10px"},
                    ),                    

                    # --- Date + Month + Week row ---
                    dbc.Row(
                        [
                            # ---------------- Date Range ----------------
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
                                md=3,
                            ),

                            # ---------------- Financial Year ----------------
                            dbc.Col(
                                [
                                    html.Label("Financial Year"),
                                    dcc.Dropdown(
                                        id="sales_fy_filter",
                                        options=[],
                                        placeholder="All FY",
                                        clearable=True,
                                    ),
                                ],
                                md=2,
                            ),

                            # ---------------- Month ----------------
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
                                md=2,
                            ),

                            # ---------------- Week ----------------
                            dbc.Col(
                                [
                                    html.Label("Week"),
                                    dcc.Dropdown(
                                        id="sales_week_filter",
                                        options=week_options,
                                        multi=False,
                                        placeholder="All Weeks",
                                    ),
                                ],
                                md=2,
                            ),

                            # ---------------- Day ----------------
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
                                md=1,
                            ),

                            # ---------------- Tabs ----------------
                            dbc.Col(
                                [
                                    html.Label("Tabs"),
                                    dcc.Dropdown(
                                        id="sales_tabs_filter",
                                        options=tabs_options,
                                        multi=False,
                                        placeholder="All Tabs",
                                    ),
                                ],
                                md=2,
                            ),
                        ],
                        style={"marginBottom": "10px"},
                    ),


                    # --- Refresh + Reset buttons ---
                    html.Div(
                        [
                            dcc.Input(
                                id="sales_search_text",
                                type="text",
                                placeholder="Type letters... e.g. 'gk'",
                                style={"width": "220px"},
                            ),
                            html.Button("Yesterday", id="btn_yesterday", n_clicks=0),
                            html.Button("Last 7 Days", id="btn_last7", n_clicks=0),
                            html.Button("This Month", id="btn_month", n_clicks=0),
                            html.Button("This Year", id="btn_year", n_clicks=0),
                            html.Button("Refresh Data", id="sales_refresh_button", n_clicks=0,
                                        style={"width": "150px"}),

                            html.Button("Reset Filters", id="sales_reset_button", n_clicks=0,
                                        style={
                                            "width": "150px",
                                            "backgroundColor": "#ef4444",
                                            "color": "white",
                                        }),
                            
                        ],
                        style={
                            "display": "flex",
                            "alignItems": "center",
                            "gap": "8px",        # spacing between all items
                            "whiteSpace": "nowrap",
                            "marginBottom": "14px",
                        },
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
                            {"name": "Net Sale", "id": "Net_Sale"},
                            {"name": "Charges", "id": "Charges"},
                            {"name": "Discount", "id": "Discount"},
                            {"name": "Total Tax", "id": "Total_Tax"},
                            {"name": "Total Final Sale", "id": "Total_Final_Sale"},
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

def build_options(df, col):
    if df is None or df.empty or col not in df.columns:
        return []

    vals = (
        df[col]
        .astype(str)
        .str.strip()
        .replace("", np.nan)
        .dropna()
        .unique()
    )

    return [{"label": v, "value": v} for v in sorted(vals)]

def safe_options(df, col):
    if df is None or df.empty:
        return []

    series = df.get(col)
    if series is None:
        return []

    return [
        {"label": v, "value": v}
        for v in sorted(series.dropna().astype(str).unique())
        if v.strip() != ""
    ]

        
# -----------------------------------------------------------
# Callback registration
# -----------------------------------------------------------

def register_callbacks(app):

    @app.callback(

        # -------------------- MAIN OUTPUTS (12) --------------------
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

        # -------------------- FILTER OPTION OUTPUTS (7) --------------------
        
        Output("sales_brand_filter", "options"),
        Output("sales_state_filter", "options"),
        Output("sales_region_filter", "options"),
        Output("sales_city_filter", "options"),
        Output("sales_type_filter", "options"),
        Output("sales_outlet_filter", "options"),
        Output("sales_fy_filter", "options"),
        Output("sales_month_filter", "options"),
        Output("sales_week_filter", "options"),

        # -------------------- FILTER VALUE OUTPUTS (11) --------------------
        Output("sales_brand_filter", "value"),
        Output("sales_state_filter", "value"),
        Output("sales_region_filter", "value"),
        Output("sales_city_filter", "value"),
        Output("sales_type_filter", "value"),
        Output("sales_outlet_filter", "value"),
        Output("date-range", "start_date"),
        Output("date-range", "end_date"),
        Output("sales_month_filter", "value"),
        Output("sales_week_filter", "value"),
        Output("sales_day_filter", "value"),
        Output("sales_tabs_filter", "value"),
        Output("sales_search_text", "value"),
        

        # -------------------- INPUTS (18) --------------------
        Input("sales_brand_filter", "value"),
        Input("sales_state_filter", "value"),
        Input("sales_region_filter", "value"),
        Input("sales_city_filter", "value"),
        Input("sales_type_filter", "value"),
        Input("sales_outlet_filter", "value"),
        Input("date-range", "start_date"),
        Input("date-range", "end_date"),
        Input("sales_fy_filter", "value"),
        Input("sales_month_filter", "value"),
        Input("sales_week_filter", "value"),
        Input("sales_day_filter", "value"),
        Input("sales_tabs_filter", "value"),
        Input("sales_search_text", "value"),
        Input("btn_yesterday", "n_clicks"),
        Input("btn_last7", "n_clicks"),
        Input("btn_month", "n_clicks"),
        Input("btn_year", "n_clicks"),
        Input("sales_refresh_button", "n_clicks"),
        Input("sales_reset_button", "n_clicks"),
    )    
    def update_sales_dashboard(
        brand_vals,
        state_vals,
        region_vals,
        city_vals,
        type_vals,
        outlet_vals,
        start_date,
        end_date,
        fy_val, 
        month_val,
        week_vals,
        day_val,
        tabs_vals,
        search_txt,
        n_yesterday,
        n_last7,
        n_month,
        n_year,
        n_refresh,
        n_reset,
    ):

        df_all = load_daily_sales()
        
        # 🔒 ABSOLUTE HARD GUARANTEE (callback-level)
        required_cols = [
            "Date", "Brand", "Region", "City", "State",
            "Type", "Outlet Name", "Tabs",
            "MonthKey", "MonthLabel", "Year",
            "WeekNumber", "YearWeek", "DayName", "DayCode"
        ]

        for col in required_cols:
            if col not in df_all.columns:
                df_all[col] = pd.NaT if col == "Date" else ""

        
        # --- Helper: build consistent empty result (14 outputs) ---
        def make_empty_result(brand_opts=None, state_opts=None, region_opts=None,
                              city_opts=None, type_opts=None, outlet_opts=None,
                              month_opts=None, week_opts=None):

            empty_fig_obj = empty_fig("No data")
            empty_kpi_row = dbc.Row(
                [
                    dbc.Col(kpi_card("Total Sale", 0.0), md=3),
                    dbc.Col(kpi_card("Final Net Sale", 0.0), md=3),
                    dbc.Col(kpi_card("AOV", 0.0), md=3),
                    dbc.Col(kpi_card("ADS", 0.0), md=3),
                ]
            )

            empty_range = html.Div(
                "No data available for selected filters",
                style={
                    "display": "inline-block",
                    "padding": "6px 12px",
                    "backgroundColor": "#14429d",
                    "color": "white",
                    "borderRadius": "999px",
                    "fontSize": "12px"
                },
            )

            return (
                empty_kpi_row, empty_range, [], [], empty_fig_obj, empty_fig_obj,
                [], [], empty_fig_obj, empty_fig_obj, empty_fig_obj, empty_fig_obj,

                brand_opts or [], state_opts or [], region_opts or [], city_opts or [], 
                type_opts or [], outlet_opts or [], month_opts or [],  week_opts or [],

                None, None, None, None, None, None, None, None, None, "ALL", "", None, None
            )

    # --- No data at all ---
        if df_all.empty:
            return make_empty_result(
                brand_opts=safe_options(df_all, "Brand"),
                state_opts=safe_options(df_all, "State"),
                region_opts=safe_options(df_all, "Region"),
                city_opts=safe_options(df_all, "City"),
                type_opts=safe_options(df_all, "Type"),
                outlet_opts=safe_options(df_all, "Outlet Name"),
                month_opts=safe_options(df_all, "MonthKey"),
                week_opts=safe_options(df_all, "YearWeek"),
            )
        
        # Work on a copy
        df = df_all.copy()

        # ---------------------------------------------------
        # FIND WHICH CONTROL TRIGGERED
        # ---------------------------------------------------
        ctx = callback_context.triggered[0]["prop_id"].split(".")[0] \
              if callback_context.triggered else None
        # --------------------------------------------------------------------
        # Default Running Month behavior (M2): current calendar month
        # - On first load (no filters/date), show current month (1st -> today)
        # - Reset (R2) will also return to this behavior
        # --------------------------------------------------------------------
        # Latest available data date
        max_date = df_all["Date"].max().date()

        running_month_start = max_date.replace(day=1)
        running_month_end = max_date   # ← yesterday / last data day


        # Determine trigger and ensure safe access to callback_context
        trig = None
        try:
            trig = callback_context.triggered[0]["prop_id"].split(".")[0] if callback_context.triggered else None
        except Exception:
            trig = None

        # Helper to detect "no user interaction" state
        no_filters_applied = (
            (not state_vals) and (not region_vals) and (not city_vals) and
            (not type_vals) and (not outlet_vals) and (not tabs_vals) and
            (not fy_val) and                         # ✅ FY counts as a filter
            (day_val in [None, "ALL"]) and
            (not search_txt or str(search_txt).strip() == "") and
            (start_date is None and end_date is None) and
            (trig in [None, ""])
        )



        # If no user action and no explicit picker range -> default to running month (M2)
        if no_filters_applied:
            start_date = running_month_start
            end_date = running_month_end

        # ---------------------------------------------------
        # BASE DATES + DEFAULTS
        # ---------------------------------------------------
        max_date = df_all["Date"].max().normalize()
        default_start = max_date.replace(day=1)
        default_end = max_date

        # Current picker values
        picker_start = pd.to_datetime(start_date).normalize() if start_date else default_start
        picker_end = pd.to_datetime(end_date).normalize() if end_date else default_end

        effective_start = picker_start
        effective_end = picker_end

        # ---------------------------------------------------
        # 🔄 RESET → force running month
        # ---------------------------------------------------
        if ctx == "sales_reset_button":
            start_date = running_month_start
            end_date = running_month_end

            brand_vals = state_vals = region_vals = city_vals = None
            type_vals = outlet_vals = tabs_vals = None

            fy_val = None
            month_val = None
            week_vals = None
            day_val = "ALL"
            # 🔑 FORCE EFFECTIVE DATES AFTER RESET
            effective_start = pd.Timestamp(running_month_start)
            effective_end = pd.Timestamp(running_month_end)
        # ---------------------------------------------------
        # 0️⃣ RESET → reset everything to default (running month)
        # ---------------------------------------------------
        # reset filters
        # ---------------------------------------------------
        # 1️⃣ QUICK BUTTONS (override date range)
        # ---------------------------------------------------
        elif ctx == "btn_yesterday":
            d = max_date #- timedelta(days=0)
            effective_start = d
            effective_end = d

        elif ctx == "btn_last7":
            effective_start = max_date - timedelta(days=6)
            effective_end = max_date

        elif ctx == "btn_month":
            effective_start = default_start
            effective_end = default_end

        elif ctx == "btn_year":
            effective_start = date(max_date.year, 1, 1)
            effective_end = max_date
        # ---------------------------------------------------
        # Build FY options (DATA DRIVEN)
        # ---------------------------------------------------
        fy_options = build_fy_options(df_all)

        # 🔒 SANITIZE FY VALUE (standalone IF)
        if fy_val and fy_val not in {o["value"] for o in fy_options}:
            fy_val = None

        # ---------------------------------------------------
        # 1️⃣b FINANCIAL YEAR OVERRIDES DATE RANGE
        # ---------------------------------------------------
        if ctx == "sales_fy_filter" and fy_val:
            try:
                start_year, end_year = map(int, fy_val.split("-"))
                effective_start = date(start_year, 4, 1)
                effective_end = date(end_year, 3, 31)
            except Exception:
                pass

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
        week_filter_vals = [week_vals] if isinstance(week_vals, str) else week_vals

        if week_filter_vals:
            df = df[df["YearWeek"].isin(week_filter_vals)]

            wdf = df_all[df_all["YearWeek"].isin(week_filter_vals)]
            if not wdf.empty:
                effective_start = wdf["Date"].min().normalize()
                effective_end = wdf["Date"].max().normalize()

        # ---------------------------------------------------
        # 4️⃣ DATE RANGE finally filters visible data
        #     (fix for TypeError: datetime64 vs date)
        # ---------------------------------------------------
        effective_start_ts = pd.Timestamp(effective_start)
        effective_end_ts = pd.Timestamp(effective_end)
        df = df[
            (df["Date"] >= effective_start_ts) &
            (df["Date"] <= effective_end_ts)
        ].copy()
        # For options (month/week) we use df_all in this range
        # For options (month/week) we use df_all in this range
        df_range_all = df_all[
            (df_all["Date"] >= effective_start_ts) & (df_all["Date"] <= effective_end_ts)
        ]
        # --- Month options mapped to date range (guaranteed to exist) ---
        try:
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
        except Exception:
            month_opts = []

# --- Week options mapped to date range (guaranteed to exist) ---
        try:
            week_df = (
                df_range_all[["YearWeek", "ISO_Year", "WeekNumber"]]
                .drop_duplicates()
                .sort_values(["ISO_Year", "WeekNumber"])
            )

            week_opts = [
                {
                    "label": f"Wk {int(row.WeekNumber)} - {int(row.ISO_Year)}",
                    "value": row.YearWeek,
                }
                for _, row in week_df.iterrows()
            ]
        except Exception:
            week_opts = []

        if df.empty:
            return make_empty_result(
                month_opts=safe_options(df_all, "MonthKey"),
                brand_opts=safe_options(df_all, "Brand"),
                state_opts=safe_options(df_all, "State"),
                region_opts=safe_options(df_all, "Region"),
                city_opts=safe_options(df_all, "City"),
                type_opts=safe_options(df_all, "Type"),
                outlet_opts=safe_options(df_all, "Outlet Name"),
                week_opts=safe_options(df_all, "YearWeek"),
            )
# ---------------------------------------------------
# MUTUAL CROSS-CASCADE FILTER LOGIC (FINAL)
# ---------------------------------------------------
        def to_list(v):
            if v is None:
                return None
            if isinstance(v, list):
                return [x for x in v if x not in ("", None)]
            s = str(v).strip()
            return [s] if s else None

    # Convert inputs to lists
        brand_list = to_list(brand_vals)
        state_list  = to_list(state_vals)
        region_list = to_list(region_vals)
        city_list   = to_list(city_vals)
        type_list   = to_list(type_vals)
        outlet_list = to_list(outlet_vals)
        tabs_list   = to_list(tabs_vals) 
        
        # Work on a local copy of df
        df_base = df.copy()   
        
        filter_map = {
            "Brand": brand_list,
            "State": state_list,
            "Region": region_list,
            "City": city_list,
            "Type": type_list,
            "Outlet Name": outlet_list,
            "Tabs": tabs_list,
        }
# ---------------------------------------------------
# APPLY ALL ACTIVE FILTERS TOGETHER
# ---------------------------------------------------
        
        # ---------------------------------------------------
# APPLY ALL FILTERS TO BASE DF (BI-DIRECTIONAL)
# ---------------------------------------------------     
        # 1️⃣ Base dataframe
        df_base = df.copy()
        df_filtered = df_base.copy()

        # 2️⃣ Apply multi-value filters (for dropdown cascading)
        for col, vals in filter_map.items():
            if vals and col in df_filtered.columns:
                df_filtered = df_filtered[df_filtered[col].isin(vals)]
# ---------------------------------------------------
# NOW UPDATE OPTIONS FROM THE FILTERED DATAFRAME
# ---------------------------------------------------
        brand_options  = build_options(df_filtered, "Brand")
        state_options  = build_options(df_filtered, "State")
        region_options = build_options(df_filtered, "Region")
        city_options   = build_options(df_filtered, "City")
        type_options   = build_options(df_filtered, "Type")
        outlet_options = build_options(df_filtered, "Outlet Name")
        

        def safe_filter_value(val, options):
            if not val:
                return None
            allowed = {o["value"] for o in options}
            return val if val in allowed else None

        filter_map = {
            "Brand": safe_filter_value(brand_vals, brand_options),
            "State": safe_filter_value(state_vals, state_options),
            "Region": safe_filter_value(region_vals, region_options),
            "City": safe_filter_value(city_vals, city_options),
            "Type": safe_filter_value(type_vals, type_options),
            "Outlet Name": safe_filter_value(outlet_vals, outlet_options),
            #"Tabs": safe_filter_value(tabs_vals, tabs_options),
        }

        # ---------------------------------------------------
        # REPLACE df WITH FILTERED VERSION FOR KPI + TABLE + CHARTS
        # ---------------------------------------------------
        df_filtered = df_base.copy()
        for col, val in filter_map.items():
            if val and col in df_filtered.columns:
                df_filtered = df_filtered[df_filtered[col] == val]

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
                df_filtered = df_filtered[df_filtered["DayName"] == day_val]
            elif day_val in ["WD", "WF", "WE"]:
                df_filtered = df_filtered[df_filtered["DayCode"] == day_val]

        # Search by outlet substring
        if search_txt:
            st = str(search_txt).strip().lower()
            if st:
                df_filtered = df_filtered[df_filtered["Outlet Name"].str.lower().str.contains(st, na=False)]

        # --- No data after filters ---
        if df.empty:
            return make_empty_result(
                month_opts=safe_options(df_all, "MonthKey"),
                brand_opts=safe_options(df_all, "Brand"),
                state_opts=safe_options(df_all, "State"),
                region_opts=safe_options(df_all, "Region"),
                city_opts=safe_options(df_all, "City"),
                type_opts=safe_options(df_all, "Type"),
                outlet_opts=safe_options(df_all, "Outlet Name"),
                week_opts=safe_options(df_all, "YearWeek"),
            )


        # ---------------------------------------------------
        # KPI section (colored cards)
        # ---------------------------------------------------
        df = df_filtered.copy()
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

        def colored_card(title, value, integer=False):
            display_value = fmt_int(value) if integer else fmt_float(value)

            return html.Div(
            [
                    html.Div(title, style={"fontSize": "12px", "color": "white"}),
                    html.Div(
                        display_value,
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
                dbc.Col(colored_card("Total Covers", k["covers"], integer=True), md=3),
                dbc.Col(colored_card("Total Bills", k["bills"], integer=True), md=3),
                dbc.Col(colored_card("Total Days", k["days"], integer=True), md=3),
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
            "Brand",
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
        # Charge summary (Region + Outlet) ✅ FINAL SAFE VERSION
        # ---------------------------------------------------

        charge_components = [
            "Restaurant Charge",
            "Packaging Charge [CART - SWIGGY]",
            "Restaurant Packaging Charges",
            "Delivery Charge",
            "Platform Fee Charge",
            "Smile Amount Charge",
        ]

        group_cols = ["Region", "Outlet Name"]

        # 🔒 Guarantee all charge columns exist BEFORE grouping
        for col in charge_components + ["Charges"]:
            if col not in df_filtered.columns:
                df_filtered[col] = 0.0
            else:
                df_filtered[col] = df_filtered[col].fillna(0.0)

        # ✅ Always aggregate ALL charge columns
        agg_dict = {c: "sum" for c in charge_components}
        agg_dict["Charges"] = "sum"

        ch_raw = (
            df_filtered
            .groupby(group_cols, as_index=False)
            .agg(agg_dict)
        )

        # ✅ Final formatting
        charge_summary_data = format_table_df(ch_raw).to_dict("records")

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
        outlet_agg = (
            df_filtered
            .groupby("Outlet Name", as_index=False)
            .agg(
                Net_Sale=("Net Sale", "sum"),
                Charges=("Charges", "sum"),
                Discount=("Discount", "sum"),
                Total_Tax=("Total Tax", "sum"),   # ✅ FIX
                Total_Final_Sale=("Final_Net_Sale", "sum"),
            )
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
        # FINAL RETURN (17 outputs IN CORRECT ORDER)
        # ---------------------------------------------------
        return (
            kpi_row,                 # 1  sales_kpi_row.children
            range_badge,             # 2  range-summary.children
            table_data,              # 3  sales_table.data
            charge_summary_data,     # 4  sales_charge_summary.data
            trend_fig,               # 5  sales_trend_graph.figure
            outlet_trend_fig,        # 6  sales_outlet_trend.figure
            outlet_summary_data,     # 7  sales_outlet_summary_table.data
            top5_data,               # 8  sales_top5_outlet_table.data
            final_area_fig,          # 9  sales_final_sale_area.figure
            region_bar_fig,          # 10 sales_final_sale_region_bar.figure
            tabwise_fig,             # 11 sales_tabwise_aov_ads_adt.figure
            charge_break_fig,        # 12 sales_charge_breakdown_outlet.figure

            # --- Cascade Filter Options ---
            brand_options,
            state_options,           
            region_options,          # 14 sales_region_filter.options
            city_options,            # 15 sales_city_filter.options
            type_options,
            outlet_options,          # 16 sales_outlet_filter.options
            fy_options,
            month_opts,              # 13 sales_month_filter.options
            week_opts,                # 17 sales_week_filter.options
            # VALUES (for RESET)
            brand_vals if ctx != "sales_reset_button" else None,
            state_vals if ctx != "sales_reset_button" else None,
            region_vals if ctx != "sales_reset_button" else None,
            city_vals if ctx != "sales_reset_button" else None,
            type_vals if ctx != "sales_reset_button" else None,
            outlet_vals if ctx != "sales_reset_button" else None,
            effective_start,
            effective_end,
            month_val if ctx != "sales_reset_button" else None,
            week_vals if ctx != "sales_reset_button" else None,
            day_val if ctx != "sales_reset_button" else "ALL",
            tabs_vals if ctx != "sales_reset_button" else None,
            search_txt if ctx != "sales_reset_button" else "",
    # 31  <-- REQUIRED
        )
