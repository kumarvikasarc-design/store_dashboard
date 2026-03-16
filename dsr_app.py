# main_app.py - Unified Multi-Page Dashboard with Sidebar
# Includes:
#   Page 1: DSR / DSR Dynamic Dashboard  (full implementation)
#   Page 2: Daypart Dashboard            (placeholder - plug your existing layout)
#   Page 3: Sales Dashboard              (placeholder - plug your existing layout)

import os
import glob
import numpy as np
import pandas as pd
from datetime import timedelta

from dash import Dash, html, dcc, Input, Output, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px

# ---------------------------
# Paths
# ---------------------------
BASE_DIR = r"C:\Users\ACER\store_dashboard"
SALES_FOLDER = os.path.join(BASE_DIR, "sales_dashboard")
STORES_DB_FILE = os.path.join(BASE_DIR, "stores_db.csv")


# ---------------------------
# Data engine (shared)
# ---------------------------

def smart_read_csv(file_path: str) -> pd.DataFrame:
    """
    Smart CSV loader:
    - Tries multiple skiprows (0..4)
    - Chooses the version with the most columns
    - Drops completely empty rows/columns
    - Trims headers
    - Returns cleaned DataFrame or empty if invalid
    """
    best_df = None
    best_cols = 0

    for skip in range(0, 5):
        try:
            df_try = pd.read_csv(file_path, skiprows=skip)
        except Exception:
            continue

        if df_try is None or df_try.empty:
            continue

        if df_try.shape[1] > best_cols:
            best_df = df_try
            best_cols = df_try.shape[1]

    if best_df is None:
        print(f"Skipping unreadable CSV: {file_path}")
        return pd.DataFrame()

    if best_df.shape[1] <= 1:
        print(f"Skipping invalid CSV (1-col): {file_path}")
        return pd.DataFrame()

    best_df = best_df.dropna(how="all")
    best_df = best_df.dropna(axis=1, how="all")
    best_df.columns = best_df.columns.astype(str).str.strip()

    return best_df.reset_index(drop=True)


def map_source_type(src: str) -> str:
    s = str(src).strip().lower()
    if s in ["pos", "dine in", "dine-in"]:
        return "Dine In"
    if s in [
        "swiggy",
        "zomato",
        "swiggy-bolt urgent",
        "magicpin-ordering",
        "magicpin",
        "bolt urgent",
        "swiggy-bolt",
        "swiggy bolt urgent",
    ]:
        return "Delivery"
    if s in ["mobile app", "app"]:
        return "App"
    return "Other"


def load_sales_data() -> pd.DataFrame:
    """Load all CSV sales files, auto-clean, compute Net Sale, Charges, Final_Net_Sale."""
    files = glob.glob(os.path.join(SALES_FOLDER, "*.csv"))
    if not files:
        print("No sales CSV files found in:", SALES_FOLDER)
        return pd.DataFrame()

    frames = []
    for f in files:
        df = smart_read_csv(f)
        if df.empty:
            continue

        required_cols = [
            "Region",
            "Outlet Name",
            "Date",
            "Source",
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
            "Net Sale",
            "Charges",
        ]
        for col in required_cols:
            if col not in df.columns:
                if col in ["Region", "Outlet Name", "Date", "Source"]:
                    df[col] = ""
                else:
                    df[col] = 0

        # ---- Hybrid Auto Date Parsing (no warnings) ----
        raw_date = df["Date"].astype(str)
        df["Date"] = pd.to_datetime(raw_date, errors="coerce")

        mask = df["Date"].isna()
        if mask.any():
            df.loc[mask, "Date"] = pd.to_datetime(
                raw_date[mask],
                dayfirst=True,
                errors="coerce",
            )

        mask2 = df["Date"].isna()
        if mask2.any():
            try:
                df.loc[mask2, "Date"] = pd.to_datetime(
                    raw_date[mask2].astype(float),
                    origin="1899-12-30",
                    unit="d",
                    errors="coerce",
                )
            except Exception:
                pass

        df = df.dropna(subset=["Date"])

        numeric_cols = [
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
            "Net Sale",
            "Charges",
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Net Sale & Charges
        df["Net Sale"] = df["Sale"] - df["Discount"]
        charge_components = [
            "Restaurant Charge",
            "Packaging Charge [CART - SWIGGY]",
            "Restaurant Packaging Charges",
            "Delivery Charge",
            "Platform Fee Charge",
            "Smile Amount Charge",
            "Packaging Charge",
        ]
        df["Charges"] = df[charge_components].sum(axis=1)
        df["Final_Net_Sale"] = df["Net Sale"] + df["Charges"]

        df["SourceType"] = df["Source"].apply(map_source_type)

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

        frames.append(df)

    valid_frames = [
        fdf for fdf in frames
        if fdf is not None and not fdf.empty and fdf.dropna(how="all").shape[1] > 0
    ]
    if not valid_frames:
        return pd.DataFrame()

    full_df = pd.concat(valid_frames, ignore_index=True)

    if "Region" not in full_df.columns:
        full_df["Region"] = "Unknown"

    # Merge stores DB if available (for City, etc.)
    if os.path.exists(STORES_DB_FILE):
        try:
            stores = pd.read_csv(STORES_DB_FILE)
            stores.columns = [str(c).strip() for c in stores.columns]
            if "Region" in stores.columns:
                stores = stores.rename(columns={"Region": "Region_store"})
            full_df = full_df.merge(
                stores,
                how="left",
                on="Outlet Name",
                suffixes=("", "_store"),
            )
        except Exception as e:
            print("Warning: could not merge stores_db.csv:", e)

    return full_df


def compute_group_kpis(df_period: pd.DataFrame) -> pd.DataFrame:
    """
    Core KPI aggregation.
    """
    if df_period.empty:
        return pd.DataFrame(
            columns=[
                "Region",
                "Outlet Name",
                "SourceType",
                "Days",
                "FinalNet",
                "Bills",
                "AOV",
                "ADT",
                "ADS",
            ]
        )

    d = df_period.copy()
    if "SourceType" not in d.columns:
        d["SourceType"] = d["Source"].apply(map_source_type)

    group_src = (
        d.groupby(["Region", "Outlet Name", "SourceType"], as_index=False)
        .agg(
            Days=("Date", "nunique"),
            FinalNet=("Final_Net_Sale", "sum"),
            Bills=("No Of Bills", "sum"),
        )
    )

    mask_overall = group_src["SourceType"].isin(["Dine In", "Delivery", "App"])
    overall = (
        group_src.loc[mask_overall]
        .groupby(["Region", "Outlet Name"], as_index=False)
        .agg(
            Days=("Days", "max"),
            FinalNet=("FinalNet", "sum"),
            Bills=("Bills", "sum"),
        )
    )
    overall["SourceType"] = "Overall"

    group = pd.concat([group_src, overall], ignore_index=True)

    group["AOV"] = np.where(group["Bills"] > 0, group["FinalNet"] / group["Bills"], 0.0)
    group["ADT"] = np.where(group["Days"] > 0, group["Bills"] / group["Days"], 0.0)
    group["ADS"] = np.where(group["Days"] > 0, group["FinalNet"] / group["Days"], 0.0)

    return group


def period_kpis(df_period: pd.DataFrame, period_name: str) -> pd.DataFrame:
    """
    Calculate AOV, ADT, ADS, Mix for a given period.
    """
    if df_period.empty:
        return pd.DataFrame(
            columns=["Region", "Outlet Name", "SourceType", "Category", "Value", "Period"]
        )

    group = compute_group_kpis(df_period)

    overall_ads = (
        group[group["SourceType"] == "Overall"][["Region", "Outlet Name", "ADS"]]
        .rename(columns={"ADS": "ADS_Overall"})
    )
    group = group.merge(overall_ads, on=["Region", "Outlet Name"], how="left")

    def compute_mix(row):
        if row["SourceType"] == "Overall":
            return 100.0 if row["ADS_Overall"] > 0 else 0.0
        if row["ADS_Overall"] > 0:
            return row["ADS"] / row["ADS_Overall"] * 100.0
        return 0.0

    group["Mix"] = group.apply(compute_mix, axis=1)

    long_df = group.melt(
        id_vars=["Region", "Outlet Name", "SourceType"],
        value_vars=["AOV", "ADT", "ADS", "Mix"],
        var_name="Category",
        value_name="Value",
    )
    long_df["Period"] = period_name
    return long_df[["Region", "Outlet Name", "SourceType", "Category", "Value", "Period"]]


def build_weekly_mis(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    d = df.copy()
    d["Year"] = d["Date"].dt.year
    d["WeekNumber"] = d["Date"].dt.isocalendar().week.astype(int)

    weekly_list = []
    for (year, week), df_week in d.groupby(["Year", "WeekNumber"]):
        group = compute_group_kpis(df_week)
        if group.empty:
            continue

        overall_ads = (
            group[group["SourceType"] == "Overall"][
                ["Region", "Outlet Name", "ADS"]
            ].rename(columns={"ADS": "ADS_Overall"})
        )
        g = group.merge(overall_ads, on=["Region", "Outlet Name"], how="left")

        def compute_mix(row):
            if row["SourceType"] == "Overall":
                return 100.0 if row["ADS_Overall"] > 0 else 0.0
            if row["ADS_Overall"] > 0:
                return row["ADS"] / row["ADS_Overall"] * 100.0
            return 0.0

        g["Mix"] = g.apply(compute_mix, axis=1)
        g["Year"] = year
        g["WeekNumber"] = week
        weekly_list.append(g)

    if not weekly_list:
        return pd.DataFrame()

    weekly = pd.concat(weekly_list, ignore_index=True)
    weekly = weekly.rename(columns={"SourceType": "Source"})
    num_cols = weekly.select_dtypes(include=[np.number]).columns
    weekly[num_cols] = weekly[num_cols].round(2)

    return weekly[
        [
            "Year",
            "WeekNumber",
            "Region",
            "Outlet Name",
            "Source",
            "AOV",
            "ADT",
            "ADS",
            "Mix",
        ]
    ]


def build_summary_and_mis(df: pd.DataFrame):
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    yesterday = df["Date"].max()

    start_wtd = yesterday - timedelta(days=yesterday.weekday())
    end_wtd = yesterday
    start_lwtd = start_wtd - timedelta(days=7)
    end_lwtd = end_wtd - timedelta(days=7)

    start_mtd = yesterday.replace(day=1)
    end_mtd = yesterday
    last_month_end = start_mtd - timedelta(days=1)
    start_lmtd = last_month_end.replace(day=1)
    day_num = min(yesterday.day, last_month_end.day)
    end_lmtd = start_lmtd.replace(day=day_num)

    start_last4 = yesterday - timedelta(days=28)
    end_last4 = yesterday
    lwsd_date = yesterday - timedelta(days=7)
    l4wsd_date = yesterday - timedelta(days=28)

    period_defs = {
        "WTD": (start_wtd, end_wtd),
        "LWTD": (start_lwtd, end_lwtd),
        "MTD": (start_mtd, end_mtd),
        "LMTD": (start_lmtd, end_lmtd),
        "Last4Weeks": (start_last4, end_last4),
        "Yesterday": (yesterday, yesterday),
        "LWSD": (lwsd_date, lwsd_date),
        "L4WSD": (l4wsd_date, l4wsd_date),
    }

    period_frames = []
    for pname, (start, end) in period_defs.items():
        mask = (df["Date"] >= start) & (df["Date"] <= end)
        df_p = df.loc[mask].copy()
        period_frames.append(period_kpis(df_p, pname))

    valid_period_frames = [
        p for p in period_frames
        if p is not None and not p.empty and p.dropna(how="all").shape[1] > 0
    ]

    if valid_period_frames:
        all_periods = pd.concat(valid_period_frames, ignore_index=True)
    else:
        all_periods = pd.DataFrame()

    if all_periods.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    summary = all_periods.pivot_table(
        index=["Region", "Outlet Name", "SourceType", "Category"],
        columns="Period",
        values="Value",
        aggfunc="first",
    ).reset_index()

    for col in ["WTD", "LWTD", "MTD", "LMTD", "Last4Weeks", "Yesterday", "LWSD", "L4WSD"]:
        if col not in summary.columns:
            summary[col] = 0.0

    summary["Change_WTD"] = np.where(
        summary["LWTD"] != 0,
        (summary["WTD"] - summary["LWTD"]) / summary["LWTD"] * 100.0,
        0.0,
    )

    summary["Change_MTD"] = np.where(
        summary["LMTD"] != 0,
        (summary["MTD"] - summary["LMTD"]) / summary["LMTD"] * 100.0,
        0.0,
    )

    summary["Month"] = ""
    summary["WeekNumber"] = 0
    summary = summary.rename(columns={"SourceType": "Source"})

    num_cols = summary.select_dtypes(include=[np.number]).columns
    summary[num_cols] = summary[num_cols].round(2)

    mtd_long = all_periods[all_periods["Period"] == "MTD"].copy()
    if mtd_long.empty:
        mis = pd.DataFrame()
    else:
        mis = (
            mtd_long.pivot_table(
                index=["Region", "SourceType"],
                columns="Category",
                values="Value",
                aggfunc="mean",
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )
        num_cols_mis = mis.select_dtypes(include=[np.number]).columns
        mis[num_cols_mis] = mis[num_cols_mis].round(2)
        mis = mis.rename(columns={"SourceType": "Source"})

    weekly_mis = build_weekly_mis(df)

    return summary, mis, weekly_mis


def empty_fig(message="No data"):
    fig = px.scatter()
    fig.update_layout(
        annotations=[dict(text=message, x=0.5, y=0.5, showarrow=False)],
        xaxis={"visible": False},
        yaxis={"visible": False},
        margin=dict(l=0, r=0, t=0, b=0),
    )
    return fig


# ---------------------------
# Initial load
# ---------------------------
sales_df = load_sales_data()
summary_df_init, mis_df_init, weekly_mis_init = build_summary_and_mis(sales_df)

SUMMARY_COLUMNS = [
    "Region",
    "Outlet Name",
    "Source",
    "Category",
    "Month",
    "WeekNumber",
    "WTD",
    "LWTD",
    "Change_WTD",
    "MTD",
    "LMTD",
    "Change_MTD",
    "Last4Weeks",
    "Yesterday",
    "LWSD",
    "L4WSD",
]
MIS_COLUMNS = mis_df_init.columns.tolist() if not mis_df_init.empty else []
WEEKLY_COLUMNS = weekly_mis_init.columns.tolist() if not weekly_mis_init.empty else []

SUMMARY_HEADER_STYLE = {
    "fontSize": "18px",
    "fontFamily": "Times New Roman",
    "textAlign": "left",
    "fontWeight": "bold",
    "whiteSpace": "normal",
}
SUMMARY_CELL_STYLE = {
    "fontSize": "12px",
    "fontFamily": "Times New Roman",
    "textAlign": "left",
    "whiteSpace": "normal",
    "height": "auto",
    "padding": "4px",
}


# ---------------------------
# DSR layout (Page 1)
# ---------------------------
def layout_dsr():
    outlet_options = (
        [
            {"label": o, "value": o}
            for o in sorted(sales_df["Outlet Name"].dropna().unique())
        ]
        if not sales_df.empty
        else []
    )

    region_options = (
        [
            {"label": r, "value": r}
            for r in sorted(sales_df["Region"].dropna().unique())
        ]
        if not sales_df.empty
        else []
    )

    city_options = (
        [
            {"label": c, "value": c}
            for c in sorted(sales_df["City"].dropna().unique())
        ]
        if ("City" in sales_df.columns and not sales_df.empty)
        else []
    )

    month_options = (
        [
            {"label": ml, "value": mk}
            for mk, ml in (
                sales_df[["MonthKey", "MonthLabel"]]
                .dropna()
                .drop_duplicates()
                .sort_values("MonthKey")
                .itertuples(index=False, name=None)
            )
        ]
        if not sales_df.empty
        else []
    )

    source_options = [
        {"label": s, "value": s}
        for s in ["Dine In", "Delivery", "App", "Other", "Mix"]
    ]
    category_options = [
        {"label": c, "value": c} for c in ["AOV", "ADT", "ADS", "Mix", "Overall"]
    ]
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

    return html.Div(
        [
            html.H3("DSR Dynamic Sales Dashboard", style={"marginTop": "5px"}),

            # Top filters
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Region"),
                            dcc.Dropdown(
                                id="region_filter",
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
                                id="city_filter",
                                options=city_options,
                                multi=True,
                                placeholder="All Cities",
                            ),
                        ],
                        md=3,
                    ),
                    dbc.Col(
                        [
                            html.Label("Outlet Name"),
                            dcc.Dropdown(
                                id="outlet_filter",
                                options=outlet_options,
                                multi=True,
                                placeholder="All Outlets",
                            ),
                        ],
                        md=3,
                    ),
                    dbc.Col(
                        [
                            html.Label("Source"),
                            dcc.Dropdown(
                                id="source_filter",
                                options=source_options,
                                multi=True,
                                placeholder="Dine In / Delivery / App / Other / Mix",
                            ),
                        ],
                        md=3,
                    ),
                ],
                style={"marginTop": "10px"},
            ),

            # Second row
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Category"),
                            dcc.Dropdown(
                                id="category_filter",
                                options=category_options,
                                multi=True,
                                placeholder="AOV / ADT / ADS / Mix / Overall",
                            ),
                        ],
                        md=3,
                    ),
                    dbc.Col(
                        [
                            html.Label("Month"),
                            dcc.Dropdown(
                                id="month_filter",
                                options=month_options,
                                multi=False,
                                placeholder="All Months",
                            ),
                        ],
                        md=3,
                    ),
                    dbc.Col(
                        [
                            html.Label("Week"),
                            dcc.Dropdown(
                                id="week_filter",
                                options=[],
                                multi=True,
                                placeholder="All Weeks",
                            ),
                        ],
                        md=3,
                    ),
                    dbc.Col(
                        [
                            html.Label("Day"),
                            dcc.Dropdown(
                                id="day_filter",
                                options=day_options,
                                multi=False,
                                value="ALL",
                            ),
                        ],
                        md=2,
                    ),
                    dbc.Col(
                        [
                            html.Label(" "),
                            html.Button(
                                "Refresh Data",
                                id="refresh_button",
                                n_clicks=0,
                                style={"width": "100%", "marginTop": "4px"},
                            ),
                        ],
                        md=1,
                    ),
                ],
                style={"marginTop": "10px"},
            ),

            # Search
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Summary search (matching letters on Outlet Name)"),
                            dcc.Input(
                                id="search_text",
                                type="text",
                                placeholder="Type letters... e.g. 'gk'",
                                style={"width": "100%"},
                            ),
                        ],
                        md=4,
                    )
                ],
                style={"marginTop": "10px", "marginBottom": "10px"},
            ),

            dcc.Tabs(
                id="tabs",
                value="tab-summary",
                children=[
                    dcc.Tab(
                        label="Summary (Running Month First)",
                        value="tab-summary",
                        children=[
                            html.Br(),
                            dash_table.DataTable(
                                id="summary_table",
                                columns=[{"name": c, "id": c} for c in SUMMARY_COLUMNS],
                                data=summary_df_init.to_dict("records")
                                if not summary_df_init.empty
                                else [],
                                style_header=SUMMARY_HEADER_STYLE,
                                style_cell=SUMMARY_CELL_STYLE,
                                style_table={"overflowX": "auto"},
                                page_size=20,
                            ),
                        ],
                    ),
                    dcc.Tab(
                        label="MIS Report (MTD)",
                        value="tab-mis",
                        children=[
                            html.Br(),
                            dash_table.DataTable(
                                id="mis_table",
                                columns=[{"name": c, "id": c} for c in MIS_COLUMNS]
                                if MIS_COLUMNS
                                else [],
                                data=mis_df_init.to_dict("records")
                                if not mis_df_init.empty
                                else [],
                                style_header=SUMMARY_HEADER_STYLE,
                                style_cell=SUMMARY_CELL_STYLE,
                                style_table={"overflowX": "auto"},
                                page_size=20,
                            ),
                        ],
                    ),
                    dcc.Tab(
                        label="Weekly MIS",
                        value="tab-weekly-mis",
                        children=[
                            html.Br(),
                            dash_table.DataTable(
                                id="weekly_mis_table",
                                columns=[{"name": c, "id": c} for c in WEEKLY_COLUMNS]
                                if WEEKLY_COLUMNS
                                else [],
                                data=weekly_mis_init.to_dict("records")
                                if not weekly_mis_init.empty
                                else [],
                                style_header=SUMMARY_HEADER_STYLE,
                                style_cell=SUMMARY_CELL_STYLE,
                                style_table={"overflowX": "auto"},
                                page_size=20,
                            ),
                        ],
                    ),
                    dcc.Tab(
                        label="Charts",
                        value="tab-charts",
                        children=[
                            html.Br(),
                            dbc.Row(
                                [
                                    dbc.Col(
                                        [
                                            html.H5("ADS Trend (Overall, per Outlet)"),
                                            dcc.Graph(
                                                id="ads_trend_graph",
                                                figure=empty_fig("No data"),
                                                style={"height": "450px"},
                                            ),
                                        ],
                                        md=12,
                                    )
                                ]
                            ),
                            html.Br(),
                            dbc.Row(
                                [
                                    dbc.Col(
                                        [
                                            html.H5("MTD AOV vs Source"),
                                            dcc.Graph(
                                                id="aov_source_graph",
                                                figure=empty_fig("No data"),
                                                style={"height": "430px"},
                                            ),
                                        ],
                                        md=6,
                                    ),
                                    dbc.Col(
                                        [
                                            html.H5("Monthly ADS Comparison (MTD vs LMTD)"),
                                            dcc.Graph(
                                                id="monthly_ads_graph",
                                                figure=empty_fig("No data"),
                                                style={"height": "430px"},
                                            ),
                                        ],
                                        md=6,
                                    ),
                                ]
                            ),
                        ],
                    ),
                ],
            ),
        ]
    )


# ---------------------------
# Daypart & Sales layouts (Page 2 & 3) - plug your existing dashboards here
# ---------------------------

def layout_daypart():
    # TODO: Replace this with your real daypart_dashboard layout
    return html.Div(
        [
            html.H3("Daypart Dashboard"),
            html.P("Placeholder: paste your daypart_dashboard components here."),
        ],
        style={"padding": "10px"},
    )


def layout_sales():
    # TODO: Replace this with your real sale_app layout
    return html.Div(
        [
            html.H3("Sales Dashboard"),
            html.P("Placeholder: paste your sale_app components here."),
        ],
        style={"padding": "10px"},
    )


# ---------------------------
# Main App + Sidebar Shell
# ---------------------------

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
)

sidebar = html.Div(
    [
        html.H2("Reports", className="display-6", style={"padding": "10px 0"}),
        html.Hr(),
        dbc.Nav(
            [
                dbc.NavLink("DSR Dashboard", href="/dsr", id="link-dsr", active="exact"),
                dbc.NavLink("Daypart Dashboard", href="/daypart", id="link-daypart", active="exact"),
                dbc.NavLink("Sales Dashboard", href="/sales", id="link-sales", active="exact"),
            ],
            vertical=True,
            pills=True,
        ),
    ],
    style={
        "position": "fixed",
        "top": 0,
        "left": 0,
        "bottom": 0,
        "width": "230px",
        "padding": "15px",
        "backgroundColor": "#f8f9fa",
        "borderRight": "1px solid #ddd",
    },
)

content = html.Div(
    id="page-content",
    style={
        "marginLeft": "250px",
        "marginRight": "20px",
        "padding": "10px 10px",
    },
)

app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        sidebar,
        content,
    ]
)


# ---------------------------
# Routing callback (switch pages)
# ---------------------------

@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname"),
)
def display_page(pathname):
    if pathname in ["/", "/dsr"]:
        return layout_dsr()
    elif pathname == "/daypart":
        return layout_daypart()
    elif pathname == "/sales":
        return layout_sales()
    else:
        return html.H3("404 - Page not found")


# ---------------------------
# DSR callbacks (reused from earlier)
# ---------------------------

@app.callback(
    Output("city_filter", "options"),
    Output("outlet_filter", "options"),
    Input("region_filter", "value"),
    Input("city_filter", "value"),
    Input("refresh_button", "n_clicks"),
)
def update_city_outlet_options(region_vals, city_vals, n_clicks):
    global sales_df
    df = sales_df
    if df.empty:
        return [], []

    if region_vals:
        df_region = df[df["Region"].isin(region_vals)]
    else:
        df_region = df

    if "City" in df_region.columns:
        cities = sorted(df_region["City"].dropna().unique())
    else:
        cities = []
    city_options_local = [{"label": c, "value": c} for c in cities]

    df_outlet = df_region
    if city_vals and "City" in df_outlet.columns:
        if isinstance(city_vals, list):
            df_outlet = df_outlet[df_outlet["City"].isin(city_vals)]
        else:
            df_outlet = df_outlet[df_outlet["City"] == city_vals]
    outlets = sorted(df_outlet["Outlet Name"].dropna().unique())
    outlet_options_local = [{"label": o, "value": o} for o in outlets]

    return city_options_local, outlet_options_local


@app.callback(
    Output("week_filter", "options"),
    Input("month_filter", "value"),
    Input("refresh_button", "n_clicks"),
)
def update_week_options(month_val, n_clicks):
    global sales_df
    df = sales_df
    if df.empty:
        return []

    if month_val:
        df = df[df["MonthKey"] == month_val]

    if df.empty or "YearWeek" not in df.columns:
        return []

    weeks = (
        df[["YearWeek", "Year", "WeekNumber"]]
        .drop_duplicates()
        .sort_values(["Year", "WeekNumber"])
    )

    options = [
        {
            "label": f"Wk {int(row.WeekNumber)} - {int(row.Year)}",
            "value": row.YearWeek,
        }
        for row in weeks.itertuples(index=False)
    ]
    return options


@app.callback(
    Output("summary_table", "data"),
    Output("mis_table", "data"),
    Output("weekly_mis_table", "data"),
    Output("ads_trend_graph", "figure"),
    Output("aov_source_graph", "figure"),
    Output("monthly_ads_graph", "figure"),
    Input("region_filter", "value"),
    Input("city_filter", "value"),
    Input("outlet_filter", "value"),
    Input("source_filter", "value"),
    Input("category_filter", "value"),
    Input("month_filter", "value"),
    Input("week_filter", "value"),
    Input("day_filter", "value"),
    Input("search_text", "value"),
    Input("refresh_button", "n_clicks"),
)
def update_dashboard(
    region_vals,
    city_vals,
    outlet_vals,
    source_vals,
    category_vals,
    month_val,
    week_vals,
    day_val,
    search_txt,
    n_clicks,
):
    global sales_df

    if n_clicks:
        sales_df = load_sales_data()

    df = sales_df.copy()
    if df.empty:
        fig = empty_fig("No data")
        empty_data = []
        return empty_data, empty_data, empty_data, fig, fig, fig

    if region_vals:
        df = df[df["Region"].isin(region_vals)]

    if city_vals and "City" in df.columns:
        if isinstance(city_vals, list):
            df = df[df["City"].isin(city_vals)]
        else:
            df = df[df["City"] == city_vals]

    if outlet_vals:
        df = df[df["Outlet Name"].isin(outlet_vals)]

    if month_val:
        df = df[df["MonthKey"] == month_val]

    if week_vals:
        if not isinstance(week_vals, list):
            week_vals = [week_vals]
        df = df[df["YearWeek"].isin(week_vals)]

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

    summary, mis, weekly = build_summary_and_mis(df)

    if not summary.empty:
        if not df.empty:
            yesterday = df["Date"].max()
            month_label = yesterday.strftime("%b-%y")
            week_number = int(yesterday.isocalendar().week)
            summary["Month"] = month_label
            summary["WeekNumber"] = week_number

        if source_vals and not (category_vals and "Overall" in category_vals):
            mask = False
            if any(sv == "Mix" for sv in source_vals):
                mask = mask | (summary["Category"] == "Mix")
            non_mix_sources = [sv for sv in source_vals if sv != "Mix"]
            if non_mix_sources:
                mask = mask | summary["Source"].isin(non_mix_sources)
            summary = summary[mask]

        if category_vals:
            cat_vals = [c for c in category_vals if c != "Overall"]
            if cat_vals:
                summary = summary[summary["Category"].isin(cat_vals)]
            if "Overall" in category_vals:
                summary = summary[summary["Source"] == "Overall"]

        if search_txt:
            st = str(search_txt).strip().lower()
            if st:
                summary = summary[
                    summary["Outlet Name"].str.lower().str.contains(st, na=False)
                ]

        num_cols = summary.select_dtypes(include=[np.number]).columns
        summary[num_cols] = summary[num_cols].round(2)
        summary_data = summary.to_dict("records")
    else:
        summary_data = []

    mis_data = mis.to_dict("records") if not mis.empty else []

    if not weekly.empty:
        if region_vals:
            weekly = weekly[weekly["Region"].isin(region_vals)]

        if city_vals and "City" in df.columns:
            if isinstance(city_vals, list):
                outlets_city = df[df["City"].isin(city_vals)]["Outlet Name"].unique()
            else:
                outlets_city = df[df["City"] == city_vals]["Outlet Name"].unique()
            weekly = weekly[weekly["Outlet Name"].isin(outlets_city)]

        if outlet_vals:
            weekly = weekly[weekly["Outlet Name"].isin(outlet_vals)]

        if source_vals:
            src_filter = [s for s in source_vals if s != "Mix"]
            if src_filter:
                weekly = weekly[weekly["Source"].isin(src_filter)]

        if search_txt:
            st = str(search_txt).strip().lower()
            if st:
                weekly = weekly[
                    weekly["Outlet Name"].str.lower().str.contains(st, na=False)
                ]

        num_cols_w = weekly.select_dtypes(include=[np.number]).columns
        weekly[num_cols_w] = weekly[num_cols_w].round(2)
        weekly_data = weekly.to_dict("records")
    else:
        weekly_data = []

    if df.empty:
        ads_trend_fig = empty_fig("No data")
    else:
        trend_df = df.copy()
        if source_vals:
            real_src = [s for s in source_vals if s in ["Dine In", "Delivery", "App", "Other"]]
            if real_src:
                trend_df = trend_df[trend_df["SourceType"].isin(real_src)]

        trend_group = (
            trend_df.groupby(["Date", "Outlet Name"], as_index=False)
            .agg(ADS=("Final_Net_Sale", "sum"))
        )

        if trend_group.empty:
            ads_trend_fig = empty_fig("No data")
        else:
            ads_trend_fig = px.line(
                trend_group,
                x="Date",
                y="ADS",
                color="Outlet Name",
                markers=True,
            )
            ads_trend_fig.update_layout(
                height=450,
                autosize=True,
                xaxis_title="Date",
                yaxis_title="ADS (Overall)",
                legend_title="Outlet",
                margin=dict(l=40, r=40, t=40, b=40),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )

    if summary.empty:
        aov_source_fig = empty_fig("No summary")
        monthly_ads_fig = empty_fig("No summary")
    else:
        aov_df = summary[summary["Category"] == "AOV"].copy()
        if aov_df.empty:
            aov_source_fig = empty_fig("No AOV data")
        else:
            aov_source_fig = px.bar(
                aov_df,
                x="Source",
                y="MTD",
                color="Outlet Name",
                barmode="group",
            )
            aov_source_fig.update_layout(
                height=430,
                autosize=True,
                xaxis_title="Source",
                yaxis_title="MTD AOV",
                legend_title="Outlet",
                margin=dict(l=40, r=40, t=40, b=40),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )

        ads_df = summary[summary["Category"] == "ADS"].copy()
        if ads_df.empty:
            monthly_ads_fig = empty_fig("No ADS data")
        else:
            ads_melt = ads_df.melt(
                id_vars=["Outlet Name"],
                value_vars=["MTD", "LMTD"],
                var_name="Period",
                value_name="ADS",
            )
            monthly_ads_fig = px.bar(
                ads_melt,
                x="Outlet Name",
                y="ADS",
                color="Period",
                barmode="group",
            )
            monthly_ads_fig.update_layout(
                height=430,
                autosize=True,
                xaxis_title="Outlet",
                yaxis_title="ADS",
                legend_title="Period",
                margin=dict(l=40, r=40, t=80, b=80),
                xaxis_tickangle=-45,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )

    return (
        summary_data,
        mis_data,
        weekly_data,
        ads_trend_fig,
        aov_source_fig,
        monthly_ads_fig,
    )


# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    app.run(debug=True)
