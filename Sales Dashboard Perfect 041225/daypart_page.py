# pages/daypart_page.py
# Coffee Island – Daypart Report Dashboard

import os
import glob
from datetime import datetime, date

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dash import html, dcc, dash_table, Input, Output
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate


# -------------------------------------------------------
# CONFIG
# -------------------------------------------------------
BASE_DIR = r"C:\Users\ACER\store_dashboard"
HOURLY_FOLDER = os.path.join(BASE_DIR, "hourly_sales")
STORE_CSV = os.path.join(BASE_DIR, "stores_db.csv")
AUTO_REFRESH_MS = 60 * 1000  # 60 seconds

CHART_H = 230
PIE_H = 230
BAR_H = 230
HEAT_H = 230
TREND_H = 230
WW_H = 200

DAYPART_ORDER = [
    "Pre Breakfast",
    "Breakfast",
    "Lunch",
    "Snack",
    "Dinner",
    "Late Night",
]

DOW_ORDER = [
    "Monday", "Tuesday", "Wednesday", "Thursday",
    "Friday", "Saturday", "Sunday",
]


# -------------------------------------------------------
# HELPERS
# -------------------------------------------------------
def get_daypart(h: int) -> str:
    if 6 <= h < 8:
        return "Pre Breakfast"
    if 8 <= h < 11:
        return "Breakfast"
    if 11 <= h < 15:
        return "Lunch"
    if 15 <= h < 19:
        return "Snack"
    if 19 <= h < 23:
        return "Dinner"
    return "Late Night"


def smart_read_csv(path: str) -> pd.DataFrame:
    """
    Detect and skip junk rows like:
    'All the values shown here are in INR.'
    by scanning first 10 lines for a header that
    contains Outlet/Date/Hour/Tab.
    """
    with open(path, "r", errors="ignore") as f:
        lines = f.readlines()

    header_idx = None
    for i, line in enumerate(lines[:10]):
        l = line.lower()
        if ("outlet" in l and "date" in l) or ("hour" in l and "tab" in l):
            header_idx = i
            break

    if header_idx is None:
        best_df = None
        best_cols = 0
        for skip in range(0, 5):
            try:
                tmp = pd.read_csv(path, skiprows=skip)
            except Exception:
                continue
            if tmp.shape[1] > best_cols:
                best_cols = tmp.shape[1]
                best_df = tmp
        if best_df is None:
            return pd.DataFrame()
        df = best_df
    else:
        df = pd.read_csv(path, skiprows=header_idx)

    df = df.dropna(how="all").dropna(axis=1, how="all")
    df.columns = df.columns.astype(str).str.strip()
    return df.reset_index(drop=True)


def parse_dates_auto(series: pd.Series) -> pd.Series:
    """Auto-detect DD-MM vs MM-DD."""
    s = series.astype(str).str.strip()
    d1 = pd.to_datetime(s, dayfirst=True, errors="coerce")
    d2 = pd.to_datetime(s, dayfirst=False, errors="coerce")
    return d1 if d1.notna().sum() >= d2.notna().sum() else d2


def build_week_range_text(df: pd.DataFrame) -> str:
    """Week number(s) + date range for header."""
    if df.empty:
        return ""
    min_d = df["Date"].min().date()
    max_d = df["Date"].max().date()
    weeks = sorted(df["Week"].unique())
    if len(weeks) == 1:
        wk = f"Week {int(weeks[0])}"
    else:
        wk = "Weeks " + ", ".join(str(int(w)) for w in weeks)
    return f"{wk} : {min_d:%d-%b-%Y} to {max_d:%d-%b-%Y}"


def empty_fig(message="No data"):
    fig = go.Figure()
    fig.update_layout(
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[dict(text=message, x=0.5, y=0.5, showarrow=False)],
        margin=dict(l=0, r=0, t=20, b=0),
    )
    return fig


# -------------------------------------------------------
# LOAD HOURLY DATA
# -------------------------------------------------------
def load_hourly_data() -> pd.DataFrame:
    files = glob.glob(os.path.join(HOURLY_FOLDER, "*.csv"))
    if not files:
        print("No hourly CSV in:", HOURLY_FOLDER)
        return pd.DataFrame()

    frames = []
    for f in files:
        raw = smart_read_csv(f)
        if not raw.empty:
            frames.append(raw)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df.columns = df.columns.astype(str).str.strip()
    lower = {c.lower(): c for c in df.columns}

    def find(candidates):
        for cand in candidates:
            if cand in lower:
                return lower[cand]
        for cand in candidates:
            for lc, orig in lower.items():
                if cand in lc:
                    return orig
        return None

    rename = {}
    c = find(["outlet name", "outlet", "store"])
    if c:
        rename[c] = "Outlet Name"
    c = find(["date", "txn date"])
    if c:
        rename[c] = "Date"
    c = find(["hour", "hours"])
    if c:
        rename[c] = "Hour"
    c = find(["tab", "order type", "channel"])
    if c:
        rename[c] = "Tab"
    c = find(["sale", "gross sale", "gross amount"])
    if c:
        rename[c] = "Sale"
    c = find(["discount"])
    if c:
        rename[c] = "Discount"
    c = find(["net sale", "net amount"])
    if c:
        rename[c] = "Net Sale"
    c = find(["total charges", "charges"])
    if c:
        rename[c] = "Total Charges"
    c = find(["nob", "no of bills", "bills"])
    if c:
        rename[c] = "NOB"

    df = df.rename(columns=rename)

    # Ensure required columns
    required = [
        "Outlet Name", "Date", "Hour", "Tab",
        "Sale", "Discount", "Net Sale", "Total Charges", "NOB",
    ]
    for col in required:
        if col not in df.columns:
            df[col] = 0

    # Date + numeric
    df["Date"] = parse_dates_auto(df["Date"])
    df = df.dropna(subset=["Date"])

    df["Hour"] = pd.to_numeric(df["Hour"], errors="coerce").fillna(0).astype(int)

    for col in ["Sale", "Discount", "Net Sale", "Total Charges", "NOB"]:
        df[col] = (
            pd.to_numeric(
                df[col].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            ).fillna(0.0)
        )

    # Net Sale if missing
    if df["Net Sale"].sum() == 0 and df["Sale"].sum() != 0:
        df["Net Sale"] = df["Sale"] - df["Discount"]

    # Total Net Sale
    df["Total Net Sale"] = df["Net Sale"] + df["Total Charges"]

    # Calendar keys
    df["Day"] = df["Date"].dt.day_name()
    df["Week"] = df["Date"].dt.isocalendar().week.astype(int)
    df["Month"] = df["Date"].dt.to_period("M").astype(str)
    df["MonthLabel"] = df["Date"].dt.strftime("%b-%Y")

    dow = df["Date"].dt.dayofweek
    df["WeekCode"] = np.select(
        [dow <= 3, dow == 4, dow >= 5],
        ["WD", "WF", "WE"],
        default="WD",
    )

    # Daypart
    df["Daypart"] = df["Hour"].apply(get_daypart)
    df["Daypart"] = pd.Categorical(
        df["Daypart"],
        categories=DAYPART_ORDER,
        ordered=True,
    )

    # Merge Region / State / City / Type from stores_db.csv
    if os.path.exists(STORE_CSV):
        try:
            store = pd.read_csv(STORE_CSV)
            store.columns = store.columns.astype(str).str.strip()

            # Expected columns:
            # Store Id, Outlet Name, Region, City, Type,
            # Area Manager, Opening Date, Status, State
            cols = {}
            for col in store.columns:
                lc = col.lower()
                if "outlet" in lc:
                    cols[col] = "Outlet Name"
                elif lc == "region":
                    cols[col] = "Region"
                elif lc == "state":
                    cols[col] = "State"
                elif lc == "city":
                    cols[col] = "City"
                elif lc == "type":
                    cols[col] = "Type"

            store = store.rename(columns=cols)
            keep = ["Outlet Name", "Region", "State", "City", "Type"]
            keep = [k for k in keep if k in store.columns]
            store = store[keep].drop_duplicates()
            df = df.merge(store, how="left", on="Outlet Name")
        except Exception as e:
            print("Error merging stores_db:", e)

    for col in ["Region", "State", "City", "Type"]:
        if col not in df.columns:
            df[col] = ""

    return df.reset_index(drop=True)


# -------------------------------------------------------
# LAYOUT
# -------------------------------------------------------
def get_layout():
    df_init = load_hourly_data()

    # Date picker defaults: EMPTY (but logic will use running month)
    start_date = None
    end_date = None

    def opts(col):
        if df_init.empty or col not in df_init.columns:
            return []
        return [
            {"label": v, "value": v}
            for v in sorted(df_init[col].dropna().unique())
        ]

    month_opts = [
        {"label": m, "value": m}
        for m in sorted(df_init["MonthLabel"].unique())
    ] if not df_init.empty else []

    week_opts = [
        {"label": f"Week {int(w)}", "value": int(w)}
        for w in sorted(df_init["Week"].unique())
    ] if not df_init.empty else []

    daypart_opts = [{"label": d, "value": d} for d in DAYPART_ORDER]

    return html.Div(
        [
            dcc.Interval(
                id="dp-interval",
                interval=AUTO_REFRESH_MS,
                n_intervals=0,
            ),

            # HEADER
            html.Div(
                [
                    html.Div(
                        [
                            html.H3(
                                "Daypart Report Dashboard",
                                style={"marginBottom": "2px"},
                            ),
                            html.Div(
                                id="dp-week-summary",
                                style={"fontSize": "12px", "color": "#6b7280"},
                            ),
                        ]
                    ),
                    html.Div(
                        id="dp-last-refresh",
                        style={
                            "marginLeft": "auto",
                            "fontSize": "12px",
                            "color": "#6b7280",
                        },
                    ),
                ],
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "space-between",
                    "padding": "10px 14px",
                    "marginBottom": "10px",
                    "backgroundColor": "white",
                    "borderRadius": "10px",
                    "boxShadow": "0 4px 10px rgba(0,0,0,0.08)",
                    "position": "sticky",
                    "top": 0,
                    "zIndex": 10,
                },
            ),

            # FILTER PANEL
            html.Div(
                [
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Label("Region"),
                                    dcc.Dropdown(
                                        id="dp-region",
                                        options=opts("Region"),
                                        multi=True,
                                        placeholder="All Regions",
                                    ),
                                ],
                                md=3,
                            ),
                            dbc.Col(
                                [
                                    html.Label("State"),
                                    dcc.Dropdown(
                                        id="dp-state",
                                        options=opts("State"),
                                        multi=True,
                                        placeholder="All States",
                                    ),
                                ],
                                md=3,
                            ),
                            dbc.Col(
                                [
                                    html.Label("City"),
                                    dcc.Dropdown(
                                        id="dp-city",
                                        options=opts("City"),
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
                                        id="dp-type",
                                        options=opts("Type"),
                                        multi=True,
                                        placeholder="All Types",
                                    ),
                                ],
                                md=3,
                            ),
                        ],
                        style={"marginBottom": "8px"},
                    ),
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Label("Outlet"),
                                    dcc.Dropdown(
                                        id="dp-outlet",
                                        options=opts("Outlet Name"),
                                        multi=True,
                                        placeholder="All Outlets",
                                    ),
                                ],
                                md=3,
                            ),
                            dbc.Col(
                                [
                                    html.Label("Tab"),
                                    dcc.Dropdown(
                                        id="dp-tab",
                                        options=opts("Tab"),
                                        multi=True,
                                        placeholder="All Tabs",
                                    ),
                                ],
                                md=3,
                            ),
                            dbc.Col(
                                [
                                    html.Label("Month"),
                                    dcc.Dropdown(
                                        id="dp-month",
                                        options=month_opts,
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
                                        id="dp-week",
                                        options=week_opts,
                                        multi=True,
                                        placeholder="All Weeks",
                                    ),
                                ],
                                md=3,
                            ),
                        ],
                        style={"marginBottom": "8px"},
                    ),
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Label("Daypart"),
                                    dcc.Dropdown(
                                        id="dp-daypart",
                                        options=daypart_opts,
                                        multi=True,
                                        placeholder="All Dayparts",
                                    ),
                                ],
                                md=4,
                            ),
                            dbc.Col(
                                [
                                    html.Label("Date Range"),
                                    dcc.DatePickerRange(
                                        id="dp-date-range",
                                        start_date=start_date,
                                        end_date=end_date,
                                        display_format="DD-MM-YYYY",
                                    ),
                                ],
                                md=4,
                            ),
                            dbc.Col(
                                [
                                    html.Label(" "),
                                    html.Button(
                                        "Reset Filters",
                                        id="dp-reset",
                                        n_clicks=0,
                                        className="btn btn-danger",
                                        style={"marginTop": "22px", "width": "100%"},
                                    ),
                                ],
                                md=4,
                            ),
                        ]
                    ),
                ],
                style={
                    "backgroundColor": "white",
                    "padding": "14px",
                    "borderRadius": "10px",
                    "marginBottom": "10px",
                    "boxShadow": "0 4px 10px rgba(0,0,0,0.08)",
                    "position": "sticky",
                    "top": 60,
                    "zIndex": 9,
                },
            ),

            # KPI CARDS
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.Div("Total Net Sale"),
                                    html.H4(id="dp-kpi-total-net"),
                                ]
                            ),
                            style={
                                "backgroundColor": "#2563eb",
                                "color": "white",
                                "borderRadius": "10px",
                            },
                        ),
                        md=4,
                    ),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.Div("Total Bills"),
                                    html.H4(id="dp-kpi-bills"),
                                ]
                            ),
                            style={
                                "backgroundColor": "#059669",
                                "color": "white",
                                "borderRadius": "10px",
                            },
                        ),
                        md=4,
                    ),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.Div("ABV (Average Bill Value)"),
                                    html.H4(id="dp-kpi-abv"),
                                ]
                            ),
                            style={
                                "backgroundColor": "#d97706",
                                "color": "white",
                                "borderRadius": "10px",
                            },
                        ),
                        md=4,
                    ),
                ],
                style={"rowGap": "10px", "marginBottom": "10px"},
            ),

            # Tables
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H5("Outlet Summary"),
                            dash_table.DataTable(
                                id="dp-outlet-summary",
                                columns=[],
                                data=[],
                                page_size=10,
                                style_table={"height": "260px", "overflowY": "auto"},
                                style_cell={
                                    "fontFamily": "Times New Roman",
                                    "fontSize": "12px",
                                    "textAlign": "left",
                                    "padding": "4px",
                                },
                                style_header={
                                    "backgroundColor": "#e5e7eb",
                                    "fontWeight": "bold",
                                },
                            ),
                        ],
                        md=6,
                    ),
                    dbc.Col(
                        [
                            html.H5("Daypart Summary"),
                            dash_table.DataTable(
                                id="dp-daypart-summary",
                                columns=[],
                                data=[],
                                page_size=10,
                                style_table={"height": "260px", "overflowY": "auto"},
                                style_cell={
                                    "fontFamily": "Times New Roman",
                                    "fontSize": "12px",
                                    "textAlign": "left",
                                    "padding": "4px",
                                },
                                style_header={
                                    "backgroundColor": "#e5e7eb",
                                    "fontWeight": "bold",
                                },
                            ),
                        ],
                        md=6,
                    ),
                ],
                style={"marginBottom": "10px"},
            ),

            # Charts
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H6("Net Sale Trend (Total Net Sale)"),
                            dcc.Graph(id="dp-net-trend", style={"height": TREND_H}),
                        ],
                        md=6,
                    ),
                    dbc.Col(
                        [
                            html.H6("Hourly Net Sale (Total Net Sale)"),
                            dcc.Graph(id="dp-hourly", style={"height": BAR_H}),
                        ],
                        md=6,
                    ),
                ],
                style={"marginBottom": "10px"},
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H6("Daypart Contribution (Total Net Sale)"),
                            dcc.Graph(id="dp-daypart-pie", style={"height": PIE_H}),
                        ],
                        md=6,
                    ),
                    dbc.Col(
                        [
                            html.H6("Net Sale by Daypart (Total Net Sale)"),
                            dcc.Graph(id="dp-daypart-bar", style={"height": BAR_H}),
                        ],
                        md=6,
                    ),
                ],
                style={"marginBottom": "10px"},
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H6("Heatmap: Day vs Hour (Total Net Sale)"),
                            dcc.Graph(id="dp-heat-day-hour", style={"height": HEAT_H}),
                        ],
                        md=6,
                    ),
                    dbc.Col(
                        [
                            html.H6("Heatmap: Day vs Daypart (Total Net Sale)"),
                            dcc.Graph(
                                id="dp-heat-day-daypart",
                                style={"height": HEAT_H},
                            ),
                        ],
                        md=6,
                    ),
                ],
                style={"marginBottom": "10px"},
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H6("Week Code Report (WD / WF / WE)"),
                            dcc.Graph(id="dp-weekcode", style={"height": WW_H}),
                        ],
                        md=12,
                    )
                ]
            ),
        ]
    )


# -------------------------------------------------------
# CALLBACKS
# -------------------------------------------------------
def register_callbacks(app):

    # -------------------------------------------------------
    # DEPENDENT FILTER CALLBACK
    # (Region/State/City/Type/Outlet/Tab/Month/Week)
    # -------------------------------------------------------
    @app.callback(
        [
            Output("dp-region", "options"),
            Output("dp-state", "options"),
            Output("dp-city", "options"),
            Output("dp-type", "options"),
            Output("dp-outlet", "options"),
            Output("dp-tab", "options"),
            Output("dp-month", "options"),
            Output("dp-week", "options"),
        ],
        [
            Input("dp-region", "value"),
            Input("dp-state", "value"),
            Input("dp-city", "value"),
            Input("dp-type", "value"),
            Input("dp-outlet", "value"),
            Input("dp-month", "value"),
        ],
    )
    def update_dependent_filters(
        region_vals, state_vals, city_vals, type_vals, outlet_vals, month_val
    ):
        df = load_hourly_data()
        if df.empty:
            return [], [], [], [], [], [], [], []

        def apply(col, vals):
            nonlocal df
            if vals:
                if not isinstance(vals, (list, tuple, set)):
                    vals = [vals]
                df = df[df[col].isin(vals)]

        apply("Region", region_vals)
        apply("State", state_vals)
        apply("City", city_vals)
        apply("Type", type_vals)
        apply("Outlet Name", outlet_vals)
        if month_val:
            df = df[df["MonthLabel"] == month_val]

        def opt(col):
            if col not in df.columns:
                return []
            vals = sorted(df[col].dropna().unique())
            return [{"label": x, "value": x} for x in vals]

        region_opts = opt("Region")
        state_opts = opt("State")
        city_opts = opt("City")
        type_opts = opt("Type")
        outlet_opts = opt("Outlet Name")
        tab_opts = opt("Tab")
        month_opts = opt("MonthLabel")

        week_vals = sorted(df["Week"].dropna().unique()) if "Week" in df.columns else []
        week_opts = [{"label": f"Week {int(w)}", "value": int(w)} for w in week_vals]

        return (
            region_opts,
            state_opts,
            city_opts,
            type_opts,
            outlet_opts,
            tab_opts,
            month_opts,
            week_opts,
        )

    # -------------------------------------------------------
    # MAIN DASHBOARD UPDATE CALLBACK
    # -------------------------------------------------------
    @app.callback(
        [
            Output("dp-kpi-total-net", "children"),
            Output("dp-kpi-bills", "children"),
            Output("dp-kpi-abv", "children"),
            Output("dp-week-summary", "children"),
            Output("dp-last-refresh", "children"),
            Output("dp-outlet-summary", "columns"),
            Output("dp-outlet-summary", "data"),
            Output("dp-daypart-summary", "columns"),
            Output("dp-daypart-summary", "data"),
            Output("dp-net-trend", "figure"),
            Output("dp-hourly", "figure"),
            Output("dp-daypart-pie", "figure"),
            Output("dp-daypart-bar", "figure"),
            Output("dp-heat-day-hour", "figure"),
            Output("dp-heat-day-daypart", "figure"),
            Output("dp-weekcode", "figure"),
        ],
        [
            Input("dp-interval", "n_intervals"),
            Input("dp-region", "value"),
            Input("dp-state", "value"),
            Input("dp-city", "value"),
            Input("dp-type", "value"),
            Input("dp-outlet", "value"),
            Input("dp-tab", "value"),
            Input("dp-month", "value"),
            Input("dp-week", "value"),
            Input("dp-daypart", "value"),
            Input("dp-date-range", "start_date"),
            Input("dp-date-range", "end_date"),
        ],
    )
    def update_dashboard(
        n_intervals,
        region_vals, state_vals, city_vals, type_vals,
        outlet_vals, tab_vals, month_val, week_vals, dp_vals,
        start_date, end_date,
    ):
        df = load_hourly_data()
        if df.empty:
            empty = empty_fig()
            now_text = f"Last refreshed: {datetime.now():%d-%b-%Y %H:%M:%S}"
            return (
                "0.00", "0", "0.00",
                "",
                now_text,
                [], [], [], [],
                empty, empty, empty, empty, empty, empty, empty,
            )

        # Dimension filters
        def apply_multi(col, vals):
            nonlocal df
            if vals:
                if not isinstance(vals, (list, tuple, set)):
                    vals = [vals]
                df = df[df[col].isin(vals)]

        apply_multi("Region", region_vals)
        apply_multi("State", state_vals)
        apply_multi("City", city_vals)
        apply_multi("Type", type_vals)
        apply_multi("Outlet Name", outlet_vals)
        apply_multi("Tab", tab_vals)
        apply_multi("Daypart", dp_vals)

        # -----------------------------
        # DATE LOGIC – Option B
        # Running month = 1st of current month → today
        # -----------------------------
        today = date.today()
        running_start = today.replace(day=1)
        running_end = today

        # If user selected date range -> use that
        if start_date or end_date:
            sd = pd.to_datetime(start_date).date() if start_date else running_start
            ed = pd.to_datetime(end_date).date() if end_date else running_end
            df = df[(df["Date"].dt.date >= sd) & (df["Date"].dt.date <= ed)]
        else:
            # Apply month/week filters first
            if month_val:
                df = df[df["MonthLabel"] == month_val]
            if week_vals:
                if not isinstance(week_vals, (list, tuple, set)):
                    week_vals = [week_vals]
                df = df[df["Week"].isin(week_vals)]
            # If no explicit month/week, default to running month (system date)
            if not month_val and not week_vals:
                df = df[
                    (df["Date"].dt.date >= running_start)
                    & (df["Date"].dt.date <= running_end)
                ]

        if df.empty:
            empty = empty_fig("No data for selected filters")
            now_text = f"Last refreshed: {datetime.now():%d-%b-%Y %H:%M:%S}"
            return (
                "0.00", "0", "0.00",
                "",
                now_text,
                [], [], [], [],
                empty, empty, empty, empty, empty, empty, empty,
            )

        # KPIs
        total_net = df["Total Net Sale"].sum()
        total_bills = df["NOB"].sum()
        abv = total_net / total_bills if total_bills > 0 else 0.0

        kpi_total = f"{total_net:,.2f}"
        kpi_bills = f"{int(total_bills):,}"
        kpi_abv = f"{abv:,.2f}"

        week_summary = build_week_range_text(df)
        last_refresh = f"Last refreshed: {datetime.now():%d-%b-%Y %H:%M:%S}"

        # Outlet Summary
        outlet = df.groupby("Outlet Name", as_index=False, observed=False).agg(
            Total_Sale=("Sale", "sum"),
            Total_Discount=("Discount", "sum"),
            Total_Charges=("Total Charges", "sum"),
            Total_Net_Sale=("Total Net Sale", "sum"),
            Total_NOB=("NOB", "sum"),
        )
        outlet_cols = [
            {"name": c.replace("_", " "), "id": c} for c in outlet.columns
        ]
        outlet_data = outlet.round(2).to_dict("records")

        # Daypart Summary
        dpg = df.groupby("Daypart", as_index=False, observed=False).agg(
            Sale=("Sale", "sum"),
            Discount=("Discount", "sum"),
            Net_Sale=("Net Sale", "sum"),
            Total_Net_Sale=("Total Net Sale", "sum"),
            Bills=("NOB", "sum"),
        )
        dpg["Contribution%"] = (
            dpg["Total_Net_Sale"] / dpg["Total_Net_Sale"].sum() * 100
        ).round(2)
        dpg["Daypart"] = pd.Categorical(
            dpg["Daypart"], categories=DAYPART_ORDER, ordered=True
        )
        dpg = dpg.sort_values("Daypart")
        dp_cols = [
            {"name": c.replace("_", " "), "id": c} for c in dpg.columns
        ]
        dp_data = dpg.round(2).to_dict("records")

        # Trend (Date)
        tr = df.groupby("Date", as_index=False)["Total Net Sale"].sum()
        tr = tr.sort_values("Date")
        fig_trend = px.line(tr, x="Date", y="Total Net Sale", markers=True)
        fig_trend.update_layout(height=TREND_H, margin=dict(l=40, r=10, t=30, b=40))

        # Hourly
        hr = df.groupby("Hour", as_index=False)["Total Net Sale"].sum()
        hr = hr.sort_values("Hour")
        fig_hour = px.bar(hr, x="Hour", y="Total Net Sale")
        fig_hour.update_layout(height=BAR_H, margin=dict(l=40, r=10, t=30, b=40))

        # Daypart pie & bar
        dp = df.groupby("Daypart", as_index=False, observed=False)["Total Net Sale"].sum()
        dp["Daypart"] = pd.Categorical(
            dp["Daypart"],
            categories=DAYPART_ORDER,
            ordered=True,
        )
        dp = dp.sort_values("Daypart")

        fig_pie = px.pie(dp, names="Daypart", values="Total Net Sale", hole=0.45)
        fig_pie.update_layout(height=PIE_H, margin=dict(l=10, r=10, t=30, b=10))

        fig_dp_bar = px.bar(dp, x="Daypart", y="Total Net Sale")
        fig_dp_bar.update_layout(height=BAR_H, margin=dict(l=40, r=10, t=30, b=40))

        # Heatmap: Day vs Hour
        dh = df.groupby(
            ["Day", "Hour"],
            as_index=False,
            observed=False,
        )["Total Net Sale"].sum()
        dh["Day"] = pd.Categorical(dh["Day"], categories=DOW_ORDER, ordered=True)
        dh = dh.sort_values(["Day", "Hour"])
        fig_heat_dh = px.density_heatmap(dh, x="Hour", y="Day", z="Total Net Sale")
        fig_heat_dh.update_layout(
            height=HEAT_H,
            margin=dict(l=60, r=10, t=30, b=40),
        )

        # Heatmap: Day vs Daypart
        dd = df.groupby(
            ["Day", "Daypart"],
            as_index=False,
            observed=False,
        )["Total Net Sale"].sum()
        dd["Day"] = pd.Categorical(dd["Day"], categories=DOW_ORDER, ordered=True)
        dd["Daypart"] = pd.Categorical(
            dd["Daypart"],
            categories=DAYPART_ORDER,
            ordered=True,
        )
        dd = dd.sort_values(["Day", "Daypart"])
        fig_heat_dd = px.density_heatmap(dd, x="Daypart", y="Day", z="Total Net Sale")
        fig_heat_dd.update_layout(
            height=HEAT_H,
            margin=dict(l=60, r=10, t=30, b=40),
        )

        # WeekCode chart
        wc = df.groupby(
            "WeekCode",
            as_index=False,
            observed=False,
        )["Total Net Sale"].sum()
        fig_wc = px.bar(wc, x="WeekCode", y="Total Net Sale")
        fig_wc.update_layout(height=WW_H, margin=dict(l=40, r=10, t=30, b=40))

        return (
            kpi_total,
            kpi_bills,
            kpi_abv,
            week_summary,
            last_refresh,
            outlet_cols,
            outlet_data,
            dp_cols,
            dp_data,
            fig_trend,
            fig_hour,
            fig_pie,
            fig_dp_bar,
            fig_heat_dh,
            fig_heat_dd,
            fig_wc,
        )

    # -------------------------------------------------------
    # RESET FILTERS CALLBACK
    # -------------------------------------------------------
    @app.callback(
        [
            Output("dp-region", "value"),
            Output("dp-state", "value"),
            Output("dp-city", "value"),
            Output("dp-type", "value"),
            Output("dp-outlet", "value"),
            Output("dp-tab", "value"),
            Output("dp-month", "value"),
            Output("dp-week", "value"),
            Output("dp-daypart", "value"),
            Output("dp-date-range", "start_date"),
            Output("dp-date-range", "end_date"),
        ],
        Input("dp-reset", "n_clicks"),
        prevent_initial_call=True,
    )
    def reset_filters(n_clicks):
        if not n_clicks:
            raise PreventUpdate
        # All filters cleared, date range None -> callback will apply running month
        return (
            None,  # Region
            None,  # State
            None,  # City
            None,  # Type
            None,  # Outlet
            None,  # Tab
            None,  # Month
            None,  # Week
            None,  # Daypart
            None,  # start_date
            None,  # end_date
        )
