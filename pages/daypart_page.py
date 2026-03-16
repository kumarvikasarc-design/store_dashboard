# pages/daypart_page.py
# Coffee Island – Daypart Report Dashboard

import os
import glob
from datetime import datetime, date
import urllib
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from dash import html, dcc, dash_table, Input, Output
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import warnings
from sqlalchemy.exc import SAWarning

warnings.filterwarnings("ignore", category=SAWarning)


# -------------------------------------------------------
# CONFIG
# -------------------------------------------------------
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=coffee_island_analytics;"
    "Trusted_Connection=yes;"
)

ENGINE = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
AUTO_REFRESH_MS = 10 * 60 * 1000  # 10 minutes

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

def ensure_week_columns(df):
    if df is None or df.empty:
        return df

    if "Date" not in df.columns:
        return df

    if "Week" not in df.columns:
        df["Week"] = df["Date"].dt.isocalendar().week.astype("Int64")

    if "WeekYear" not in df.columns:
        df["WeekYear"] = (
            df["Date"].dt.isocalendar().week.astype(str).str.zfill(2)
            + "-"
            + df["Date"].dt.isocalendar().year.astype(str).str[-2:]
        )

    return df

def get_month_options(df: pd.DataFrame):
    if df is None or df.empty or "Date" not in df.columns:
        return []

    month_df = (
        df[["MonthLabel", "Date"]]
        .dropna()
        .assign(MonthDate=lambda d: d["Date"].dt.to_period("M").dt.to_timestamp())
        .drop_duplicates("MonthLabel")
        .sort_values("MonthDate", ascending=False)
    )

    return [
        {"label": row["MonthLabel"], "value": row["MonthLabel"]}
        for _, row in month_df.iterrows()
    ]

def parse_dates_auto(series: pd.Series) -> pd.Series:
    """Auto-detect DD-MM vs MM-DD."""
    s = series.astype(str).str.strip()
    d1 = pd.to_datetime(s, dayfirst=True, errors="coerce")
    d2 = pd.to_datetime(s, dayfirst=False, errors="coerce")
    return d1 if d1.notna().sum() >= d2.notna().sum() else d2


def build_week_range_text(df: pd.DataFrame) -> str:
    """Week-Year(s) + date range for header (safe & cross-year)."""

    if df is None or df.empty or "Date" not in df.columns:
        return ""

    # Ensure Date type
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    # Ensure WeekYear exists
    if "WeekYear" not in df.columns:
        iso = df["Date"].dt.isocalendar()
        df["WeekYear"] = (
            iso.week.astype(str).str.zfill(2)
            + "-"
            + iso.year.astype(str).str[-2:]
        )

    # Build week list (date-aware, ordered)
    week_df = (
        df[["WeekYear", "Date"]]
        .drop_duplicates("WeekYear")
        .sort_values("Date")
    )

    weeks = week_df["WeekYear"].tolist()

    min_d = df["Date"].min().date()
    max_d = df["Date"].max().date()

    if len(weeks) == 1:
        wk = f"Week {weeks[0]}"
    else:
        wk = "Weeks " + ", ".join(weeks)

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
from functools import lru_cache

@lru_cache(maxsize=1)
def load_hourly_data() -> pd.DataFrame:
    query = """
    SELECT
        d.Sale_Date            AS [Date],
        d.Sale_Hour            AS [Hour],
        d.Tab                  AS [Tab],

        s.Outlet_Name           AS [Outlet Name],
        s.Brand                 AS [Brand],
        s.Region                AS [Region],
        s.State                 AS [State],
        s.City                  AS [City],
        s.Store_type            AS [Type],

        d.Sale                  AS [Sale],
        d.Discount              AS [Discount],
        d.Net_Sale              AS [Net Sale],
        d.Total_Charges         AS [Total Charges],
        d.Gross_Sale            AS [Gross Sale],
        d.No_Of_Bills           AS [NOB]

    FROM dbo.daypart_raw d
    INNER JOIN dbo.stores_master s
        ON d.Outlet_Name = s.Outlet_Name
    WHERE d.Sale_Date >= DATEADD(MONTH,-6,GETDATE())
    """

    df = pd.read_sql(query, ENGINE)

    if df.empty:
        return df

    # ---------------------------
    # SAME LOGIC AS BEFORE
    # ---------------------------
    df["Date"] = pd.to_datetime(df["Date"])
    df["Hour"] = df["Hour"].astype(int)

    df["Total Net Sale"] = df["Net Sale"] + df["Total Charges"]

    df["Day"] = df["Date"].dt.day_name()
    df["Week"] = df["Date"].dt.isocalendar().week.astype("Int64")
    df["WeekYear"] = (
        df["Date"].dt.isocalendar().week.astype(str).str.zfill(2)
        + "-"
        + df["Date"].dt.isocalendar().year.astype(str).str[-2:]
    )

    df["Month"] = df["Date"].dt.to_period("M").astype(str)
    df["MonthLabel"] = df["Date"].dt.strftime("%b-%Y")

    # Financial Year (India)
    df["FY"] = np.where(
        df["Date"].dt.month >= 4,
        "FY " + df["Date"].dt.year.astype(str) + "-" + (df["Date"].dt.year + 1).astype(str).str[-2:],
        "FY " + (df["Date"].dt.year - 1).astype(str) + "-" + df["Date"].dt.year.astype(str).str[-2:]
    )

    dow = df["Date"].dt.dayofweek
    df["WeekCode"] = np.select(
        [dow <= 3, dow == 4, dow >= 5],
        ["WD", "WF", "WE"],
        default="WD",
    )

    df["Daypart"] = df["Hour"].apply(get_daypart)
    df["Daypart"] = pd.Categorical(df["Daypart"], DAYPART_ORDER, ordered=True)

    return df.reset_index(drop=True)
HOURLY_DF = load_hourly_data()
# -------------------------------------------------------
# LAYOUT
# -------------------------------------------------------
def get_layout():
    df_init = HOURLY_DF
    if df_init is None:
        df_init = pd.DataFrame()

    # Date picker defaults: EMPTY (but logic will use running month)
    start_date = None
    end_date = None

    def opts(col):
        if df_init is None or df_init.empty or col not in df_init.columns:
            return []
        return [
            {"label": v, "value": v}
            for v in sorted(df_init[col].dropna().unique())
        ]


    month_opts = get_month_options(df_init)

    #] if not df_init.empty else []

    if not df_init.empty and {"Week", "WeekYear", "Date"}.issubset(df_init.columns):
        week_df = (
            df_init[["Week", "WeekYear", "Date"]]
            .drop_duplicates("WeekYear")
            .sort_values("Date", ascending=False)
        )
        week_opts = [
            {"label": f"Week {row['WeekYear']}", "value": row["WeekYear"]}
            for _, row in week_df.iterrows()
        ]
    else:
        week_opts = []

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
                # ---------------- ROW 1 ----------------
                dbc.Row(
                    [
                        dbc.Col(
                            [html.Label("Brand"),
                            dcc.Dropdown(id="dp-brand", options=opts("Brand"),
                                        multi=False, placeholder="All Brands")],
                            md=2,
                        ),
                        dbc.Col(
                            [html.Label("State"),
                            dcc.Dropdown(id="dp-state", options=opts("State"),
                                        multi=False, placeholder="All States")],
                            md=2,
                        ),
                        dbc.Col(
                            [html.Label("Region"),
                            dcc.Dropdown(id="dp-region", options=opts("Region"),
                                        multi=False, placeholder="All Regions")],
                            md=2,
                        ),
                        dbc.Col(
                            [html.Label("City"),
                            dcc.Dropdown(id="dp-city", options=opts("City"),
                                        multi=False, placeholder="All Cities")],
                            md=2,
                        ),
                        dbc.Col(
                            [html.Label("Type"),
                            dcc.Dropdown(id="dp-type", options=opts("Type"),
                                        multi=False, placeholder="All Types")],
                            md=2,
                        ),
                        dbc.Col(
                            [html.Label("Outlet"),
                            dcc.Dropdown(id="dp-outlet", options=opts("Outlet Name"),
                                        multi=False, placeholder="All Outlets")],
                            md=2,
                        ),
                    ],
                    className="g-2",
                ),

                # ---------------- ROW 2 ----------------
                dbc.Row(
                    [
                        dbc.Col(
                            [html.Label("Tab"),
                            dcc.Dropdown(id="dp-tab", options=opts("Tab"),
                                        multi=False, placeholder="All Tabs")],
                            md=2,
                        ),
                        dbc.Col(
                            [
                                html.Label("Financial Year"),
                                dcc.Dropdown(
                                    id="dp-fy",
                                    options=[
                                        {"label": fy, "value": fy}
                                        for fy in sorted(df_init["FY"].dropna().unique(), reverse=True)
                                    ] if not df_init.empty else [],
                                    multi=False,
                                    placeholder="All FY",
                                ),
                            ],
                            md=2,
                        ),
                        dbc.Col(
                            [html.Label("Month"),
                            dcc.Dropdown(id="dp-month", options=month_opts,
                                        multi=False, placeholder="All Months")],
                            md=2,
                        ),
                        dbc.Col(
                            [html.Label("Week"),
                            dcc.Dropdown(id="dp-week", options=week_opts,
                                        multi=False, placeholder="All Weeks")],
                            md=2,
                        ),
                        dbc.Col(
                            [html.Label("Daypart"),
                            dcc.Dropdown(id="dp-daypart", options=daypart_opts,
                                        multi=False, placeholder="All Dayparts")],
                            md=3,
                        ),
                    ],
                    className="g-2 mt-1",
                ),

                # ---------------- ROW 3 ----------------
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.Label("Date Range"),
                                html.Div(
                                dcc.DatePickerRange(
                                    id="dp-date-range",
                                    start_date=start_date,
                                    end_date=end_date,
                                    display_format="DD-MM-YYYY",
                                ),
                                style={"width": "100%",
                                       "whiteSpace": "nowrap",
                                       },
                                ),
                            ],
                            md=4,
                        ),
                        dbc.Col(
                            [
                                html.Label(" ", style={"visibility": "hidden"}),
                                dbc.Button(
                                    "Reset Filters",
                                    id="dp-reset",
                                    n_clicks=0,
                                    color="danger",
                                    className="w-100",
                                    style={"height": "38px", "width": "100%"},
                                ),
                            ],
                            md=2,
                        ),
                    ],
                    className="g-2 align-items-end mt-1",
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
            Output("dp-brand", "options"),
            Output("dp-state", "options"),
            Output("dp-region", "options"),
            Output("dp-city", "options"),
            Output("dp-type", "options"),
            Output("dp-outlet", "options"),
            Output("dp-tab", "options"),
            Output("dp-fy", "options"),
            Output("dp-month", "options"),
            Output("dp-week", "options"),
        ],
        [
            Input("dp-brand", "value"),
            Input("dp-state", "value"),
            Input("dp-region", "value"),
            Input("dp-city", "value"),
            Input("dp-type", "value"),
            Input("dp-fy", "value"),
            Input("dp-outlet", "value"),
            Input("dp-month", "value"),
        ],
    )
    def update_dependent_filters(
        brand_vals, state_vals, region_vals, city_vals,
        type_vals, fy_vals, outlet_vals, month_val
    ):
        df = HOURLY_DF.copy()
        if df.empty:
            return [], [], [], [], [], [], [], [], [], [], []

        def apply(col, vals):
            nonlocal df
            if vals:
                if not isinstance(vals, (list, tuple, set)):
                    vals = [vals]
                df = df[df[col].isin(vals)]

        # Apply cascading filters
        apply("Brand", brand_vals)
        apply("State", state_vals)
        apply("Region", region_vals)
        apply("City", city_vals)
        apply("Type", type_vals)
        apply("Outlet Name", outlet_vals)

        if fy_vals:
            df = df[df["FY"] == fy_vals]

        if month_val:
            df = df[df["MonthLabel"] == month_val]

        def opt(col):
            if col not in df.columns:
                return []
            return [{"label": v, "value": v} for v in sorted(df[col].dropna().unique())]

        # Financial Year options (latest first)
        fy_opts = (
            [{"label": fy, "value": fy}
            for fy in sorted(df["FY"].dropna().unique(), reverse=True)]
            if "FY" in df.columns else []
        )

        # Week options (WeekYear sorted latest first)
        df = ensure_week_columns(df)

        week_df = (
            df[["Week", "WeekYear", "Date"]]
            .drop_duplicates("WeekYear")
            .sort_values("Date", ascending=False)
        )

        week_opts = [
            {"label": f"Week {row['WeekYear']}", "value": row["WeekYear"]}
            for _, row in week_df.iterrows()
        ]

        return (
            opt("Brand"),
            opt("State"),
            opt("Region"),
            opt("City"),
            opt("Type"),
            opt("Outlet Name"),
            opt("Tab"),
            fy_opts,
            get_month_options(df),
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
            Input("dp-brand", "value"),
            Input("dp-state", "value"),
            Input("dp-region", "value"),
            Input("dp-city", "value"),
            Input("dp-type", "value"),
            Input("dp-outlet", "value"),
            Input("dp-tab", "value"),
            Input("dp-fy", "value"),
            Input("dp-month", "value"),
            Input("dp-week", "value"),
            Input("dp-daypart", "value"),
            Input("dp-date-range", "start_date"),
            Input("dp-date-range", "end_date"),
        ],
    )
    def update_dashboard(
        n_intervals,
        brand_vals, state_vals, region_vals, city_vals, type_vals,
        outlet_vals, tab_vals, fy_val, month_val, week_vals, dp_vals,
        start_date, end_date,
    ):
        df = HOURLY_DF.copy()
        if df.empty:
            empty = empty_fig()
            now_text = f"Last refreshed: {datetime.now():%d-%b-%Y %H:%M:%S}"
            return (
                "0.00", "0", "0.00",
                "",
                now_text,
                [], [], [], [],
                empty, empty, empty, empty, empty, empty, empty, empty
            )
        df = ensure_week_columns(df)

        # Dimension filters
        def apply_multi(col, vals):
            nonlocal df
            if vals:
                if not isinstance(vals, (list, tuple, set)):
                    vals = [vals]
                df = df[df[col].isin(vals)]
        apply_multi("Brand", brand_vals)
        apply_multi("State", state_vals)
        apply_multi("Region", region_vals)
        apply_multi("City", city_vals)
        apply_multi("Type", type_vals)
        apply_multi("Outlet Name", outlet_vals)
        apply_multi("Tab", tab_vals)
        apply_multi("Daypart", dp_vals)
        
        # ✅ FY filter
        if fy_val:
            df = df[df["FY"] == fy_val]
            month_val = None
            week_vals = None
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
                df = df[df["WeekYear"].isin(week_vals)]
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
        pivot = dh.pivot(index="Day", columns="Hour", values="Total Net Sale")

        fig_heat_dh = px.imshow(
            pivot,
            aspect="auto",
            labels=dict(x="Hour", y="Day", color="Total Net Sale"),
        )
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
        pivot_dd = dd.pivot(index="Day", columns="Daypart", values="Total Net Sale")

        fig_heat_dd = px.imshow(
            pivot_dd,
            aspect="auto",
            labels=dict(x="Daypart", y="Day", color="Total Net Sale"),
        )
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
            Output("dp-brand", "value"),
            Output("dp-state", "value"),
            Output("dp-region", "value"),
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
            None,  # Brand
            None,  # State
            None,  # Region
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
