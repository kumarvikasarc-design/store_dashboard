# =========================================================
# MENU MIX DASHBOARD — CLEAN, STABLE, AUTO-DETECT VERSION
# =========================================================

import os, glob, re
import pandas as pd
import dash
from dash import dcc, html, Input, dash_table, State, Output, no_update
import dash_bootstrap_components as dbc
import plotly.express as px
from dash import callback_context

# =========================================================
# PATHS
# =========================================================
BASE_DIR = r"C:\Users\ACER\store_dashboard"
ITEM_DIR = os.path.join(BASE_DIR, "item_source")
STORE_FILE = os.path.join(BASE_DIR, "stores_db.csv")

# =========================================================
# LOAD SALES DATA (MULTI CSV)
# =========================================================
files = glob.glob(os.path.join(ITEM_DIR, "*.csv"))
if not files:
    raise FileNotFoundError("❌ No Item Source CSV found")

dfs = []
for f in files:
    df = pd.read_csv(f)
    df["__file"] = os.path.basename(f)
    dfs.append(df)

sales_df = pd.concat(dfs, ignore_index=True)

# =========================================================
# AUTO COLUMN DETECTION
# =========================================================
def find_col(possible):
    for p in possible:
        for c in sales_df.columns:
            if p.lower() == c.lower():
                return c
    return None

COL = {
    "date": find_col(["date", "business date"]),
    "source": find_col(["source"]),
    "tab": find_col(["tab"]),
    "section": find_col(["section"]),
    "supercat": find_col(["super category name", "super category"]),
    "cat": find_col(["category name", "category"]),
    "item": find_col(["item name", "item"]),
    "net": find_col(["net amount", "net sales"]),
    "qty": find_col(["item qty", "quantity"]),
    "bills": find_col(["bills"])
}

SKIP_SUPER_CATEGORIES = [
    "Emp Meal",
    "Emp_Bev",
    "ADDON",
]
#Define mapping (ONE TIME – top of file)
FOOD_KEYWORDS = [
    "FOOD", "SNACK", "MEAL", "DESSERT", "BAKERY"
]

BEVERAGE_KEYWORDS = [
    "BEVERAGE", "COFFEE", "TEA", "DRINK"
]

# =========================================================
# DATE HANDLING
# =========================================================
sales_df[COL["date"]] = pd.to_datetime(sales_df[COL["date"]], errors="coerce")

today = pd.Timestamp.today().normalize()
yesterday = today - pd.Timedelta(days=1)
MONTH_START = today.replace(day=1)


sales_df["Date"] = pd.to_datetime(sales_df["Date"], errors="coerce")

sales_df["Year"] = sales_df["Date"].dt.year
sales_df["Month"] = sales_df["Date"].dt.month
sales_df["Month Name"] = sales_df["Date"].dt.strftime("%b")
sales_df["Week"] = sales_df["Date"].dt.isocalendar().week
sales_df["Weekday"] = sales_df["Date"].dt.day_name()

# Indian Financial Year
sales_df["Financial Year"] = pd.NA

mask = sales_df["Date"].notna()

sales_df.loc[mask, "Financial Year"] = sales_df.loc[mask, "Date"].apply(
    lambda d: f"{d.year}-{d.year+1}" if d.month >= 4 else f"{d.year-1}-{d.year}"
)


# Day Type
sales_df["Day Type"] = sales_df["Weekday"].map({
    "Monday": "WD",
    "Tuesday": "WD",
    "Wednesday": "WD",
    "Thursday": "WD",
    "Friday": "WF",
    "Saturday": "WE",
    "Sunday": "WE",
})

# =========================================================
# STORE MASTER
# =========================================================
store_df = pd.read_csv(STORE_FILE)

def norm(s):
    return (
        s.astype(str)
        .str.upper()
        .str.strip()
        .replace("NAN", None)
    )

for c in ["Brand", "Region", "State", "City", "Type", "Outlet Name"]:
    if c in store_df.columns:
        store_df[c] = norm(store_df[c])

def top_n_with_others(df, group_col, value_col, n=10):
    top = (
        df.groupby(group_col)[value_col]
        .sum()
        .sort_values(ascending=False)
        .head(n)
    )

    rest = (
        df.groupby(group_col)[value_col]
        .sum()
        .iloc[n:]
        .sum()
    )

    if rest > 0:
        top["Others"] = rest

    return top.reset_index()

def classify_fb(x):
    x = str(x).upper()
    if any(k in x for k in BEVERAGE_KEYWORDS):
        return "Beverage"
    if any(k in x for k in FOOD_KEYWORDS):
        return "Food"
    return "Other"

if COL["supercat"]:
    sales_df["Food_Beverage"] = sales_df[COL["supercat"]].apply(classify_fb)

def detect_col(df, keywords):
    for c in df.columns:
        c_low = c.lower().replace("_", " ")
        if any(k in c_low for k in keywords):
            return c
    return None


COL = {
    "date": detect_col(sales_df, ["date"]),
    "source": detect_col(sales_df, ["source"]),
    "tab": detect_col(sales_df, ["tab"]),                 # Tab Name
    "section": detect_col(sales_df, ["section"]),
    "supercat": detect_col(sales_df, ["super category"]),
    "cat": detect_col(sales_df, ["category"]),
    "item": detect_col(sales_df, ["item name"]),
    "net": detect_col(sales_df, ["net amount", "net sales"]),
    "qty": detect_col(sales_df, ["item qty", "quantity"]),
    "bills": detect_col(sales_df, ["bills"]),
}
COL["date"] = detect_col(sales_df, ["date"])

print("📅 DATE COLUMN:", COL["date"])

print("✅ AUTO-DETECTED COLUMNS:", COL)

# ==============================
# SAFE OUTLET_KEY CREATION
# ==============================

def make_outlet_key(series):
    return (
        series.astype(str)
        .str.upper()
        .str.strip()
        .str.replace(r"[–—]", "-", regex=True)
        .str.replace(r"\s+", " ", regex=True)
    )

# 🔹 Store master
if "Outlet Name" not in store_df.columns:
    raise ValueError("❌ 'Outlet Name' missing in store master")

store_df["OUTLET_KEY"] = make_outlet_key(store_df["Outlet Name"])

# 🔹 Sales data
outlet_col = find_col(["deployment name", "outlet", "store", "location"])
if not outlet_col:
    raise ValueError("❌ No outlet column found in sales data")

sales_df["OUTLET_KEY"] = make_outlet_key(sales_df[outlet_col])

def load_sales_data():
    files = glob.glob(os.path.join(ITEM_DIR, "*.csv"))
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        df = pd.read_csv(f)
        df["__file"] = os.path.basename(f)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)

# =========================================================
# FILTER ENGINE
# =========================================================
def apply_filters(
    df,
    start_date=None,
    end_date=None,
    brand=None,
    region=None,
    state=None,
    city=None,
    type_=None,
    outlet=None,
    source=None,
    tab=None,
    section=None,
    supercat=None,
    cat=None,
    item=None,
    fy=None,
    month=None,
    week=None,
    day=None,
):

    d = df.copy()

    # ======================
    # DATE FILTER
    # ======================
    if COL["date"]:
        if start_date:
            d = d[d[COL["date"]] >= pd.to_datetime(start_date)]
        if end_date:
            d = d[d[COL["date"]] <= pd.to_datetime(end_date)]

    # ======================
    # STORE MASTER FILTERS
    # ======================
    sdf = store_df.copy()

    if brand:
        sdf = sdf[sdf["Brand"].isin(brand if isinstance(brand, list) else [brand])]
    if region:
        sdf = sdf[sdf["Region"].isin(region if isinstance(region, list) else [region])]
    if state:
        sdf = sdf[sdf["State"].isin(state if isinstance(state, list) else [state])]
    if city:
        sdf = sdf[sdf["City"].isin(city if isinstance(city, list) else [city])]
    if type_:
        sdf = sdf[sdf["Type"].isin(type_ if isinstance(type_, list) else [type_])]
    if outlet:
        sdf = sdf[sdf["OUTLET_KEY"].isin(outlet if isinstance(outlet, list) else [outlet])]

    # ⛔ NO OUTLETS → NO DATA
    if "OUTLET_KEY" in sdf.columns:
        valid_outlets = sdf["OUTLET_KEY"].dropna().unique()
        d = d[d["OUTLET_KEY"].isin(valid_outlets)]

    # ======================
    # SALES-LEVEL FILTERS
    # ======================
    if source and COL["source"]:
        d = d[d[COL["source"]].isin(source if isinstance(source, list) else [source])]

    if tab and COL["tab"]:
        d = d[d[COL["tab"]].isin(tab if isinstance(tab, list) else [tab])]

    if section and COL["section"]:
        d = d[d[COL["section"]].isin(section if isinstance(section, list) else [section])]

    if supercat and COL["supercat"]:
        d = d[d[COL["supercat"]].isin(supercat if isinstance(supercat, list) else [supercat])]

    if cat and COL["cat"]:
        d = d[d[COL["cat"]].isin(cat if isinstance(cat, list) else [cat])]

    if item and COL["item"]:
        d = d[d[COL["item"]].isin(item if isinstance(item, list) else [item])]

    # ======================
    # EXCLUDE SUPER CATEGORIES (FINAL + SAFE)
    # ======================
    if COL["supercat"] and SKIP_SUPER_CATEGORIES:
        d = d[
            ~d[COL["supercat"]].isin(SKIP_SUPER_CATEGORIES)
        ]

    # ======================
    # TIME DIMENSIONS
    # ======================
    if fy:
        d = d[d["Financial Year"].isin(fy if isinstance(fy, list) else [fy])]

    if month:
        year, m = map(int, month.split("-"))
        d = d[
            (d["Date"].dt.year == year) &
            (d["Date"].dt.month == m)
        ]

    if week:
        y, w = week.split("-W")
        d = d[
            (d["Date"].dt.isocalendar().year == int(y)) &
            (d["Date"].dt.isocalendar().week == int(w))
        ]


    if day:
        if isinstance(day, list):
            mask = False
            for v in day:
                mask |= (
                    (d["Day Type"] == v) if v in ["WD", "WF", "WE"]
                    else (d["Weekday"] == v)
                )
            d = d[mask]
        else:
            d = d[d["Day Type"] == day] if day in ["WD", "WF", "WE"] else d[d["Weekday"] == day]

    return d




# =========================================================
# LAYOUT
# =========================================================
def dropdown(id, ph, multi=False):
    return dcc.Dropdown(
        id=id,
        placeholder=ph,
        multi=multi,
        searchable=True,
        clearable=True,
        className="fixed-filter",
    )

def get_layout():
    return html.Div([
        dcc.Store(id="selected-outlet"),
        dcc.Store(id="clicked-supercat"),

        html.H3("Menu Mix Dashboard"),

        dbc.Row([
            dbc.Col(dropdown("mm-brand", "Brand"), md=2),
            dbc.Col(dropdown("mm-region", "Region"), md=2),
            dbc.Col(dropdown("mm-state", "State"), md=2),
            dbc.Col(dropdown("mm-city", "City"), md=2),
            dbc.Col(dropdown("mm-type", "Type"), md=2),
            dbc.Col(dropdown("mm-outlet", "Outlet"), md=2),
        ]),

        dbc.Row([
            dbc.Col(dropdown("mm-source", "Source"), md=2),
            dbc.Col(dropdown("mm-tab", "Tab"), md=2),
            dbc.Col(dropdown("mm-section", "Section"), md=2),
            dbc.Col(dropdown("mm-supercat", "Super Category"), md=2),
            dbc.Col(dropdown("mm-cat", "Category"), md=2),
            dbc.Col(dropdown("mm-item", "Item"), md=2),
        ], className="mt-2"),

        dbc.Row([
            dbc.Col(
                dcc.DatePickerRange(
                    id="mm-date",
                    start_date=MONTH_START,
                    end_date=yesterday,
                    display_format="DD-MM-YYYY",
                    clearable=False,
                ), md=4
            ),
            dbc.Col(dcc.Dropdown(id="mm-fy_filter", placeholder="Financial Year"), md=2),
            dbc.Col(dcc.Dropdown(id="mm-month_filter", placeholder="Month"), md=2),
            dbc.Col(dcc.Dropdown(id="mm-week_filter", placeholder="Week"), md=2),
            dbc.Col(dcc.Dropdown(id="mm-day_filter", placeholder="Day"), md=1),
            dbc.Col(
                dbc.Button(
                    "Reset Filters",
                    id="mm-reset_filters",
                    color="secondary",
                    outline=True,
                    className="w-100"
                ),
                md=1
            ),
        ], className="g-2 mb-3"),
        dbc.Row([
                dbc.Col(
                    dcc.RadioItems(
                        id="mm-metric",
                        options=[
                            {"label": "Net Sales", "value": "net"},
                            {"label": "Quantity", "value": "qty"},
                        ],
                        value="net",
                        inline=True,
                    ),
                    md=3
                ),

                dbc.Col(
                    dcc.Dropdown(
                        id="mm-topn",
                        options=[
                            {"label": "Top 5", "value": 5},
                            {"label": "Top 10", "value": 10},
                            {"label": "Top 20", "value": 20},
                        ],
                        value=10,
                        clearable=False,
                    ),
                    md=2
                ),

                # 🔎 hidden store for click drilldown
                dcc.Store(id="mm-clicked-item"),
            ], className="mb-3"),
            dbc.Row([

                dbc.Col(
                    dbc.Card(
                        dbc.CardBody([
                            html.Div("NET SALES", className="text-muted"),
                            html.H3(id="kpi-net", className="text-success fw-bold")
                        ]),
                        id="kpi-net-card",
                        style={
                                "backgroundColor": "#25c7eb",
                                "color": "white",
                                "borderRadius": "10px",
                                "cursor": "pointer",
                            },
                    ), md=3
                ),

                dbc.Col(
                    dbc.Card(
                        dbc.CardBody([
                            html.Div("DISCOUNT", className="text-muted"),
                            html.H3(id="kpi-discount", className="text-danger fw-bold")
                        ]),
                        id="kpi-discount-card",
                        style={
                                "backgroundColor": "#a625eb",
                                "color": "white",
                                "borderRadius": "10px",
                                "cursor": "pointer",
                            },
                    ), md=3
                ),

                dbc.Col(
                    dbc.Card(
                        dbc.CardBody([
                            html.Div("ITEM QTY", className="text-muted"),
                            html.H3(id="kpi-qty", className="text-primary fw-bold")
                        ]),
                        id="kpi-qty-card",
                        style={
                                "backgroundColor": "#25eb25",
                                "color": "white",
                                "borderRadius": "10px",
                                "cursor": "pointer",
                            },
                    ), md=3
                ),

            ], className="mb-4"),
            
            
            html.H5("📊 Outlet-wise Sales Summary", className="mt-3 mb-1"),
            html.Small(
                "Item Quantity • Net Sales • Discount • Tax • Total Amount",
                className="text-muted"
            ),
            dash_table.DataTable(
                id="outlet-summary-table",
                columns=[
                    {"name": c, "id": c}
                    for c in [
                        "Outlet Name", "Item Qty", "Sub Total",
                        "Discount", "Net", "Total Tax", "Total Amount"
                    ]
                ],
                data=[],
                row_selectable="single",
                selected_rows=[],
                page_size=15,
                sort_action="native",
                filter_action="native",
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "right", "padding": "6px"},
                style_header={"fontWeight": "bold"},
            ),
        html.Hr(),
        html.H4(id="outlet-supercat-title"),

        dbc.Row([
            dbc.Col(
                dash_table.DataTable(
                    id="outlet-supercat-table",
                    style_table={"overflowX": "auto"},
                    style_cell={"textAlign": "right"},
                ),
                md=6
            ),
            dbc.Col(
                dcc.Graph(id="outlet-supercat-pie"),
                md=6
            ),
        ]),


        html.Hr(),

        dcc.Graph(id="mm-chart"),
        dbc.Row([
            dbc.Col(dcc.Graph(id="top_items_chart")),
        ]),

        dbc.Row([
            dbc.Col(dcc.Graph(id="category_contribution_chart"), md=6),
            dbc.Col(dcc.Graph(id="sales_trend_chart"), md=6),
            
        ]),
        dbc.Row([
            dbc.Col(dcc.Graph(id="section_pie_chart"), md=6),
            dbc.Col(dcc.Graph(id="item_bills_pie_chart"), md=6),
        ]),
        html.H4("📊 Super Category → Item Drilldown"),
            dash_table.DataTable(
                id="supercat-item-table",
                data=[],
                columns=[],
                page_size=10,
                style_table={"overflowX": "auto"},
            ),
            dcc.Graph(id="supercat-item-pie"),

        dbc.Row([
            dbc.Col(dash_table.DataTable(
                id="bev-table",
                page_size=10,
                sort_action="native",
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "right"},
                style_header={"fontWeight": "bold"},
            ), md=6),

            dbc.Col(dcc.Graph(id="bev-pie"), md=6),
        ], className="mb-4"),

        dbc.Row([
            dbc.Col(dash_table.DataTable(
                id="food-table",
                page_size=10,
                sort_action="native",
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "right"},
                style_header={"fontWeight": "bold"},
            ), md=6),

            dbc.Col(dcc.Graph(id="food-pie"), md=6),
        ], className="mb-4"),
        html.H5("📊 Top Super Categories by Menu Mix Contribution", className="mt-4"),
        
        dbc.Row([
            dbc.Col(
                dash_table.DataTable(
                    id="top-supercat-table",
                    page_size=10,
                    sort_action="native",
                    style_table={"overflowX": "auto"},
                    style_cell={"textAlign": "right"},
                    style_header={"fontWeight": "bold"},
                ),
                md=6
            ),
             dbc.Col(dcc.Graph(id="top-supercat-pie"), md=6),
        ], className="mb-4"),
        html.H5("📊 Bottom Super Categories by Menu Mix Contribution", className="mt-4"),
        dbc.Row([
            dbc.Col(
                dash_table.DataTable(
                    id="bottom-supercat-table",
                    page_size=10,
                    sort_action="native",
                    style_table={"overflowX": "auto"},
                    style_cell={"textAlign": "right"},
                    style_header={"fontWeight": "bold"},
                ),
                md=6
            ),
            dbc.Col(dcc.Graph(id="bottom-supercat-pie"), md=6),
        ], className="mb-4"),
        html.Hr(),
        html.H4("🍽️ Food vs Beverage Contribution"),

        dbc.Row([
            dbc.Col(
                dash_table.DataTable(
                    id="food-bev-table",
                    style_table={"overflowX": "auto"},
                    style_cell={"textAlign": "right"},
                ),
                md=6
            ),
            dbc.Col(
                dcc.Graph(id="food-bev-pie"),
                md=6
            ),
        ]),
        html.H4("📦 Source-wise Item Contribution"),
        dbc.Row([
            dbc.Col(dcc.Graph(id="source-item-pie"), md=6),
            dbc.Col(
                dash_table.DataTable(
                    id="source-item-table",
                    page_size=10,
                    sort_action="native",
                    filter_action="native",
                ),
                md=6
            )
        ]),
        html.H4("📦 Tab-wise Item Contribution"),
        dbc.Row([
            dbc.Col(dcc.Graph(id="tab-item-pie"), md=6),
            dbc.Col(
                dash_table.DataTable(
                    id="tab-item-table",
                    page_size=10,
                    sort_action="native",
                    filter_action="native",
                ),
                md=6
            )
        ]),


        html.H4("Bottom Item Table"),
        dbc.Row([
            dbc.Col(
                dash_table.DataTable(
                    id="bottom-items-table",
                    page_size=10,
                    sort_action="native",
                    style_table={"overflowX": "auto"},
                    style_cell={"textAlign": "right"},
                    style_header={"fontWeight": "bold"},
                ),
                md=12
            ),
        ]),


    ], style={"padding": "10px"})

# =========================================================
# CALLBACKS
# =========================================================
def register_callbacks(app):

    # preload static dropdowns
    @app.callback(
        Output("mm-brand", "options"),
        Output("mm-region", "options"),
        Output("mm-state", "options"),
        Output("mm-city", "options"),
        Output("mm-type", "options"),
        Output("mm-outlet", "options"),
        Output("mm-source", "options"),
        Output("mm-tab", "options"),
        Output("mm-section", "options"),
        Output("mm-supercat", "options"),
        Output("mm-cat", "options"),
        Output("mm-item", "options"),
        Output("mm-fy_filter", "options"),
        Input("mm-date", "start_date"),
    )
    def preload(_):

        fy_opts = (
            sales_df["Financial Year"]
            .dropna()
            .unique()
            .tolist()
        )

        fy_opts = sorted(fy_opts, reverse=True)

        [{"label": fy, "value": fy} for fy in fy_opts]

        return (
            [{"label": b, "value": b} for b in sorted(store_df["Brand"].dropna().unique())],
            [{"label": r, "value": r} for r in sorted(store_df["Region"].dropna().unique())],
            [{"label": s, "value": s} for s in sorted(store_df["State"].dropna().unique())],
            [{"label": c, "value": c} for c in sorted(store_df["City"].dropna().unique())],
            [{"label": t, "value": t} for t in sorted(store_df["Type"].dropna().unique())],
            [{"label": o, "value": o} for o in sorted(store_df["OUTLET_KEY"].dropna().unique())],

            [{"label": s, "value": s} for s in sorted(sales_df[COL["source"]].dropna().unique())]
            if COL["source"] else [],

            [{"label": t, "value": t} for t in sorted(sales_df[COL["tab"]].dropna().unique())]
            if COL["tab"] else [],

            [{"label": s, "value": s} for s in sorted(sales_df[COL["section"]].dropna().unique())]
            if COL["section"] else [],

            [
                {"label": s, "value": s}
                for s in sorted(
                    set(sales_df[COL["supercat"]].dropna().unique())
                    - set(SKIP_SUPER_CATEGORIES)
                )
            ]
            if COL["supercat"] else [],

            [{"label": c, "value": c} for c in sorted(sales_df[COL["cat"]].dropna().unique())]
            if COL["cat"] else [],

            [{"label": i, "value": i} for i in sorted(sales_df[COL["item"]].dropna().unique())]
            if COL["item"] else [],

            [{"label": fy, "value": fy} for fy in fy_opts],
        )

    print("REGION COUNT:", store_df["Region"].nunique())
    print("TAB COL:", COL["tab"])

    @app.callback(
        Output("mm-brand", "value"),
        Output("mm-region", "value"),
        Output("mm-state", "value"),
        Output("mm-city", "value"),
        Output("mm-type", "value"),
        Output("mm-outlet", "value"),
        Output("mm-source", "value"),
        Output("mm-tab", "value"),
        Output("mm-section", "value"),
        Output("mm-supercat", "value"),
        Output("mm-cat", "value"),
        Output("mm-item", "value"),
        Output("mm-fy_filter", "value"),
        Output("mm-month_filter", "value"),
        Output("mm-week_filter", "value"),
        Output("mm-day_filter", "value"),
        Input("mm-reset_filters", "n_clicks"),
        prevent_initial_call=True
    )
    def reset_filters(_):
        today = pd.Timestamp.today()
        return (
            None, None, None, None, None, None,
            None, None, None, None, None, None,
            None, None, None, None, 
        )

    @app.callback(
        Output("mm-clicked-item", "data"),
        Input("top_items_chart", "clickData"),
        prevent_initial_call=True
    )
    def capture_item_click(clickData):
        if not clickData:
            return no_update

        return clickData["points"][0]["x"]


    @app.callback(
        Output("mm-date", "start_date"),
        Output("mm-date", "end_date"),
        Input("mm-reset_filters", "n_clicks"),
        Input("mm-month_filter", "value"),
        Input("mm-week_filter", "value"),
        prevent_initial_call=True
    )
    def control_mm_date(reset_clicks, month_value, week_value):

        ctx = callback_context
        if not ctx.triggered:
            raise dash.exceptions.PreventUpdate

        trigger = ctx.triggered[0]["prop_id"].split(".")[0]

        TODAY = pd.Timestamp.today().normalize()
        MONTH_START = TODAY.replace(day=1)

        # 🔴 RESET → RUNNING MONTH
        if trigger == "mm-reset_filters":
            return MONTH_START, TODAY

        # 🟡 MONTH → DATE RANGE
        if trigger == "mm-month_filter" and month_value:
            year, month = map(int, month_value.split("-"))
            df = sales_df[
                (sales_df["Date"].dt.year == year) &
                (sales_df["Date"].dt.month == month)
            ]

            if df.empty:
                return no_update, no_update

            return df["Date"].min(), df["Date"].max()

        # 🟢 WEEK → DATE RANGE
        if trigger == "mm-week_filter" and week_value:
            y, w = week_value.split("-W")
            df = sales_df[
                (sales_df["Date"].dt.isocalendar().year == int(y)) &
                (sales_df["Date"].dt.isocalendar().week == int(w))
            ]

            if df.empty:
                return no_update, no_update

            return df["Date"].min(), df["Date"].max()

        return no_update, no_update



    @app.callback(
        Output("mm-chart", "figure"),
        Input("mm-metric", "value"),
        Input("mm-date", "start_date"),
        Input("mm-date", "end_date"),
        Input("mm-brand", "value"),
        Input("mm-region", "value"),
        Input("mm-state", "value"),
        Input("mm-city", "value"),
        Input("mm-type", "value"),
        Input("mm-outlet", "value"),
        Input("mm-source", "value"),
        Input("mm-tab", "value"),
        Input("mm-section", "value"),
        Input("mm-supercat", "value"),
        Input("mm-cat", "value"),
        Input("mm-item", "value"),
        Input("mm-day_filter", "value"),
    )
    def update_drill_chart(metric, *filters):

        df = apply_filters(sales_df, *filters)

        if df.empty:
            return px.bar(title="No data")

        metric_map = {
            "net": COL["net"],
            "qty": COL["qty"],
            "discount": find_col(["discount"]),
            "gross": None,
        }

        if metric == "gross":
            tax_col = find_col(["tax", "gst"])
            df["_gross"] = df[COL["net"]] + (df[tax_col] if tax_col else 0)
            ycol = "_gross"
            title = "Gross Amount by Item"
        else:
            ycol = metric_map.get(metric)
            title = f"{metric.upper()} by Item"

        agg = (
            df.groupby(COL["item"], as_index=False)[ycol]
            .sum()
            .sort_values(ycol, ascending=False)
        )

        return px.bar(
            agg,
            x=COL["item"],
            y=ycol,
            title=title
        )


    @app.callback(
        Output("mm-month_filter", "options"),
        Input("mm-fy_filter", "value"),
    )
    def load_months(fy):

        df = sales_df[sales_df["Date"].notna()].copy()

            # FY optional
        if fy:
            df = df[df["Financial Year"] == fy]

        df["MonthKey"] = df["Date"].dt.to_period("M")

        months = (
            df["MonthKey"]
            .drop_duplicates()
            .sort_values(ascending=False)
        )

        return [
            {
                "label": m.strftime("%b-%Y"),   # Jan-2026
                "value": m.strftime("%Y-%m")
            }
            for m in months
        ]

    
    @app.callback(
        Output("mm-week_filter", "options"),
        Input("mm-date", "start_date"),
        Input("mm-date", "end_date"),
        Input("mm-month_filter", "value"),
    )
    def load_weeks(start_date, end_date, month_value):

        df = sales_df[sales_df["Date"].notna()].copy()

        # 🟢 Priority 1 — Month selected
        if month_value:
            year, month = map(int, month_value.split("-"))
            df = df[
                (df["Date"].dt.year == year) &
                (df["Date"].dt.month == month)
            ]

        # 🟡 Priority 2 — Date range selected
        elif start_date and end_date:
            df = df[
                (df["Date"] >= pd.to_datetime(start_date)) &
                (df["Date"] <= pd.to_datetime(end_date))
            ]

        # 🔴 No data
        if df.empty:
            return []

        df["ISO_Year"] = df["Date"].dt.isocalendar().year
        df["ISO_Week"] = df["Date"].dt.isocalendar().week

        weeks = (
            df[["ISO_Year", "ISO_Week"]]
            .drop_duplicates()
            .sort_values(["ISO_Year", "ISO_Week"], ascending=False)
        )

        return [
            {
                "label": f"W{int(w):02d}-{int(y)}",
                "value": f"{int(y)}-W{int(w):02d}"
            }
            for y, w in weeks.itertuples(index=False)
        ]



    @app.callback(
        Output("mm-day_filter", "options"),
        Input("mm-date", "start_date"),
    )
    def day_options(_):
        return (
            [
                {"label": "WD (Mon–Thu)", "value": "WD"},
                {"label": "WF (Friday)", "value": "WF"},
                {"label": "WE (Sat–Sun)", "value": "WE"},
            ]
            + [
                {"label": d, "value": d}
                for d in [
                    "Monday", "Tuesday", "Wednesday",
                    "Thursday", "Friday", "Saturday", "Sunday"
                ]
            ]
        )


    @app.callback(
        Output("top_items_chart", "figure"),
        Output("category_contribution_chart", "figure"),
        Output("sales_trend_chart", "figure"),

        Input("mm-date", "start_date"),
        Input("mm-date", "end_date"),
        Input("mm-brand", "value"),
        Input("mm-region", "value"),
        Input("mm-state", "value"),
        Input("mm-city", "value"),
        Input("mm-type", "value"),
        Input("mm-outlet", "value"),
        Input("mm-source", "value"),
        Input("mm-tab", "value"),
        Input("mm-section", "value"),
        Input("mm-supercat", "value"),
        Input("mm-cat", "value"),
        Input("mm-item", "value"),
        Input("mm-day_filter", "value"),

        Input("mm-metric", "value"),
        Input("mm-topn", "value"),
        Input("mm-clicked-item", "data"),
    )
    def update_charts(
        start, end,
        brand, region, state, city, type_, outlet,
        source, tab, section, supercat, cat, item, day,
        metric, topn, clicked_item
    ):

        metric_col = COL["net"] if metric == "net" else COL["qty"]
        metric_label = "Net Sales" if metric == "net" else "Quantity"

        df = apply_filters(
            sales_df,
            start_date=start,
            end_date=end,
            brand=brand,
            region=region,
            state=state,
            city=city,
            type_=type_,
            outlet=outlet,
            source=source,
            tab=tab,
            section=section,
            supercat=supercat,
            cat=cat,
            item=item,
            day=day,
        )

        # 🔎 DRILLDOWN FROM CLICK
        if clicked_item:
            df = df[df[COL["item"]] == clicked_item]

        if df.empty:
            return px.bar(title="No data"), px.pie(title="No data"), px.line(title="No data")

        # ==================================================
        # TOP ITEMS
        # ==================================================
        top_items = (
            df.groupby(COL["item"], as_index=False)[metric_col]
            .sum()
            .sort_values(metric_col, ascending=False)
            .head(topn)
        )

        fig_top = px.bar(
            top_items,
            x=COL["item"],
            y=metric_col,
            title=f"Top {topn} Items by {metric_label}"
        )
        fig_top.update_layout(xaxis_tickangle=-45)

        # ==================================================
        # CATEGORY CONTRIBUTION
        # ==================================================
        if COL["supercat"]:
            cat_df = (
                df.groupby(COL["supercat"])[metric_col]
                .sum()
                .reset_index()
            )

            fig_cat = px.pie(
                cat_df,
                names=COL["supercat"],
                values=metric_col,
                title=f"{metric_label} Contribution by Super Category"
            )
        else:
            fig_cat = px.pie(title="Super Category not available")

        # ==================================================
        # SALES TREND
        # ==================================================
        daily = (
            df.groupby("Date")[metric_col]
            .sum()
            .reset_index()
        )

        weekly = (
            df.assign(Week=df["Date"].dt.to_period("W").astype(str))
            .groupby("Week")[metric_col]
            .sum()
            .reset_index()
        )

        fig_trend = px.line(
            daily,
            x="Date",
            y=metric_col,
            markers=True,
            title=f"{metric_label} Trend"
        )

        fig_trend.add_scatter(
            x=weekly["Week"],
            y=weekly[metric_col],
            mode="lines+markers",
            name="Weekly Total",
            yaxis="y2"
        )

        fig_trend.update_layout(
            yaxis2=dict(overlaying="y", side="right", title=f"Weekly {metric_label}"),
            legend=dict(orientation="h")
        )

        return fig_top, fig_cat, fig_trend
    
    @app.callback(
        Output("clicked-supercat", "data"),
        Input("category_contribution_chart", "clickData"),
        prevent_initial_call=True
    )
    def capture_supercat(clickData):
        if not clickData:
            return no_update
        return clickData["points"][0]["label"]
    
    @app.callback(
        Output("kpi-net", "children"),
        Output("kpi-discount", "children"),
        Output("kpi-qty", "children"),
        Output("outlet-summary-table", "data"),

        Input("mm-date", "start_date"),
        Input("mm-date", "end_date"),
        Input("mm-brand", "value"),
        Input("mm-region", "value"),
        Input("mm-state", "value"),
        Input("mm-city", "value"),
        Input("mm-type", "value"),
        Input("mm-outlet", "value"),
        Input("mm-source", "value"),
        Input("mm-tab", "value"),
        Input("mm-section", "value"),
        Input("mm-supercat", "value"),
        Input("mm-cat", "value"),
        Input("mm-item", "value"),
        Input("mm-day_filter", "value"),
    )
    def update_kpis_and_outlet_table(
        start, end,
        brand, region, state, city, type_, outlet,
        source, tab, section, supercat, cat, item, day
    ):

        df = apply_filters(
            sales_df,
            start_date=start,
            end_date=end,
            brand=brand,
            region=region,
            state=state,
            city=city,
            type_=type_,
            outlet=outlet,
            source=source,
            tab=tab,
            section=section,
            supercat=supercat,
            cat=cat,
            item=item,
            day=day,
        )

        if df.empty:
            return "₹0", "₹0", "0", []

        qty_col = COL["qty"]
        net_col = COL["net"]
        disc_col = find_col(["discount"])
        tax_cols = [c for c in sales_df.columns if "tax" in c.lower() or "gst" in c.lower()]
        gross_col = find_col(["gross amount", "total amount"])

        # 🔹 KPI values
        net_val = df[net_col].sum()
        qty_val = df[qty_col].sum()
        discount_val = df[disc_col].sum() if disc_col else 0

        # =========================
        # OUTLET SUMMARY (SAFE + AUTO)
        # =========================

        summary = df.groupby("OUTLET_KEY").agg(
            **{
                "Item Qty": pd.NamedAgg(column=qty_col, aggfunc="sum"),
                "Net": pd.NamedAgg(column=net_col, aggfunc="sum"),
            }
        )

        # Discount
        summary["Discount"] = (
            df.groupby("OUTLET_KEY")[disc_col].sum()
            if disc_col else 0
        )

        # Total Tax (handles CGST + SGST + IGST automatically)
        if tax_cols:
            summary["Total Tax"] = (
                df.groupby("OUTLET_KEY")[tax_cols]
                .sum()
                .sum(axis=1)
            )
        else:
            summary["Total Tax"] = 0

        # Sub Total = Net + Discount
        summary["Sub Total"] = summary["Net"] + summary["Discount"]

        # Total Amount
        if gross_col:
            summary["Total Amount"] = df.groupby("OUTLET_KEY")[gross_col].sum()
        else:
            summary["Total Amount"] = summary["Net"] + summary["Total Tax"]

        summary = (
            summary.reset_index()
            .rename(columns={"OUTLET_KEY": "Outlet Name"})
            .round(2)
        )
        
        return (
            f"₹ {net_val:,.0f}",
            f"₹ {discount_val:,.0f}",
            f"{int(qty_val):,}",
            summary.to_dict("records"),
        )


    @app.callback(
        Output("selected-outlet", "data"),
        Input("outlet-summary-table", "selected_rows"),
        State("outlet-summary-table", "data"),
    )
    def capture_outlet(selected_rows, rows):

        if not selected_rows or not rows:
            return None

        row_idx = selected_rows[0]
        return rows[row_idx]["Outlet Name"]

    @app.callback(
        Output("outlet-supercat-title", "children"),
        Input("selected-outlet", "data"),
    )
    def update_title(outlet):
        if outlet:
            return f"🏬 Outlet → Super Category Drilldown ({outlet})"
        return "🏬 Outlet → Super Category Drilldown (Select an outlet)"


    @app.callback(
        Output("section_pie_chart", "figure"),
        Input("mm-date", "start_date"),
        Input("mm-date", "end_date"),
        Input("mm-brand", "value"),
        Input("mm-region", "value"),
        Input("mm-state", "value"),
        Input("mm-city", "value"),
        Input("mm-type", "value"),
        Input("mm-outlet", "value"),
        Input("mm-source", "value"),
        Input("mm-tab", "value"),
        Input("mm-section", "value"),
        Input("mm-supercat", "value"),
        Input("mm-cat", "value"),
        Input("mm-item", "value"),
        Input("mm-day_filter", "value"),
    )
    def section_pie(
        start, end,
        brand, region, state, city, type_, outlet,
        source, tab, section, supercat, cat, item, day
    ):

        if not COL["section"]:
            return px.pie(title="Section not available")

        df = apply_filters(
            sales_df,
            start_date=start,
            end_date=end,
            brand=brand,
            region=region,
            state=state,
            city=city,
            type_=type_,
            outlet=outlet,
            source=source,
            tab=tab,
            section=section,
            supercat=supercat,
            cat=cat,
            item=item,
            day=day,
        )

        if df.empty:
            return px.pie(title="No data")

        sec_df = (
            df.groupby(COL["section"])[COL["net"]]
            .sum()
            .reset_index()
            .sort_values(COL["net"], ascending=False)
        )

        fig = px.pie(
            sec_df,
            names=COL["section"],
            values=COL["net"],
            title="Sales Contribution by Section",
            hole=0.4
        )

        fig.update_traces(textinfo="percent+label")
        return fig

    @app.callback(
        Output("item_bills_pie_chart", "figure"),
        Input("mm-date", "start_date"),
        Input("mm-date", "end_date"),
        Input("mm-brand", "value"),
        Input("mm-region", "value"),
        Input("mm-state", "value"),
        Input("mm-city", "value"),
        Input("mm-type", "value"),
        Input("mm-outlet", "value"),
        Input("mm-source", "value"),
        Input("mm-tab", "value"),
        Input("mm-section", "value"),
        Input("mm-supercat", "value"),
        Input("mm-cat", "value"),
        Input("mm-item", "value"),
        Input("mm-day_filter", "value"),
        Input("mm-topn", "value"),
    )
    def item_bills_pie(
        start, end,
        brand, region, state, city, type_, outlet,
        source, tab, section, supercat, cat, item, day,
        topn
    ):

        if not COL["bills"]:
            return px.pie(title="Bills column not available")

        df = apply_filters(
            sales_df,
            start_date=start,
            end_date=end,
            brand=brand,
            region=region,
            state=state,
            city=city,
            type_=type_,
            outlet=outlet,
            source=source,
            tab=tab,
            section=section,
            supercat=supercat,
            cat=cat,
            item=item,
            day=day,
        )

        if df.empty:
            return px.pie(title="No data")

        pie_df = top_n_with_others(
            df,
            group_col=COL["item"],
            value_col=COL["bills"],
            n=topn or 10
        )

        fig = px.pie(
            pie_df,
            names=COL["item"],
            values=COL["bills"],
            title="Item Contribution by Bills",
            hole=0.4
        )

        fig.update_traces(textinfo="percent+label")
        return fig




    @app.callback(
        Output("outlet-item-table", "data"),
        Output("outlet-item-table", "columns"),
        Input("selected-outlet", "data"),
        Input("mm-date", "start_date"),
        Input("mm-date", "end_date"),
    )
    def outlet_item_drill(outlet_name, start, end):

        if not outlet_name:
            return [], []

        df = apply_filters(
            sales_df,
            start_date=start,
            end_date=end,
            outlet=[outlet_name],  # ✅ OUTLET_KEY == Outlet Name
        )

        if df.empty:
            return [], []

        agg = (
            df.groupby(COL["item"], as_index=False)
            .agg({
                COL["qty"]: "sum",
                COL["net"]: "sum"
            })
            .sort_values(COL["net"], ascending=False)
            .round(2)
        )

        return (
            agg.to_dict("records"),
            [{"name": c, "id": c} for c in agg.columns]
        )

    @app.callback(
        Output("top-supercat-table", "data"),
        Output("top-supercat-table", "columns"),
        Output("top-supercat-pie", "figure"),
        Output("bottom-supercat-table", "data"),
        Output("bottom-supercat-table", "columns"),
        Output("bottom-supercat-pie", "figure"),
        Output("bottom-items-table", "data"),
        Output("bottom-items-table", "columns"),

        Input("mm-date", "start_date"),
        Input("mm-date", "end_date"),
        Input("mm-brand", "value"),
        Input("mm-region", "value"),
        Input("mm-state", "value"),
        Input("mm-city", "value"),
        Input("mm-type", "value"),
        Input("mm-outlet", "value"),
        Input("mm-source", "value"),
        Input("mm-tab", "value"),
        Input("mm-section", "value"),
        Input("mm-supercat", "value"),
        Input("mm-cat", "value"),
        Input("mm-item", "value"),
        Input("mm-day_filter", "value"),

        Input("mm-metric", "value"),   # net / qty
        Input("mm-topn", "value"),     # 5 / 10 / 20
    )
    def menu_mix_tables(
        start, end,
        brand, region, state, city, type_, outlet,
        source, tab, section, supercat, cat, item, day,
        metric, topn
    ):

        metric_col = COL["net"] if metric == "net" else COL["qty"]

        df = apply_filters(
            sales_df,
            start_date=start,
            end_date=end,
            brand=brand,
            region=region,
            state=state,
            city=city,
            type_=type_,
            outlet=outlet,
            source=source,
            tab=tab,
            section=section,
            supercat=supercat,
            cat=cat,
            item=item,
            day=day,
        )

        if df.empty or not COL["supercat"]:
            empty_fig = px.pie(title="No data")
            return [], [], empty_fig, [], [], empty_fig, [], []


        total_value = df[metric_col].sum()
        n = topn or 10
        # ==============================
        # SUPER CATEGORY CONTRIBUTION
        # ==============================
        supercat_df = (
            df.groupby(COL["supercat"])[metric_col]
            .sum()
            .reset_index()
            .rename(columns={metric_col: "Value"})
        )

        supercat_df["Contribution %"] = (
            supercat_df["Value"] / total_value * 100
        ).round(2)

        supercat_df = supercat_df.sort_values("Contribution %", ascending=False)

        cols_super = [
            {"name": "Super Category", "id": COL["supercat"]},
            {"name": "Value", "id": "Value"},
            {"name": "Contribution %", "id": "Contribution %"},
        ]

        top_super = supercat_df.head(n).round(2)
        bottom_super = supercat_df.tail(n).sort_values("Contribution %").round(2)

        top_super_pie = (
            px.pie(top_super, names=COL["supercat"], values="Value",
                title="Top Super Category Contribution", hole=0.4)
            if not top_super.empty else px.pie(title="No data")
        )

        bottom_super_pie = (
            px.pie(bottom_super, names=COL["supercat"], values="Value",
                title="Bottom Super Category Contribution", hole=0.4)
            if not bottom_super.empty else px.pie(title="No data")
        )
        # ==============================
        # BOTTOM ITEMS (5 / 10 / 20)
        # ==============================
        item_df = (
            df.groupby(COL["item"])[metric_col]
            .sum()
            .reset_index()
            .rename(columns={metric_col: "Value"})
            .sort_values("Value")
            .head(topn or 10)
        )

        item_df["Contribution %"] = (
            item_df["Value"] / total_value * 100
        ).round(2)

        cols_item = [
            {"name": "Item Name", "id": COL["item"]},
            {"name": "Value", "id": "Value"},
            {"name": "Contribution %", "id": "Contribution %"},
        ]

        return (
            top_super.to_dict("records"),
            cols_super,
            top_super_pie,
            bottom_super.to_dict("records"),
            cols_super,
            bottom_super_pie,
            item_df.to_dict("records"),
            cols_item,
        )

    @app.callback(
        Output("bev-table", "data"),
        Output("bev-table", "columns"),
        Output("bev-pie", "figure"),

        Output("food-table", "data"),
        Output("food-table", "columns"),
        Output("food-pie", "figure"),

        Input("mm-date", "start_date"),
        Input("mm-date", "end_date"),
        Input("mm-brand", "value"),
        Input("mm-region", "value"),
        Input("mm-state", "value"),
        Input("mm-city", "value"),
        Input("mm-type", "value"),
        Input("mm-outlet", "value"),
        Input("mm-source", "value"),
        Input("mm-tab", "value"),
        Input("mm-section", "value"),
        Input("mm-supercat", "value"),
        Input("mm-cat", "value"),
        Input("mm-item", "value"),
        Input("mm-day_filter", "value"),

        Input("mm-metric", "value"),
        Input("mm-topn", "value"),
    )
    def beverage_food_contribution(
        start, end,
        brand, region, state, city, type_, outlet,
        source, tab, section, supercat, cat, item, day,
        metric, topn
    ):

        metric_col = COL["net"] if metric == "net" else COL["qty"]
        metric_label = "Net Sales" if metric == "net" else "Quantity"

        df = apply_filters(
            sales_df,
            start_date=start,
            end_date=end,
            brand=brand,
            region=region,
            state=state,
            city=city,
            type_=type_,
            outlet=outlet,
            source=source,
            tab=tab,
            section=section,
            supercat=supercat,
            cat=cat,
            item=item,
            day=day,
        )

        if df.empty or not COL["section"]:
            empty_fig = px.pie(title="No data")
            return [], [], empty_fig, [], [], empty_fig

        # -----------------------------
        # BEVERAGE
        # -----------------------------
        bev_df = df[
            df[COL["section"]].str.contains("BEVERAGE", case=False, na=False)
        ]

        bev_agg = (
            bev_df.groupby(COL["item"])[metric_col]
            .sum()
            .sort_values(ascending=False)
            .head(topn or 10)
            .reset_index()
            .rename(columns={metric_col: "Value"})
        )

        bev_total = bev_df[metric_col].sum()
        bev_agg["Contribution %"] = (bev_agg["Value"] / bev_total * 100).round(2)

        bev_cols = [
            {"name": "Item Name", "id": COL["item"]},
            {"name": metric_label, "id": "Value"},
            {"name": "Contribution %", "id": "Contribution %"},
        ]

        bev_pie = px.pie(
            bev_agg,
            names=COL["item"],
            values="Value",
            title="Top Beverage Contribution",
            hole=0.4
        )

        # -----------------------------
        # FOOD
        # -----------------------------
        food_df = df[
            df[COL["section"]].str.contains("FOOD", case=False, na=False)
        ]

        food_agg = (
            food_df.groupby(COL["item"])[metric_col]
            .sum()
            .sort_values(ascending=False)
            .head(topn or 10)
            .reset_index()
            .rename(columns={metric_col: "Value"})
        )

        food_total = food_df[metric_col].sum()
        food_agg["Contribution %"] = (food_agg["Value"] / food_total * 100).round(2)

        food_cols = bev_cols  # same structure

        food_pie = px.pie(
            food_agg,
            names=COL["item"],
            values="Value",
            title="Top Food Contribution",
            hole=0.4
        )

        return (
            bev_agg.to_dict("records"),
            bev_cols,
            bev_pie,

            food_agg.to_dict("records"),
            food_cols,
            food_pie,
        )

    @app.callback(
        Output("food-bev-table", "data"),
        Output("food-bev-table", "columns"),
        Output("food-bev-pie", "figure"),

        Input("mm-date", "start_date"),
        Input("mm-date", "end_date"),
        Input("mm-brand", "value"),
        Input("mm-region", "value"),
        Input("mm-state", "value"),
        Input("mm-city", "value"),
        Input("mm-type", "value"),
        Input("mm-outlet", "value"),
        Input("mm-source", "value"),
        Input("mm-tab", "value"),
        Input("mm-section", "value"),
        Input("mm-supercat", "value"),
        Input("mm-cat", "value"),
        Input("mm-item", "value"),
        Input("mm-day_filter", "value"),
        Input("mm-metric", "value"),
    )
    def food_vs_bev(
        start, end,
        brand, region, state, city, type_, outlet,
        source, tab, section, supercat, cat, item, day,
        metric
    ):
        metric_col = COL["net"] if metric == "net" else COL["qty"]

        df = apply_filters(
            sales_df,
            start_date=start,
            end_date=end,
            brand=brand, region=region, state=state, city=city,
            type_=type_, outlet=outlet,
            source=source, tab=tab, section=section,
            supercat=supercat, cat=cat, item=item, day=day,
        )

        if df.empty or "Food_Beverage" not in df:
            return [], [], px.pie(title="No data")

        fb_df = (
            df.groupby("Food_Beverage")[metric_col]
            .sum()
            .reset_index()
            .rename(columns={metric_col: "Value"})
        )

        total = fb_df["Value"].sum()
        fb_df["Contribution %"] = (fb_df["Value"] / total * 100).round(2)

        fig = px.pie(
            fb_df,
            names="Food_Beverage",
            values="Value",
            hole=0.4,
            title="Food vs Beverage Contribution"
        )

        cols = [
            {"name": "Type", "id": "Food_Beverage"},
            {"name": "Value", "id": "Value"},
            {"name": "Contribution %", "id": "Contribution %"},
        ]

        return fb_df.round(2).to_dict("records"), cols, fig

    @app.callback(
        Output("outlet-supercat-table", "data"),
        Output("outlet-supercat-table", "columns"),
        Output("outlet-supercat-pie", "figure"),

        Input("selected-outlet", "data"),
        Input("mm-date", "start_date"),
        Input("mm-date", "end_date"),
        Input("mm-metric", "value"),
    )
    def outlet_supercat_drill(outlet_name, start, end, metric):

        if not outlet_name or not COL["supercat"]:
            return [], [], px.pie(title="Select an outlet")

        metric_col = COL["net"] if metric == "net" else COL["qty"]

        df = apply_filters(
            sales_df,
            start_date=start,
            end_date=end,
            outlet=[outlet_name],
        )

        if df.empty:
            return [], [], px.pie(title="No data")

        sc_df = (
            df.groupby(COL["supercat"])[metric_col]
            .sum()
            .reset_index()
            .rename(columns={metric_col: "Value"})
            .sort_values("Value", ascending=False)
        )

        total = sc_df["Value"].sum()
        sc_df["Contribution %"] = (sc_df["Value"] / total * 100).round(2)

        fig = px.pie(
            sc_df,
            names=COL["supercat"],
            values="Value",
            hole=0.4,
            title=f"Super Category Mix — {outlet_name}"
        )

        cols = [
            {"name": "Super Category", "id": COL["supercat"]},
            {"name": "Value", "id": "Value"},
            {"name": "Contribution %", "id": "Contribution %"},
        ]

        return sc_df.round(2).to_dict("records"), cols, fig

    @app.callback(
        Output("supercat-item-table", "data"),
        Output("supercat-item-table", "columns"),
        Output("supercat-item-pie", "figure"),

        Input("clicked-supercat", "data"),
        Input("mm-date", "start_date"),
        Input("mm-date", "end_date"),
    )
    def supercat_item_drill(supercat, start, end):

        if not supercat:
            return [], [], px.pie(title="Click a Super Category")

        df = apply_filters(
            sales_df,
            start_date=start,
            end_date=end,
        )

        df = df[df[COL["supercat"]] == supercat]

        if df.empty:
            return [], [], px.pie(title="No data")

        agg = (
            df.groupby(COL["item"])[COL["net"]]
            .sum()
            .reset_index()
            .sort_values(COL["net"], ascending=False)
        )

        agg["Contribution %"] = (agg[COL["net"]] / agg[COL["net"]].sum() * 100).round(2)

        cols = [
            {"name": "Item Name", "id": COL["item"]},
            {"name": "Net Sales", "id": COL["net"]},
            {"name": "Contribution %", "id": "Contribution %"},
        ]

        fig = px.pie(
            agg.head(10),
            names=COL["item"],
            values=COL["net"],
            title=f"Top Items in {supercat}",
            hole=0.4
        )

        return agg.to_dict("records"), cols, fig

    @app.callback(
        Output("source-item-pie", "figure"),
        Output("source-item-table", "data"),
        Output("source-item-table", "columns"),
        Input("mm-source", "value"),
        Input("mm-date", "start_date"),
        Input("mm-date", "end_date"),
    )
    def source_item_contribution(source, start, end):

        if not source:
            return px.pie(title="Select Source"), [], []

        df = apply_filters(
            sales_df,
            start_date=start,
            end_date=end,
            source=source
        )

        if df.empty:
            return px.pie(title="No data"), [], []

        agg = (
            df.groupby(COL["item"])[COL["net"]]
            .sum()
            .reset_index()
            .sort_values(COL["net"], ascending=False)
        )

        pie_df = agg.head(10)

        fig = px.pie(
            pie_df,
            names=COL["item"],
            values=COL["net"],
            title=f"Top Items by Source: {source}",
            hole=0.4
        )

        return (
            fig,
            agg.round(2).to_dict("records"),
            [{"name": c, "id": c} for c in agg.columns]
        )

    @app.callback(
        Output("tab-item-pie", "figure"),
        Output("tab-item-table", "data"),
        Output("tab-item-table", "columns"),
        Input("mm-tab", "value"),
        Input("mm-date", "start_date"),
        Input("mm-date", "end_date"),
    )
    def tab_item_contribution(tab, start, end):

        if not tab:
            return px.pie(title="Select Tab"), [], []

        df = apply_filters(
            sales_df,
            start_date=start,
            end_date=end,
            tab=tab
        )

        if df.empty:
            return px.pie(title="No data"), [], []

        agg = (
            df.groupby(COL["item"])[COL["net"]]
            .sum()
            .reset_index()
            .sort_values(COL["net"], ascending=False)
        )

        pie_df = agg.head(10)

        fig = px.pie(
            pie_df,
            names=COL["item"],
            values=COL["net"],
            title=f"Top Items by Tab: {tab}",
            hole=0.4
        )

        return (
            fig,
            agg.round(2).to_dict("records"),
            [{"name": c, "id": c} for c in agg.columns]
        )




# =========================================================
# EXPORTS
# =========================================================
__all__ = ["get_layout", "register_callbacks"]
