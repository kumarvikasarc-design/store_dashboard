import os
import time
import dash
import pandas as pd
from datetime import datetime, timedelta
from dash import Dash, html, dcc, Input, Output, State, dash_table
import plotly.express as px
import plotly.graph_objects as go

# ----------------------------------------------------------------
# 📁 File Path
# ----------------------------------------------------------------
DATA_PATH = r"C:\Users\ACER\dashboard_app\Dashboard.xlsx"
SHEET_NAME = "dashboard"  # change if your sheet name differs

# ----------------------------------------------------------------
# 🧩 Helper Functions
# ----------------------------------------------------------------
def get_col(df, possibles):
    """Find best-matching column from list of possible names (case-insensitive substring)."""
    for p in possibles:
        for col in df.columns:
            if p.lower() in str(col).lower():
                return df[col]
    # fallback: return a zero series same length as df
    return pd.Series([0] * len(df), index=df.index)

def read_and_prepare(path):
    """Read excel and normalize columns we need (flexible matching)."""
    df = pd.read_excel(path, sheet_name=SHEET_NAME)
    df.columns = df.columns.str.strip().str.lower()

    # Date / region / outlet
    df["date"] = pd.to_datetime(get_col(df, ["date", "txn date", "bill date"]), errors="coerce", dayfirst=True)
    df["region"] = get_col(df, ["region"])
    df["outlet name"] = get_col(df, ["outlet", "outlet name", "store"])

    # Tabs / covers - keep as string (some sheets have "A/B", some numeric)
    tabs_col = get_col(df, ["tabs", "covers"])
    # try numeric -> back to string for uniformity
    try:
        tabs_numeric = pd.to_numeric(tabs_col, errors="coerce")
        df["tabs"] = tabs_numeric.fillna(0).astype(int).astype(str)
    except Exception:
        df["tabs"] = tabs_col.astype(str).str.strip().replace("nan", "")

    # flexible numeric columns mapping
    num_cols = {
        "no of items": ["no of items", "items"],
        "no of bills": ["no of bills", "bills"],
        "sale": ["sale", "gross sale", "total sale"],
        "discount": ["discount", "disc"],
        "restaurant charge": ["restaurant charge"],
        "packaging charge [cart - swiggy]": ["packaging charge [cart - swiggy]", "packaging charge"],
        "restaurant packaging charges": ["restaurant packaging charges"],
        "delivery charge": ["delivery charge"],
        "platform fee charge": ["platform fee charge", "platform charge"],
        "smile amount charge": ["smile amount charge"],
        "total charges": ["total charges", "charges", "charge total"],
        "net sale": ["net sale", "net amount"],
        "gst @18%": ["gst @18%"],
        "gst @5%": ["gst @5%"],
        "ecom_gst@5%": ["ecom_gst@5%"],
        "gst @40%": ["gst @40%"],
        "total tax": ["total tax", "total tax amount", "tax amount", "tax"],
        "total amount": ["total amount", "grand total", "bill total", "invoice total"],
        "covers": ["covers", "no of covers", "total covers"]
    }

    for std, possibles in num_cols.items():
        df[std] = pd.to_numeric(get_col(df, possibles), errors="coerce").fillna(0)

    # drop rows w/o date
    df = df.dropna(subset=["date"])

    # tidy text fields
    df["outlet name"] = df["outlet name"].astype(str).str.strip()
    df["region"] = df["region"].astype(str).str.strip()
    df["tabs"] = df["tabs"].astype(str).str.strip()

    # debug: show detected columns once
    print("✅ Loaded columns:", list(df.columns))
    return df

# -------------------------------------------------------------
# 🚀 Initialize Dash App
# -------------------------------------------------------------
app = Dash(__name__)
server = app.server   # for deployment

# ----------------------------------------------------------------
# 🧠 Cache/Reload logic
# ----------------------------------------------------------------
_last_load_time = 0
_df_cache = None
_last_updated_str = "Never"

def get_data():
    """Return a copy of prepared df and last updated string. Reloads when file changes."""
    global _last_load_time, _df_cache, _last_updated_str
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Excel file not found: {DATA_PATH}")
    file_time = os.path.getmtime(DATA_PATH)
    if _df_cache is None or file_time > _last_load_time:
        print("🔄 Reloading Excel data...")
        _df_cache = read_and_prepare(DATA_PATH)
        _last_load_time = file_time
        _last_updated_str = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime(file_time))
    return _df_cache.copy(), _last_updated_str

# ----------------------------------------------------------------
# 🚀 Dash App Setup
# ----------------------------------------------------------------
app = Dash(__name__)
app.title = "Sales Dashboard"

# initial sample data for layout defaults
df_init, _ = get_data()
min_date = df_init["date"].min()
max_date = df_init["date"].max()
regions = sorted(df_init["region"].dropna().unique())

# build a reasonable set of detail table columns (use normalized names)
detail_columns = [
    "region", "outlet name", "date", "tabs",
    "no of items", "no of bills", "sale", "discount",
    "restaurant charge", "packaging charge [cart - swiggy]",
    "restaurant packaging charges", "delivery charge", "platform fee charge",
    "smile amount charge", "total charges", "net sale", "gst @18%",
    "gst @5%", "ecom_gst@5%", "gst @40%", "total tax", "total amount", "covers"
]
# ensure columns exist in df_init else drop from columns list
detail_columns = [c for c in detail_columns if c in df_init.columns]

app.layout = html.Div([
    html.H2("📊 Coffee Island Sales Dashboard", style={"textAlign": "center"}),

    html.Div([
        html.Label("Select Date Range:"),
        dcc.DatePickerRange(
            id="date-range",
            start_date=min_date,
            end_date=max_date,
            min_date_allowed=min_date,
            max_date_allowed=max_date
        ),
        html.Label("Select Region:", style={"marginLeft": "15px"}),
        dcc.Dropdown(
            id="region-filter",
            options=[{"label": r, "value": r} for r in regions],
            placeholder="Select a Region"
        ),
        html.Label("Select Outlet:", style={"marginLeft": "15px"}),
        dcc.Dropdown(id="outlet-filter", multi=True, placeholder="All Outlets"),
        html.Label("Select Tabs:", style={"marginLeft": "15px"}),
        dcc.Dropdown(id="tabs-filter", multi=True, placeholder="All Tabs"),
        html.Button("🔄 Refresh", id="refresh-btn", n_clicks=0, style={"marginLeft": "15px"}),
    ], style={"display": "flex", "alignItems": "center", "gap": "10px", "marginBottom": "10px", "flexWrap": "wrap"}),

    html.Div([
        html.Button("Yesterday", id="btn-yesterday", n_clicks=0),
        html.Button("Last 7 Days", id="btn-week", n_clicks=0),
        html.Button("This Month", id="btn-month", n_clicks=0),
        html.Button("This Year", id="btn-year", n_clicks=0),
    ], style={"display": "flex", "gap": "10px", "marginBottom": "15px"}),

    html.Div(id="kpi-cards", style={"display": "flex", "gap": "15px", "flexWrap": "wrap"}),
    html.Div(id="range-summary", style={"textAlign": "center", "fontWeight": "bold", "marginBottom": "15px"}),

    html.H4("Outlet Summary", style={"textAlign": "center", "marginTop": "20px"}),
    dash_table.DataTable(
        id="outlet-summary-table",
        columns=[
            {"name": "Outlet Name", "id": "Outlet Name"},
            {"name": "Net Sale", "id": "Net Sale", "type": "numeric"},
            {"name": "Discount", "id": "Discount", "type": "numeric"},
            {"name": "Total Tax", "id": "Total Tax", "type": "numeric"},
            {"name": "Charges", "id": "Charges", "type": "numeric"},
            {"name": "Total Sale", "id": "Total Sale", "type": "numeric"},
        ],
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "center", "padding": "8px"},
        style_header={"backgroundColor": "#0074D9", "color": "white", "fontWeight": "bold"},
        page_size=10
    ),

    dcc.Graph(id="trend-fig"),
    html.Div(id="top5-days"),

    html.Div([
        dcc.Graph(id="ts-sales", style={"flex": "1"}),
        dcc.Graph(id="sales-by-outlet", style={"flex": "1"})
    ], style={"display": "flex", "flexWrap": "wrap"}),

    html.Div([
        dcc.Graph(id="sales-by-region", style={"flex": "1"}),
        dcc.Graph(id="sales-by-tabs", style={"flex": "1"})
    ], style={"display": "flex", "flexWrap": "wrap"}),

    html.Div([
        dcc.Graph(id="tax-breakdown", style={"flex": "1"}),
        dcc.Graph(id="charge-breakdown", style={"flex": "1"})
    ], style={"display": "flex", "flexWrap": "wrap"}),

    html.H4("Detailed Records", style={"marginTop": "20px"}),
    dash_table.DataTable(
        id="detail-table",
        columns=[{"name": col.title(), "id": col} for col in detail_columns],
        page_size=10,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "center", "padding": "8px"},
    ),

    html.H4("💰 Charge Summary (Region & Outlet)", style={"marginTop": "25px"}),
    dash_table.DataTable(
        id="charge-summary",
        columns=[
            {"name": "Region", "id": "region"},
            {"name": "Outlet Name", "id": "outlet name"},
            {"name": "Restaurant Charge", "id": "restaurant charge"},
            {"name": "Packaging (Cart - Swiggy)", "id": "packaging charge [cart - swiggy]"},
            {"name": "Restaurant Packaging", "id": "restaurant packaging charges"},
            {"name": "Delivery Charge", "id": "delivery charge"},
            {"name": "Platform Fee", "id": "platform fee charge"},
            {"name": "Smile Amount", "id": "smile amount charge"},
            {"name": "Total Charges", "id": "total charges"},
        ],
        page_size=10,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "center", "padding": "8px"},
    ),

    html.Div(id="status-info", style={"textAlign": "right", "color": "gray", "marginTop": "10px"}),

    dcc.Interval(id="auto-refresh", interval=30 * 1000, n_intervals=0)
], style={"padding": "16px"})

# ----------------------------------------------------------------
# 🔁 Callbacks: filters population
# ----------------------------------------------------------------
@app.callback(
    Output("outlet-filter", "options"),
    Output("outlet-filter", "value"),
    Input("region-filter", "value")
)
def update_outlet_options(selected_region):
    df, _ = get_data()
    if not selected_region:
        outlets = sorted(df["outlet name"].dropna().unique())
    else:
        outlets = sorted(df[df["region"] == selected_region]["outlet name"].dropna().unique())
    options = [{"label": "All Outlets", "value": "All Outlets"}] + [{"label": o, "value": o} for o in outlets]
    return options, ["All Outlets"]

@app.callback(
    Output("tabs-filter", "options"),
    Output("tabs-filter", "value"),
    Input("region-filter", "value"),
    Input("outlet-filter", "value")
)
def update_tabs_options(region, outlets):
    df, _ = get_data()
    if region and region != "":
        df = df[df["region"] == region]
    if outlets and "All Outlets" not in outlets:
        df = df[df["outlet name"].isin(outlets)]
    tabs = sorted([t for t in df["tabs"].dropna().unique() if str(t).strip() != ""])
    options = [{"label": "All Tabs", "value": "All Tabs"}] + [{"label": t, "value": t} for t in tabs]
    return options, ["All Tabs"]

# ----------------------------------------------------------------
# 🔁 Main dashboard update callback
# ----------------------------------------------------------------
@app.callback(
    Output("kpi-cards", "children"),
    Output("range-summary", "children"),
    Output("trend-fig", "figure"),
    Output("top5-days", "children"),
    Output("ts-sales", "figure"),
    Output("sales-by-outlet", "figure"),
    Output("sales-by-region", "figure"),
    Output("sales-by-tabs", "figure"),
    Output("outlet-summary-table", "data"),
    Output("tax-breakdown", "figure"),
    Output("charge-breakdown", "figure"),
    Output("detail-table", "data"),
    Output("charge-summary", "data"),
    Output("status-info", "children"),
    Input("auto-refresh", "n_intervals"),
    Input("refresh-btn", "n_clicks"),
    Input("btn-yesterday", "n_clicks"),
    Input("btn-week", "n_clicks"),
    Input("btn-month", "n_clicks"),
    Input("btn-year", "n_clicks"),
    State("date-range", "start_date"),
    State("date-range", "end_date"),
    State("region-filter", "value"),
    State("outlet-filter", "value"),
    State("tabs-filter", "value"),
)
def update_dashboard(_, __, n_yesterday, n_week, n_month, n_year, start_date, end_date, region, outlets, tabs):
    df, updated_str = get_data()
    now = datetime.now()

    # Quick buttons
    ctx = dash.callback_context
    if ctx.triggered:
        btn = ctx.triggered[0]["prop_id"].split(".")[0]
        if btn == "btn-yesterday":
            start_date = (now - timedelta(days=1)).date()
            end_date = (now - timedelta(days=1)).date()
        elif btn == "btn-week":
            start_date = ((now - timedelta(days=1)) - timedelta(days=6)).date()
            end_date = (now - timedelta(days=1)).date()
        elif btn == "btn-month":
            start_date = now.replace(day=1).date()
            end_date = (now - timedelta(days=1)).date()
        elif btn == "btn-year":
            start_date = now.replace(month=1, day=1).date()
            end_date = (now - timedelta(days=1)).date()
        elif btn == "refresh-btn":
            df, updated_str = get_data()

    # apply region filter
    if region:
        df = df[df["region"] == region]

    # apply outlets (All Outlets semantics)
    if outlets and "All Outlets" not in outlets:
        df = df[df["outlet name"].isin(outlets)]

    # apply tabs (All Tabs semantics)
    if tabs and "All Tabs" not in tabs:
        df = df[df["tabs"].isin(tabs)]

    # apply date filter
    if start_date and end_date:
        df = df[(df["date"] >= pd.to_datetime(start_date)) & (df["date"] <= pd.to_datetime(end_date))]

    # If no data after filtering -> return placeholders matching output types & order
    if df.empty:
        empty_cards = [html.Div("No data", style={"padding": "8px"})]
        range_text_empty = ""
        # children for top5-days can be a small message div
        top5_children = html.Div("No records")
        status_text_empty = f"Last Updated: {updated_str} | No data"
        return (
            empty_cards,                  # kpi-cards.children
            range_text_empty,             # range-summary.children
            go.Figure(),                  # trend-fig.figure
            top5_children,                # top5-days.children
            go.Figure(),                  # ts-sales.figure
            go.Figure(),                  # sales-by-outlet.figure
            go.Figure(),                  # sales-by-region.figure
            go.Figure(),                  # sales-by-tabs.figure
            [],                           # outlet-summary-table.data
            go.Figure(),                  # tax-breakdown.figure
            go.Figure(),                  # charge-breakdown.figure
            [],                           # detail-table.data
            [],                           # charge-summary.data
            status_text_empty             # status-info.children
        )

    # KPI calculations
    total_sale = df["sale"].sum()
    total_discount = df["discount"].sum()
    total_net = df["net sale"].sum()
    total_tax = df["total tax"].sum() if "total tax" in df.columns else 0
    total_amount = df["total amount"].sum() if "total amount" in df.columns else 0
    total_covers = df["covers"].sum() if "covers" in df.columns else 0
    total_bills = df["no of bills"].sum() if "no of bills" in df.columns else 0
    total_days = df["date"].dt.date.nunique()
    num_outlets = df["outlet name"].nunique() if "outlet name" in df.columns else 0

    avg_sale_per_bill = total_net / total_bills if total_bills else 0
    avg_sale_per_day = total_net / total_days if total_days else 0
    avg_sale_per_outlet = total_net / num_outlets if num_outlets else 0

    def card(title, value, color):
        return html.Div([
            html.H6(title, style={"margin": "4px 0 4px 0"}),
            html.H3(f"{value:,.2f}", style={"margin": "0"})
        ], style={
            "backgroundColor": color,
            "color": "white",
            "padding": "10px 20px",
            "borderRadius": "10px",
            "minWidth": "160px",
            "textAlign": "center"
        })

    kpis = [
        ("Total Sale", total_sale, "#17a2b8"),
        ("Discount", total_discount, "#ffc107"),
        ("Net Sale", total_net, "#28a745"),
        ("Avg Sale per Bill", avg_sale_per_bill, "#20c997"),
        ("Avg Sale per Day", avg_sale_per_day, "#0d513c"),
        ("Avg Sale per Outlet", avg_sale_per_outlet, "#6f42c1"),
        ("Total Tax", total_tax, "#6c757d"),
        ("Total Amount", total_amount, "#007bff"),
        ("Covers", total_covers, "#6f42c1")
    ]
    cards = [card(label, value, color) for label, value, color in kpis]

    # Trend (daily per outlet) + top5 days by avg sale per bill
    daily_trend = df.groupby(["date", "outlet name"], as_index=False).agg({"net sale": "sum", "no of bills": "sum"})
    daily_trend["avg sale per bill"] = (daily_trend["net sale"] / daily_trend["no of bills"].replace(0, pd.NA)).fillna(0).round(2)

    if daily_trend["outlet name"].nunique() > 1:
        trend_fig = px.line(
            daily_trend, x="date", y="net sale", color="outlet name",
            title="📈 Daily Net Sale Trend (Outlet-wise)",
            markers=True,
            hover_data={"net sale": ":,.0f", "no of bills": ":,.0f", "avg sale per bill": ":,.2f"}
        )
    else:
        trend_fig = px.area(
            daily_trend, x="date", y="net sale",
            title="📈 Daily Net Sale Trend",
            markers=True,
            hover_data={"net sale": ":,.0f", "no of bills": ":,.0f", "avg sale per bill": ":,.2f"}
        )
        trend_fig.update_traces(fill="tozeroy")

    trend_fig.update_layout(xaxis_title="Date", yaxis_title="Net Sale (₹)", hovermode="x unified", height=420)

    top5_avg = daily_trend.sort_values("avg sale per bill", ascending=False).head(5).copy()
    if not top5_avg.empty:
        top5_avg["date"] = top5_avg["date"].dt.strftime("%d-%m-%Y")
    top5_table = dash_table.DataTable(
        id="top5-days-table",
        columns=[
            {"name": "Outlet", "id": "outlet name"},
            {"name": "Date", "id": "date"},
            {"name": "Net Sale (₹)", "id": "net sale"},
            {"name": "Bills", "id": "no of bills"},
            {"name": "Avg Sale/Bill (₹)", "id": "avg sale per bill"}
        ],
        data=top5_avg.to_dict("records"),
        style_table={"overflowX": "auto", "marginTop": "10px"},
        style_header={"backgroundColor": "#343a40", "color": "white", "fontWeight": "bold"},
        style_cell={"textAlign": "center", "padding": "6px"},
        page_size=5
    )

    # Outlet Summary Table
    outlet_summary = (
        df.groupby("outlet name", as_index=False)
        .agg({"net sale": "sum", "discount": "sum", "total tax": "sum", "total charges": "sum", "total amount": "sum"})
        .rename(columns={"outlet name": "Outlet Name", "net sale": "Net Sale", "discount": "Discount",
                         "total tax": "Total Tax", "total charges": "Charges", "total amount": "Total Sale"})
        .round(2)
    )

    # Other charts
    fig_ts = px.line(df, x="date", y="net sale", color="outlet name", title="Net Sale Over Time")
    outlet_sum = df.groupby("outlet name", as_index=False)["net sale"].sum()
    fig_outlet = px.bar(outlet_sum.sort_values("net sale", ascending=False), x="outlet name", y="net sale", title="Net Sale by Outlet")
    fig_region = px.bar(df.groupby("region", as_index=False)["net sale"].sum(), x="region", y="net sale", title="Net Sale by Region", color="region")

    # Tabs stats: net sale, avg per bill, avg per day
    df["date_only"] = df["date"].dt.date
    tabs_stats = (
        df.groupby("tabs")
          .agg({"net sale": "sum", "no of bills": "sum", "date_only": pd.Series.nunique})
          .reset_index()
    )
    tabs_stats["avg_sale_per_bill"] = tabs_stats.apply(lambda r: r["net sale"] / r["no of bills"] if r["no of bills"] else 0, axis=1)
    tabs_stats["avg_sale_per_day"] = tabs_stats.apply(lambda r: r["net sale"] / r["date_only"] if r["date_only"] else 0, axis=1)
    tabs_stats = tabs_stats.sort_values("net sale", ascending=False)

    fig_tabs = go.Figure()
    fig_tabs.add_trace(go.Bar(x=tabs_stats["tabs"], y=tabs_stats["net sale"], name="Net Sale"))
    fig_tabs.add_trace(go.Scatter(x=tabs_stats["tabs"], y=tabs_stats["avg_sale_per_bill"], name="Avg Sale per Bill", yaxis="y2", mode="lines+markers"))
    fig_tabs.add_trace(go.Scatter(x=tabs_stats["tabs"], y=tabs_stats["avg_sale_per_day"], name="Avg Sale per Day", yaxis="y2", mode="lines+markers", line=dict(dash="dot")))
    fig_tabs.update_layout(title="Tabs-wise Net Sale, Avg Sale per Bill & Avg Sale per Day", xaxis_title="Tabs", yaxis=dict(title="Net Sale"), yaxis2=dict(title="Average Values", overlaying="y", side="right"), legend=dict(orientation="h", y=-0.2), bargap=0.3)

    # Tax composition
    tax_cols = [c for c in ["gst @18%", "gst @5%", "ecom_gst@5%", "gst @40%"] if c in df.columns]
    if tax_cols:
        tax_sum = df[tax_cols].sum().reset_index()
        tax_sum.columns = ["tax_type", "amount"]
        fig_tax = px.pie(tax_sum, names="tax_type", values="amount", title="Tax Composition")
    else:
        fig_tax = px.pie(values=[1], names=["No Tax Columns"], title="Tax Composition (no tax cols found)")

    # Charge breakdown (stacked by available charge columns)
    charge_cols = ["restaurant charge", "packaging charge [cart - swiggy]", "restaurant packaging charges", "delivery charge", "platform fee charge", "smile amount charge"]
    available = [c for c in charge_cols if c in df.columns]
    if available:
        fig_charge = px.bar(df, x="outlet name", y=available, barmode="stack", title="Charge Breakdown by Outlet")
    else:
        fig_charge = px.bar(title="Charge Breakdown - no charge columns found")

    # Charge summary table (region x outlet totals)
    if available:
        charge_summary = df.groupby(["region", "outlet name"], as_index=False)[available + ["total charges"]].sum().round(2)
    else:
        charge_summary = pd.DataFrame(columns=["region", "outlet name", "total charges"])

    # Data table records (detail)
    table_data = df.to_dict("records")

    range_text = f"📅 Showing from {pd.to_datetime(start_date).strftime('%d-%m-%Y')} to {pd.to_datetime(end_date).strftime('%d-%m-%Y')}"
    status_text = f"Last Updated: {updated_str} | Rows: {len(df)} | Region: {region or 'All'}"

    # Return in the exact same order as Outputs declared above
    return (
        cards,                                 # kpi-cards.children
        range_text,                            # range-summary.children
        trend_fig,                             # trend-fig.figure
        top5_table,                            # top5-days.children
        fig_ts,                                # ts-sales.figure
        fig_outlet,                            # sales-by-outlet.figure
        fig_region,                            # sales-by-region.figure
        fig_tabs,                              # sales-by-tabs.figure
        outlet_summary.to_dict("records"),     # outlet-summary-table.data
        fig_tax,                               # tax-breakdown.figure
        fig_charge,                            # charge-breakdown.figure
        table_data,                            # detail-table.data
        charge_summary.to_dict("records"),     # charge-summary.data
        status_text                            # status-info.children
    )

# ----------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)
