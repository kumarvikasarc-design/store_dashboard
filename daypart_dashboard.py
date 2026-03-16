# daypart_dashboard_final_complete_resetA.py
# Final Daypart Dashboard — single Reset button (below filters) + safe master callback

import os
import json
from io import StringIO
from datetime import datetime

import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, Input, Output, State, dash_table

# -----------------------------
# CONFIG
# -----------------------------
BASE_DIR = r"C:\Users\ACER\store_dashboard"
DAYPART_FILE = os.path.join(BASE_DIR, "Daypart.xlsx")
STORE_CSV = os.path.join(BASE_DIR, "stores_db.csv")
AUTO_REFRESH_MS = 60 * 1000 # 60 seconds
LOGO_PATH = "/assets/logo.png" # optional: place logo.png into assets/ folder

# Chart heights (compact)
CHART_H = 230
PIE_H = 230
BAR_H = 230
HEAT_H = 230
TREND_H = 230
WW_H = 200

# -----------------------------
# DATA HELPERS
# -----------------------------
def safe_read_daypart():
    if not os.path.exists(DAYPART_FILE):
        cols = ["Outlet Name", "Date", "Hour", "Tab", "Sale", "Discount", "Net Sale", "NOB"]
        return pd.DataFrame(columns=cols)
    df = pd.read_excel(DAYPART_FILE, sheet_name="daypart", engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]

    for col in ["Outlet Name", "Date", "Hour", "Tab", "Sale", "Discount", "Net Sale", "NOB"]:
        if col not in df.columns:
            df[col] = 0 if col in ["Sale", "Discount", "Net Sale", "NOB"] else None

    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
    df["Hour"] = pd.to_numeric(df["Hour"], errors="coerce").fillna(0).astype(int)
    df["Month"] = df["Date"].dt.strftime("%b-%Y")
    df["Week"] = df["Date"].dt.isocalendar().week
    df["Year"] = df["Date"].dt.year
    df["Weekday"] = df["Date"].dt.weekday
    df["Day_Name"] = df["Date"].dt.day_name()

    df["Week Code"] = df["Weekday"].apply(lambda x: "WD" if x <= 3 else ("WF" if x == 4 else "WE"))

    def get_daypart(h):
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

    df["Daypart"] = df["Hour"].apply(get_daypart)
    df["IsWeekend"] = df["Weekday"].apply(lambda x: "Weekend" if x >= 5 else "Weekday")

    for col in ["Sale", "Discount", "Net Sale", "NOB"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["Outlet_clean"] = df["Outlet Name"].astype(str).str.strip().str.lower()
    return df

def safe_read_store_csv():
    if not os.path.exists(STORE_CSV):
        return pd.DataFrame(columns=["Outlet Name","Region","City","Type"])
    s = pd.read_csv(STORE_CSV)
    s.columns = [str(c).strip() for c in s.columns]
    for col in ["Outlet Name","Region","City","Type"]:
        if col not in s.columns:
            s[col] = None
    s["Outlet_clean"] = s["Outlet Name"].astype(str).str.strip().str.lower()
    return s

def prepare_merged_df():
    day = safe_read_daypart()
    store = safe_read_store_csv()
    merged = day.merge(store[["Outlet_clean","Region","City","Type"]], left_on="Outlet_clean", right_on="Outlet_clean", how="left")
    merged.drop(columns=["Outlet_clean"], inplace=True, errors="ignore")
    for c in ["Region","City","Type"]:
        if c not in merged.columns:
            merged[c] = None
    return merged

def empty_figure(title="No data"):
    fig = px.scatter()
    fig.update_layout(title=title, height=CHART_H, margin=dict(l=20,r=20,t=30,b=20))
    return fig

# -----------------------------
# DASH APP
# -----------------------------
app = Dash(__name__, suppress_callback_exceptions=True)
server = app.server

df = safe_read_daypart()

# -----------------------------
# LAYOUT
# -----------------------------
app.layout = html.Div(dir="ltr", style={"fontFamily":"Times New Roman","backgroundColor":"#F5F7FA","padding":"12px"}, children=[
    # Header
    html.Div(style={"textAlign":"center"}, children=[
        html.Img(src=LOGO_PATH, style={"height":"48px","display":"block","margin":"0 auto"}, hidden=False),
        html.H1("Daypart Report Dashboard", style={"margin":"6px 0","fontSize":"26px","fontWeight":"700"}),
        html.Div("Interactive Daypart & Store analytics", style={"fontSize":"12px","color":"#666"})
    ]),

    # top info
    html.Div(style={"display":"flex","justifyContent":"space-between","alignItems":"center","marginTop":"6px","marginBottom":"8px"}, children=[
        html.Div(id="week-display", style={"fontSize":"13px"}),
        html.Div(id="last-reload", style={"fontSize":"12px","color":"#666"})
    ]),

    # data storage + auto-refresh
    dcc.Store(id="df-store", data=df.to_dict('records')),
    dcc.Interval(id="interval-component", interval=AUTO_REFRESH_MS, n_intervals=0),

    # Filters area
    html.Div(style={"backgroundColor":"#fff","padding":"8px","borderRadius":"8px","boxShadow":"0 6px 14px rgba(0,0,0,0.06)","marginBottom":"10px"}, children=[
        html.Div(style={"display":"flex","gap":"8px","flexWrap":"wrap","alignItems":"center"}, children=[
            html.Div([html.Label("Region"), dcc.Dropdown(id="region-dd", multi=True)], style={"width":"18%"}),
            html.Div([html.Label("City"), dcc.Dropdown(id="city-dd", multi=True)], style={"width":"18%"}),
            html.Div([html.Label("Type"), dcc.Dropdown(id="type-dd", multi=True)], style={"width":"14%"}),
            html.Div([html.Label("Tab"), dcc.Dropdown(id="tab-dd", multi=True)], style={"width":"12%"}),
            html.Div([html.Label("Outlet"), dcc.Dropdown(id="outlet-dd", multi=True)], style={"width":"18%"}),
            html.Div([html.Label("Agg"), dcc.Dropdown(id="freq-dd", options=[{"label":"Weekly","value":"WEEKLY"},{"label":"Monthly","value":"MONTHLY"}], value="WEEKLY")], style={"width":"6%"}),
        ]),

        html.Div(style={"display":"flex","gap":"8px","marginTop":"8px","flexWrap":"wrap","alignItems":"center"}, children=[
            html.Div([html.Label("Month"), dcc.Dropdown(id="month-dd", multi=True)], style={"width":"22%"}),
            html.Div([html.Label("Week"), dcc.Dropdown(id="week-dd", multi=True)], style={"width":"18%"}),
            html.Div([html.Label("Daypart"), dcc.Dropdown(id="daypart-dd", multi=True)], style={"width":"22%"}),
            
            # --- DATE RANGE WITH BORDER STYLING (CORRECTED) ---
            html.Div(style={"width":"22%"}, children=[
                html.Label("Date Range"), 
                dcc.DatePickerRange(
                    id="date-range", 
                    # Use style to mimic the dropdown border
                    style={
                        'border': '1px solid #ccc',
                        'borderRadius': '2px',
                        'width': '90%',
                        'display': 'flex',
                        'alignItems': 'center',
                        'padding': '1px 0',
                        'height': '38px' 
                    }
                )
            ]),
            # --------------------------------------------------
        ]),

        # Reset button (Option 2: below all filters)
        html.Div(style={"display":"flex","justifyContent":"flex-start","marginTop":"10px"}, children=[
            html.Button("Reset", id="reset-all-btn", n_clicks=0, style={"background":"#b80000","color":"white","padding":"8px 16px","borderRadius":"6px"})
        ])
    ]),

    # KPI cards
    html.Div(style={"display":"flex","gap":"10px","marginBottom":"10px"}, children=[
        html.Div(id="kpi-net", style={"flex":"1","background":"linear-gradient(90deg,#6dd3ff,#4aa0ff)","color":"white","padding":"10px","borderRadius":"6px"}),
        html.Div(id="kpi-bills", style={"flex":"1","background":"linear-gradient(90deg,#ffd36d,#ff9a4a)","color":"white","padding":"10px","borderRadius":"6px"}),
        html.Div(id="kpi-abv", style={"flex":"1","background":"linear-gradient(90deg,#b992ff,#7f6bff)","color":"white","padding":"10px","borderRadius":"6px"}),
    ]),

    # Tables
    html.Div(style={"display":"flex","gap":"10px","marginBottom":"10px"}, children=[
        html.Div(style={"width":"50%","backgroundColor":"#fff","padding":"8px","borderRadius":"8px","boxShadow":"0 3px 10px rgba(0,0,0,0.06)"}, children=[
            html.H3("Outlet Summary", style={"fontSize":"20px","fontWeight":"700","textAlign":"center"}),
            dash_table.DataTable(id="summary-table", page_size=6,
                                 style_table={"overflowX":"auto"},
                                 style_header={"fontFamily":"Times New Roman","fontSize":"14px","fontWeight":"700","textAlign":"center"},
                                 style_cell={"fontFamily":"Times New Roman","fontSize":"12px","textAlign":"left","whiteSpace":"normal"})
        ]),
        html.Div(style={"width":"50%","backgroundColor":"#fff","padding":"8px","borderRadius":"8px","boxShadow":"0 3px 10px rgba(0,0,0,0.06)"}, children=[
            html.H3("Daypart Summary", style={"fontSize":"20px","fontWeight":"700","textAlign":"center"}),
            dash_table.DataTable(id="daypart-summary", page_size=6,
                                 style_table={"overflowX":"auto"},
                                 style_header={"fontFamily":"Times New Roman","fontSize":"14px","fontWeight":"700","textAlign":"center"},
                                 style_cell={"fontFamily":"Times New Roman","fontSize":"12px","textAlign":"left","whiteSpace":"normal"})
        ]),
    ]),

    # Charts grid
    html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr","gap":"10px","marginBottom":"10px"}, children=[
        html.Div(style={"backgroundColor":"#fff","padding":"8px","borderRadius":"8px","boxShadow":"0 6px 14px rgba(0,0,0,0.06)"}, children=[
            html.H4("Net Sale Trend", style={"fontFamily":"Times New Roman","textAlign":"left"}),
            dcc.Graph(id="net-sale-trend", config={"displayModeBar": False}, style={"height": f"{TREND_H}px"})
        ]),
        html.Div(style={"backgroundColor":"#fff","padding":"8px","borderRadius":"8px","boxShadow":"0 6px 14px rgba(0,0,0,0.06)"}, children=[
            html.H4("Hourly Net Sale", style={"fontFamily":"Times New Roman","textAlign":"left"}),
            dcc.Graph(id="hourly-daypart", config={"displayModeBar": False}, style={"height": f"{CHART_H}px"})
        ]),
    ]),

    html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr","gap":"10px","marginBottom":"10px"}, children=[
        html.Div(style={"backgroundColor":"#fff","padding":"8px","borderRadius":"8px","boxShadow":"0 6px 14px rgba(0,0,0,0.06)"}, children=[
            html.H4("Daypart Contribution", style={"fontFamily":"Times New Roman","textAlign":"left"}),
            dcc.Graph(id="daypart-pie", config={"displayModeBar": False}, style={"height": f"{PIE_H}px"})
        ]),
        html.Div(style={"backgroundColor":"#fff","padding":"8px","borderRadius":"8px","boxShadow":"0 6px 14px rgba(0,0,0,0.06)"}, children=[
            html.H4("Net Sale by Daypart", style={"fontFamily":"Times New Roman","textAlign":"left"}),
            dcc.Graph(id="daypart-bar", config={"displayModeBar": False}, style={"height": f"{BAR_H}px"})
        ]),
    ]),

    html.Div(style={"display":"grid","gridTemplateColumns":"1fr 1fr","gap":"10px","marginBottom":"10px"}, children=[
        html.Div(style={"backgroundColor":"#fff","padding":"8px","borderRadius":"8px","boxShadow":"0 6px 14px rgba(0,0,0,0.06)"}, children=[
            html.H4("Heatmap: Day vs Hour", style={"fontFamily":"Times New Roman","textAlign":"left"}),
            dcc.Graph(id="heatmap-day-hour", config={"displayModeBar": False}, style={"height": f"{HEAT_H}px"})
        ]),
        html.Div(style={"backgroundColor":"#fff","padding":"8px","borderRadius":"8px","boxShadow":"0 6px 14px rgba(0,0,0,0.06)"}, children=[
            html.H4("Heatmap: Day vs Daypart", style={"fontFamily":"Times New Roman","textAlign":"left"}),
            dcc.Graph(id="heatmap-day-daypart", config={"displayModeBar": False}, style={"height": f"{HEAT_H}px"})
        ]),
    ]),

    html.Div(style={"backgroundColor":"#fff","padding":"8px","borderRadius":"8px","boxShadow":"0 6px 14px rgba(0,0,0,0.06)","marginBottom":"8px"}, children=[
        html.H4("Weekday vs Weekend", style={"fontFamily":"Times New Roman","textAlign":"left"}),
        dcc.Graph(id="weekday-weekend-comp", config={"displayModeBar": False}, style={"height": f"{WW_H}px"})
    ]),

    html.Div(style={"textAlign":"center","color":"#666","marginTop":"8px"}, children=[
        html.Small("Dashboard auto-refreshes every 60s.")
    ]),
    html.Div(style={
    "backgroundColor": "#fff",
    "padding": "8px",
    "borderRadius": "8px",
    "boxShadow": "0 6px 14px rgba(0,0,0,0.06)",
    "marginBottom": "10px"}, children=[
    html.H4("Week Code Report (WD / WF / WE)", 
             style={"fontFamily":"Times New Roman","textAlign":"left"}),
    dcc.Graph(id="week-code-chart", config={"displayModeBar": False})
]),

])

# -----------------------------
# Refresh callback: reload files + last-reload + week-display
# -----------------------------
@app.callback(
    Output("df-store", "data"),
    Output("last-reload", "children"),
    Output("week-display", "children"),
    Input("interval-component", "n_intervals"),
    Input("date-range", "start_date"),
    Input("date-range", "end_date"),
)
def refresh_and_week(n_intervals, start_date, end_date):
    try:
        merged = prepare_merged_df()
        json_data = merged.to_json(orient="records", date_format="iso")
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        last_text = f"Last refreshed: {ts}"
    except Exception as e:
        json_data = json.dumps([])
        last_text = f"Error refreshing: {e}"

    if start_date and end_date:
        s = pd.to_datetime(start_date)
        e = pd.to_datetime(end_date)
        week_num = s.isocalendar().week
        week_text = f"Week {week_num}: {s.strftime('%d %b %Y')} – {e.strftime('%d %b %Y')}"
    else:
        week_text = "Week: — (select date range)"

    return json_data, last_text, week_text

# -----------------------------
# Reset All Filters callback (ONLY clears filters; DOES NOT set week-dd.value)
# -----------------------------
@app.callback(
    Output("region-dd", "value"),
    Output("city-dd", "value"),
    Output("type-dd", "value"),
    Output("outlet-dd", "value"),
    Output("tab-dd", "value"),
    Output("month-dd", "value"),
    Output("daypart-dd", "value"),
    Output("date-range", "start_date"),
    Output("date-range", "end_date"),
    Input("reset-all-btn", "n_clicks"),
    prevent_initial_call=True
)
def reset_all(n_clicks):
    # Clear all filters; week value is not directly returned here
    return [], [], [], [], [], [], [], None, None

# -----------------------------
# Master callback: single callback with safe return wrapper
# -----------------------------
@app.callback(
    Output("summary-table", "data"),
    Output("summary-table", "columns"),
    Output("daypart-summary", "data"),
    Output("daypart-summary", "columns"),
    Output("kpi-net", "children"),
    Output("kpi-bills", "children"),
    Output("kpi-abv", "children"),
    Output("net-sale-trend", "figure"),
    Output("hourly-daypart", "figure"),
    Output("daypart-pie", "figure"),
    Output("daypart-bar", "figure"),
    Output("heatmap-day-hour", "figure"),
    Output("heatmap-day-daypart", "figure"),
    Output("weekday-weekend-comp", "figure"),
    Output("region-dd", "options"),
    Output("city-dd", "options"),
    Output("type-dd", "options"),
    Output("tab-dd", "options"),
    Output("month-dd", "options"),
    Output("week-dd", "options"),
    Output("daypart-dd", "options"),
    Output("outlet-dd", "options"),
    Output("week-dd", "value"),
    Output("week-code-chart", "figure"),

    Input("df-store", "data"),
    Input("region-dd", "value"),
    Input("city-dd", "value"),
    Input("type-dd", "value"),
    Input("tab-dd", "value"),
    Input("freq-dd", "value"),
    Input("outlet-dd", "value"),
    Input("month-dd", "value"),
    Input("week-dd", "value"),
    Input("daypart-dd", "value"),
    Input("date-range", "start_date"),
    Input("date-range", "end_date"),
    Input("net-sale-trend", "clickData"),
    Input("daypart-bar", "clickData"),
)
def master_update(df_json, region_sel, city_sel, type_sel, tab_sel, freq, outlet_sel, month_sel, week_sel, daypart_sel, start_date, end_date, trend_click, daypart_click):
    try:
        if not df_json:
            raise ValueError("No data in df-store")

        df = pd.read_json(StringIO(df_json), orient="records")
        base = df.copy()

        # Region options
        region_opts = [{"label": r, "value": r} for r in sorted(base["Region"].dropna().unique())]

        # Cascading options
        opt_base = base.copy()
        if region_sel:
            opt_base = opt_base[opt_base["Region"].isin(region_sel)]
        city_opts = [{"label": c, "value": c} for c in sorted(opt_base["City"].dropna().unique())]

        if city_sel:
            opt_base = opt_base[opt_base["City"].isin(city_sel)]
        type_opts = [{"label": t, "value": t} for t in sorted(opt_base["Type"].dropna().unique())]

        if type_sel:
            opt_base = opt_base[opt_base["Type"].isin(type_sel)]
        tab_opts = [{"label": t, "value": t} for t in sorted(opt_base["Tab"].dropna().unique())]

        month_opts = [{"label": m, "value": m} for m in sorted(df["Month"].dropna().unique())]
        daypart_opts = [{"label": d, "value": d} for d in sorted(df["Daypart"].dropna().unique())]

        outlet_base = df.copy()
        if region_sel:
            outlet_base = outlet_base[outlet_base["Region"].isin(region_sel)]
        if city_sel:
            outlet_base = outlet_base[outlet_base["City"].isin(city_sel)]
        if type_sel:
            outlet_base = outlet_base[outlet_base["Type"].isin(type_sel)]
        outlet_opts = [{"label": o, "value": o} for o in sorted(outlet_base["Outlet Name"].dropna().unique())]

        # Apply filters to create filtered dataset
        filtered = df.copy()
        if region_sel:
            filtered = filtered[filtered["Region"].isin(region_sel)]
        if city_sel:
            filtered = filtered[filtered["City"].isin(city_sel)]
        if type_sel:
            filtered = filtered[filtered["Type"].isin(type_sel)]
        if tab_sel:
            filtered = filtered[filtered["Tab"].isin(tab_sel)]
        if outlet_sel:
            filtered = filtered[filtered["Outlet Name"].isin(outlet_sel)]
        if month_sel:
            filtered = filtered[filtered["Month"].isin(month_sel)]

        # Date range filter with clear handling
        if start_date is None or end_date is None:
            date_filtered = False
        else:
            sd = pd.to_datetime(start_date)
            ed = pd.to_datetime(end_date)
            filtered = filtered[(filtered["Date"] >= sd) & (filtered["Date"] <= ed)]
            date_filtered = True

        # Week options (based on filtered)
        week_vals = sorted(filtered["Week"].dropna().unique())
        week_opts = [{"label": int(w), "value": int(w)} for w in week_vals]

        # Apply week filter regardless of date range
        if week_sel:
            try:
                wlist = [int(w) for w in week_sel]
                filtered = filtered[filtered["Week"].isin(wlist)]
            except Exception:
                pass
        else:
            week_sel = []

        if daypart_sel:
            filtered = filtered[filtered["Daypart"].isin(daypart_sel)]
            
        # Drill-down behavior
        drill_df = filtered.copy()
        if daypart_click:
            try:
                dp = daypart_click["points"][0].get("x")
                if dp in drill_df["Daypart"].unique():
                    drill_df = drill_df[drill_df["Daypart"] == dp]
            except Exception:
                pass
        elif trend_click:
            try:
                xval = trend_click["points"][0].get("x")
                if xval in filtered["Outlet Name"].unique():
                    drill_df = drill_df[drill_df["Outlet Name"] == xval]
            except Exception:
                pass
        
        # Week Code Report
        try:
            if drill_df.empty:
                fig_wc = empty_figure("No Week Code Data")
            else:
                wc = drill_df.groupby("Week Code", as_index=False).agg(
                    Net_Sale=("Net Sale", "sum"),
                    Bills=("NOB", "sum")
                )
                fig_wc = px.bar(
                    wc, x="Week Code", y="Net_Sale",
                    text="Net_Sale",
                    title="Performance by Week Code"
                )
                fig_wc.update_traces(texttemplate="%{y:.2f}")
                fig_wc.update_layout(
                    height=260,
                    margin=dict(l=30, r=30, t=34, b=34),
                    title_font_size=14,
                    bargap=0.40
                )
        except Exception:
            fig_wc = empty_figure("Week Code Error")

        # KPIs
        total_net = float(drill_df["Net Sale"].sum()) if not drill_df.empty else 0.0
        total_bills = int(drill_df["NOB"].sum()) if not drill_df.empty else 0
        avg_bill = (total_net / total_bills) if total_bills > 0 else 0.0

        kpi_net = html.Div([html.Div("Total Net Sale", style={"fontSize":"12px","opacity":0.9}),
                            html.Div(f"₹ {total_net:,.2f}", style={"fontSize":"18px","fontWeight":"700","marginTop":"6px"})])
        kpi_bills = html.Div([html.Div("Total Bills", style={"fontSize":"12px","opacity":0.9}),
                              html.Div(f"{total_bills}", style={"fontSize":"18px","fontWeight":"700","marginTop":"6px"})])
        kpi_abv = html.Div([html.Div("Average Bill (ABV)", style={"fontSize":"12px","opacity":0.9}),
                            html.Div(f"₹ {avg_bill:,.2f}", style={"fontSize":"18px","fontWeight":"700","marginTop":"6px"})])

        # Summary table
        if drill_df.empty:
            summary = pd.DataFrame(columns=["Outlet Name","Total_Sale","Total_Discount","Total_Net_Sale","Total_NOB"])
        else:
            summary = drill_df.groupby("Outlet Name", as_index=False).agg(
                Total_Sale=("Sale","sum"),
                Total_Discount=("Discount","sum"),
                Total_Net_Sale=("Net Sale","sum"),
                Total_NOB=("NOB","sum")
            ).sort_values("Total_Net_Sale", ascending=False)
            for c in ["Total_Sale","Total_Discount","Total_Net_Sale"]:
                summary[c] = summary[c].round(2)
        summary_cols = [{"name": c, "id": c} for c in summary.columns]
        summary_data = summary.to_dict("records")

        # Daypart summary
        if drill_df.empty:
            dp = pd.DataFrame(columns=["Daypart","Sale","Discount","Net_Sale","Bills","Contribution %"])
        else:
            dp = drill_df.groupby("Daypart", as_index=False).agg(
                Sale=("Sale","sum"),
                Discount=("Discount","sum"),
                Net_Sale=("Net Sale","sum"),
                Bills=("NOB","sum")
            )
            for c in ["Sale","Discount","Net_Sale"]:
                dp[c] = dp[c].round(2)
            total = dp["Net_Sale"].sum() if not dp.empty else 0
            dp["Contribution %"] = (dp["Net_Sale"] / total * 100).round(2) if total != 0 else 0
            dp = dp.sort_values("Net_Sale", ascending=False)
        dp_cols = [{"name": c, "id": c} for c in dp.columns]
        dp_data = dp.to_dict("records")

        # Charts
        try:
            if drill_df.empty:
                fig_trend = empty_figure("No Trend Data")
            else:
                if freq == "MONTHLY":
                    trend = drill_df.groupby("Month", as_index=False).agg(Total_Net_Sale=("Net Sale","sum"))
                    fig_trend = px.bar(trend, x="Month", y="Total_Net_Sale", title="Monthly Net Sale")
                else:
                    trend = drill_df.groupby(["Year","Week"], as_index=False).agg(Total_Net_Sale=("Net Sale","sum"))
                    trend["Label"] = trend["Year"].astype(str) + "-W" + trend["Week"].astype(str)
                    fig_trend = px.bar(trend, x="Label", y="Total_Net_Sale", title="Weekly Net Sale")
                fig_trend.update_traces(texttemplate="%{y:.2f}", textfont_size=9)
                fig_trend.update_layout(height=TREND_H, margin=dict(l=20, r=20, t=34, b=30), title_font_size=12, bargap=0.28)
        except Exception:
            fig_trend = empty_figure("Trend error")

        try:
            if drill_df.empty:
                fig_hour = empty_figure("No Hourly Data")
            else:
                hourly = drill_df.groupby("Hour", as_index=False).agg(Net_Sale=("Net Sale","sum"))
                hourly["Net_Sale"] = hourly["Net_Sale"].round(2)
                fig_hour = px.bar(hourly, x="Hour", y="Net_Sale", title="Hourly Net Sale")
                fig_hour.update_traces(texttemplate="%{y:.2f}", textfont_size=9, marker_color="#ff9a4a")
                fig_hour.update_layout(height=CHART_H, margin=dict(l=20, r=20, t=34, b=30), title_font_size=12)
        except Exception:
            fig_hour = empty_figure("Hourly error")

        # Pie & Bar
        try:
            if dp.empty:
                fig_pie = empty_figure("No Data")
                fig_bar = empty_figure("No Data")
            else:
                fig_pie = px.pie(dp, names="Daypart", values="Net_Sale", title="Daypart Contribution")
                fig_pie.update_traces(hole=0.45, textinfo="percent+label", textfont_size=10)
                fig_pie.update_layout(height=PIE_H, margin=dict(l=10, r=60, t=30, b=12), title_font_size=12,
                                     legend=dict(orientation="v", font_size=9, yanchor="middle", y=0.5, xanchor="left", x=1.02))

                fig_bar = px.bar(dp, x="Daypart", y="Net_Sale", text="Net_Sale", title="Net Sale by Daypart")
                fig_bar.update_traces(texttemplate="%{y:.2f}", textfont_size=9, marker_color="#4aa0ff")
                fig_bar.update_layout(height=BAR_H, margin=dict(l=20, r=20, t=30, b=30), title_font_size=12, bargap=0.28)
        except Exception:
            fig_pie = empty_figure("Pie error")
            fig_bar = empty_figure("Bar error")

        # Heatmaps
        try:
            if drill_df.empty:
                heat_day_hour = empty_figure("No Data")
            else:
                temp = drill_df.copy()
                temp["DayName"] = temp["Date"].dt.day_name()
                order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
                hh = temp.groupby(["DayName","Hour"], as_index=False).agg(Net_Sale=("Net Sale","sum"))
                pivot_h = hh.pivot(index="DayName", columns="Hour", values="Net_Sale").reindex(order).fillna(0)
                heat_day_hour = px.imshow(pivot_h.values, labels=dict(x="Hour", y="Day", color="Net Sale"),
                                         x=pivot_h.columns, y=pivot_h.index, aspect="auto", title="Heatmap (Day vs Hour)")
                heat_day_hour.update_layout(height=HEAT_H, margin=dict(l=20, r=20, t=30, b=20), title_font_size=12)
        except Exception:
            heat_day_hour = empty_figure("Heatmap error")

        try:
            if drill_df.empty:
                heat_day_dp = empty_figure("No Data")
            else:
                temp = drill_df.copy()
                temp["DayName"] = temp["Date"].dt.day_name()
                order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
                hh2 = temp.groupby(["DayName","Daypart"], as_index=False).agg(Net_Sale=("Net Sale","sum"))
                pivot_dp = hh2.pivot(index="DayName", columns="Daypart", values="Net_Sale").reindex(order).fillna(0)
                dp_order = ["Pre Breakfast","Breakfast","Lunch","Snack","Dinner","Late Night"]
                cols_present = [c for c in dp_order if c in pivot_dp.columns] + [c for c in pivot_dp.columns if c not in dp_order]
                pivot_dp = pivot_dp[cols_present]
                heat_day_dp = px.imshow(pivot_dp.values, labels=dict(x="Daypart", y="Day", color="Net Sale"),
                                        x=pivot_dp.columns, y=pivot_dp.index, aspect="auto", title="Heatmap (Day vs Daypart)")
                heat_day_dp.update_layout(height=HEAT_H, margin=dict(l=20, r=20, t=30, b=20), title_font_size=12)
        except Exception:
            heat_day_dp = empty_figure("Heatmap error")

        # Weekday vs Weekend
        try:
            if drill_df.empty:
                fig_ww = empty_figure("No Data")
            else:
                comp = drill_df.groupby("IsWeekend", as_index=False).agg(Net_Sale=("Net Sale","sum"), Bills=("NOB","sum"))
                fig_ww = px.bar(comp, x="IsWeekend", y="Net_Sale", text="Net_Sale", title="Weekday vs Weekend Net Sale")
                fig_ww.update_traces(texttemplate="%{y:.2f}", textfont_size=9)
                fig_ww.update_layout(height=WW_H, margin=dict(l=20, r=20, t=28, b=28), title_font_size=12, bargap=0.36)
        except Exception:
            fig_ww = empty_figure("WW error")

        # Return values (must match Output order)
        return (
            summary_data, summary_cols,
            dp_data, dp_cols,
            kpi_net, kpi_bills, kpi_abv,
            fig_trend, fig_hour, fig_pie, fig_bar, heat_day_hour, heat_day_dp, fig_ww,
            region_opts, city_opts, type_opts, tab_opts, month_opts, week_opts, daypart_opts, outlet_opts,
            week_sel,
            fig_wc
        )

    except Exception as e:
        # SAFE fallback — never return None and preserve expected output count
        print("Master callback error:", e)
        empty_fig = px.scatter()
        empty_fig.update_layout(height=CHART_H, margin=dict(l=20,r=20,t=30,b=20))
        empty_cols = [{"name": "NoData", "id": "NoData"}]
        return (
            [], empty_cols,
            [], empty_cols,
            html.Div("₹ 0.00"), html.Div("0"), html.Div("₹ 0.00"),
            empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig,
            [], [], [], [], [], [], [], [], [],
            empty_fig # for fig_wc
        )

# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    print("Starting Daypart Dashboard (full optimized). Visit http://127.0.0.1:8050")
    app.run(host="0.0.0.0", port=8050, debug=True)