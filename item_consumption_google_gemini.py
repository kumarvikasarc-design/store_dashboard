import pandas as pd
import os
from dash import Dash, dcc, html, Input, Output, State, dash_table, no_update
from datetime import date
import json
import plotly.express as px

# ===========================================================
# 1️⃣ CONFIGURATION & PATHS
# ===========================================================
analysis_folder = r"C:\Users\ACER\store_dashboard\item_source"
recipe_folder   = r"C:\Users\ACER\store_dashboard\inventory\recipe"
stores_db_path  = r"C:\Users\ACER\store_dashboard\stores_db.csv"
stock_master_path = r"C:\Users\ACER\store_dashboard\inventory\stockitem_master.csv"

def load_static_masters():
    """Loads masters that don't change as frequently as sales."""
    sm = pd.read_csv(stores_db_path, encoding="utf-8")
    sk = pd.read_csv(stock_master_path, encoding="utf-8")
    sk.columns = sk.columns.str.strip()
    sk['Item Name Clean'] = sk['Item Name'].str.lower().str.strip()
    return sm, sk

# Load initial masters
store_master, stock_master = load_static_masters()

# Global tracking for smart-reload
DATA_CACHE = None
LAST_MTIME = 0

TODAY = pd.Timestamp.today()
YESTERDAY = TODAY - pd.Timedelta(days=1)

# Detect Outlet Column in Master
possible_outlet_cols = ['Outlet', 'Outlet Name', 'Store', 'Store Name', 'Deployment Name', 'Outlet_Name']
OUTLET_COL = next((c for c in possible_outlet_cols if c in store_master.columns), None)

# ===========================================================
# 2️⃣ HELPER FUNCTIONS
# ===========================================================

def get_latest_mtime():
    """Checks the last modified time of all files in source folders."""
    files = []
    for folder in [analysis_folder, recipe_folder]:
        if os.path.exists(folder):
            files.extend([os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".csv")])
    return max([os.path.getmtime(f) for f in files]) if files else 0

def parse_recipes(recipes):
    parsed = []
    current_recipe = None
    for _, row in recipes.iterrows():
        header = row['Recipe Name']
        if pd.notna(header) and str(header).strip() != "":
            current_recipe = str(header).strip()
            continue
        if pd.notna(row['Item Name']) and str(row['Item Name']).strip() != "":
            parsed.append({
                "Recipe Name": current_recipe,
                "Ingredient Name": row['Item Name'],
                "Ingredient Qty": row['Item Qty'],
                "Ingredient UOM": row['Item Unit'],
            })
    return pd.DataFrame(parsed)

UOM_PLURAL = {'KG': ('Kg', 'Kgs'), 'G': ('g', 'g'), 'L': ('L', 'L'), 'ML': ('ml', 'ml'), 'PCS': ('Pc', 'Pcs'), 'NOS': ('No', 'Nos'), 'BOX': ('Box', 'Boxes')}
UOM_DECIMALS = {"KG": 3, "KGS": 3, "G": 3, "L": 3, "ML": 3, "PCS": 0, "NOS": 0}

def apply_qty_formatter(row, qty_mode, QTY_COL, UOM_COL):
    qty = row[QTY_COL]
    uom = str(row[UOM_COL]).upper()
    decimals = UOM_DECIMALS.get(uom, 2)
    qty_rounded = round(qty, decimals)
    singular, plural = UOM_PLURAL.get(uom, (uom, uom))
    display_uom = singular if qty_rounded == 1 else plural
    return f"{qty_rounded:.{decimals}f} {display_uom}"

def get_selected_outlets(brand, region, state, city, type_, outlet):
    dff = store_master.copy()
    if brand: dff = dff[dff['Brand'] == brand]
    if region: dff = dff[dff['Region'] == region]
    if state: dff = dff[dff['State'] == state]
    if city: dff = dff[dff['City'] == city]
    if type_: dff = dff[dff['Type'] == type_]
    if outlet: dff = dff[dff[OUTLET_COL] == outlet]
    return dff[OUTLET_COL].dropna().unique()

# ===========================================================
# 3️⃣ DATA PROCESSING (HEAVY LIFTER)
# ===========================================================

def prepare_final_data():
    """Scans folders, merges data, and calculates consumption."""
    analysis_files = [os.path.join(analysis_folder, f) for f in os.listdir(analysis_folder) if f.endswith(".csv")]
    recipe_files = [os.path.join(recipe_folder, f) for f in os.listdir(recipe_folder) if f.endswith(".csv")]
    
    if not analysis_files or not recipe_files: return pd.DataFrame()

    analysis = pd.concat((pd.read_csv(f) for f in analysis_files), ignore_index=True)
    recipe_raw = pd.concat((pd.read_csv(f) for f in recipe_files), ignore_index=True)
    recipe_expanded = parse_recipes(recipe_raw)

    analysis['Item Name Clean'] = analysis['Item Name'].astype(str).str.lower().str.strip()
    recipe_expanded['Recipe Name Clean'] = recipe_expanded['Recipe Name'].astype(str).str.lower().str.strip()
    recipe_expanded['Ingredient Name Clean'] = recipe_expanded['Ingredient Name'].astype(str).str.lower().str.strip()

    merged = analysis.merge(recipe_expanded, left_on='Item Name Clean', right_on='Recipe Name Clean', how='left')
    merged = merged.merge(stock_master[['Item Name Clean', 'Purchase UOM', 'Base UOM', 'Conversion Factor']], 
                          left_on='Ingredient Name Clean', right_on='Item Name Clean', how='left')

    merged['Ingredient Qty'] = pd.to_numeric(merged['Ingredient Qty'], errors='coerce').fillna(0)
    merged['Item Qty'] = pd.to_numeric(merged['Item Qty'], errors='coerce').fillna(0)
    merged['Conversion Factor'] = pd.to_numeric(merged['Conversion Factor'], errors='coerce').fillna(1)

    merged['Consumed Qty Purchase UOM'] = merged['Item Qty'] * merged['Ingredient Qty']
    merged['Consumed Qty Base UOM'] = merged['Consumed Qty Purchase UOM'] * merged['Conversion Factor']

    date_col = next((c for c in ['Bill Date', 'BillDate', 'Business Date', 'Date'] if c in merged.columns), None)
    merged['Bill Date'] = pd.to_datetime(merged[date_col], errors='coerce') if date_col else pd.NaT
    
    merged['FY'] = merged['Bill Date'].apply(lambda x: f"{x.year}-{x.year+1}" if pd.notna(x) and x.month >= 4 else f"{x.year-1}-{x.year}" if pd.notna(x) else None)
    merged['Month'] = merged['Bill Date'].dt.month_name()
    merged['Week'] = merged['Bill Date'].dt.isocalendar().week

    return merged

# ===========================================================
# 4️⃣ DASHBOARD LAYOUT
# ===========================================================
app = Dash(__name__)

app.layout = html.Div([
    dcc.Interval(id='interval-component', interval=5*60*1000, n_intervals=0),
    dcc.Store(id='full-data-storage'),
    
    html.H2("Store Consumption Dashboard", style={"textAlign": "center"}),
    html.P(id='last-update-text', style={"textAlign": "center", "color": "gray"}),

    html.Div([
        html.Div([
            dcc.Dropdown(id="f_brand", placeholder="Brand"),
            dcc.Dropdown(id="f_region", placeholder="Region"),
            dcc.Dropdown(id="f_state", placeholder="State"),
            dcc.Dropdown(id="f_city", placeholder="City"),
            dcc.Dropdown(id="f_type", placeholder="Type"),
            dcc.Dropdown(id="f_outlet", placeholder="Outlet"),
        ], style={"display": "grid", "gridTemplateColumns": "repeat(6, 1fr)", "gap": "8px"}),
        
        html.Div([
            dcc.DatePickerRange(id="f_date", start_date=TODAY.replace(day=1), end_date=YESTERDAY, display_format="DD-MM-YYYY"),
            dcc.Dropdown(id="f_fy", placeholder="FY"),
            dcc.Dropdown(id="f_month", placeholder="Month"),
            dcc.Dropdown(id="f_week", placeholder="Week"),
            dcc.Dropdown(id="f_item", placeholder="Item Sold"),
            dcc.Dropdown(id="f_ing", placeholder="Ingredient"),
        ], style={"display": "grid", "gridTemplateColumns": "2fr 1fr 1fr 1fr 2fr 2fr", "gap": "8px", "marginTop": "10px"}),
    ], style={"padding": "10px", "borderBottom": "1px solid #ddd"}),

    dcc.RadioItems(id="qty_mode", options=[{"label": "Purchase Qty", "value": "PURCHASE"}, {"label": "Base Qty", "value": "BASE"}], value="PURCHASE", inline=True, style={"margin": "10px"}),
    
    html.Div(id="kpi_container", style={"display": "grid", "gridTemplateColumns": "repeat(3, 1fr)", "gap": "15px", "margin": "10px"}),

    dcc.Graph(id="total_consumption_graph"),
    dcc.Graph(id="outlet_consumption_graph"),
    dcc.Graph(id="item_consumption_graph"),

    html.H4("Ingredient Consumption Data"),
    dash_table.DataTable(id="tbl_total", page_size=15, style_table={"overflowX": "auto"}),
    dash_table.DataTable(id="tbl_outlet", page_size=15, style_table={"overflowX": "auto"})
])

# ===========================================================
# 5️⃣ CALLBACKS (THE AUTO-RELOAD ENGINE)
# ===========================================================

@app.callback(
    Output('full-data-storage', 'data'),
    Output('last-update-text', 'children'),
    Input('interval-component', 'n_intervals'),
)
def auto_sync_data(n):
    global DATA_CACHE, LAST_MTIME
    latest = get_latest_mtime()
    
    # Reload only if files changed OR first run
    if DATA_CACHE is None or latest > LAST_MTIME:
        print(f"🔄 Changes detected. Reloading CSVs...")
        DATA_CACHE = prepare_final_data()
        LAST_MTIME = latest
        ts = pd.Timestamp.now().strftime("%H:%M:%S")
        return DATA_CACHE.to_json(date_format='iso', orient='split'), f"Last Sync: {ts} (Auto-update enabled)"
    
    return no_update, no_update

@app.callback(
    [Output("tbl_total", "data"), Output("tbl_total", "columns"),
     Output("tbl_outlet", "data"), Output("tbl_outlet", "columns"),
     Output("total_consumption_graph", "figure"), Output("outlet_consumption_graph", "figure"),
     Output("item_consumption_graph", "figure"), Output("kpi_container", "children")],
    [Input("full-data-storage", "data"), Input("qty_mode", "value"),
     Input("f_brand", "value"), Input("f_region", "value"), Input("f_state", "value"), 
     Input("f_city", "value"), Input("f_type", "value"), Input("f_outlet", "value"),
     Input("f_date", "start_date"), Input("f_date", "end_date"),
     Input("f_item", "value"), Input("f_ing", "value")]
)
def update_all_components(json_data, qty_mode, brand, region, state, city, type_, outlet, start_date, end_date, f_item, f_ing):
    if not json_data: return [], [], [], [], {}, {}, {}, []
    
    df = pd.read_json(json_data, orient='split')
    df['Bill Date'] = pd.to_datetime(df['Bill Date'])
    S_OUTLET = next(c for c in possible_outlet_cols if c in df.columns)
    
    QTY_COL = "Consumed Qty Base UOM" if qty_mode == "BASE" else "Consumed Qty Purchase UOM"
    UOM_COL = "Base UOM" if qty_mode == "BASE" else "Purchase UOM"
    
    outlets = get_selected_outlets(brand, region, state, city, type_, outlet)
    filtered = df[df[S_OUTLET].isin(outlets)]
    if start_date and end_date:
        filtered = filtered[(filtered['Bill Date'] >= start_date) & (filtered['Bill Date'] <= end_date)]
    if f_item: filtered = filtered[filtered['Item Name'] == f_item]
    if f_ing: filtered = filtered[filtered['Ingredient Name'] == f_ing]

    if filtered.empty: return [], [], [], [], {}, {}, {}, [html.Div("No Data Found")]

    # Tables logic
    total = filtered.groupby(['Ingredient Name', UOM_COL], as_index=False)[QTY_COL].sum()
    total['Qty'] = total.apply(lambda r: apply_qty_formatter(r, qty_mode, QTY_COL, UOM_COL), axis=1)
    
    outlet_agg = filtered.groupby([S_OUTLET, 'Ingredient Name', UOM_COL], as_index=False)[QTY_COL].sum()
    outlet_agg['Qty'] = outlet_agg.apply(lambda r: apply_qty_formatter(r, qty_mode, QTY_COL, UOM_COL), axis=1)

    # Graphs
    fig1 = px.bar(total, x="Ingredient Name", y=QTY_COL, title="Total Consumption")
    fig2 = px.bar(outlet_agg, x=S_OUTLET, y=QTY_COL, color="Ingredient Name", title="Outlet Consumption")
    
    item_breakdown = filtered.groupby(['Item Name', 'Ingredient Name'], as_index=False)[QTY_COL].sum()
    fig3 = px.bar(item_breakdown, x="Item Name", y=QTY_COL, color="Ingredient Name", title="Item Breakdown")

    # KPI
    top = total.sort_values(QTY_COL, ascending=False).iloc[0]
    kpi = [html.Div([html.B("Top Ingredient: "), html.Span(f"{top['Ingredient Name']} ({top['Qty']})")], 
                    style={"padding": "15px", "border": "1px solid #ddd", "borderRadius": "8px"})]

    return (total.to_dict("records"), [{"name": i, "id": i} for i in total.columns],
            outlet_agg.to_dict("records"), [{"name": i, "id": i} for i in outlet_agg.columns],
            fig1, fig2, fig3, kpi)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8051, debug=True, use_reloader=False)