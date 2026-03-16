import pandas as pd
import os
import json
from dash import Dash, dcc, html, Input, Output, State, dash_table, no_update
import plotly.express as px
from datetime import date

# ===========================================================
# 1️⃣ CONFIGURATION & PATHS
# ===========================================================
analysis_folder = r"C:\Users\ACER\store_dashboard\item_source"
recipe_folder   = r"C:\Users\ACER\store_dashboard\inventory\recipe"
stores_db_path  = r"C:\Users\ACER\store_dashboard\stores_db.csv"
stock_master_path = r"C:\Users\ACER\store_dashboard\inventory\stockitem_master.csv"

# Global tracking for smart-reload to avoid heavy disk reads
LAST_MTIME = 0
STORE_MASTER = None
STOCK_MASTER = None

def load_static_masters():
    """Loads masters that change infrequently."""
    sm = pd.read_csv(stores_db_path, encoding="utf-8")
    sm.columns = sm.columns.str.strip()
    
    sk = pd.read_csv(stock_master_path, encoding="utf-8")
    sk.columns = sk.columns.str.strip()
    sk['Item Name Clean'] = sk['Item Name'].str.lower().str.strip()
    return sm, sk

# Initial Load
STORE_MASTER, STOCK_MASTER = load_static_masters()

# Detect Outlet Column in Master
possible_outlet_cols = ['Outlet', 'Outlet Name', 'Store', 'Store Name', 'Deployment Name', 'Outlet_Name']
OUTLET_COL = next((c for c in possible_outlet_cols if c in STORE_MASTER.columns), "Outlet")

# ===========================================================
# 2️⃣ DATA PROCESSING CORE
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
        header = row.get('Recipe Name')
        if pd.notna(header) and str(header).strip() != "":
            current_recipe = str(header).strip()
            continue
        if pd.notna(row.get('Item Name')) and str(row.get('Item Name')).strip() != "":
            parsed.append({
                "Recipe Name": current_recipe,
                "Ingredient Name": row['Item Name'],
                "Ingredient Qty": row['Item Qty'],
                "Ingredient UOM": row['Item Unit'],
            })
    return pd.DataFrame(parsed)

def prepare_final_data():
    """Scans, merges, and prepares the main dataframe."""
    a_files = [os.path.join(analysis_folder, f) for f in os.listdir(analysis_folder) if f.endswith(".csv")]
    r_files = [os.path.join(recipe_folder, f) for f in os.listdir(recipe_folder) if f.endswith(".csv")]
    
    if not a_files or not r_files: return pd.DataFrame()

    analysis = pd.concat((pd.read_csv(f, encoding="utf-8") for f in a_files), ignore_index=True)
    recipe_raw = pd.concat((pd.read_csv(f, encoding="utf-8") for f in r_files), ignore_index=True)
    recipe_expanded = parse_recipes(recipe_raw)

    analysis['Item Name Clean'] = analysis['Item Name'].astype(str).str.lower().str.strip()
    recipe_expanded['Recipe Name Clean'] = recipe_expanded['Recipe Name'].astype(str).str.lower().str.strip()
    recipe_expanded['Ingredient Name Clean'] = recipe_expanded['Ingredient Name'].astype(str).str.lower().str.strip()

    merged = analysis.merge(recipe_expanded, left_on='Item Name Clean', right_on='Recipe Name Clean', how='left')
    merged = merged.merge(STOCK_MASTER[['Item Name Clean', 'Purchase UOM', 'Base UOM', 'Conversion Factor']], 
                          left_on='Ingredient Name Clean', right_on='Item Name Clean', how='left')

    merged['Ingredient Qty'] = pd.to_numeric(merged['Ingredient Qty'], errors='coerce').fillna(0)
    merged['Item Qty'] = pd.to_numeric(merged['Item Qty'], errors='coerce').fillna(0)
    merged['Conversion Factor'] = pd.to_numeric(merged['Conversion Factor'], errors='coerce').fillna(1)

    merged['Consumed Qty Purchase UOM'] = merged['Item Qty'] * merged['Ingredient Qty']
    merged['Consumed Qty Base UOM'] = merged['Consumed Qty Purchase UOM'] * merged['Conversion Factor']

    date_col = next((c for c in ['Bill Date', 'BillDate', 'Business Date', 'Date'] if c in merged.columns), None)
    merged['Bill Date'] = pd.to_datetime(merged[date_col], errors='coerce') if date_col else pd.Timestamp.today()
    
    # Financial Year and Time groupings
    merged['FY'] = merged['Bill Date'].apply(lambda x: f"{x.year}-{x.year+1}" if x.month >= 4 else f"{x.year-1}-{x.year}")
    merged['Month'] = merged['Bill Date'].dt.month_name()
    merged['Week'] = merged['Bill Date'].dt.isocalendar().week
    
    return merged

def apply_qty_formatter(qty, uom):
    uom = str(uom).upper()
    decimals = 3 if uom in ["KG", "G", "L", "ML"] else 0
    return f"{qty:,.{decimals}f} {uom.capitalize()}"

# ===========================================================
# 3️⃣ LAYOUT
# ===========================================================
app = Dash(__name__)

app.layout = html.Div([
    dcc.Interval(id='auto-sync-trigger', interval=5*60*1000, n_intervals=0), # 5 min sync
    dcc.Store(id='global-data-store'),
    
    html.Div([
        html.H2("Real-Time Consumption Dashboard", style={"margin": "0"}),
        html.P(id="sync-status", style={"color": "gray", "fontSize": "12px"})
    ], style={"textAlign": "center", "padding": "20px"}),

    # Filters
    html.Div([
        html.Div([
            dcc.Dropdown(id="f_brand", placeholder="Brand"),
            dcc.Dropdown(id="f_region", placeholder="Region"),
            dcc.Dropdown(id="f_outlet", placeholder="Outlet"),
            dcc.DatePickerRange(id="f_date", display_format="DD-MM-YYYY"),
            dcc.Dropdown(id="f_ing", placeholder="Ingredient"),
        ], style={"display": "grid", "gridTemplateColumns": "repeat(5, 1fr)", "gap": "10px"}),
        
        dcc.RadioItems(
            id="qty_mode", 
            options=[{"label": "Purchase UOM", "value": "PURCHASE"}, {"label": "Base UOM", "value": "BASE"}], 
            value="PURCHASE", inline=True, style={"marginTop": "15px"}
        ),
    ], style={"padding": "20px", "background": "#f9f9f9", "borderBottom": "1px solid #eee"}),

    html.Div(id="kpi-row", style={"display": "flex", "justifyContent": "center", "padding": "20px"}),

    html.Div([
        dcc.Graph(id="main-bar-chart"),
        dcc.Graph(id="outlet-breakdown-chart"),
    ], style={"display": "grid", "gridTemplateColumns": "1fr 1fr"}),

    html.Div([
        html.H4("Detailed Consumption Table"),
        dash_table.DataTable(
            id="data-table",
            page_size=10,
            style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'}
        )
    ], style={"padding": "20px"})
])

# ===========================================================
# 4️⃣ CALLBACKS
# ===========================================================

@app.callback(
    Output('global-data-store', 'data'),
    Output('sync-status', 'children'),
    Input('auto-sync-trigger', 'n_intervals')
)
def sync_data(n):
    global LAST_MTIME
    current_mtime = get_latest_mtime()
    
    if n == 0 or current_mtime > LAST_MTIME:
        df = prepare_final_data()
        LAST_MTIME = current_mtime
        print(f"✅ Data Synced at {pd.Timestamp.now()}")
        return df.to_json(date_format='iso', orient='split'), f"Last update: {pd.Timestamp.now().strftime('%H:%M:%S')}"
    
    return no_update, no_update

@app.callback(
    [Output("main-bar-chart", "figure"),
     Output("outlet-breakdown-chart", "figure"),
     Output("data-table", "data"),
     Output("data-table", "columns"),
     Output("kpi-row", "children")],
    [Input("global-data-store", "data"),
     Input("qty_mode", "value"),
     Input("f_brand", "value"),
     Input("f_outlet", "value"),
     Input("f_date", "start_date"),
     Input("f_date", "end_date"),
     Input("f_ing", "value")]
)
def update_dashboard(json_data, qty_mode, brand, outlet, start, end, f_ing):
    if not json_data: return {}, {}, [], [], []
    
    df = pd.read_json(json_data, orient='split')
    QTY_COL = "Consumed Qty Base UOM" if qty_mode == "BASE" else "Consumed Qty Purchase UOM"
    UOM_COL = "Base UOM" if qty_mode == "BASE" else "Purchase UOM"

    # Filter logic
    dff = df.copy()
    if outlet: dff = dff[dff[OUTLET_COL] == outlet]
    if start: dff = dff[dff['Bill Date'] >= start]
    if end: dff = dff[dff['Bill Date'] <= end]
    if f_ing: dff = dff[dff['Ingredient Name'] == f_ing]

    if dff.empty: return {}, {}, [], [], [html.H3("No data for selection")]

    # Aggregations
    total_ing = dff.groupby(['Ingredient Name', UOM_COL], as_index=False)[QTY_COL].sum()
    
    # Graphs
    fig1 = px.bar(total_ing, x="Ingredient Name", y=QTY_COL, title="Total Consumption by Ingredient")
    fig2 = px.sunburst(dff, path=[OUTLET_COL, 'Ingredient Name'], values=QTY_COL, title="Consumption Spread")

    # Table formatting
    table_data = total_ing.copy()
    table_data['Display Qty'] = table_data.apply(lambda x: apply_qty_formatter(x[QTY_COL], x[UOM_COL]), axis=1)
    cols = [{"name": i, "id": i} for i in ["Ingredient Name", "Display Qty"]]

    # KPI
    top_item = total_ing.sort_values(QTY_COL, ascending=False).iloc[0]
    kpi = html.Div([
        html.Span("Highest Consumption: ", style={"color": "gray"}),
        html.B(f"{top_item['Ingredient Name']} ({apply_qty_formatter(top_item[QTY_COL], top_item[UOM_COL])})")
    ], style={"padding": "10px", "border": "1px solid #ddd", "borderRadius": "5px"})

    return fig1, fig2, table_data.to_dict('records'), cols, [kpi]

# Dropdown Population
@app.callback(
    [Output("f_brand", "options"), Output("f_outlet", "options"), Output("f_ing", "options")],
    Input("global-data-store", "data")
)
def populate_filters(json_data):
    if not json_data: return [], [], []
    df = pd.read_json(json_data, orient='split')
    return (
        [{"label": x, "value": x} for x in sorted(STORE_MASTER['Brand'].unique())],
        [{"label": x, "value": x} for x in sorted(df[OUTLET_COL].unique())],
        [{"label": x, "value": x} for x in sorted(df['Ingredient Name'].unique())]
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8051, debug=False)