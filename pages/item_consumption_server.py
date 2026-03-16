import pandas as pd
import os
from dash import html, dcc, Input, Output, Dash
import plotly.express as px
from dash import dash_table
from datetime import date
#import threading, time
print("✅ Script started")

# ===========================================================
# 1️⃣ LOAD ALL CSV FILES FROM WINDOWS FOLDERS
# ===========================================================
TODAY = pd.Timestamp.today()
CURRENT_YEAR = TODAY.year
CURRENT_MONTH = TODAY.month
YESTERDAY = TODAY - pd.Timedelta(days=1)

analysis_folder = r"C:\Users\ACER\store_dashboard\item_source"
recipe_folder   = r"C:\Users\ACER\store_dashboard\inventory\recipe"
store_master = pd.read_csv(
    r"C:\Users\ACER\store_dashboard\stores_db.csv",
    encoding="utf-8"
)
stock_master = pd.read_csv(
    r"C:\Users\ACER\store_dashboard\inventory\stockitem_master.csv",
    encoding="utf-8"
)

stock_master.columns = stock_master.columns.str.strip()
stock_master['Item Name Clean'] = stock_master['Item Name'].str.lower().str.strip()

def load_all_analysis_files():
    dfs = []
    for file in os.listdir(analysis_folder):
        if file.endswith(".csv"):
            full_path = os.path.join(analysis_folder, file)
            df = pd.read_csv(full_path, encoding="utf-8")
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def load_all_recipe_files():
    dfs = []
    for file in os.listdir(recipe_folder):
        if file.endswith(".csv"):
            full_path = os.path.join(recipe_folder, file)
            df = pd.read_csv(full_path, encoding="utf-8")
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

# ===============================
# AUTO DETECT OUTLET COLUMN
# ===============================
store_master.columns = store_master.columns.str.strip()

possible_outlet_cols = [
    'Outlet', 'Outlet Name', 'Store', 'Store Name',
    'Deployment Name', 'Outlet_Name'
]

OUTLET_COL = None
for c in possible_outlet_cols:
    if c in store_master.columns:
        OUTLET_COL = c
        break

if OUTLET_COL is None:
    raise ValueError("❌ No outlet column found in stores_db.csv")

print("Using outlet column:", OUTLET_COL)

# ===========================================================
# 2️⃣ CLEAN + EXPAND RECIPE FILES
# ===========================================================

def parse_recipes(recipes):
    parsed = []
    current_recipe = None

    for i, row in recipes.iterrows():
        recipe_header = row['Recipe Name']
        ingredient = row['Item Name']

        # New recipe header
        if pd.notna(recipe_header) and recipe_header.strip() != "":
            current_recipe = recipe_header.strip()
            continue

        # Ingredient rows
        if pd.notna(ingredient) and ingredient.strip() != "":
            parsed.append({
                "Recipe Name": current_recipe,
                "Ingredient Name": row['Item Name'],
                "Ingredient Qty": row['Item Qty'],
                "Ingredient UOM": row['Item Unit'],
                "Item Tab Type": row['Item Tab Type']
            })

    parsed_df = pd.DataFrame(parsed)
    return parsed_df

UOM_PLURAL = {
    'KG': ('Kg', 'Kgs'),
    'G': ('g', 'g'),
    'L': ('L', 'L'),
    'ML': ('ml', 'ml'),
    'PCS': ('Pc', 'Pcs'),
    'NOS': ('No', 'Nos'),
    'BOX': ('Box', 'Boxes')
}

UOM_DECIMALS = {
    "KG": 3,
    "KGS": 3,
    "GRAM": 3,
    "GM": 3,
    "G": 3,
    "L": 3,
    "LTR": 3,
    "LITRE": 3,
    "ML": 3,

    "PCS": 0,
    "PC": 0,
    "NOS": 0,
    "NO": 0,
    "UNIT": 0,
}
def get_latest_mtime():
    """Return latest modified time of all CSVs in analysis & recipe folders"""
    files = []

    for folder in [analysis_folder, recipe_folder]:
        if os.path.exists(folder):
            files.extend(
                os.path.join(folder, f)
                for f in os.listdir(folder)
                if f.endswith(".csv")
            )

    if not files:
        return 0

    return max(os.path.getmtime(f) for f in files)
            
def format_qty_with_uom(row):
    qty = row['Consumed Qty Purchase UOM']
    uom = str(row['Purchase UOM']).upper()

    decimals = UOM_DECIMALS.get(uom, 2)
    qty_rounded = round(qty, decimals)

    singular, plural = UOM_PLURAL.get(uom, (uom, uom))
    display_uom = singular if qty_rounded == 1 else plural

    return f"{qty_rounded:.{decimals}f} {display_uom}"

def format_base_qty(row):
    qty = row['Consumed Qty Base UOM']
    uom = str(row['Base UOM']).upper()

    decimals = UOM_DECIMALS.get(uom, 2)
    qty_rounded = round(qty, decimals)

    singular, plural = UOM_PLURAL.get(uom, (uom, uom))
    display_uom = singular if qty_rounded == 1 else plural

    return f"{qty_rounded:.{decimals}f} {display_uom}"


# ===========================================================
# 3️⃣ MERGE ANALYSIS + RECIPE → CALCULATE CONSUMPTION
# ===========================================================

def prepare_final_data():

    analysis = load_all_analysis_files()
    recipes = load_all_recipe_files()
    recipe_expanded = parse_recipes(recipes)

    # Clean names for matching
    analysis['Item Name Clean'] = analysis['Item Name'].str.lower().str.strip()
    recipe_expanded['Recipe Name Clean'] = recipe_expanded['Recipe Name'].str.lower().str.strip()
    recipe_expanded['Ingredient Name Clean'] = recipe_expanded['Ingredient Name'].str.lower().str.strip()

    # Merge
    merged = analysis.merge(
        recipe_expanded,
        left_on='Item Name Clean',
        right_on='Recipe Name Clean',
        how='left'
    )

    merged['Consumed Qty Purchase UOM'] = merged['Item Qty'] * merged['Ingredient Qty']

    # ===============================
    # MERGE PURCHASE UOM & CONVERSION
    # ===============================
    merged = merged.merge(
        stock_master[[
            'Item Name Clean',
            'Purchase UOM',
            'Base UOM',
            'Conversion Factor'
        ]],
        left_on='Ingredient Name Clean',
        right_on='Item Name Clean',
        how='left'
    )
    merged['Consumed Qty Purchase UOM'] = merged.apply(convert_to_purchase_uom, axis=1)
    merged['Consumed Qty Base UOM'] = merged.apply(convert_to_base_uom, axis=1)
    return merged

def convert_to_purchase_uom(row):
    if pd.isna(row['Conversion Factor']):
        return row['Consumed Qty Purchase UOM']  # no conversion available
    if row['Ingredient UOM'] == row['Base UOM']:
        return row['Consumed Qty Purchase UOM'] / row['Conversion Factor']
    return row['Consumed Qty Purchase UOM']

def convert_to_base_uom(row):
    if pd.isna(row['Conversion Factor']):
        return row['Consumed Qty Purchase UOM']
    if row['Ingredient UOM'] == row['Base UOM']:
        return row['Consumed Qty Purchase UOM'] * row['Conversion Factor']
    return row['Consumed Qty Purchase UOM']

def apply_qty_formatter(row, qty_mode, QTY_COL, UOM_COL):
    if qty_mode == "BASE":
        return format_base_qty({
            "Consumed Qty Base UOM": row[QTY_COL],
            "Base UOM": row[UOM_COL]
        })
    else:
        return format_qty_with_uom({
            "Consumed Qty Purchase UOM": row[QTY_COL],
            "Purchase UOM": row[UOM_COL]
        })

def conversion_tooltip(row):
    if pd.isna(row.get("Conversion Factor")):
        return "Conversion not defined"

    puom = row.get("Purchase UOM", "")
    buom = row.get("Base UOM", "")
    factor = row["Conversion Factor"]

    return f"1 {puom} = {factor:g} {buom}"

#df = prepare_final_data()
# ❗ MUST exist at import time
df = pd.DataFrame()

def get_df():
    global df
    if df.empty:
        print("📦 Loading data...")
        df = prepare_final_data()
    return df


# ===============================
# AUTO DETECT SALES OUTLET COLUMN
# ===============================
def get_sales_outlet_col():
    df = get_df()
    possible_cols = [
        'Deployment Name', 'Outlet', 'Outlet Name', 'Store Name'
    ]
    for c in possible_cols:
        if c in df.columns:
            return c
    raise ValueError("❌ No outlet column found in sales data")


# ===============================
# DATE / MONTH / FY PREPARATION
# ===============================

def ensure_date_columns():
    df = get_df()

    possible_dates = [
        'Bill Date', 'BillDate', 'Business Date',
        'Date', 'Txn Date', 'Invoice Date'
    ]

    date_col = None
    for col in possible_dates:
        if col in df.columns:
            date_col = col
            break

    if date_col is None:
        raise ValueError("❌ No date column found in analysis CSV files")

    if 'Bill Date' not in df.columns:
        df['Bill Date'] = pd.to_datetime(df[date_col], errors='coerce')

        df['Year'] = df['Bill Date'].dt.year
        df['Month'] = df['Bill Date'].dt.month_name()

        df['FY'] = df['Bill Date'].apply(
            lambda x: f"{x.year}-{x.year+1}" if x.month >= 4 else f"{x.year-1}-{x.year}"
        )
        df['Week'] = df['Bill Date'].dt.isocalendar().week
        df['MonthShort'] = df['Bill Date'].dt.strftime('%b')
        df['YearShort'] = df['Bill Date'].dt.strftime('%y')

    return df


def format_date_range(start_date, end_date):
    if not start_date or not end_date:
        return ""
    start = pd.to_datetime(start_date).strftime("%d-%b-%Y")
    end = pd.to_datetime(end_date).strftime("%d-%b-%Y")

    if start == end:
        return start
    return f"{start} → {end}"

def get_selected_outlets(brand, region, state, city, type_, outlet):
    dff = store_master.copy()

    if brand:
        dff = dff[dff['Brand'] == brand]
    if region:
        dff = dff[dff['Region'] == region]
    if state:
        dff = dff[dff['State'] == state]
    if city:
        dff = dff[dff['City'] == city]
    if type_:
        dff = dff[dff['Type'] == type_]
    if outlet:
        dff = dff[dff[OUTLET_COL] == outlet]   # ✅ FIXED

    return dff[OUTLET_COL].dropna().unique()    # ✅ FIXED

df_init = ensure_date_columns()
# ===========================================================
# 4️⃣ DASHBOARD LAYOUT
# ===========================================================
app = Dash(__name__)

app.layout = html.Div([
#def get_layout(): 
    #return =html.Div([

        html.H2("Store Consumption Dashboard", style={"textAlign": "center"}),

        # =========================
        # 🔹 FILTER SECTION
        # =========================
        html.Div([

        # -------- LINE 1 : STORE FILTERS --------
        html.Div([
            dcc.Dropdown(id="f_brand", placeholder="Brand"),
            dcc.Dropdown(id="f_region", placeholder="Region"),
            dcc.Dropdown(id="f_state", placeholder="State"),
            dcc.Dropdown(id="f_city", placeholder="City"),
            dcc.Dropdown(id="f_type", placeholder="Type"),
            dcc.Dropdown(id="f_outlet", placeholder="Outlet"),
        ], style={
            "display": "grid",
            "gridTemplateColumns": "repeat(6, 1fr)",
            "gap": "8px",
            "marginBottom": "10px"
        }),

        # -------- LINE 2 : DATE + ITEM FILTERS (SINGLE LINE) --------
        html.Div([

            dcc.DatePickerRange(
                id="f_date",
                start_date=df_init['Bill Date'].min(),
                end_date=df_init['Bill Date'].max(),
                display_format="DD-MM-YYYY"
            ),

            dcc.Dropdown(
                id="f_fy",
                options=[],          # ✅ EMPTY
                placeholder="Financial Year",
                clearable=True
            ),

            dcc.Dropdown(
                id="f_month",
                options=[],          # ✅ EMPTY
                placeholder="Month",
                clearable=True
            ),

            dcc.Dropdown(
                id="f_week",
                options=[],
                placeholder="Week",
                clearable=True
            ),

            dcc.Dropdown(
                id="f_item",
                options=[],
                placeholder="Item Sold",
                multi=False
            ),

            dcc.Dropdown(
                id="f_ing",
                options=[],
                placeholder="Ingredient",
                multi=False
            ),

        ], style={
            "display": "grid",
            "gridTemplateColumns": "2fr 1fr 1fr 1fr 2fr 2fr",
            "gap": "8px",
            "marginBottom": "15px"
        }),

    ], style={
        "padding": "10px",
        "borderBottom": "1px solid #ddd"
    }),

    html.Div([
        dcc.RadioItems(
            id="qty_mode",
            options=[
                {"label": "Purchase Qty", "value": "PURCHASE"},
                {"label": "Base Qty", "value": "BASE"},
            ],
            value="PURCHASE",   # ✅ default
            inline=True,
            labelStyle={"marginRight": "15px"}
        ),
        
        html.Button("Reload CSV", id="reload"),
        ], style={
            "margin": "10px 0",
            "fontWeight": "600"
        }),
        
        html.Div(id="dummy", style={"display": "none"}),
            dcc.Interval(
                id="init_trigger",
                interval=200,
                n_intervals=0,
                max_intervals=1
            ),

        html.Div(
            id="kpi_container",
            style={
                "display": "grid",
                "gridTemplateColumns": "repeat(3, 1fr)",
                "gap": "15px",
                "marginBottom": "20px"
            }
        ),

        # =========================
        # 📊 GRAPHS SECTION
        # =========================
        html.Div([
            dcc.Graph(id="total_consumption_graph"),
            dcc.Graph(id="outlet_consumption_graph"),
            dcc.Graph(id="item_consumption_graph"),
        ]),

        # =========================
        # 📋 TABLES SECTION
        # =========================
        html.Div([

            html.H4("Total Ingredient Consumption"),
            dash_table.DataTable(
                id="tbl_total",
                page_size=15,
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "left"}
            ),

            html.Br(),

            html.H4("Outlet-wise Ingredient Consumption"),
            dash_table.DataTable(
                id="tbl_outlet",
                page_size=15,
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "left"}
            ),

        ], style={"marginTop": "20px"})

    ])



# ===========================================================
# 5️⃣ CALLBACKS
# ===========================================================
#def register_callbacks(app):
@app.callback(
    Output("f_brand", "options"),
    Output("f_region", "options"),
    Output("f_state", "options"),
    Output("f_city", "options"),
    Output("f_type", "options"),
    Output("f_outlet", "options"),

    Input("init_trigger", "n_intervals"),
    Input("f_brand", "value"),
    Input("f_region", "value"),
    Input("f_state", "value"),
    Input("f_city", "value"),
    Input("f_type", "value"),
    Input("f_outlet", "value"),
)
def cascade_filters(_, brand, region, state, city, type_, outlet):

    dff = store_master.copy()

    if brand:
        dff = dff[dff['Brand'] == brand]
    if region:
        dff = dff[dff['Region'] == region]
    if state:
        dff = dff[dff['State'] == state]
    if city:
        dff = dff[dff['City'] == city]
    if type_:
        dff = dff[dff['Type'] == type_]
    if outlet:
        dff = dff[dff[OUTLET_COL] == outlet]

    return (
        [{"label": x, "value": x} for x in sorted(store_master['Brand'].dropna().unique())],
        [{"label": x, "value": x} for x in sorted(dff['Region'].dropna().unique())],
        [{"label": x, "value": x} for x in sorted(dff['State'].dropna().unique())],
        [{"label": x, "value": x} for x in sorted(dff['City'].dropna().unique())],
        [{"label": x, "value": x} for x in sorted(dff['Type'].dropna().unique())],
        [{"label": x, "value": x} for x in sorted(dff[OUTLET_COL].dropna().unique())],
    )



@app.callback(
    Output("tbl_total", "data"),
    Output("tbl_total", "columns"),
    Output("tbl_outlet", "data"),
    Output("tbl_outlet", "columns"),
    Output("tbl_total", "tooltip_data"),
    Output("tbl_outlet", "tooltip_data"),

    
    Input("qty_mode", "value"),
    Input("f_brand", "value"),
    Input("f_region", "value"),
    Input("f_state", "value"),
    Input("f_city", "value"),
    Input("f_type", "value"),
    Input("f_outlet", "value"),

    Input("f_date", "start_date"),
    Input("f_date", "end_date"),
    Input("f_fy", "value"),
    Input("f_month", "value"),
    Input("f_week", "value"),
    
    Input("f_item", "value"),   # ✅ ADD
    Input("f_ing", "value"),    # ✅ ADD
)
def update_tables(
    qty_mode,
    brand, region, state, city, type_, outlet,
    start_date, end_date, f_fy, f_month, f_week,
    f_item, f_ing          # ✅ ADD THESE
):
    filtered = ensure_date_columns().copy()
    #filtered = get_df().copy()

    # =========================
    # QTY MODE SWITCH
    # =========================
    if qty_mode == "BASE":
        QTY_COL = "Consumed Qty Base UOM"
        UOM_COL = "Base UOM"
        qty_formatter = format_base_qty
        qty_label = "Base Qty"
    else:
        QTY_COL = "Consumed Qty Purchase UOM"
        UOM_COL = "Purchase UOM"
        qty_formatter = format_qty_with_uom
        qty_label = "Purchase Qty"
        
    # Store filter
    outlets = get_selected_outlets(brand, region, state, city, type_, outlet)
    SALES_OUTLET_COL = get_sales_outlet_col()
    filtered = filtered[filtered[SALES_OUTLET_COL].isin(outlets)]

    # =========================
    # DATE FILTER (DEFAULT = YESTERDAY)
    # =========================
    # Convert once (safe)
    start = pd.to_datetime(start_date) if start_date else None
    end = pd.to_datetime(end_date) if end_date else None

    # Apply date filter
    if start is not None and end is not None:
        filtered = filtered[
            (filtered['Bill Date'] >= start) &
            (filtered['Bill Date'] <= end)
        ]

    # 🔥 FALLBACK if no data after filter
    if filtered.empty:
        df_all = ensure_date_columns()
        latest_date = df_all['Bill Date'].max()
        filtered = df_all[df_all['Bill Date'] == latest_date]



    if f_fy:
        filtered = filtered[filtered['FY'] == f_fy]

    if f_month:
        filtered = filtered[filtered['Month'] == f_month]

    if f_week:
        filtered = filtered[filtered['Week'] == f_week]
    
    # ---------------------------
    # 🔥 ITEM SOLD FILTER (FIX)
    # ---------------------------
    if f_item:
        if isinstance(f_item, str):
            f_item = [f_item]
        filtered = filtered[filtered['Item Name'].isin(f_item)]

    # ---------------------------
    # 🔥 INGREDIENT FILTER (FIX)
    # ---------------------------
    if f_ing:
        if isinstance(f_ing, str):
            f_ing = [f_ing]
        filtered = filtered[filtered['Ingredient Name'].isin(f_ing)]
    
    date_range_text = format_date_range(start_date, end_date)

    total = (
        filtered.groupby(
            ['Ingredient Name', UOM_COL],
            as_index=False
        )
        .agg({
            QTY_COL: 'sum',
            'Conversion Factor': 'first'
        })
    )

    outlet_tbl = (
        filtered.groupby(
            [SALES_OUTLET_COL, 'Ingredient Name', 'Purchase UOM', 'Base UOM'],
            as_index=False
        )
        .agg({
            QTY_COL: 'sum',
            'Conversion Factor': 'first'
        })
    )
    total_tooltip = [
        {
            "Ingredient Name": {
                "value": conversion_tooltip(row),
                "type": "markdown"
            }
        }
        for _, row in total.iterrows()
    ]
    outlet_tooltip = [
        {
            "Ingredient Name": {
                "value": conversion_tooltip(row),
                "type": "markdown"
            }
        }
        for _, row in outlet_tbl.iterrows()
    ]


    # ✅ formatted display column
    qty_col_name = "Base Qty" if qty_mode == "BASE" else "Purchase Qty"
    total['Qty'] = total.apply(
        lambda r: apply_qty_formatter(r, qty_mode, QTY_COL, UOM_COL),
        axis=1
    )

    outlet_tbl['Qty'] = outlet_tbl.apply(
        lambda r: apply_qty_formatter(r, qty_mode, QTY_COL, UOM_COL),
        axis=1
    )

    total['Date Range'] = date_range_text
    outlet_tbl['Date Range'] = date_range_text
    
    outlet_tbl = outlet_tbl.rename(columns={SALES_OUTLET_COL: "Outlet Name"})
    # ✅ keep only display columns
    total = total.rename(columns={UOM_COL: "UOM", "Qty": qty_col_name})
    outlet_tbl = outlet_tbl.rename(columns={UOM_COL: "UOM", "Qty": qty_col_name})
    
    total = total[['Date Range', 'Ingredient Name', 'UOM', qty_col_name]]
    outlet_tbl = outlet_tbl[['Date Range', 'Outlet Name', 'Ingredient Name', 'UOM', qty_col_name]]

    return (
        total.to_dict("records"),
        [{"name": c, "id": c} for c in total.columns],
        outlet_tbl.to_dict("records"),
        [{"name": c, "id": c} for c in outlet_tbl.columns],
        total_tooltip,
        outlet_tooltip,
    )



@app.callback(
    Output("total_consumption_graph", "figure"),
    Output("outlet_consumption_graph", "figure"),
    Output("item_consumption_graph", "figure"),
    
    Input("qty_mode", "value"),   # 🔥 ADD FIRST
    Input("f_brand", "value"),
    Input("f_region", "value"),
    Input("f_state", "value"),
    Input("f_city", "value"),
    Input("f_type", "value"),
    Input("f_outlet", "value"),

    Input("f_date", "start_date"),
    Input("f_date", "end_date"),
    Input("f_fy", "value"),
    Input("f_month", "value"),
    Input("f_week", "value"),

    Input("f_item", "value"),        # ✅ ADD
    Input("f_ing", "value"),         # ✅ ADD
)

def update_graphs(
    qty_mode, brand, region, state, city, type_, outlet,
    start_date, end_date, f_fy, f_month, f_week,
    f_item, f_ing
):
    filtered = ensure_date_columns().copy()
    #filtered = get_df().copy()

    # =========================
    # QTY MODE SWITCH
    # =========================
    if qty_mode == "BASE":
        QTY_COL = "Consumed Qty Base UOM"
        UOM_COL = "Base UOM"
        title_suffix = " (Base Qty)"
    else:
        QTY_COL = "Consumed Qty Purchase UOM"
        UOM_COL = "Purchase UOM"
        title_suffix = " (Purchase Qty)"
    
    # Store filters
    outlets = get_selected_outlets(brand, region, state, city, type_, outlet)
    SALES_OUTLET_COL = get_sales_outlet_col()
    filtered = filtered[filtered[SALES_OUTLET_COL].isin(outlets)]

    # =========================
    # DATE FILTER (DEFAULT = YESTERDAY)
    # =========================
    # Convert once (safe)
    start = pd.to_datetime(start_date) if start_date else None
    end = pd.to_datetime(end_date) if end_date else None

    # Apply date filter
    if start is not None and end is not None:
        filtered = filtered[
            (filtered['Bill Date'] >= start) &
            (filtered['Bill Date'] <= end)
        ]

    # 🔥 FALLBACK if no data after filter
    if filtered.empty:
        df_all = ensure_date_columns()
        latest_date = df_all['Bill Date'].max()
        filtered = df_all[df_all['Bill Date'] == latest_date]

    if f_fy:
        filtered = filtered[filtered['FY'] == f_fy]

    if f_month:
        filtered = filtered[filtered['Month'] == f_month]

    if f_week:
        filtered = filtered[filtered['Week'] == f_week]

    # 🔥 ITEM FILTER
    if f_item:
        if isinstance(f_item, str):
            f_item = [f_item]
        filtered = filtered[filtered['Item Name'].isin(f_item)]

    # 🔥 INGREDIENT FILTER
    if f_ing:
        if isinstance(f_ing, str):
            f_ing = [f_ing]
        filtered = filtered[filtered['Ingredient Name'].isin(f_ing)]


    # =========================
    # TOTAL INGREDIENT GRAPH
    # =========================
    total_df = (
        filtered
        .groupby(['Ingredient Name', UOM_COL], as_index=False)[QTY_COL]
        .sum()
    )
    
    fig_total = px.bar(
        total_df,
        x="Ingredient Name",
        y=QTY_COL,
        color=UOM_COL,
        title="Total Ingredient Consumption" + title_suffix,
        text_auto=".3f"
    )

    fig_total.update_traces(
        hovertemplate="%{y:.3f} %{customdata}",
        customdata=total_df[UOM_COL]
    )

    fig_total.update_yaxes(tickformat=".3f")
    # =========================
    # OUTLET-WISE GRAPH
    # =========================
    outlet_df = (
        filtered
        .groupby([SALES_OUTLET_COL, 'Ingredient Name'], as_index=False)[QTY_COL]
        .sum()
    )

    outlet_df = outlet_df.rename(columns={SALES_OUTLET_COL: "Outlet Name"})
    fig_outlet = px.bar(
        outlet_df,
        x="Outlet Name",
        y=QTY_COL,
        color="Ingredient Name",
        title="Outlet-wise Ingredient Consumption" + title_suffix,
        text_auto=".3f"
    )

    # =========================
    # ITEM → INGREDIENT GRAPH
    # =========================
    item_df = (
        filtered
        .groupby(['Item Name', 'Ingredient Name'], as_index=False)[QTY_COL]
        .sum()
    )

    fig_item = px.bar(
        item_df,
        x="Item Name",
        y=QTY_COL,
        color="Ingredient Name",
        title="Item → Ingredient Breakdown" + title_suffix,
        text_auto=".3f"
    )
    qty_col_name = "Base Qty" if qty_mode == "BASE" else "Purchase Qty"
    fig_total.update_yaxes(title_text=qty_col_name)
    fig_outlet.update_yaxes(title_text=qty_col_name)
    fig_item.update_yaxes(title_text=qty_col_name)

    return fig_total, fig_outlet, fig_item

@app.callback(
    Output("f_ing", "options"),
    Input("f_item", "value")
)
def update_ingredient_options(selected_item):
    df = ensure_date_columns()

    if not selected_item:
        return [{"label": x, "value": x} for x in sorted(df['Ingredient Name'].dropna().unique())]

    if isinstance(selected_item, str):
        selected_item = [selected_item]

    ing_df = df[df['Item Name'].isin(selected_item)]
    return [{"label": x, "value": x} for x in sorted(ing_df['Ingredient Name'].dropna().unique())]


@app.callback(
    Output("f_ing", "value"),
    Input("f_item", "value")
)
def reset_ingredient_on_item_change(_):
    return None

@app.callback(
    Output("kpi_container", "children"),
    Input("qty_mode", "value"),
    Input("f_brand", "value"),
    Input("f_region", "value"),
    Input("f_state", "value"),
    Input("f_city", "value"),
    Input("f_type", "value"),
    Input("f_outlet", "value"),
    Input("f_date", "start_date"),
    Input("f_date", "end_date"),
    Input("f_fy", "value"),
    Input("f_month", "value"),
    Input("f_week", "value"),
    Input("f_item", "value"),
    Input("f_ing", "value"),
)
def update_kpis(
    qty_mode, brand, region, state, city, type_, outlet,
    start_date, end_date, f_fy, f_month, f_week, f_item, f_ing
):
    filtered = ensure_date_columns().copy()
    #filtered = get_df().copy()

    # Qty mode
    if qty_mode == "BASE":
        QTY_COL = "Consumed Qty Base UOM"
        UOM_COL = "Base UOM"
        qty_label = "Base Qty"
    else:
        QTY_COL = "Consumed Qty Purchase UOM"
        UOM_COL = "Purchase UOM"
        qty_label = "Purchase Qty"

    # Store filter
    outlets = get_selected_outlets(brand, region, state, city, type_, outlet)
    SALES_OUTLET_COL = get_sales_outlet_col()
    filtered = filtered[filtered[SALES_OUTLET_COL].isin(outlets)]

    # Date filter
    # Convert once (safe)
    start = pd.to_datetime(start_date) if start_date else None
    end = pd.to_datetime(end_date) if end_date else None

    # Apply date filter
    if start is not None and end is not None:
        filtered = filtered[
            (filtered['Bill Date'] >= start) &
            (filtered['Bill Date'] <= end)
        ]

    # 🔥 FALLBACK if no data after filter
    if filtered.empty:
        df_all = ensure_date_columns()
        latest_date = df_all['Bill Date'].max()
        filtered = df_all[df_all['Bill Date'] == latest_date]


    # Other filters
    if f_fy:
        filtered = filtered[filtered['FY'] == f_fy]
    if f_month:
        filtered = filtered[filtered['Month'] == f_month]
    if f_week:
        filtered = filtered[filtered['Week'] == f_week]
    if f_item:
        filtered = filtered[filtered['Item Name'].isin([f_item] if isinstance(f_item, str) else f_item)]
    if f_ing:
        filtered = filtered[filtered['Ingredient Name'].isin([f_ing] if isinstance(f_ing, str) else f_ing)]

    if filtered.empty:
        return []

    # Top ingredient
    top_ing = (
        filtered
        .groupby(['Ingredient Name', UOM_COL], as_index=False)[QTY_COL]
        .sum()
        .sort_values(QTY_COL, ascending=False)
        .iloc[0]
    )

    qty_text = apply_qty_formatter(top_ing, qty_mode, QTY_COL, UOM_COL)

    return [
        html.Div([
            html.Div("Top Ingredient", style={"fontSize": "14px", "color": "#666"}),
            html.Div(top_ing['Ingredient Name'], style={"fontSize": "20px", "fontWeight": "700"}),
            html.Div(qty_text, style={"fontSize": "16px", "color": "#0d6efd"})
        ], style={
            "padding": "15px",
            "border": "1px solid #ddd",
            "borderRadius": "8px",
            "background": "#fff"
        })
    ]
@app.callback(
    Output("dummy", "children"),
    Input("reload", "n_clicks"),
    prevent_initial_call=True
)
def reload(_):
    global df
    df = prepare_final_data()
    return ""
@app.callback(
    Output("f_fy", "options"),
    Output("f_month", "options"),
    Output("f_week", "options"),
    Output("f_item", "options"),
    Input("init_trigger", "n_intervals")   # ✅ SAFE trigger
)
def init_dropdowns(_):
    df = ensure_date_columns()

    fy_opts = [{"label": x, "value": x} for x in sorted(df['FY'].dropna().unique())]

    month_opts = [
        {"label": f"{m} - {y}", "value": m}
        for m, y in (
            df[['Month', 'YearShort']]
            .dropna()
            .drop_duplicates()
            .sort_values(['YearShort', 'Month'])
            .itertuples(index=False, name=None)
        )
    ]

    week_opts = [
        {"label": f"Week {int(w)}", "value": int(w)}
        for w in sorted(df['Week'].dropna().unique())
    ]

    item_opts = [{"label": x, "value": x} for x in sorted(df['Item Name'].dropna().unique())]

    return fy_opts, month_opts, week_opts, item_opts




# ===========================================================
# 6️⃣ RUN SERVER
# ===========================================================
if __name__ == "__main__":
    print("🚀 Starting Dash server on http://127.0.0.1:8051")
    app.run(host="0.0.0.0", port=8051, debug=True, use_reloader=False)
#