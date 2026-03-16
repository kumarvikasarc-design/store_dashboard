from dash import Dash, html, dcc, Input, Output
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import glob
import os

# ======================================================
# PATHS
# ======================================================
ENTRY_PATH  = r"C:\Users\ACER\store_dashboard\inventory\warehouse_stockentry"
INDENT_PATH = r"C:\Users\ACER\store_dashboard\inventory\Indent_report"

# ======================================================
# CANONICAL COLUMN NAMES
# ======================================================
COL_DATE      = "date"
COL_WAREHOUSE = "Warehouse"
COL_OUTLET    = "Outlet"
COL_ITEM      = "Item"
COL_CATEGORY  = "Category"

# ======================================================
# ENTRY LOADER (STOCK IN)
# ======================================================
def load_entry_data(path):
    files = glob.glob(os.path.join(path, "*.csv"))
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f, engine="python", on_bad_lines="skip")
            df.columns = df.columns.astype(str).str.strip()

            df[COL_DATE] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
            df[COL_WAREHOUSE] = df["Warehouse"]
            df[COL_OUTLET] = "Internal Warehouse"
            df[COL_ITEM] = df["Item Name"]
            df[COL_CATEGORY] = df["Category Name"]

            df["qty_in"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
            df["qty_out"] = 0
            df["source"] = "ENTRY"

            dfs.append(df[[COL_DATE, COL_WAREHOUSE, COL_OUTLET, COL_ITEM, COL_CATEGORY, "qty_in", "qty_out", "source"]])
        except Exception as e:
            print(f"❌ Error loading Entry file {f}: {e}")

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# ======================================================
# INDENT PARSER (STOCK OUT)
# ======================================================
import csv

# ======================================================
# INDENT PARSER (STOCK OUT) — ROBUST VERSION
# ======================================================
def parse_indent_csv(file_path):
    header_row_index = None
    
    # Use standard csv module to find the header row index safely
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            # Clean row values to check for keywords
            clean_row = [str(item).strip().lower() for item in row]
            if "item name" in clean_row and "received qty" in clean_row:
                header_row_index = i
                break

    if header_row_index is None:
        print(f"❌ Header keywords not found in {os.path.basename(file_path)}")
        return pd.DataFrame()

    # Load data starting from the identified header row
    df = pd.read_csv(
        file_path, 
        skiprows=header_row_index, 
        engine="python", 
        on_bad_lines="skip"
    )

    # Clean and normalize column names (remove spaces and case sensitivity)
    df.columns = [str(col).strip().replace('\n', ' ').lower() for col in df.columns]
    
    # Remove empty rows or footer rows (check if 'item name' exists)
    df = df.dropna(subset=['item name'])
    
    return df

def load_indent_data(path):
    files = glob.glob(os.path.join(path, "*.csv"))
    rows = []

    if not files:
        print(f"⚠️ No CSV files found in {path}")
        return pd.DataFrame()

    for f in files:
        try:
            df = parse_indent_csv(f)
            if df.empty:
                continue

            # Mapping logic with .get() to avoid KeyErrors
            # We use .strip() on values because CSVs often have trailing spaces in cells
            df[COL_DATE] = pd.to_datetime(df["received date"], dayfirst=True, errors="coerce")
            df[COL_WAREHOUSE] = df["supplier"].astype(str).str.strip()
            
            # Combine Receiver and Receiver Kitchen for the Outlet column
            df[COL_OUTLET] = df["receiver"].fillna(df.get("receiver kitchen", "Unknown")).astype(str).str.strip()
            
            df[COL_ITEM] = df["item name"].astype(str).str.strip()
            df[COL_CATEGORY] = df["category name"].astype(str).str.strip()

            df["qty_in"] = 0
            df["qty_out"] = pd.to_numeric(df["received qty"], errors="coerce").fillna(0)
            df["source"] = "INDENT"

            # Filter to required columns
            final_df = df[[COL_DATE, COL_WAREHOUSE, COL_OUTLET, COL_ITEM, COL_CATEGORY, "qty_in", "qty_out", "source"]]
            
            # Drop rows where date or item name failed to parse
            final_df = final_df.dropna(subset=[COL_DATE, COL_ITEM])
            
            rows.append(final_df)
            print(f"✅ Successfully loaded {len(final_df)} rows from {os.path.basename(f)}")

        except Exception as e:
            print(f"⚠️ Error processing {os.path.basename(f)}: {e}")

    if not rows:
        return pd.DataFrame(columns=[COL_DATE, COL_WAREHOUSE, COL_OUTLET, COL_ITEM, COL_CATEGORY, "qty_in", "qty_out", "source"])

    return pd.concat(rows, ignore_index=True)

# ======================================================
# INITIALIZE DATA
# ======================================================
entry_df = load_entry_data(ENTRY_PATH)
indent_df = load_indent_data(INDENT_PATH)

tx_df = pd.concat([entry_df, indent_df], ignore_index=True)
tx_df[COL_CATEGORY] = tx_df[COL_CATEGORY].fillna("Uncategorized")
tx_df = tx_df.dropna(subset=[COL_DATE]) # Remove rows with invalid dates


# ======================================================
# LAYOUT FUNCTIONS (Exported to main_app)
# ======================================================

def get_layout():
    """This function is called by main_app.py"""
    return dbc.Container([
        html.H2("Warehouse Stock Summary", className="my-4"),
        dbc.Card(dbc.CardBody([
            dbc.Row([
                dbc.Col(dcc.Dropdown(id="f_warehouse", placeholder="Warehouse", 
                                     options=sorted(tx_df[COL_WAREHOUSE].unique())), md=3),
                dbc.Col(dcc.Dropdown(id="f_category", placeholder="Category"), md=3),
                dbc.Col(dcc.Dropdown(id="f_item", placeholder="Item"), md=3),
                dbc.Col(dbc.Button("Reset", id="btn_reset", color="secondary", outline=True), md=3),
            ], className="mb-3"),
            dbc.Row([
                dbc.Col(dcc.DatePickerRange(
                    id="f_date",
                    start_date=tx_df[COL_DATE].min(),
                    end_date=tx_df[COL_DATE].max(),
                    display_format="DD-MM-YYYY"
                ), md=6),
            ])
        ]), className="mb-4"),
        html.Div(id="summary-content")
    ], fluid=True)

# ======================================================
# CALLBACKS
# ======================================================

def register_callbacks(app):
    @app.callback(
        Output("f_warehouse", "options"),
        Output("f_category", "options"),
        Output("f_item", "options"),
        Input("f_warehouse", "value"),
        Input("f_category", "value"),
    )
    def cascade_filters(w, c):
        df = tx_df.copy()
        wh_opts = sorted(df[COL_WAREHOUSE].dropna().unique())
        if w: df = df[df[COL_WAREHOUSE] == w]
        cat_opts = sorted(df[COL_CATEGORY].dropna().unique())
        if c: df = df[df[COL_CATEGORY] == c]
        item_opts = sorted(df[COL_ITEM].dropna().unique())
        return wh_opts, cat_opts, item_opts

    @app.callback(
        Output("f_warehouse", "value"),
        Output("f_category", "value"),
        Output("f_item", "value"),
        Output("f_date", "start_date"),
        Output("f_date", "end_date"),
        Input("btn_reset", "n_clicks")
    )
    def reset_filters(n):
        if not n: raise PreventUpdate
        return None, None, None, tx_df[COL_DATE].min(), tx_df[COL_DATE].max()

    @app.callback(
        Output("summary-content", "children"),
        Input("f_warehouse", "value"),
        Input("f_category", "value"),
        Input("f_item", "value"),
        Input("f_date", "start_date"),
        Input("f_date", "end_date"),
    )
    def update_summary(w, c, i, sd, ed):
        df = tx_df.copy()
        if sd: df = df[df[COL_DATE] >= pd.to_datetime(sd)]
        if ed: df = df[df[COL_DATE] <= pd.to_datetime(ed)]
        if w: df = df[df[COL_WAREHOUSE] == w]
        if c: df = df[df[COL_CATEGORY] == c]
        if i: df = df[df[COL_ITEM] == i]

        if df.empty:
            return html.Div("No data found for selected filters.", className="text-center p-5")

        # KPI Calculations
        total_in = int(df["qty_in"].sum())
        total_out = int(df["qty_out"].sum())
        
        kpi_grp = df.groupby([COL_WAREHOUSE, COL_ITEM]).agg(
            qin=("qty_in", "sum"), qout=("qty_out", "sum")
        ).reset_index()
        curr_stock = int(kpi_grp["qin"].sum() - kpi_grp["qout"].sum())
        dead_items = (kpi_grp["qout"] == 0).sum()

        # Graphs
        bar = px.bar(kpi_grp.groupby(COL_WAREHOUSE).sum(numeric_only=True).reset_index(), 
                     x=COL_WAREHOUSE, y="qin", title="Total Stock In by Warehouse")
        
        trend = px.line(df.groupby(COL_DATE)[["qty_in", "qty_out"]].sum().reset_index(),
                        x=COL_DATE, y=["qty_in", "qty_out"], title="Daily Movement")

        return [
            dbc.Row([
                dbc.Col(dbc.Card(dbc.CardBody([html.H6("Stock In"), html.H4(f"{total_in:,}")]), color="success", outline=True), md=3),
                dbc.Col(dbc.Card(dbc.CardBody([html.H6("Stock Out"), html.H4(f"{total_out:,}")]), color="danger", outline=True), md=3),
                dbc.Col(dbc.Card(dbc.CardBody([html.H6("Current Stock"), html.H4(f"{curr_stock:,}")]), color="info", outline=True), md=3),
                dbc.Col(dbc.Card(dbc.CardBody([html.H6("Dead Items"), html.H4(f"{dead_items:,}")]), color="warning", outline=True), md=3),
            ], className="mb-4"),
            dbc.Row([
                dbc.Col(dcc.Graph(figure=bar), md=6),
                dbc.Col(dcc.Graph(figure=trend), md=6),
            ])
        ]
