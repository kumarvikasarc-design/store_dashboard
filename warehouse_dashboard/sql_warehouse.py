from dash.dash_table.Format import Group
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from db_connection import engine
import dash_table, dash

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
app.title = "Warehouse ERP Dashboard"

# ==========================================================
# 🔥 MASTER DATA LOAD (FILTERS)
# ==========================================================

def load_master_filters():

    df = pd.read_sql("""
        SELECT DISTINCT 
            s.Brand,
            s.State,
            s.Region,
            s.City,
            s.Store_Type,
            s.Outlet_Name
        FROM stores_master s
    """, engine)

    return df


def load_items():
    return pd.read_sql("""
        SELECT DISTINCT
            item_id,
            Item_Name,
            Category_Name,
            Super_Category
        FROM stockitem_master
    """, engine)


# ==========================================================
# 🔥 CORE STOCK QUERY (FINAL FORMULA)
# ==========================================================

def load_stock_summary():

    query = """
    WITH StockIn AS (
        SELECT 
            item_id,
            warehouse,
            SUM(qty_in) as stock_in,
            SUM(qty_in * unit_price) as stock_in_value
        FROM warehouse_stockentry
        GROUP BY item_id, warehouse

        UNION ALL

        SELECT 
            NULL as item_id,
            Warehouse as warehouse,
            SUM(Opening_Stock) as stock_in,
            0 as stock_in_value
        FROM warehouse_opening_stock
        GROUP BY Warehouse
    ),

    StockOut AS (
        SELECT 
            i.item_id,
            i.warehouse,
            SUM(i.indent_qty - ISNULL(o.consumption_qty,0)) as indent_out
        FROM warehouse_indent i
        LEFT JOIN outlet_consumption o 
            ON i.item_id = o.item_id
        GROUP BY i.item_id, i.warehouse

        UNION ALL

        SELECT 
            item_id,
            warehouse,
            SUM(qty) as indent_out
        FROM warehouse_wastage
        GROUP BY item_id, warehouse
    )

    SELECT 
        si.warehouse,
        si.item_id,
        SUM(si.stock_in) as Stock_In,
        SUM(ISNULL(so.indent_out,0)) as Stock_Out,
        SUM(si.stock_in) - SUM(ISNULL(so.indent_out,0)) as Available_Qty
    FROM StockIn si
    LEFT JOIN StockOut so 
        ON si.item_id = so.item_id
        AND si.warehouse = so.warehouse
    GROUP BY si.warehouse, si.item_id
    """

    return pd.read_sql(query, engine)


# ==========================================================
# 🔥 KPI CARDS
# ==========================================================

def build_kpi(df):

    total_in = df["Stock_In"].sum()
    total_out = df["Stock_Out"].sum()
    available = df["Available_Qty"].sum()

    return dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H4("Stock In"),
            html.H3(f"{total_in:,.0f}")
        ])), width=3),

        dbc.Col(dbc.Card(dbc.CardBody([
            html.H4("Stock Out"),
            html.H3(f"{total_out:,.0f}")
        ])), width=3),

        dbc.Col(dbc.Card(dbc.CardBody([
            html.H4("Available Qty"),
            html.H3(f"{available:,.0f}")
        ])), width=3),
    ])
    

# ==========================================================
# 🔥 LEDGER QUERY
# ==========================================================

def load_ledger(item_id):

    query = f"""
    SELECT 
        entry_date as Date,
        warehouse as Warehouse,
        item_name as Item,
        uom,
        qty_in as Stock_In,
        0 as Stock_Out
    FROM warehouse_stockentry
    WHERE item_id = {item_id}

    UNION ALL

    SELECT 
        indent_date,
        warehouse,
        item_name,
        uom,
        0,
        indent_qty
    FROM warehouse_indent
    WHERE item_id = {item_id}
    """

    return pd.read_sql(query, engine)


# ==========================================================
# 🔥 AGING BUCKET
# ==========================================================

def load_aging():

    query = """
    SELECT 
        item_name,
        MAX(inserted_at) as Last_Movement,
        SUM(closing_qty) as Closing_Stock
    FROM outlet_consumption
    GROUP BY item_name
    """

    df = pd.read_sql(query, engine)

    df["Age_Days"] = (pd.Timestamp.today() - pd.to_datetime(df["Last_Movement"])).dt.days

    def bucket(x):
        if x <= 30: return "0-30"
        if x <= 60: return "30-60"
        return "60+"

    df["Bucket"] = df["Age_Days"].apply(bucket)

    return df


# ==========================================================
# 🔥 LAYOUT
# ==========================================================

stock_df = load_stock_summary()
aging_df = load_aging()

app.layout = dbc.Container([

    html.H2("Warehouse ERP Dashboard", className="mt-3"),
     # 🔵 FILTER ROW 1
    dbc.Row([
        dbc.Col(dcc.Dropdown(id="brand_filter", placeholder="Brand")),
        dbc.Col(dcc.Dropdown(id="state_filter", placeholder="State")),
        dbc.Col(dcc.Dropdown(id="region_filter", placeholder="Region")),
        dbc.Col(dcc.Dropdown(id="city_filter", placeholder="City")),
        dbc.Col(dcc.Dropdown(id="type_filter", placeholder="Store Type")),
        dbc.Col(dcc.Dropdown(id="outlet_filter", placeholder="Outlet")),
    ], className="mb-2"),

    # 🔵 FILTER ROW 2
    dbc.Row([
        dbc.Col(dcc.DatePickerRange(id="date_filter")),
        dbc.Col(dcc.Dropdown(id="fy_filter", placeholder="Financial Year")),
        dbc.Col(dcc.Dropdown(id="month_filter", placeholder="Month")),
        dbc.Col(dcc.Dropdown(id="supercat_filter", placeholder="Super Category")),
        dbc.Col(dcc.Dropdown(id="category_filter", placeholder="Category")),
        dbc.Col(dcc.Dropdown(id="item_filter", placeholder="Item")),
    ], className="mb-3"),

    # 🔵 TABS
    dcc.Tabs([

        dcc.Tab(label="Warehouse Summary", id="tab1"),
        dcc.Tab(label="Warehouse Ledger", id="tab2"),
        dcc.Tab(label="Item Aging", id="tab3"),
        dcc.Tab(label="Outlet Consumption", id="tab4"),
        dcc.Tab(label="Alerts", id="tab5"),
        dcc.Tab(label="Wastage", id="tab6"),
        dcc.Tab(label="Ordering Planning", id="tab7"),

    ]),

    
    build_kpi(stock_df),

    dcc.Tabs([

        dcc.Tab(label="Warehouse Stock Summary", children=[
            dash_table.DataTable(
                data=stock_df.to_dict("records"),
                columns=[{"name":i,"id":i} for i in stock_df.columns],
                page_size=15
            )
        ]),

        dcc.Tab(label="Warehouse Ledger", children=[
            html.Div("Select item from dropdown (extend with filter)")
        ]),

        dcc.Tab(label="Item Aging", children=[
            dcc.Graph(
                figure=px.bar(
                    aging_df.groupby("Bucket")["Closing_Stock"].sum().reset_index(),
                    x="Bucket",
                    y="Closing_Stock"
                )
            ),
            dash_table.DataTable(
                data=aging_df.to_dict("records"),
                columns=[{"name":i,"id":i} for i in aging_df.columns]
            )
        ])

    ])

], fluid=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8052, debug=False)
