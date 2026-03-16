from dash import html, dcc
import dash_bootstrap_components as dbc

def get_layout():

    return dbc.Container([

        html.H3("🏬 Warehouse Enterprise Dashboard", className="mb-3"),

        dbc.Row([
            dbc.Col(dcc.Dropdown(id="wh_filter", placeholder="Select Warehouse"), md=3),
            dbc.Col(dcc.Dropdown(id="item_filter", placeholder="Select Item"), md=3),
        ], className="mb-3"),

        dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardBody([
                    html.H5("Available Stock Value"),
                    html.H3(id="total_stock_value")
                ])
            ]), md=3),

            dbc.Col(dbc.Card([
                dbc.CardBody([
                    html.H5("Negative Items"),
                    html.H3(id="negative_items")
                ])
            ]), md=3),
        ], className="mb-4"),

        html.Hr(),

        html.H4("📊 Live Stock Table"),

        dbc.Spinner(
            dbc.Table(id="stock_table", bordered=True, hover=True, striped=True)
        ),

        html.Hr(),

        html.H4("🧊 Expiry Alerts"),
        dbc.Table(id="expiry_table", bordered=True, hover=True),

        dcc.Interval(id="auto_refresh", interval=300000)  # 5 min refresh

    ], fluid=True)
