import dash
from dash import dcc, html
import plotly.express as px
import pandas as pd
from db_connection import engine

app = dash.Dash(__name__)
app.title = "Coffee Island Warehouse Dashboard"

# ================= LOAD FROM SQL =================

def load_data():

    consumption = pd.read_sql("""
        SELECT 
            c.outlet_name,
            s.City,
            m.Category_Name,
            c.item_name,
            c.consumption_qty,
            c.consumption_amt,
            c.closing_qty,
            c.closing_amt,
            c.opening_date,
            c.closing_date
        FROM outlet_consumption c
        LEFT JOIN stores_master s ON c.store_id = s.Store_Id
        LEFT JOIN stockitem_master m ON c.item_id = m.item_id
    """, engine)

    expiry = pd.read_sql("""
        SELECT 
            warehouse,
            item_name,
            qty,
            expiry_date
        FROM warehouse_item_expiry
    """, engine)

    return consumption, expiry

# load first time
consumption_df, expiry_df = load_data()

# ================= CHARTS =================

def build_layout(consumption_df, expiry_df):

    fig1 = px.bar(
        consumption_df.groupby("Category_Name", as_index=False)["consumption_amt"].sum(),
        x="Category_Name",
        y="consumption_amt",
        title="Category Consumption Value"
    )

    fig2 = px.bar(
        consumption_df.groupby("outlet_name", as_index=False)["consumption_amt"].sum(),
        x="outlet_name",
        y="consumption_amt",
        title="Outlet Consumption"
    )

    fig3 = px.bar(
        expiry_df.sort_values("expiry_date").head(15),
        x="item_name",
        y="qty",
        title="Top Expiry Items"
    )

    return html.Div([
        html.H1("☕ Coffee Island Live Warehouse Dashboard", style={'textAlign':'center'}),

        dcc.Graph(figure=fig1),
        dcc.Graph(figure=fig2),
        dcc.Graph(figure=fig3),

        dcc.Interval(
            id='interval-refresh',
            interval=60000,   # refresh every 60 sec
            n_intervals=0
        )
    ])

app.layout = build_layout(consumption_df, expiry_df)

# ================= AUTO REFRESH =================

@app.callback(
    dash.Output('interval-refresh','id'),
    dash.Input('interval-refresh','n_intervals')
)
def refresh_data(n):
    global consumption_df, expiry_df
    consumption_df, expiry_df = load_data()
    return 'interval-refresh'

# ================= RUN =================
if __name__ == "__main__":
    app.run(debug=True, port=8051)
