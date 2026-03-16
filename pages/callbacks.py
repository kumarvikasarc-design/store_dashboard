from dash import Input, Output, html
import dash_table, dash
import pandas as pd
from db_connection import engine
from filters import get_filter_data
from queries import WAREHOUSE_STOCK_SUMMARY, LEDGER_QUERY
app = dash.Dash(__name__)
# =============================
# TAB 1 WAREHOUSE SUMMARY
# =============================
@app.callback(
    Output("tab1","children"),
    Input("brand_filter","value")
)
def load_summary(_):

    df = pd.read_sql(WAREHOUSE_STOCK_SUMMARY, engine)

    if df.empty:
        return html.Div("No Data")

    # ===== KPIs =====
    total_stock_in = round(df["stock_in_value"].sum(),2)
    total_stock_out = round(df["indent_value"].sum(),2)
    total_qty = round(df["available_qty"].sum(),2)
    total_val = round(df["available_value"].sum(),2)

    kpi = html.Div([
        html.Div(f"Stock In Value ₹ {total_stock_in:,.0f}",className="kpi"),
        html.Div(f"Stock Out Value ₹ {total_stock_out:,.0f}",className="kpi"),
        html.Div(f"Available Qty {total_qty:,.0f}",className="kpi"),
        html.Div(f"Stock Value ₹ {total_val:,.0f}",className="kpi"),
    ],style={"display":"flex","gap":"30px","fontSize":"22px","margin":"15px"})

    table = dash_table.DataTable(
        data=df.to_dict("records"),
        page_size=20,
        style_table={'overflowX': 'auto'},
        style_cell={'fontSize':13}
    )

    return html.Div([kpi, table])
@app.callback(
    Output("tab2","children"),
    Input("item_filter","value")
)
def ledger(item):

    q = LEDGER_QUERY
    if item:
        q += f" WHERE item_name='{item}'"

    df = pd.read_sql(q, engine)

    return dash_table.DataTable(
        data=df.to_dict("records"),
        page_size=25
    )

filter_df = get_filter_data()

# BRAND → STATE
@app.callback(
    Output("state_filter","options"),
    Input("brand_filter","value")
)
def state_filter(brand):

    dff = filter_df
    if brand:
        dff = dff[dff.Brand==brand]

    return [{"label":i,"value":i} for i in sorted(dff.State.dropna().unique())]


# STATE → CITY
@app.callback(
    Output("city_filter","options"),
    Input("state_filter","value")
)
def city_filter(state):

    dff = filter_df
    if state:
        dff = dff[dff.State==state]

    return [{"label":i,"value":i} for i in sorted(dff.City.dropna().unique())]