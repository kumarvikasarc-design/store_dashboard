from dash import Output, Input, html
import dash_bootstrap_components as dbc
from queries import get_live_stock, get_expiry

def register_callbacks(app):

    @app.callback(
        Output("stock_table","children"),
        Output("total_stock_value","children"),
        Output("negative_items","children"),
        Output("expiry_table","children"),
        Input("auto_refresh","n_intervals")
    )
    def load_dashboard(_):

        df = get_live_stock()

        if df.empty:
            return "No data",0,0,""

        total_value = round(df["available_stock"].sum(),2)
        negative = len(df[df["available_stock"] < 0])

        table = dbc.Table.from_dataframe(df.head(100), striped=True, bordered=True, hover=True)

        # expiry
        exp = get_expiry()
        exp_table = dbc.Table.from_dataframe(exp.head(50), striped=True, bordered=True, hover=True)

        return table,total_value,negative,exp_table
