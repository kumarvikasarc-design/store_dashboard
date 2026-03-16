from dash import html, dcc, Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
try:
    from .summary_page import tx_df, COL_DATE, COL_WAREHOUSE, COL_ITEM
except ImportError:
    # Direct execution fallback
    import sys, os
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

    from pages.warehouse.summary_page import (
        tx_df,
        COL_DATE,
        COL_WAREHOUSE,
        COL_ITEM,
    )

def get_layout():
    return dbc.Container([
        html.H4("Warehouse Stock Aging"),
        html.Div(id="aging-content")
    ], fluid=True)

def register_callbacks(app):

    @app.callback(
        Output("aging-content", "children"),
        Input("f_warehouse", "value"),
        Input("f_category", "value"),
        Input("f_item", "value"),
        Input("f_date", "end_date"),
    )
    def update_aging(w, c, i, end_date):

        df = tx_df.copy()
        df = df[df["qty_in"] > 0]  # STOCK-IN only

        if w:
            df = df[df[COL_WAREHOUSE] == w]
        if c:
            df = df[df[COL_CATEGORY] == c]
        if i:
            df = df[df[COL_ITEM] == i]

        ref_date = pd.to_datetime(end_date)

        last_in = (
            df.groupby([COL_WAREHOUSE, COL_ITEM], as_index=False)
              .agg(last_entry=(COL_DATE, "max"))
        )

        last_in["age_days"] = (ref_date - last_in["last_entry"]).dt.days

        def bucket(d):
            if d <= 7:
                return "0–7 Days"
            elif d <= 15:
                return "8–15 Days"
            elif d <= 30:
                return "16–30 Days"
            return "30+ Days"

        last_in["aging_bucket"] = last_in["age_days"].apply(bucket)

        summary = (
            last_in["aging_bucket"]
            .value_counts()
            .reset_index()
            .rename(columns={"index": "Bucket", "aging_bucket": "Items"})
        )

        bar = px.bar(
            summary,
            x="Bucket",
            y="Items",
            title="Stock Aging Distribution"
        )

        return dbc.Row([
            dbc.Col(dcc.Graph(figure=bar), md=6),
            dbc.Col(
                dbc.Table.from_dataframe(
                    last_in.sort_values("age_days", ascending=False).head(10),
                    striped=True, bordered=True
                ),
                md=6
            )
        ])
