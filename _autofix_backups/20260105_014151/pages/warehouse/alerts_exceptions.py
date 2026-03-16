from dash import html, dcc, Input, Output
import dash_bootstrap_components as dbc
import pandas as pd

try:
    from .summary_page import (
        tx_df,
        COL_DATE,
        COL_WAREHOUSE,
        COL_ITEM,
        COL_CATEGORY,
    )
except ImportError:
    # fallback when running directly (IDE/debug)
    import sys, os
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
    from pages.warehouse.summary_page import (
        tx_df,
        COL_DATE,
        COL_WAREHOUSE,
        COL_ITEM,
        COL_CATEGORY,
    )

ALERT_DEAD_DAYS = 30

def get_layout():
    return dbc.Container([
        html.H4("Warehouse Alerts & Exceptions"),
        html.Div(id="alerts-content")
    ], fluid=True)

def register_callbacks(app):

    @app.callback(
        Output("alerts-content", "children"),
        Input("f_warehouse", "value"),
        Input("f_category", "value"),
        Input("f_item", "value"),
        Input("f_date", "end_date"),
    )
    def update_alerts(w, c, i, end_date):

        df = tx_df.copy()

        if w:
            df = df[df[COL_WAREHOUSE] == w]
        if c:
            df = df[df[COL_CATEGORY] == c]
        if i:
            df = df[df[COL_ITEM] == i]

        ref_date = pd.to_datetime(end_date)

        # ---------------- STOCK POSITION ----------------
        kpi = (
            df.groupby([COL_WAREHOUSE, COL_ITEM, COL_CATEGORY], as_index=False)
              .agg(
                  stock_in=("qty_in", "sum"),
                  stock_out=("qty_out", "sum"),
                  last_entry=(COL_DATE, "max")
              )
        )

        kpi["current_stock"] = kpi["stock_in"] - kpi["stock_out"]
        kpi["age_days"] = (ref_date - kpi["last_entry"]).dt.days

        # ---------------- ALERTS ----------------
        negative_stock = kpi[kpi["current_stock"] < 0]
        zero_stock     = kpi[kpi["current_stock"] == 0]
        dead_stock     = kpi[kpi["age_days"] > ALERT_DEAD_DAYS]

        alerts = []

        # -------- KPI BANNERS --------
        alerts.append(
            dbc.Row([
                dbc.Col(dbc.Alert(f"❌ Negative Stock: {len(negative_stock)}", color="danger"), md=3),
                dbc.Col(dbc.Alert(f"⚠️ Zero Stock Items: {len(zero_stock)}", color="warning"), md=3),
                dbc.Col(dbc.Alert(f"🕒 Dead Stock (> {ALERT_DEAD_DAYS} days): {len(dead_stock)}", color="secondary"), md=3),
                dbc.Col(dbc.Alert(f"📦 Total Items: {len(kpi)}", color="info"), md=3),
            ], className="mb-3")
        )

        # -------- TABLE SECTIONS --------
        if not negative_stock.empty:
            alerts.extend([
                html.H5("❌ Negative Stock Items"),
                dbc.Table.from_dataframe(
                    negative_stock.sort_values("current_stock"),
                    striped=True, bordered=True, hover=True
                ),
                html.Hr()
            ])

        if not zero_stock.empty:
            alerts.extend([
                html.H5("⚠️ Zero Stock Items"),
                dbc.Table.from_dataframe(
                    zero_stock,
                    striped=True, bordered=True, hover=True
                ),
                html.Hr()
            ])

        if not dead_stock.empty:
            alerts.extend([
                html.H5("🕒 Dead Stock (No Movement > 30 Days)"),
                dbc.Table.from_dataframe(
                    dead_stock.sort_values("age_days", ascending=False),
                    striped=True, bordered=True, hover=True
                )
            ])

        if not alerts:
            return dbc.Alert(
                "No alerts detected for the selected filters.",
                color="success"
            )

        return alerts
