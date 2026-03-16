from dash import html, dcc, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px

try:
    # Normal app execution (main_app.py)
    from summary_page import (
        tx_df,
        COL_DATE,
        COL_WAREHOUSE,
        COL_OUTLET,
        COL_ITEM,
    )
except ImportError:
    # Direct execution fallback
    import sys, os
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

    from pages.warehouse.summary_page import (
        tx_df,
        COL_DATE,
        COL_WAREHOUSE,
        COL_OUTLET,
        COL_ITEM,
    )

# ======================================================
# LAYOUT
# ======================================================
def get_layout():
    return dbc.Container(
        [
            html.H4("Outlet Consumption"),
            html.Div(id="outlet-content"),
        ],
        fluid=True,
    )


# ======================================================
# CALLBACKS
# ======================================================
def register_callbacks(app):

    @app.callback(
        Output("outlet-content", "children"),
        Input("f_warehouse", "value"),
        Input("f_category", "value"),
        Input("f_item", "value"),
        Input("f_date", "start_date"),
        Input("f_date", "end_date"),
    )
    def update_outlet(w, c, i, sd, ed):

        # ---------- SAFETY ----------
        if tx_df is None or tx_df.empty:
            return dbc.Alert("No transaction data available.", color="warning")

        df = tx_df.copy()

        # ---- ONLY STOCK OUT ----
        df = df[df["qty_out"] > 0]

        if df.empty:
            return dbc.Alert("No outlet consumption data found.", color="warning")

        # ---- FILTERS ----
        if sd:
            df = df[df[COL_DATE] >= sd]
        if ed:
            df = df[df[COL_DATE] <= ed]
        if w:
            df = df[df[COL_WAREHOUSE] == w]
        if c:
            df = df[df[COL_CATEGORY] == c]
        if i:
            df = df[df[COL_ITEM] == i]

        if df.empty:
            return dbc.Alert("No data after applying filters.", color="warning")

        # ================= KPI CALCULATIONS =================
        outlet_summary = (
            df.groupby(COL_OUTLET, as_index=False)
              .agg(
                  total_qty=("qty_out", "sum"),
                  indent_days=(COL_DATE, "nunique"),
              )
              .sort_values("total_qty", ascending=False)
        )

        top_outlets = outlet_summary.head(10)

        outlet_bar = px.bar(
            top_outlets,
            x=COL_OUTLET,
            y="total_qty",
            title="Top 10 Consuming Outlets",
            text_auto=True,
        )

        item_kpi = (
            df.groupby(COL_ITEM, as_index=False)
              .agg(total_qty=("qty_out", "sum"))
              .sort_values("total_qty", ascending=False)
              .head(10)
        )

        item_bar = px.bar(
            item_kpi,
            x=COL_ITEM,
            y="total_qty",
            title="Top 10 Consumed Items",
            text_auto=True,
        )

        # ================= UI =================
        return dbc.Container(
            [
                dbc.Row(
                    [
                        dbc.Col(dcc.Graph(figure=outlet_bar), md=6),
                        dbc.Col(dcc.Graph(figure=item_bar), md=6),
                    ],
                    className="mb-4",
                ),
                html.H5("Outlet-wise Consumption Summary"),
                dbc.Table.from_dataframe(
                    outlet_summary,
                    striped=True,
                    bordered=True,
                    hover=True,
                    size="sm",
                ),
            ],
            fluid=True,
        )
