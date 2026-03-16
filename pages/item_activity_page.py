import pandas as pd
import urllib
from sqlalchemy import create_engine
from dash import html, dcc, dash_table, Input, Output, callback
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
from dash import callback_context
import warnings
from sqlalchemy.exc import SAWarning
from dash import no_update
import io
warnings.filterwarnings("ignore", category=SAWarning)


# 🔹 Column display names (UI only)
DISPLAY_NAMES = {
    "deployment_name": "Outlet Name",
    "item_name": "Item Name",
    "partner_names": "Partner",
    "activity_user": "Updated By",
    "out_of_stock_time": "Out of Stock Time",
    "in_stock_time": "In Stock Time",
    "out_of_stock_hours": "Downtime (hrs)",
    "out_date": "Out Date",
}
DISPLAY_NAMES["downtime_bucket"] = "Downtime Bucket"

params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=coffee_island_analytics;"
    "Trusted_Connection=yes;"
)

ENGINE = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

def load_item_activity(start_date, end_date, outlet, item):

    start_date = (
        pd.to_datetime(start_date)
        .normalize()
        .strftime("%Y-%m-%d %H:%M:%S")
    )

    end_date = (
        pd.to_datetime(end_date)
        .normalize()
        .replace(hour=23, minute=59, second=59)
        .strftime("%Y-%m-%d %H:%M:%S")
    )

    query = """
    WITH ordered_events AS (
        SELECT
            deployment_name,
            item_name,
            partner_names,
            activity_user,
            activity,

            DATETIMEFROMPARTS(
                YEAR(from_date),
                MONTH(from_date),
                DAY(from_date),
                DATEPART(HOUR, from_time),
                DATEPART(MINUTE, from_time),
                DATEPART(SECOND, from_time),
                0
            ) AS event_ts,

            LEAD(activity) OVER (
                PARTITION BY deployment_name, item_name
                ORDER BY
                    DATETIMEFROMPARTS(
                        YEAR(from_date),
                        MONTH(from_date),
                        DAY(from_date),
                        DATEPART(HOUR, from_time),
                        DATEPART(MINUTE, from_time),
                        DATEPART(SECOND, from_time),
                        0
                    )
            ) AS next_activity,

            LEAD(
                DATETIMEFROMPARTS(
                    YEAR(from_date),
                    MONTH(from_date),
                    DAY(from_date),
                    DATEPART(HOUR, from_time),
                    DATEPART(MINUTE, from_time),
                    DATEPART(SECOND, from_time),
                    0
                )
            ) OVER (
                PARTITION BY deployment_name, item_name
                ORDER BY
                    DATETIMEFROMPARTS(
                        YEAR(from_date),
                        MONTH(from_date),
                        DAY(from_date),
                        DATEPART(HOUR, from_time),
                        DATEPART(MINUTE, from_time),
                        DATEPART(SECOND, from_time),
                        0
                    )
            ) AS next_event_ts
        FROM dbo.activity_log
        WHERE item_type = 'item'
    )
    SELECT
        deployment_name,
        item_name,
        partner_names,
        activity_user,
        event_ts AS out_of_stock_time,
        next_event_ts AS in_stock_time,
        DATEDIFF(MINUTE, event_ts, next_event_ts) AS out_of_stock_minutes,
        CAST(event_ts AS date) AS out_date
    FROM ordered_events
    WHERE activity = 'out of stock'
    AND next_activity = 'in stock'
    AND (
            event_ts BETWEEN ? AND ?
        OR next_event_ts BETWEEN ? AND ?
    )
    """

    params = [
        start_date,
        end_date,
        start_date,
        end_date,
    ]

    if outlet:
        query += " AND deployment_name = ?"
        params.append(outlet)

    if item:
        query += " AND item_name = ?"
        params.append(item)

    return pd.read_sql(query, ENGINE, params=tuple(params))

def load_currently_out_of_stock(outlet=None, item=None):
    query = """
    WITH latest_status AS (
        SELECT
            deployment_name,
            item_name,
            activity,
            CAST(CONCAT(from_date,' ',CONVERT(varchar,from_time,108)) AS datetime) AS event_ts,
            ROW_NUMBER() OVER (
                PARTITION BY deployment_name, item_name
                ORDER BY CAST(CONCAT(from_date,' ',CONVERT(varchar,from_time,108)) AS datetime) DESC
            ) rn
        FROM dbo.activity_log
        WHERE item_type = 'item'
    )
    SELECT deployment_name, item_name, event_ts
    FROM latest_status
    WHERE rn = 1 AND activity = 'out of stock'
    """

    params = []

    if outlet:
        query += " AND deployment_name = ?"
        params.append(outlet)

    if item:
        query += " AND item_name = ?"
        params.append(item)

    return pd.read_sql(query, ENGINE, params=tuple(params))

def outlet_downtime_ranking(df):
    ranking = (
        df.groupby("deployment_name")
        .agg(
            total_events=("item_name", "count"),
            total_hours=("out_of_stock_hours", "sum"),
            avg_hours=("out_of_stock_hours", "mean"),
        )
        .reset_index()
    )

    ranking["total_hours"] = ranking["total_hours"].round(2)
    ranking["avg_hours"] = ranking["avg_hours"].round(2)

    ranking = ranking.sort_values("total_hours", ascending=False)

    return ranking[
        ["deployment_name", "total_events", "total_hours", "avg_hours"]
    ]

def load_available_months():
    q = """
    SELECT DISTINCT
        YEAR(from_date)  AS year_num,
        MONTH(from_date) AS month_num
    FROM dbo.activity_log
    WHERE item_type = 'item'
      AND from_date IS NOT NULL
      AND YEAR(from_date) IS NOT NULL
      AND MONTH(from_date) IS NOT NULL
    ORDER BY year_num, month_num
    """

    df = pd.read_sql(q, ENGINE)

    if df.empty:
        return pd.DataFrame(columns=["label", "value"])

    # Ensure numeric safety
    df["year_num"] = pd.to_numeric(df["year_num"], errors="coerce")
    df["month_num"] = pd.to_numeric(df["month_num"], errors="coerce")

    df = df.dropna(subset=["year_num", "month_num"])

    # Build proper datetime safely
    df["date"] = pd.to_datetime(
        dict(
            year=df["year_num"].astype(int),
            month=df["month_num"].astype(int),
            day=1
        )
    )

    df["label"] = df["date"].dt.strftime("%b %Y")
    df["value"] = df["date"].dt.strftime("%Y-%m")

    return df[["label", "value"]]



BLINK_STYLE = {
    "@keyframes blink-red": {
        "0%": {"backgroundColor": "#fee2e2"},
        "50%": {"backgroundColor": "#ef4444", "color": "white"},
        "100%": {"backgroundColor": "#fee2e2"},
    }
}

def get_layout():
    return dbc.Container(
        fluid=True,
        children=[

            html.H4("📦 Item Stock Activity Dashboard"),

            dcc.Interval(
                id="ia_live-refresh",
                interval=6000 * 1000,  # 60 seconds
                n_intervals=0
            ),

            dbc.Row(
                [
                    # 📅 DATE RANGE
                    dbc.Col(
                        [
                            html.Label("Date Range", className="fw-semibold"),
                            dcc.DatePickerRange(
                                id="ia_date-range",
                                display_format="DD-MM-YYYY",
                                start_date=pd.Timestamp.today().replace(day=1).date(),
                                end_date=pd.Timestamp.today().date(),
                                clearable=False,
                                style={"width": "100%"},
                            ),
                        ],
                        md=3,
                    ),

                    # 📆 MONTH
                    dbc.Col(
                        [
                            html.Label("Month", className="fw-semibold"),
                            dcc.Dropdown(
                                id="ia_month-filter",
                                placeholder="Select Month",
                                clearable=False,
                            ),
                        ],
                        md=2,
                    ),

                    # 🏬 OUTLET
                    dbc.Col(
                        [
                            html.Label("Outlet Name", className="fw-semibold"),
                            dcc.Dropdown(
                                id="ia_outlet-filter",
                                placeholder="Select Outlet",
                            ),
                        ],
                        md=3,
                    ),

                    # 🍽 ITEM
                    dbc.Col(
                        [
                            html.Label("Item Name", className="fw-semibold"),
                            dcc.Dropdown(
                                id="ia_item-filter",
                                placeholder="Select Item",
                            ),
                        ],
                        md=4,
                    ),
                ],
                className="mb-3 align-items-end",
            ),

            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    "Out of Stock Events",
                                    className="fw-semibold text-white"
                                ),
                                dbc.CardBody(
                                    html.H4(id="ia_kpi-events", className="text-white")
                                ),
                            ],
                            style={
                                "background": "linear-gradient(135deg, #ec4899, #db2777)",
                                "borderRadius": "12px",
                                "boxShadow": "0 6px 16px rgba(0,0,0,0.15)",
                                "border": "none",
                            },
                        ),
                        md=3,
                    ),

                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    "Total Downtime (hrs)",
                                    className="fw-semibold text-white"
                                ),
                                dbc.CardBody(
                                    html.H4(id="ia_kpi-downtime", className="text-white")
                                ),
                            ],
                            style={
                                "background": "linear-gradient(135deg, #059669, #047857)",
                                "borderRadius": "12px",
                                "boxShadow": "0 6px 16px rgba(0,0,0,0.15)",
                                "border": "none",
                            },
                        ),
                        md=3,
                    ),

                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    "Avg Downtime (hrs)",
                                    className="fw-semibold text-white"
                                ),
                                dbc.CardBody(
                                    html.H4(id="ia_kpi-avg", className="text-white")
                                ),
                            ],
                            style={
                                "background": "linear-gradient(135deg, #f59e0b, #d97706)",
                                "borderRadius": "12px",
                                "boxShadow": "0 6px 16px rgba(0,0,0,0.15)",
                                "border": "none",
                            },
                        ),
                        md=3,
                    ),
                ],
                className="mb-4 g-3",
            ),

            dbc.Row(
                [
                    dbc.Col(
                        dbc.Alert(
                            [
                                html.H5("🔴 Currently Out of Stock"),
                                dash_table.DataTable(
                                    id="ia_live-out-table",
                                    page_size=8,
                                    style_table={"overflowX": "auto"},
                                    style_cell={"textAlign": "left"},
                                    style_data_conditional=[
                                        {
                                            "if": {
                                                "filter_query": "{since_hours} >= 24",
                                            },
                                            "animation": "blink-red 1.2s infinite",
                                            "fontWeight": "bold",
                                        }
                                    ],
                                ),
                            ],
                            color="danger",
                        ),
                        md=12,
                    )
                ],
                className="mb-4",
            ),
            dbc.Button(
                "⬇ Export Currently Out of Stock",
                id="ia_export-live-btn",
                color="danger",
                size="sm",
                className="mt-2"
            ),
            dcc.Download(id="ia_download-live"),

            html.H5("⏱ Item Activity Ledger"),
            dash_table.DataTable(
                id="ia_ledger-table",
                page_size=20,
                sort_action="native",
                filter_action="native",
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "left"},
            ),
            dbc.Button(
                "⬇ Export Ledger",
                id="ia_export-ledger-btn",
                color="primary",
                size="sm",
                className="mt-2"
            ),
            dcc.Download(id="ia_download-ledger"),

            dbc.Row([
                dbc.Col(
                    [
                        html.H5("🏬 Outlet-wise Downtime Ranking"),
                        dash_table.DataTable(
                            id="ia_outlet-ranking-table",
                            page_size=10,
                            sort_action="native",
                            style_table={"overflowX": "auto"},
                            style_cell={"textAlign": "left"},
                        )
                    ],
                    md=12
                )
            ], className="mb-4"),
        ]
    )

def register_callbacks(app):

    @app.callback(
        # 🔹 FILTER CONTROLS
        Output("ia_month-filter", "options"),
        Output("ia_month-filter", "value"),
        Output("ia_date-range", "start_date"),
        Output("ia_date-range", "end_date"),
        Output("ia_outlet-filter", "options"),
        Output("ia_item-filter", "options"),
        Output("ia_item-filter", "value"),

        # 🔹 DASHBOARD OUTPUTS
        Output("ia_ledger-table", "data"),
        Output("ia_ledger-table", "columns"),
        Output("ia_kpi-events", "children"),
        Output("ia_kpi-downtime", "children"),
        Output("ia_kpi-avg", "children"),
        Output("ia_outlet-ranking-table", "data"),
        Output("ia_outlet-ranking-table", "columns"),
        Output("ia_live-out-table", "data"),
        Output("ia_live-out-table", "columns"),

        # 🔹 INPUTS
        Input("ia_month-filter", "value"),
        Input("ia_date-range", "start_date"),
        Input("ia_date-range", "end_date"),
        Input("ia_outlet-filter", "value"),
        Input("ia_item-filter", "value"),
        Input("ia_live-refresh", "n_intervals"),
    )
    def unified_dashboard(
        selected_month,
        start_date,
        end_date,
        outlet,
        item,
        _
    ):
        ctx = callback_context
        trigger = ctx.triggered_id

        # --------------------------------------------------
        # 1️⃣ MONTH OPTIONS + DEFAULT
        # --------------------------------------------------
        month_df = load_available_months()
        month_options = month_df.to_dict("records")

        if not selected_month and month_options:
            selected_month = month_options[-1]["value"]

        # --------------------------------------------------
        # 2️⃣ MONTH → DATE RANGE
        # --------------------------------------------------
        if trigger == "ia_month-filter" and selected_month:
            year, month = map(int, selected_month.split("-"))
            start = pd.Timestamp(year, month, 1)

            today = pd.Timestamp.today()
            end = today if (year == today.year and month == today.month) \
                else start + pd.offsets.MonthEnd(1)

            start_date = start.date()
            end_date = end.date()

        if not start_date or not end_date:
            raise PreventUpdate

        # --------------------------------------------------
        # 3️⃣ OUTLET OPTIONS
        # --------------------------------------------------
        outlet_df = pd.read_sql(
            "SELECT DISTINCT deployment_name FROM dbo.activity_log",
            ENGINE
        )

        outlet_options = [
            {"label": o, "value": o}
            for o in sorted(outlet_df["deployment_name"].dropna())
        ]

        # --------------------------------------------------
        # 4️⃣ ITEM OPTIONS (DEPENDS ON OUTLET)
        # --------------------------------------------------
        if outlet:
            q = """
            SELECT DISTINCT item_name
            FROM dbo.activity_log
            WHERE item_type='item'
              AND deployment_name = ?
            """
            item_df = pd.read_sql(q, ENGINE, params=(outlet,))
        else:
            q = """
            SELECT DISTINCT item_name
            FROM dbo.activity_log
            WHERE item_type='item'
            """
            item_df = pd.read_sql(q, ENGINE)

        item_options = [
            {"label": i, "value": i}
            for i in sorted(item_df["item_name"].dropna())
        ]

        # Reset item if outlet changed
        if trigger == "ia_outlet-filter":
            item = None

        # --------------------------------------------------
        # 5️⃣ MAIN LEDGER DATA
        # --------------------------------------------------
        df = load_item_activity(start_date, end_date, outlet, item)

        if df.empty:
            return (
                month_options,
                selected_month,
                start_date,
                end_date,
                outlet_options,
                item_options,
                None,
                [], [], 0, 0, 0,
                [], [], [], []
            )

        # Convert minutes → hours FIRST
        df["out_of_stock_hours"] = (df["out_of_stock_minutes"] / 60).round(2)
        df["out_of_stock_hours"] = df["out_of_stock_hours"].clip(lower=0)
        df.drop(columns=["out_of_stock_minutes"], inplace=True)

        def bucket_hours(h):
            if h < 4:
                return "0–4 hrs"
            elif h < 10:
                return "4–10 hrs"
            elif h < 24:
                return "10–24 hrs"
            else:
                return "24+ hrs"

        df["downtime_bucket"] = df["out_of_stock_hours"].apply(bucket_hours)

        # THEN compute ranking
        ranking_df = outlet_downtime_ranking(df)
        

        for col in ["out_of_stock_time", "in_stock_time"]:
            df[col] = pd.to_datetime(df[col]).dt.strftime("%d-%b-%Y %H:%M:%S")

        df["out_date"] = pd.to_datetime(df["out_date"]).dt.strftime("%d-%b-%Y")
        
        ledger_columns = [
            {"name": DISPLAY_NAMES.get(c, c.replace("_", " ").title()), "id": c}
            for c in df.columns
        ]

        ranking_columns = [
            {"name": "Outlet Name", "id": "deployment_name"},
            {"name": "Out of Stock Events", "id": "total_events"},
            {"name": "Total Downtime (hrs)", "id": "total_hours"},
            {"name": "Avg Downtime (hrs)", "id": "avg_hours"},
        ]

        # --------------------------------------------------
        # 6️⃣ LIVE OUT-OF-STOCK (FILTERED)
        # --------------------------------------------------
        live_df = load_currently_out_of_stock(outlet, item)

        if live_df.empty:
            live_data, live_columns = [], []
        else:
            live_df["_ts"] = pd.to_datetime(live_df["event_ts"])
            live_df["event_ts"] = live_df["_ts"].dt.strftime("%d-%b-%Y %H:%M:%S")
            live_df["since_hours"] = (
                (pd.Timestamp.now() - live_df["_ts"])
                .dt.total_seconds() / 3600
            ).round(2)

            live_columns = [
                {"name": "Outlet Name", "id": "deployment_name"},
                {"name": "Item Name", "id": "item_name"},
                {"name": "Out Since", "id": "event_ts"},
                {"name": "Hours Down", "id": "since_hours"},
            ]

            live_data = live_df.drop(columns="_ts").to_dict("records")

        # --------------------------------------------------
        # 7️⃣ FINAL RETURN (ORDER MATTERS)
        # --------------------------------------------------
        return (
            month_options,
            selected_month,
            start_date,
            end_date,
            outlet_options,
            item_options,
            item,
            df.to_dict("records"),
            ledger_columns,
            len(df),
            round(df["out_of_stock_hours"].sum(), 2),
            round(df["out_of_stock_hours"].mean(), 2),
            ranking_df.to_dict("records"),
            ranking_columns,
            live_data,
            live_columns,
        )


    @app.callback(
        Output("ia_download-ledger", "data"),
        Output("ia_download-live", "data"),
        Input("ia_export-ledger-btn", "n_clicks"),
        Input("ia_export-live-btn", "n_clicks"),
        Input("ia_month-filter", "value"),
        Input("ia_date-range", "start_date"),
        Input("ia_date-range", "end_date"),
        Input("ia_outlet-filter", "value"),
        Input("ia_item-filter", "value"),
        prevent_initial_call=True
    )
    def export_data(
        ledger_click,
        live_click,
        selected_month,
        start_date,
        end_date,
        outlet,
        item
    ):

        ctx = callback_context
        if not ctx.triggered:
            raise PreventUpdate

        trigger_id = ctx.triggered_id

        # Format date for filename
        start_str = pd.to_datetime(start_date).strftime("%d-%b-%Y")
        end_str = pd.to_datetime(end_date).strftime("%d-%b-%Y")

        # --------------------------------------------------
        # 🔵 Ledger Export
        # --------------------------------------------------
        if trigger_id == "ia_export-ledger-btn":

            df = load_item_activity(start_date, end_date, outlet, item)

            if df.empty:
                raise PreventUpdate

            # Convert minutes → hours
            df["out_of_stock_hours"] = (
                df["out_of_stock_minutes"] / 60
            ).round(2)

            # Remove unwanted column
            df.drop(columns=["out_of_stock_minutes"], inplace=True)

            # Rename columns
            df = df.rename(columns={
                "deployment_name": "Outlet Name",
                "item_name": "Item Name",
                "partner_names": "Partner Names",
                "activity_user": "Activity User",
                "out_of_stock_time": "Out of Stock Time",
                "in_stock_time": "In Stock Time",
                "out_date": "Out Date",
                "out_of_stock_hours": "Out of Stock Hours",
            })

            # Reorder columns
            df = df[
                [
                    "Outlet Name",
                    "Item Name",
                    "Partner Names",
                    "Activity User",
                    "Out of Stock Time",
                    "In Stock Time",
                    "Out Date",
                    "Out of Stock Hours",
                ]
            ]

            filename = f"Ledger_{start_str}_to_{end_str}.xlsx"

            return (
                dcc.send_data_frame(df.to_excel, filename, index=False),
                None
            )


        # --------------------------------------------------
        # 🔴 Currently Out of Stock Export
        # --------------------------------------------------
        if trigger_id == "ia_export-live-btn":

            live_df = load_currently_out_of_stock(outlet, item)

            if live_df.empty:
                raise PreventUpdate

            live_df["since_hours"] = (
                (pd.Timestamp.now() - pd.to_datetime(live_df["event_ts"]))
                .dt.total_seconds() / 3600
            ).round(2)

            # Rename columns
            live_df = live_df.rename(columns={
                "deployment_name": "Outlet Name",
                "item_name": "Item Name",
                "event_ts": "Out Since",
                "since_hours": "Hours Down",
            })

            # Reorder columns
            live_df = live_df[
                [
                    "Outlet Name",
                    "Item Name",
                    "Out Since",
                    "Hours Down",
                ]
            ]

            filename = f"Currently_Out_Stock_{start_str}_to_{end_str}.xlsx"

            return (
                None,
                dcc.send_data_frame(live_df.to_excel, filename, index=False)
            )
        return no_update, no_update

