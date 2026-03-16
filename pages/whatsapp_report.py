import pandas as pd
import os
import dash
from dash import html, dcc, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
from dash import State
import plotly.io as pio
import tempfile
import zipfile
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Image, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors
from dash import dash_table

pio.defaults.default_width = 900
pio.defaults.default_height = 500
pio.defaults.default_scale = 1
pio.defaults.mathjax = None
# =============================
# Load Data
# =============================
BASE_DIR = r"C:\Users\ACER\store_dashboard\whatsapp_valuefirst"
file_path = os.path.join(BASE_DIR, "Whataspp_message_report.xlsx")

df = pd.read_excel(file_path)

# Clean column names
df.columns = (
    df.columns
    .str.replace("'", "", regex=False)
    .str.replace(".", "", regex=False)
    .str.strip()
    .str.replace(" ", "_")
)

# Fix dates
df['Sent_At'] = pd.to_datetime(df['Sent_At'], errors='coerce')
df['Delivered_At'] = pd.to_datetime(df['Delivered_At'], errors='coerce')

df['Sent_At'] = df['Sent_At'].dt.date
df['Delivered_At'] = df['Delivered_At'].dt.date

# Fix mobile numbers
df['Recipient_M_No'] = df['Recipient_M_No'].astype(str).str.replace(".0", "", regex=False)

# =============================
# KPIs
# =============================
total_msgs = len(df)
read_msgs = (df['Status'] == 'Read').sum()
failed_msgs = (df['Status'] == 'Failed').sum()
active_days = df['Sent_At'].nunique()
delivery_rate = round((read_msgs / total_msgs) * 100, 2)



print("Total Messages:", total_msgs)

# =============================
# Dash App
# =============================
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    suppress_callback_exceptions=True
)

# =============================
# KPI Card Component
# =============================
def kpi_card(title, id_, color):
    return dbc.Card(
        dbc.CardBody([
            html.H6(title, className="text-muted"),
            html.H3(id=id_, className="fw-bold")
        ]),
        color=color,
        inverse=True,
        className="shadow-sm"
    )

kpi_row = dbc.Row([
    dbc.Col(kpi_card("Total Messages", "kpi_total", "primary"), md=3),
    dbc.Col(kpi_card("Read Messages", "kpi_read", "success"), md=3),
    dbc.Col(kpi_card("Failed Messages", "kpi_failed", "danger"), md=3),
    dbc.Col(kpi_card("Delivery %", "kpi_delivery", "info"), md=3),
], className="mb-3")


# =============================
# Date Filter
# =============================
date_filter = dcc.DatePickerRange(
    id="date_filter",
    min_date_allowed=df['Sent_At'].min(),
    max_date_allowed=df['Sent_At'].max(),
    start_date=df['Sent_At'].min(),
    end_date=df['Sent_At'].max(),
    display_format="DD-MM-YYYY"
)

FAILURE_REASON_MAP = {
    130472: (
        "User's number is part of an experiment\n"
        "• This number is part of a Meta-led experiment where marketing messages are\n" 
        "   intentionally blocked to study engagement. Such users won’t receive\n" 
        "   marketing templates unless:\n"
        "• Active customer service window\n"
        "• Existing marketing conversation\n"
        "• a free-entry point conversation exists"
    ),
    131000: (
        "• General Meta platform error (WhatsApp Business API).\n"
        "• Non-specific error from Meta systems."
    ),
    131026: (
        "Recipient cannot receive message:\n"
        "• Number not registered on WhatsApp\n"
        "• Terms & Privacy not accepted\n"
        "• Outdated WhatsApp version"
    ),
    131049: (
        "• Marketing message limit reached.\n"
        "• Per-user 24-hour marketing limit exceeded."
    ),
    100: ("Misc. Error is the error where operators do not provide us the reason of failure.\n"
          " It generally happens when receiving end operators find multiple reasons of failure\n" 
          " while delivering the message to the handsets."),
    500: (" "),
    
    None: "Status needs to be updated"
}
df['Failure_Description'] = df['Reason_of_failure'].map(FAILURE_REASON_MAP)

df['Failure_Description'] = df['Failure_Description'].fillna(
    "Unknown / Unmapped error"
)

# =============================
# Chart Function
# =============================
def daily_chart(data):
    daily = data.groupby('Sent_At').size().reset_index(name='Messages')
    fig = px.line(
        daily,
        x='Sent_At',
        y='Messages',
        markers=True,
        title="Messages Trend (Daily)"
    )
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=20, r=20, t=50, b=20)
    )
    return fig
def status_chart(data):
    status_df = (
        data.groupby('Status')
        .size()
        .reset_index(name='Count')
    )

    fig = px.bar(
        status_df,
        x='Status',
        y='Count',
        text='Count',
        title="Message Status Summary"
    )
    fig.update_layout(template="plotly_white")
    return fig

def read_delivered_chart(data):
    daily = (
        data.groupby(['Sent_At', 'Status'])
        .size()
        .reset_index(name='Count')
    )

    daily = daily[daily['Status'].isin(['Read', 'Delivered'])]

    fig = px.line(
        daily,
        x='Sent_At',
        y='Count',
        color='Status',
        markers=True,
        title="Read & Delivered Messages (Date-wise)"
    )
    fig.update_layout(template="plotly_white")
    return fig

def delivery_vs_failed_chart(data):
    delivered = (data['Status'] == 'Delivered').sum()
    failed = (data['Status'] == 'Failed').sum()

    df_pie = pd.DataFrame({
        "Status": ["Delivered", "Failed"],
        "Count": [delivered, failed]
    })

    fig = px.pie(
        df_pie,
        names="Status",
        values="Count",
        hole=0.45,
        title="Delivered vs Failed (%)"
    )
    fig.update_layout(template="plotly_white")
    return fig

def failure_reason_chart(data):
    failed = data[data['Status'] == 'Failed']

    if failed.empty:
        return px.pie(title="No Failed Messages")

    reason_df = (
        failed
        .groupby(['Reason_of_failure', 'Failure_Description'])
        .size()
        .reset_index(name='Count')
    )

    reason_df["Short_Label"] = reason_df["Reason_of_failure"].astype(str) + " – Failure"

    fig = px.pie(
        reason_df,
        names="Short_Label",
        values="Count",
        title="Failure Reason Breakdown",
        hover_data=["Failure_Description", "Reason_of_failure"]
    )

    fig.update_traces(textposition='inside', textinfo='percent+label')

    # ✅ ADD THIS BLOCK HERE
    fig.update_layout(
        template="plotly_white",
        legend=dict(
            orientation="v",
            y=0.5,
            font=dict(size=10)
        )
    )

    return fig


def build_kpi_table(data, styles):
    total = len(data)
    delivered = (data['Status'] == 'Delivered').sum()
    read = (data['Status'] == 'Read').sum()
    failed = (data['Status'] == 'Failed').sum()
    delivery_pct = round((delivered / total) * 100, 2) if total else 0

    kpi_data = [
        ["Metric", "Value"],
        ["Total Messages", total],
        ["Delivered", delivered],
        ["Read", read],
        ["Failed", failed],
        ["Delivery %", f"{delivery_pct}%"]
    ]

    table = Table(kpi_data, colWidths=[200, 200])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
        ('GRID', (0,0), (-1,-1), 1, colors.grey),
        ('FONT', (0,0), (-1,0), 'Helvetica-Bold'),
        ('ALIGN', (1,1), (-1,-1), 'CENTER'),
        ('BOTTOMPADDING', (0,0), (-1,-1), 10),
    ]))

    return table

def pdf_header(canvas, doc, start_date, end_date):
    canvas.saveState()

    # Title
    canvas.setFont("Helvetica-Bold", 14)
    canvas.drawString(2 * cm, 28 * cm, "WhatsApp Message Report")

    # Date range
    canvas.setFont("Helvetica", 10)
    canvas.drawString(
        2 * cm,
        27.3 * cm,
        f"Date Range: {start_date} to {end_date}"
    )

    # Line
    canvas.setLineWidth(0.5)
    canvas.line(2 * cm, 27 * cm, 19 * cm, 27 * cm)

    canvas.restoreState()

def get_date_wise_pivot(data):
    pivot = (
        data
        .groupby(["Sent_At", "Status"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
        .sort_values("Sent_At")
    )

    for col in ["Delivered", "Read", "Failed"]:
        if col not in pivot.columns:
            pivot[col] = 0

    pivot["Total"] = pivot[["Delivered", "Read", "Failed"]].sum(axis=1)

    # ✅ FORMAT DATE FOR DISPLAY (DD-MM-YYYY)
    pivot["Sent_At"] = pd.to_datetime(pivot["Sent_At"]).dt.strftime("%d-%m-%Y")

    return pivot


def build_pivot_summary_table(data):
    # Ensure data is sorted by date
    data = data.sort_values("Sent_At")

    pivot = (
        data
        .groupby(["Sent_At", "Status"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    # Ensure required columns exist
    for col in ["Delivered", "Read", "Failed"]:
        if col not in pivot.columns:
            pivot[col] = 0

    pivot["Total"] = pivot[["Delivered", "Read", "Failed"]].sum(axis=1)

    table_data = [
        ["Date", "Delivered", "Read", "Failed", "Total"]
    ]

    for _, row in pivot.iterrows():
        table_data.append([
            pd.to_datetime(row["Sent_At"]).strftime("%d-%m-%Y"),
            int(row["Delivered"]),
            int(row["Read"]),
            int(row["Failed"]),
            int(row["Total"])
        ])


    table = Table(
        table_data,
        repeatRows=1,
        colWidths=[90, 80, 80, 80, 80]
    )

    table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('FONT', (0,0), (-1,0), 'Helvetica-Bold'),
        ('ALIGN', (1,1), (-1,-1), 'CENTER'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))

    return table

def build_failure_reason_table(data):
    failed = data[data["Status"] == "Failed"]

    if failed.empty:
        return Paragraph("No failed messages in selected date range.", getSampleStyleSheet()["Normal"])

    summary = (
        failed
        .groupby(["Reason_of_failure", "Failure_Description"])
        .size()
        .reset_index(name="Count")
        .sort_values("Count", ascending=False)
    )

    table_data = [
        ["Error Code", "Failure Reason", "Count"]
    ]

    for _, row in summary.iterrows():
        table_data.append([
            str(row["Reason_of_failure"]),
            row["Failure_Description"],
            int(row["Count"])
        ])

    table = Table(
        table_data,
        colWidths=[70, 320, 60],  # better wrapping
        repeatRows=1
    )

    table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('FONT', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('ALIGN', (2,1), (2,-1), 'CENTER'),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))
    summary = summary.sort_values("Count", ascending=False)
    return table


# =============================
# Layout
# =============================
app.layout = dbc.Container([
    html.H2("📊 WhatsApp Message Report", className="my-3 fw-bold"),

    kpi_row,

    dbc.Card([
        dbc.CardBody([
            html.Label("Select Date Range", className="fw-bold"),
            date_filter
        ])
    ], className="mb-3 shadow-sm"),

    dbc.Card([
        dbc.CardBody([
            dcc.Graph(id="daily_graph")
        ])
    ], className="shadow-sm"),
    dbc.Row([
    dbc.Col(
        dbc.Card(
            dbc.CardBody([
                dcc.Graph(id="status_graph")
            ]),
            className="shadow-sm"
        ),
        md=6
    ),

    dbc.Col(
        dbc.Card(
            dbc.CardBody([
                dcc.Graph(id="read_delivered_graph")
            ]),
            className="shadow-sm"
        ),
        md=6
    ),
    ], className="mb-3"),
    dbc.Row([
    dbc.Col(
        dbc.Card(
            dbc.CardBody([
                dcc.Graph(id="delivery_failed_graph")
            ]),
            className="shadow-sm"
        ),
        md=6
    ),
    dbc.Col(
        dbc.Card(
            dbc.CardBody([
                dcc.Graph(id="failure_reason_graph")
            ]),
            className="shadow-sm"
        ),
        md=6
    ),
    ], className="mb-3"),
    dbc.Card([
    dbc.CardBody([
        html.H5("📅 Date-wise Status Summary", className="fw-bold"),
            dash_table.DataTable(
                id="date_status_table",
                columns=[
                    {"name": "Date", "id": "Sent_At"},
                    {"name": "Delivered", "id": "Delivered"},
                    {"name": "Read", "id": "Read"},
                    {"name": "Failed", "id": "Failed"},
                    {"name": "Total", "id": "Total"},
                ],
                data=[],
                page_size=15,
                sort_action="native",
                style_table={"overflowX": "auto"},
                style_header={
                    "backgroundColor": "#f8f9fa",
                    "fontWeight": "bold",
                    "textAlign": "center",
                },
                style_cell={
                    "textAlign": "center",
                    "padding": "6px",
                    "fontSize": "13px",
                },
            )
        ])
    ], className="shadow-sm mb-3"),

    dbc.Button(
    "📥 Download PDF Report",
    id="download_pdf",
    color="dark",
    className="mb-3"
    ),
    dcc.Download(id="pdf_download")


], fluid=True)

# =============================
# Callback
# =============================
@app.callback(
    Output("kpi_total", "children"),
    Output("kpi_read", "children"),
    Output("kpi_failed", "children"),
    Output("kpi_delivery", "children"),

    Output("daily_graph", "figure"),
    Output("status_graph", "figure"),
    Output("read_delivered_graph", "figure"),
    Output("delivery_failed_graph", "figure"),
    Output("failure_reason_graph", "figure"),
    Output("date_status_table", "data"),

    Input("date_filter", "start_date"),
    Input("date_filter", "end_date")
)

def update_dashboard(start, end):

    if start and end:
        filtered = df[
            (df['Sent_At'] >= pd.to_datetime(start).date()) &
            (df['Sent_At'] <= pd.to_datetime(end).date())
        ]
    else:
        filtered = df
    total = len(filtered)
    read = (filtered['Status'] == 'Read').sum()
    failed = (filtered['Status'] == 'Failed').sum()
    delivery_pct = round((read / total) * 100, 2) if total else 0

    pivot_df = get_date_wise_pivot(filtered)

    return (
        total,
        read,
        failed,
        f"{delivery_pct}%",   # KPI values

        daily_chart(filtered),
        status_chart(filtered),
        read_delivered_chart(filtered),
        delivery_vs_failed_chart(filtered),
        failure_reason_chart(filtered),
        pivot_df.to_dict("records")
    )


@app.callback(
    Output("pdf_download", "data"),
    Input("download_pdf", "n_clicks"),
    State("date_filter", "start_date"),
    State("date_filter", "end_date"),
    prevent_initial_call=True
)
def download_pdf(n_clicks, start, end):

    print("PDF download started")  # DEBUG

    if start and end:
        filtered = df[
            (df['Sent_At'] >= pd.to_datetime(start).date()) &
            (df['Sent_At'] <= pd.to_datetime(end).date())
        ]
    else:
        filtered = df

    # LIMIT DATA (VERY IMPORTANT)
    pdf_data = filtered

    with tempfile.TemporaryDirectory() as tmpdir:
        pdf_path = os.path.join(tmpdir, "WhatsApp_Report.pdf")

        doc = SimpleDocTemplate(
            pdf_path,
            pagesize=A4,
            topMargin=3.5 * cm
        )
        styles = getSampleStyleSheet()
        elements = []

        # KPI PAGE
        elements.append(Paragraph("WhatsApp Message Report", styles["Title"]))
        elements.append(Spacer(1, 12))
        elements.append(build_kpi_table(filtered, styles))
        elements.append(PageBreak())

        # CHARTS (use FULL data)
        charts = [
            ("Daily Trend", daily_chart(filtered)),
            ("Status Summary", status_chart(filtered)),
            ("Read vs Delivered", read_delivered_chart(filtered)),
            ("Delivered vs Failed", delivery_vs_failed_chart(filtered)),
            #("Failure Reason", failure_reason_chart(filtered)),
        ]

        for title, fig in charts:
            img_path = os.path.join(tmpdir, f"{title}.png")
            pio.write_image(fig, img_path, format="png", width=900, height=500)

            elements.append(Paragraph(title, styles["Heading2"]))
            elements.append(Spacer(1, 10))
            elements.append(Image(img_path, width=500, height=280))
            elements.append(PageBreak())

        # PIVOT SUMMARY
        elements.append(Paragraph("Date-wise Status Summary", styles["Heading1"]))
        elements.append(Spacer(1, 12))
        elements.append(build_pivot_summary_table(filtered))
        elements.append(PageBreak())

        # FAILURE REASON SECTION
        elements.append(Paragraph("Failure Reason Analysis", styles["Heading1"]))
        elements.append(Spacer(1, 12))

        # Failure chart image
        img_path = os.path.join(tmpdir, "Failure Reason.png")
        pio.write_image(failure_reason_chart(filtered), img_path, format="png", width=900, height=500)
        elements.append(Image(img_path, width=500, height=280))

        elements.append(Spacer(1, 15))

        # Failure reason table
        elements.append(Paragraph("Failure Reason Details", styles["Heading2"]))
        elements.append(Spacer(1, 10))
        elements.append(build_failure_reason_table(filtered))

        elements.append(PageBreak())

        doc.build(
            elements,
            onFirstPage=lambda c, d: pdf_header(
                c, d,
                start_date = pd.to_datetime(filtered['Sent_At'].min()).strftime("%d-%m-%Y"),
                end_date = pd.to_datetime(filtered['Sent_At'].max()).strftime("%d-%m-%Y")

            ),
            onLaterPages=lambda c, d: pdf_header(
                c, d,
                start_date = pd.to_datetime(filtered['Sent_At'].min()).strftime("%d-%m-%Y"),
                end_date = pd.to_datetime(filtered['Sent_At'].max()).strftime("%d-%m-%Y")
            )
        )

        # SEND TO BROWSER (IMPORTANT)
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()

        print("PDF ready, sending to browser")  # DEBUG

        return dcc.send_bytes(
            pdf_bytes,
            filename="WhatsApp_Report.pdf"
        )






# =============================
# Run Server
# =============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8052, debug=False, use_reloader=False)
