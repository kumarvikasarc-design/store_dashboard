# app.py - Final cleaned version (uses exact paths provided)
import os
import base64
import pandas as pd
from datetime import datetime, timedelta, date
from dash import Dash, html, dcc, Input, Output, State, dash_table, callback_context
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.io as pio

# ---------------------------
# File paths (confirmed)
# ---------------------------
BASE_DIR = r"C:\Users\ACER\store_dashboard"
DSR_FILE = os.path.join(BASE_DIR, "DSR_Dashboard.csv")
STORE_CSV = os.path.join(BASE_DIR, "stores_db.csv")
MONTH_FORMAT = "%b-%y"   # e.g., "Nov-25"

# ---------------------------
# Helpers
# ---------------------------
def safe_read_csv(path):
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        print(f"File not found: {path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"ERROR reading csv {path}: {e}")
        return pd.DataFrame()

def month_key(m):
    try:
        return datetime.strptime(m, MONTH_FORMAT)
    except Exception:
        return datetime.min

def make_sparkline_base64(x, y, width=320, height=60):
    # optional helper; requires kaleido to export. fallback returns empty string.
    try:
        if len(x) == 0:
            return ""
        fig = px.line(x=x, y=y, markers=True)
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            height=height,
            width=width
        )
        img_bytes = pio.to_image(fig, format='png', width=width, height=height, scale=1)
        b64 = base64.b64encode(img_bytes).decode('ascii')
        return f"![spark](data:image/png;base64,{b64})"
    except Exception:
        return ""

# ---------------------------
# Load CSVs
# ---------------------------
raw = safe_read_csv(DSR_FILE)
stores = safe_read_csv(STORE_CSV)

print(f"Loaded DSR rows: {len(raw)} from {DSR_FILE}")
print(f"Loaded Stores rows: {len(stores)} from {STORE_CSV}")

# Normalize column names to exact strings (strip whitespace)
if not raw.empty:
    raw.columns = [str(c).strip() for c in raw.columns]
if not stores.empty:
    stores.columns = [str(c).strip() for c in stores.columns]

# Map alternate column names to canonical column names (non-destructive)
col_map = {}
for c in raw.columns:
    lc = c.lower().strip()
    if lc in ("store id", "store_id", "storecode", "store code"):
        col_map[c] = "Store Code"
    if lc in ("outlet name", "outlet", "store name", "store_name"):
        col_map[c] = "Outlet Name"
    if lc in ("date", "business date", "business_date"):
        col_map[c] = "Date"
    if lc in ("source", "channel"):
        col_map[c] = "Source"
    if lc in ("no of items", "no_of_items", "items"):
        col_map[c] = "No Of Items"
    if lc in ("no of bills", "no_of_bills", "bills", "transactions"):
        col_map[c] = "No Of Bills"
    if lc in ("sale", "gross amount", "total amount"):
        col_map[c] = "Sale"
    if lc in ("net sale", "netsale", "net_sale"):
        col_map[c] = "Net Sale"
    if lc in ("hour",):
        col_map[c] = "Hour"

if col_map:
    raw = raw.rename(columns=col_map)

# Auto-format Date column
if "Date" in raw.columns:
    raw["Date"] = pd.to_datetime(raw["Date"], errors="coerce", dayfirst=True)
    raw["Month"] = raw["Date"].dt.strftime("%b-%y")

# Ensure minimal expected columns exist
expected = ['Date', 'Outlet Name', 'Source', 'Net Sale', 'Charges', 'No Of Bills', 'Store Code']
for c in expected:
    if c not in raw.columns:
        raw[c] = None

# Safe list of numeric columns (based on your sample)
numeric_cols = [
    "No Of Items", "No Of Bills", "Employee Meal Bills",
    "Sale", "Discount", "Restaurant Charge",
    "Packaging Charge [CART - SWIGGY]", "Restaurant Packaging Charges",
    "Delivery Charge", "Platform Fee Charge", "Smile Amount Charge",
    "Packaging Charge", "Charges", "Net Sale",
    "GST @18%", "GST @5%", "ECom_GST@5%", "GST @40%", "GST @12%",
    "Packaging charge@20Rs", "Total Tax", "Total Amount",
    "Round Off"
]

for c in numeric_cols:
    if c in raw.columns:
        col = raw[c]
        if isinstance(col, pd.DataFrame):
            print(f"Flattening duplicate column: {c}")
            col = col.sum(axis=1)  # or .iloc[:,0] if you only want the first
        raw[c] = pd.to_numeric(col, errors="coerce").fillna(0.0)

# ---- FLATTEN DUPLICATE COLUMNS (Sale, Discount, Charges, etc.) ----
def flatten_duplicate_cols(df, colname):
    """Combine numeric duplicates like 'Sale', 'Sale.1', 'Sale.2'."""
    cols = [c for c in df.columns if c == colname or c.startswith(colname + ".")]
    if len(cols) == 1:
        # only one column
        df[colname] = pd.to_numeric(df[cols[0]], errors='coerce').fillna(0)
    else:
        # combine all duplicates
        df[colname] = df[cols].apply(pd.to_numeric, errors='coerce').fillna(0).sum(axis=1)

    # drop the duplicates (keep only the main column)
    for c in cols:
        if c != colname:
            df.drop(columns=c, inplace=True)

    return df

# Flatten Sale, Discount, Packaging Charge, etc.
for cname in [
    "Sale",
    "Discount",
    "Restaurant Charge",
    "Packaging Charge",
    "Packaging Charge [CART - SWIGGY]",
    "Restaurant Packaging Charges",
    "Delivery Charge",
    "Platform Fee Charge",
    "Smile Amount Charge"
]:
    if any(c == cname or c.startswith(cname + ".") for c in raw.columns):
        flatten_duplicate_cols(raw, cname)

# ---------------------------
# FIXED CHARGES + NET SALE LOGIC
# ---------------------------

# ---- CALCULATIONS ----

# Charges
raw["Charges"] = (
    raw["Restaurant Charge"]
    + raw["Packaging Charge [CART - SWIGGY]"]
    + raw["Restaurant Packaging Charges"]
    + raw["Delivery Charge"]
    + raw["Platform Fee Charge"]
    + raw["Smile Amount Charge"]
    + raw["Packaging Charge"]
)

# Net Sale = Sale - Discount
raw["Net Sale"] = raw["Sale"] - raw["Discount"]

# NetSaleAdj = Net Sale + Charges
raw["NetSaleAdj"] = raw["Net Sale"] + raw["Charges"]

# Ensure Gross Amount exists
if "Gross Amount" not in raw.columns:
    raw["Gross Amount"] = raw["Net Sale"].fillna(0) + raw["Total Tax"].fillna(0)

# ---------------------------
# STORE MASTER NORMALIZATION & MERGE (FIXED)
# ---------------------------
if stores.empty:
    stores = pd.DataFrame(columns=[
        'Store Id', 'Outlet Name', 'Region', 'City',
        'Type', 'Area Manager', 'Opening Date', 'Status'
    ])
else:
    # Ensure expected columns exist
    for col in ['Store Id', 'Outlet Name', 'Region', 'City',
                'Type', 'Area Manager', 'Opening Date', 'Status']:
        if col not in stores.columns:
            stores[col] = None

    # Strip whitespace in key text columns
    for col in ['Store Id', 'Outlet Name', 'Region', 'City']:
        if col in stores.columns:
            stores[col] = stores[col].astype(str).str.strip()

# Standardize join keys in RAW
if 'Outlet Name' in raw.columns:
    raw['Outlet Name'] = raw['Outlet Name'].astype(str).str.strip()
if 'Store Code' in raw.columns:
    raw['Store Code'] = raw['Store Code'].astype(str).str.strip()

# If raw lacks Store Id but has Store Code, create Store Id from Store Code
if 'Store Id' not in raw.columns and 'Store Code' in raw.columns:
    raw['Store Id'] = raw['Store Code']
elif 'Store Id' in raw.columns:
    raw['Store Id'] = raw['Store Id'].astype(str).str.strip()

# Also strip Store Id in stores if present
if 'Store Id' in stores.columns:
    stores['Store Id'] = stores['Store Id'].astype(str).str.strip()

# Merge region/city from store master using best available keys
merge_keys = []
if 'Outlet Name' in raw.columns and 'Outlet Name' in stores.columns:
    merge_keys.append('Outlet Name')
if 'Store Id' in raw.columns and 'Store Id' in stores.columns:
    merge_keys.append('Store Id')

if merge_keys:
    print(f"Merging store master on keys: {merge_keys}")
    raw = raw.merge(
        stores[['Store Id', 'Outlet Name', 'Region', 'City']].drop_duplicates(),
        on=merge_keys,
        how='left'
    )
else:
    print("WARNING: Could not merge store master – no common keys found.")

# ---------------------------
# Working dataframe
# ---------------------------
df = raw.copy()

# Derived fields
if "Date" in df.columns:
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Month"] = df["Date"].dt.strftime("%b-%y")
else:
    df["Month"] = None

try:
    df['Week'] = df['Date'].dt.isocalendar().week
    df['Year'] = df['Date'].dt.isocalendar().year
except Exception:
    df['Week'] = df['Date'].dt.week
    df['Year'] = df['Date'].dt.year
df['DayName'] = df['Date'].dt.day_name()

# Hour fallback
if 'Hour' in df.columns:
    df['Hour'] = pd.to_numeric(df['Hour'], errors='coerce').fillna(12).astype(int)
else:
    try:
        df['Hour'] = df['Date'].dt.hour.fillna(12).astype(int)
    except Exception:
        df['Hour'] = 12

# DayCode mapping
def get_day_code(dayname):
    if dayname in ['Monday', 'Tuesday', 'Wednesday', 'Thursday']:
        return 'WD'
    if dayname == 'Friday':
        return 'WF'
    return 'WE'

df['DayCode'] = df['DayName'].apply(lambda d: get_day_code(d) if pd.notna(d) else '')

# Daypart mapping (custom ranges)
def in_range_wrap(hour, start_h, end_h):
    if start_h <= end_h:
        return start_h <= hour <= end_h
    else:
        return hour >= start_h or hour <= end_h

def map_custom_daypart(h):
    try:
        h = int(h) % 24
    except Exception:
        return "Unknown"
    if in_range_wrap(h, 6, 8):
        return "Pre Breakfast"
    if in_range_wrap(h, 11, 15):
        return "Lunch"
    if in_range_wrap(h, 15, 19):
        return "Snacks"
    if in_range_wrap(h, 19, 23):
        return "Dinner"
    if in_range_wrap(h, 23, 6):
        return "Late Night"
    return "Unknown"

df['Daypart'] = df['Hour'].apply(map_custom_daypart)

# SourceParticular mapping
DELIVERY_SOURCES = set(["Swiggy", "Zomato", "MagicPin-Ordering", "Swiggy-Bolt Urgent"])
APP_SOURCES = set(["Mobile App", "App"])
DINEIN_SOURCES = set(["POS", "DineIn", "Dine In"])

def map_source_particular(src):
    try:
        s = str(src)
        if s in APP_SOURCES:
            return 'App'
        if s in DINEIN_SOURCES:
            return 'DineIn'
        if s in DELIVERY_SOURCES or any(x.lower() in s.lower() for x in ['swiggy', 'zomato', 'magicpin']):
            return 'Delivery'
        return 'Other'
    except Exception:
        return 'Other'

df['SourceParticular'] = df['Source'].apply(map_source_particular) if 'Source' in df.columns else 'Other'

# NetSaleAdj (Net Sale + Charges) - safe creation
if 'Net Sale' in df.columns and 'Charges' in df.columns:
    df['NetSaleAdj'] = df['Net Sale'].fillna(0.0) + df['Charges'].fillna(0.0)
else:
    # fallback - if already present, keep; else zeros
    df['NetSaleAdj'] = df.get('NetSaleAdj', 0.0)

# Dropdown values
REGIONS = sorted(df['Region'].dropna().unique().tolist()) if 'Region' in df.columns else []
CITIES = sorted(df['City'].dropna().unique().tolist()) if 'City' in df.columns else []
OUTLETS = sorted(df['Outlet Name'].dropna().unique().tolist()) if 'Outlet Name' in df.columns else []

if 'Date' in df.columns and not df['Date'].isna().all():
    REPORT_DATE = df['Date'].max().date()
else:
    REPORT_DATE = date.today()

MONTHS = sorted(df["Month"].dropna().unique(), key=month_key)

# ---------------------------
# Metric helpers
# ---------------------------
def iso_week_start(d):
    if isinstance(d, datetime):
        d = d.date()
    return d - timedelta(days=d.weekday())

def compute_ads(df_period):
    if df_period.empty:
        return 0.0
    days = df_period['Date'].dt.date.nunique()
    return df_period['NetSaleAdj'].sum() / days if days > 0 else 0.0

def compute_adt(df_period):
    if df_period.empty:
        return 0.0
    days = df_period['Date'].dt.date.nunique()
    return df_period['No Of Bills'].sum() / days if days > 0 else 0.0

def compute_aov(df_period):
    bills = df_period['No Of Bills'].sum()
    return df_period['NetSaleAdj'].sum() / bills if bills > 0 else 0.0

def build_full_summary(dff, report_date=None):
    rows = []
    rd = REPORT_DATE if report_date is None else (pd.to_datetime(report_date).date() if not isinstance(report_date, date) else report_date)
    if dff.empty or not set(['Outlet Name', 'Month']).issubset(set(dff.columns)):
        return pd.DataFrame(rows)

    grouped = dff.groupby(['Outlet Name', 'Month'])
    for (outlet, month), g in grouped:
        total_net_for_outlet_month = g['NetSaleAdj'].sum()
        wstart = iso_week_start(rd)
        days_into_week = (rd - wstart).days
        lw_start = wstart - timedelta(days=7)
        last4_start = wstart - timedelta(days=28)
        last4_end = wstart - timedelta(days=1)

        g_overall = g
        g_app = g[g['SourceParticular'] == 'App']
        g_dine = g[g['SourceParticular'] == 'DineIn']
        g_del = g[g['SourceParticular'] == 'Delivery']

        Particulars = [('Overall', g_overall), ('App', g_app), ('DineIn', g_dine), ('Delivery', g_del)]
        for cat_name, cat_df in Particulars:
            wtd = cat_df[(cat_df['Date'].dt.date >= wstart) & (cat_df['Date'].dt.date <= rd)]
            lwtd = cat_df[(cat_df['Date'].dt.date >= lw_start) & (cat_df['Date'].dt.date <= (lw_start + timedelta(days=days_into_week)))]
            mstart = rd.replace(day=1)
            mtd = cat_df[(cat_df['Date'].dt.date >= mstart) & (cat_df['Date'].dt.date <= rd)]
            prev_month_end = mstart - timedelta(days=1)
            prev_month_start = prev_month_end.replace(day=1)
            lmt_end = prev_month_start + timedelta(days=rd.day - 1)
            lmt = cat_df[(cat_df['Date'].dt.date >= prev_month_start) & (cat_df['Date'].dt.date <= lmt_end)]
            prev_week = cat_df[(cat_df['Date'].dt.date >= lw_start) & (cat_df['Date'].dt.date <= (lw_start + timedelta(days=6)))]
            last4 = cat_df[(cat_df['Date'].dt.date >= last4_start) & (cat_df['Date'].dt.date <= last4_end)]
            yesterday = rd - timedelta(days=1)
            ydf = cat_df[cat_df['Date'].dt.date == yesterday]
            lwsd = cat_df[cat_df['Date'].dt.date == (yesterday - timedelta(days=7))]
            l4_vals = []
            for i in range(1, 5):
                dtemp = yesterday - timedelta(days=7 * i)
                l4_vals.append(cat_df[cat_df['Date'].dt.date == dtemp]['NetSaleAdj'].sum())
            l4wsd_avg = (sum(l4_vals) / 4.0) if l4_vals else 0.0

            wtd_val = round(wtd['NetSaleAdj'].sum(), 2)
            lwtd_val = round(lwtd['NetSaleAdj'].sum(), 2)
            change_pct = None
            if lwtd_val != 0:
                change_pct = round((wtd_val - lwtd_val) / lwtd_val * 100.0, 2)
            mtd_val = round(mtd['NetSaleAdj'].sum(), 2)
            lmt_val = round(lmt['NetSaleAdj'].sum(), 2)
            prev_week_val = round(prev_week['NetSaleAdj'].sum(), 2)
            last4_val = round(last4['NetSaleAdj'].sum(), 2)
            y_val = round(ydf['NetSaleAdj'].sum(), 2)
            lwsd_val = round(lwsd['NetSaleAdj'].sum(), 2)
            aov = round(compute_aov(cat_df), 2)
            adt = round(compute_adt(cat_df), 2)
            ads = round(compute_ads(cat_df), 2)

            mix_pct = round((cat_df['NetSaleAdj'].sum() / total_net_for_outlet_month * 100.0), 2) if total_net_for_outlet_month > 0 else 0.0

            row = {
                "Outlet Name": outlet,
                "Month": month,
                "Particular": cat_name,
                "WTD": wtd_val,
                "LWTD": lwtd_val,
                "Change (%)": change_pct,
                "MTD": mtd_val,
                "LMTD": lmt_val,
                "WeekNumber": int(wstart.isocalendar()[1]),
                "WeekRange": f"{wstart.strftime('%d-%b')} to {(wstart + timedelta(days=6)).strftime('%d-%b-%Y')}",
                "PreviousWeek": prev_week_val,
                "Last4Weeks": last4_val,
                "Yesterday": y_val,
                "LWSD": lwsd_val,
                "L4WSD": round(l4wsd_avg, 2),
                "AOV": aov,
                "ADT": adt,
                "ADS": ads,
                "Mix %": mix_pct
            }
            rows.append(row)
    return pd.DataFrame(rows)

# ---------------------------
# Dash app + layout
# ---------------------------
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
server = app.server

app.index_string = '''<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>DSR Dashboard</title>
        {%css%}
        <style>
            .kpi-animate { animation: fadeIn 0.9s ease-in-out; }
            @keyframes fadeIn { from {opacity:0; transform:translateY(6px);} to {opacity:1; transform:translateY(0);} }
            .kpi-pulse { animation: pulse 1.2s infinite; }
            @keyframes pulse { 0%{transform:scale(1);} 50%{transform:scale(1.02);} 100%{transform:scale(1);} }
            .hamburger { font-size: 22px; cursor: pointer; padding:6px; }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>{%config%}{%scripts%}{%renderer%}</footer>
    </body>
</html>'''

SUMMARY_COLUMNS = [
    {"name": "Outlet Name", "id": "Outlet Name"},
    {"name": "Month", "id": "Month"},
    {"name": "Particular", "id": "Particular"},
    {"name": "AOV", "id": "AOV"},
    {"name": "ADT", "id": "ADT"},
    {"name": "ADS", "id": "ADS"},
    {"name": "WTD", "id": "WTD"},
    {"name": "LWTD", "id": "LWTD"},
    {"name": "Change (%)", "id": "Change (%)"},
    {"name": "MTD", "id": "MTD"},
    {"name": "LMTD", "id": "LMTD"},
    {"name": "WeekNumber", "id": "WeekNumber"},
    {"name": "WeekRange", "id": "WeekRange"},
    {"name": "PreviousWeek", "id": "PreviousWeek"},
    {"name": "Last4Weeks", "id": "Last4Weeks"},
    {"name": "Yesterday", "id": "Yesterday"},
    {"name": "LWSD", "id": "LWSD"},
    {"name": "L4WSD", "id": "L4WSD"},
    {"name": "Mix %", "id": "Mix %"}
]

RAW_COLUMNS = [{"name": c, "id": c} for c in df.columns]

# App layout
app.layout = dbc.Container([
    dcc.Location(id='url'),
    dbc.Row([
        dbc.Col(html.Span("☰", id="open-sidebar", className="hamburger"), width="auto"),
        dbc.Col(html.H2("DSR Analytics", style={'textAlign': 'center', 'marginTop': '6px'})),
    ], align='center', className='mb-2'),

    # Offcanvas sidebar
    dbc.Offcanvas(
        [
            html.H5("Filters & Daypart"),
            html.Hr(),
            html.Label("Region"),
            dcc.Dropdown(id='side-region', options=[{'label': r, 'value': r} for r in REGIONS], multi=True),
            html.Br(),
            html.Label("City"),
            dcc.Dropdown(id='side-city', options=[{'label': c, 'value': c} for c in CITIES], multi=True),
            html.Br(),
            html.Label("Outlet"),
            dcc.Dropdown(id='side-outlet', options=[{'label': o, 'value': o} for o in OUTLETS], multi=True),
            html.Br(),
            html.Label("Daypart"),
            dcc.Dropdown(id='dp-daypart', options=[
                {'label': 'Pre Breakfast', 'value': 'Pre Breakfast'},
                {'label': 'Lunch', 'value': 'Lunch'},
                {'label': 'Snacks', 'value': 'Snacks'},
                {'label': 'Dinner', 'value': 'Dinner'},
                {'label': 'Late Night', 'value': 'Late Night'},
            ], multi=True),
            html.Br(),
            html.Label("Daypart Date Range"),
            dcc.DatePickerRange(id='dp-date',
                                start_date=(REPORT_DATE - timedelta(days=14)),
                                end_date=REPORT_DATE),
            html.Br(), html.Br(),
            dbc.Button("Apply Daypart", id='dp-apply', color='primary'),
            html.Hr(),
            html.Div("DayCode KPIs", className='fw-bold'),
            html.Div(id='daycode-cards'),
        ],
        id="offcanvas",
        title="Controls",
        is_open=False,
        placement='start'
    ),

    html.Div(id='tab-content', style={'marginTop': '12px'}),
    dcc.Store(id='selected-outlet', data=None),

    # Modal for details
    dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle("Summary Row - Detail")),
            dbc.ModalBody([
                html.Div(id='modal-meta'),
                dash_table.DataTable(
                    id='modal-raw-table',
                    page_size=15,
                    style_table={'overflowX': 'auto'},
                    columns=RAW_COLUMNS
                )
            ]),
            dbc.ModalFooter(dbc.Button("Close", id="modal-close", className="ms-auto", n_clicks=0))
        ],
        id="detail-modal", is_open=False, size="xl"
    )
], fluid=True)

# ---------------------------
# Main content rendering
# ---------------------------
@app.callback(Output('tab-content', 'children'), Input('url', 'pathname'))
def render_tab(_):
    return dbc.Container([
        dbc.Card(dbc.CardBody([
            dbc.Row([
                dbc.Col(dcc.Dropdown(
                    id='region-drop',
                    options=[{'label': r, 'value': r} for r in REGIONS],
                    multi=True,
                    placeholder="Region"
                ), md=3),
                dbc.Col(dcc.Dropdown(
                    id='city-drop',
                    options=[{'label': c, 'value': c} for c in CITIES],
                    multi=True,
                    placeholder="City"
                ), md=3),
                dbc.Col(dcc.Dropdown(
                    id='outlet-drop',
                    options=[{'label': o, 'value': o} for o in OUTLETS],
                    multi=True,
                    placeholder="Outlet"
                ), md=3),
                dbc.Col(dcc.Dropdown(
                    id='month-drop',
                    options=[{'label': m, 'value': m} for m in MONTHS],
                    placeholder="Month"
                ), md=3),
            ], className='g-2'),
            html.Br(),
            dbc.Row([
                dbc.Col(dcc.Dropdown(
                    id='particular-filter',
                    options=[
                        {'label': 'Overall', 'value': 'Overall'},
                        {'label': 'App', 'value': 'App'},
                        {'label': 'DineIn', 'value': 'DineIn'},
                        {'label': 'Delivery', 'value': 'Delivery'}
                    ],
                    value=None,
                    clearable=True
                ), md=3),
                dbc.Col(dcc.Dropdown(
                    id='week-drop',
                    options=[],
                    multi=True,
                    placeholder="WeekNumber"
                ), md=3),
                dbc.Col(dbc.Button("🔄 Refresh", id='refresh-btn', color='primary'), md=2),
                dbc.Col(dbc.Button("⟲ Reset Filters", id='reset-btn', color='secondary'), md=2)
            ])
        ]), className='mb-3'),

        dbc.Row(id='kpi-row', className='g-2 mb-3'),

        dbc.Row([
            dbc.Col(dcc.Graph(id='region-chart'), md=4),
            dbc.Col(dcc.Graph(id='monthly-chart'), md=4),
            dbc.Col(dcc.Graph(id='weekly-chart'), md=4)
        ], className='mb-3'),

        dbc.Row([
            dbc.Col(dcc.Graph(id='source-chart'), md=6),
            dbc.Col(dcc.Graph(id='outlet-chart'), md=6)
        ], className='mb-3'),

        dbc.Row([
            dbc.Col(html.H5("Summary Table (click a row to open details)"), md=8),
            dbc.Col(html.Div(id='daycode-compare-wrapper', children=[
                html.H6("DayCode Comparison"),
                dcc.Graph(id='daycode-compare')
            ]), md=4)
        ]),

        dash_table.DataTable(
            id='summary-table',
            columns=SUMMARY_COLUMNS,
            data=[],
            page_size=12,
            style_table={'overflowX': 'auto'},
            filter_action='native',
            sort_action='native',
            row_selectable='single',
            style_header={
                "backgroundColor": "#f8f9fa",
                "fontWeight": "700",
                "fontSize": "18px",
                "fontFamily": "Times New Roman",
                "textAlign": "left",
                "border": "1px solid #ccc"
            },
            style_cell={
                "fontFamily": "Times New Roman",
                "fontSize": "14px",
                "textAlign": "left",
                "padding": "6px"
            }
        ),

        html.H5("DSR Raw Data", style={'marginTop': '18px'}),
        dash_table.DataTable(
            id='dsr-raw',
            columns=RAW_COLUMNS,
            page_size=8,
            style_table={'overflowX': 'auto'},
            filter_action='native',
            sort_action='native',
            style_header={
                "backgroundColor": "#f8f9fa",
                "fontWeight": "700",
                "fontSize": "18px",
                "fontFamily": "Times New Roman",
                "textAlign": "left",
                "border": "1px solid #ccc"
            },
            style_cell={
                "fontFamily": "Times New Roman",
                "fontSize": "14px",
                "textAlign": "left",
                "padding": "6px"
            }
        ),
    ], fluid=True)

# ---------------------------
# Offcanvas toggle (hamburger)
# ---------------------------
@app.callback(
    Output("offcanvas", "is_open"),
    Input("open-sidebar", "n_clicks"),
    State("offcanvas", "is_open"),
    prevent_initial_call=False
)
def toggle_offcanvas(n, is_open):
    if n:
        return not is_open
    return is_open

# ---------------------------
# Reset filters
# ---------------------------
@app.callback(
    Output('region-drop', 'value'),
    Output('city-drop', 'value'),
    Output('outlet-drop', 'value'),
    Output('month-drop', 'value'),
    Output('week-drop', 'value'),
    Output('particular-filter', 'value'),
    Input('reset-btn', 'n_clicks'),
    prevent_initial_call=True
)
def reset_filters(n):
    return None, None, None, None, None, 'Overall'

# ---------------------------
# Update city/outlet options when region chosen or region-chart clicked
# ---------------------------
@app.callback(
    Output('city-drop', 'options'),
    Output('outlet-drop', 'options'),
    Input('region-drop', 'value'),
    Input('region-chart', 'clickData'),
    prevent_initial_call=False
)
def update_city_outlet_options(region_vals, region_click):
    active_regions = None
    if region_click and 'points' in region_click:
        try:
            active_regions = [region_click['points'][0]['label']]
        except Exception:
            active_regions = region_vals
    else:
        active_regions = region_vals

    df_f = df[df['Region'].isin(active_regions)] if active_regions else df
    cities = sorted(df_f['City'].dropna().unique().tolist()) if 'City' in df_f.columns else []
    outlets = sorted(df_f['Outlet Name'].dropna().unique().tolist()) if 'Outlet Name' in df_f.columns else []
    return [{'label': c, 'value': c} for c in cities], [{'label': o, 'value': o} for o in outlets]

# ---------------------------
# Update week options when month selected / clicked
# ---------------------------
@app.callback(
    Output('week-drop', 'options'),
    Input('month-drop', 'value'),
    Input('monthly-chart', 'clickData'),
    prevent_initial_call=False
)
def update_week_options(month_val, month_click):
    sel_month = None
    if month_click and 'points' in month_click:
        try:
            sel_month = month_click['points'][0]['x']
        except Exception:
            sel_month = month_val
    else:
        sel_month = month_val

    if sel_month:
        weeks = sorted(df[df['Month'] == sel_month]['Week'].dropna().unique().tolist())
    else:
        weeks = sorted(df['Week'].dropna().unique().tolist())
    return [{'label': f"Week {int(w)}", 'value': int(w)} for w in weeks]

# ---------------------------
# Main update: KPIs, charts, tables
# ---------------------------
@app.callback(
    Output('kpi-row', 'children'),
    Output('region-chart', 'figure'),
    Output('monthly-chart', 'figure'),
    Output('weekly-chart', 'figure'),
    Output('source-chart', 'figure'),
    Output('outlet-chart', 'figure'),
    Output('summary-table', 'data'),
    Output('dsr-raw', 'data'),
    Input('region-drop', 'value'),
    Input('city-drop', 'value'),
    Input('outlet-drop', 'value'),
    Input('month-drop', 'value'),
    Input('week-drop', 'value'),
    Input('particular-filter', 'value'),
    Input('refresh-btn', 'n_clicks'),
    Input('region-chart', 'clickData'),
    Input('monthly-chart', 'clickData'),
    Input('weekly-chart', 'clickData'),
    Input('source-chart', 'clickData'),
    Input('selected-outlet', 'data'),
    prevent_initial_call=False
)
def update_dashboard(region_val, city_val, outlet_val, month_val, week_val, particular_val,
                     refresh_clicks, region_click, monthly_click, weekly_click, source_click, selected_store):
    dff = df.copy()

    # Filters
    if region_val:
        dff = dff[dff['Region'].isin(region_val)]
    if city_val:
        dff = dff[dff['City'].isin(city_val)]
    if outlet_val:
        dff = dff[dff['Outlet Name'].isin(outlet_val)]
    if month_val:
        dff = dff[dff['Month'] == month_val]
    if week_val:
        if isinstance(week_val, list):
            dff = dff[dff['Week'].isin(week_val)]
        else:
            try:
                dff = dff[dff['Week'] == int(week_val)]
            except Exception:
                pass

    # Click drilldowns
    if region_click and 'points' in region_click:
        try:
            clicked_region = region_click['points'][0]['label']
            dff = dff[dff['Region'] == clicked_region]
        except Exception:
            pass
    if monthly_click and 'points' in monthly_click:
        try:
            clicked_month = monthly_click['points'][0]['x']
            dff = dff[dff['Month'] == clicked_month]
        except Exception:
            pass
    if weekly_click and 'points' in weekly_click:
        try:
            clicked_week = int(weekly_click['points'][0]['x'])
            dff = dff[dff['Week'] == clicked_week]
        except Exception:
            pass
    if source_click and 'points' in source_click:
        try:
            clicked_source = source_click['points'][0]['x']
            dff = dff[dff['Source'] == clicked_source]
        except Exception:
            pass

    # Top metrics helper
    def top_metrics(dframe, report_date=REPORT_DATE):
        if dframe.empty:
            return {
                "WTD": 0.0,
                "LWTD": 0.0,
                "Change": None,
                "MTD": 0.0,
                "LMTD": 0.0,
                "Yesterday": 0.0,
                "LWSD": 0.0,
                "L4WSD": 0.0,
                "AOV": 0.0,
                "ADT": 0.0,
                "ADS": 0.0
            }
        rd = pd.to_datetime(report_date).date() if report_date is not None else REPORT_DATE
        wstart = iso_week_start(rd)
        days_into_week = (rd - wstart).days
        lw_start = wstart - timedelta(days=7)
        mstart = rd.replace(day=1)
        prev_month_end = mstart - timedelta(days=1)
        prev_month_start = prev_month_end.replace(day=1)

        wtd = dframe[(dframe['Date'].dt.date >= wstart) & (dframe['Date'].dt.date <= rd)]
        lwtd = dframe[(dframe['Date'].dt.date >= lw_start) & (dframe['Date'].dt.date <= (lw_start + timedelta(days=days_into_week)))]
        mtd = dframe[(dframe['Date'].dt.date >= mstart) & (dframe['Date'].dt.date <= rd)]
        lmtd = dframe[(dframe['Date'].dt.date >= prev_month_start) & (dframe['Date'].dt.date <= (prev_month_start + timedelta(days=rd.day - 1)))]
        yesterday = rd - timedelta(days=1)
        y = dframe[dframe['Date'].dt.date == yesterday]
        lw_sd = dframe[dframe['Date'].dt.date == (yesterday - timedelta(days=7))]

        l4_vals = []
        for i in range(1, 5):
            dtemp = yesterday - timedelta(days=7 * i)
            l4_vals.append(dframe[dframe['Date'].dt.date == dtemp]['NetSaleAdj'].sum())
        l4_avg = (sum(l4_vals) / 4.0) if l4_vals else 0.0

        total_days = dframe['Date'].dt.date.nunique()
        total_bills = dframe['No Of Bills'].sum()
        total_sales = dframe['NetSaleAdj'].sum()
        aov = total_sales / total_bills if total_bills > 0 else 0.0
        adt = total_bills / total_days if total_days > 0 else 0.0
        ads = total_sales / total_days if total_days > 0 else 0.0

        lwtd_sum = lwtd['NetSaleAdj'].sum()
        wtd_sum = wtd['NetSaleAdj'].sum()
        change_pct = None
        if lwtd_sum != 0:
            change_pct = round((wtd_sum - lwtd_sum) / lwtd_sum * 100.0, 2)

        return {
            "WTD": round(wtd_sum, 2),
            "LWTD": round(lwtd_sum, 2),
            "Change": change_pct,
            "MTD": round(mtd['NetSaleAdj'].sum(), 2),
            "LMTD": round(lmtd['NetSaleAdj'].sum(), 2),
            "Yesterday": round(y['NetSaleAdj'].sum(), 2),
            "LWSD": round(lw_sd['NetSaleAdj'].sum(), 2),
            "L4WSD": round(l4_avg, 2),
            "AOV": round(aov, 2),
            "ADT": round(adt, 2),
            "ADS": round(ads, 2)
        }

    metrics = top_metrics(dff)

    def kpi_card(title, value):
        return dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    html.Div(title, style={'fontSize': '12px', 'color': '#6c757d'}),
                    html.Div(value, className='h5', style={'fontWeight': '700'})
                ]),
                className='kpi-animate',
                style={'minHeight': '72px'}
            ),
            md=3
        )

    kpi_children = []
    items = [
        ("WTD Net Sale", f"₹ {metrics.get('WTD', 0):,.2f}"),
        ("LWTD Net Sale", f"₹ {metrics.get('LWTD', 0):,.2f}"),
        ("Change %", f"{metrics.get('Change', 'N/A')}%"),
        ("MTD Net Sale", f"₹ {metrics.get('MTD', 0):,.2f}"),
        ("LMTD Net Sale", f"₹ {metrics.get('LMTD', 0):,.2f}"),
        ("Yesterday", f"₹ {metrics.get('Yesterday', 0):,.2f}"),
        ("LWSD", f"₹ {metrics.get('LWSD', 0):,.2f}"),
        ("L4WSD Avg", f"₹ {metrics.get('L4WSD', 0):,.2f}"),
        ("AOV", f"₹ {metrics.get('AOV', 0):,.2f}"),
        ("ADT", f"{metrics.get('ADT', 0):.1f} bills/day"),
        ("ADS", f"₹ {metrics.get('ADS', 0):,.2f}/day"),
        ("MIX", "—")
    ]
    for title, val in items:
        kpi_children.append(kpi_card(title, val))

    # Charts (region, month, week, source, outlet)
    try:
        if not dff.empty and 'Region' in dff.columns:
            region_agg = dff.groupby('Region', as_index=False)['NetSaleAdj'].sum()
            region_fig = px.pie(region_agg, names='Region', values='NetSaleAdj', title='Region-wise Net Sale', hole=0.35)
        else:
            region_fig = px.pie(
                pd.DataFrame({'Region': ['No data'], 'NetSaleAdj': [0]}),
                names='Region',
                values='NetSaleAdj',
                title='Region-wise Net Sale',
                hole=0.35
            )
    except Exception:
        region_fig = px.pie(
            pd.DataFrame({'Region': ['No data'], 'NetSaleAdj': [0]}),
            names='Region',
            values='NetSaleAdj',
            title='Region-wise Net Sale',
            hole=0.35
        )

    if not dff.empty:
        overall_m = dff.groupby('Month', as_index=False)['NetSaleAdj'].sum().rename(columns={'NetSaleAdj': 'Net Sale'})
        try:
            overall_m['__dt'] = pd.to_datetime(overall_m['Month'], format='%b-%y')
            overall_m = overall_m.sort_values('__dt').drop(columns='__dt')
        except Exception:
            pass
        month_fig = px.bar(overall_m, x='Month', y='Net Sale', title='Monthly Trend (Adj)')
    else:
        month_fig = px.bar(
            pd.DataFrame({'Month': ['No data'], 'Net Sale': [0]}),
            x='Month',
            y='Net Sale',
            title='Monthly Trend (Adj)'
        )

    if not dff.empty:
        wdf = dff.groupby('Week', as_index=False)['NetSaleAdj'].sum().sort_values('Week').rename(columns={'NetSaleAdj': 'Net Sale'})
        week_fig = px.line(wdf, x='Week', y='Net Sale', title='Weekly Trend (Adj)', markers=True)
    else:
        week_fig = px.line(
            pd.DataFrame({'Week': [0], 'Net Sale': [0]}),
            x='Week',
            y='Net Sale',
            title='Weekly Trend'
        )

    if not dff.empty:
        source_agg = dff.groupby('Source', as_index=False)['NetSaleAdj'].sum().sort_values('NetSaleAdj', ascending=False)
        source_fig = px.bar(source_agg, x='Source', y='NetSaleAdj', title='Source-wise Net Sale (Adj)')
    else:
        source_fig = px.bar(
            pd.DataFrame({'Source': ['No data'], 'NetSaleAdj': [0]}),
            x='Source',
            y='NetSaleAdj',
            title='Source-wise Net Sale (Adj)'
        )

    out_agg = dff.groupby('Outlet Name', as_index=False)['NetSaleAdj'].sum().sort_values('NetSaleAdj', ascending=False).head(20)
    if not out_agg.empty:
        out_fig = px.bar(out_agg, x='Outlet Name', y='NetSaleAdj', title='Top Outlets (Adj)')
    else:
        out_fig = px.bar(
            pd.DataFrame({'Outlet Name': ['No data'], 'NetSaleAdj': [0]}),
            x='Outlet Name',
            y='NetSaleAdj',
            title='Top Outlets (Adj)'
        )

    # Highlight selected outlet if present
    sel = selected_store if isinstance(selected_store, dict) else None
    if sel:
        sel_outlet = sel.get('Outlet Name')
        try:
            if not out_agg.empty and sel_outlet in out_agg['Outlet Name'].values:
                opacities = [1.0 if x == sel_outlet else 0.25 for x in out_agg['Outlet Name']]
                out_fig.update_traces(marker=dict(opacity=opacities))
        except Exception:
            pass

        try:
            sel_monthly = dff[dff['Outlet Name'] == sel_outlet].groupby('Month', as_index=False)['NetSaleAdj'].sum().rename(columns={'NetSaleAdj': 'Net Sale'})
            if not sel_monthly.empty:
                try:
                    sel_monthly['__dt'] = pd.to_datetime(sel_monthly['Month'], format='%b-%y')
                    sel_monthly = sel_monthly.sort_values('__dt').drop(columns='__dt')
                except Exception:
                    pass
                month_fig.add_scatter(
                    x=sel_monthly['Month'],
                    y=sel_monthly['Net Sale'],
                    mode='lines+markers',
                    name=f"Selected: {sel_outlet}",
                    line=dict(width=4)
                )
        except Exception:
            pass

        try:
            sel_weekly = dff[dff['Outlet Name'] == sel_outlet].groupby('Week', as_index=False)['NetSaleAdj'].sum().sort_values('Week').rename(columns={'NetSaleAdj': 'Net Sale'})
            if not sel_weekly.empty:
                week_fig.add_scatter(
                    x=sel_weekly['Week'],
                    y=sel_weekly['Net Sale'],
                    mode='lines+markers',
                    name=f"Selected: {sel_outlet}",
                    line=dict(width=3)
                )
        except Exception:
            pass

    # Build summary and raw data
    full_summary = build_full_summary(dff, report_date=REPORT_DATE)
    if particular_val and particular_val != 'Overall':
        summary_df = full_summary[full_summary['Particular'] == particular_val].copy()
    else:
        summary_df = full_summary.copy()

    summary_data = summary_df.to_dict('records') if not summary_df.empty else []

    dsr_display = dff.copy()
    if 'Date' in dsr_display.columns:
        dsr_display['Date'] = dsr_display['Date'].dt.strftime('%d-%m-%Y')
    if 'NetSaleAdj' in dsr_display.columns:
        dsr_display['NetSaleAdj'] = dsr_display['NetSaleAdj'].round(2)
    dsr_data = dsr_display.to_dict('records')

    return kpi_children, region_fig, month_fig, week_fig, source_fig, out_fig, summary_data, dsr_data

# ---------------------------
# Row click -> set selected outlet & open modal
# ---------------------------
@app.callback(
    Output('selected-outlet', 'data'),
    Output("detail-modal", "is_open"),
    Output("modal-meta", "children"),
    Output("modal-raw-table", "data"),
    Input("summary-table", "active_cell"),
    State("summary-table", "data"),
    Input("modal-close", "n_clicks"),
    State("detail-modal", "is_open"),
    prevent_initial_call=True
)
def handle_summary_row(active_cell, summary_rows, close_clicks, is_open):
    ctx = callback_context
    triggered = ctx.triggered[0]['prop_id'] if ctx.triggered else None
    if triggered and triggered.startswith("modal-close"):
        return None, False, None, []
    if not active_cell or not summary_rows:
        raise PreventUpdate
    r = active_cell.get('row')
    if r is None or r >= len(summary_rows):
        raise PreventUpdate
    row = summary_rows[r]
    outlet = row.get('Outlet Name')
    month = row.get('Month')
    particular = row.get('Particular')

    # filter raw df
    dff = df.copy()
    if outlet:
        dff = dff[dff['Outlet Name'] == outlet]
    if month:
        dff = dff[dff['Month'] == month]
    if particular == 'App':
        dff = dff[dff['SourceParticular'] == 'App']
    elif particular == 'DineIn':
        dff = dff[dff['SourceParticular'] == 'DineIn']
    elif particular == 'Delivery':
        dff = dff[dff['SourceParticular'] == 'Delivery']

    meta = html.Div([
        html.P(f"Outlet: {outlet}"),
        html.P(f"Month: {month}"),
        html.P(f"Particular: {particular}"),
        html.P(f"Rows matched: {len(dff)}")
    ])
    raw_data = dff.to_dict('records')
    sel_payload = {"Outlet Name": outlet, "Month": month, "Particular": particular}
    return sel_payload, True, meta, raw_data

# ---------------------------
# Daypart callback (sidebar)
# ---------------------------
@app.callback(
    Output("dp-breakfast", "children"),
    Output("dp-lunch", "children"),
    Output("dp-snacks", "children"),
    Output("dp-dinner", "children"),
    Output("dp-trend", "figure"),
    Output("dp-heatmap", "figure"),
    Output("dp-table", "data"),
    Input("dp-apply", "n_clicks"),
    State("side-outlet", "value"),
    State("side-region", "value"),
    State("dp-daypart", "value"),
    State("dp-date", "start_date"),
    State("dp-date", "end_date"),
    prevent_initial_call=True
)
def update_daypart(n_clicks, dp_outlet, dp_region, dp_daypart, start_date, end_date):
    dpf = df.copy()
    if dp_outlet:
        dpf = dpf[dpf['Outlet Name'].isin(dp_outlet)]
    if dp_region:
        dpf = dpf[dpf['Region'].isin(dp_region)]
    if dp_daypart:
        dpf = dpf[dpf['Daypart'].isin(dp_daypart)]
    try:
        if start_date:
            s = pd.to_datetime(start_date).date()
            dpf = dpf[dpf['Date'].dt.date >= s]
        if end_date:
            e = pd.to_datetime(end_date).date()
            dpf = dpf[dpf['Date'].dt.date <= e]
    except Exception:
        pass

    breakfast_sum = dpf[dpf['Daypart'] == 'Pre Breakfast']['NetSaleAdj'].sum()
    lunch_sum = dpf[dpf['Daypart'] == 'Lunch']['NetSaleAdj'].sum()
    snacks_sum = dpf[dpf['Daypart'] == 'Snacks']['NetSaleAdj'].sum()
    dinner_sum = dpf[dpf['Daypart'] == 'Dinner']['NetSaleAdj'].sum()

    if not dpf.empty:
        trend = dpf.groupby(['Date', 'Daypart'], as_index=False)['NetSaleAdj'].sum().rename(columns={'NetSaleAdj': 'Net Sale'})
        fig_trend = px.line(trend, x='Date', y='Net Sale', color='Daypart', title='Daypart Trend')
    else:
        fig_trend = px.line(
            pd.DataFrame({'Date': [REPORT_DATE], 'Net Sale': [0], 'Daypart': ['No data']}),
            x='Date',
            y='Net Sale',
            color='Daypart',
            title='Daypart Trend'
        )

    if not dpf.empty:
        heat_df = dpf.groupby(['Hour', 'Daypart'], as_index=False)['NetSaleAdj'].sum().pivot(index='Hour', columns='Daypart', values='NetSaleAdj').fillna(0)
        heat_fig = px.imshow(
            heat_df.values,
            x=heat_df.columns,
            y=heat_df.index,
            aspect='auto',
            labels={'x': 'Daypart', 'y': 'Hour'},
            title='Hourly Heatmap (Net Sale)'
        )
    else:
        heat_fig = px.imshow(
            [[0]],
            x=['No data'],
            y=[0],
            labels={'x': 'Daypart', 'y': 'Hour'},
            title='Hourly Heatmap (No data)'
        )

    table_data = dpf.to_dict('records')
    return (
        f"₹ {breakfast_sum:,.2f}",
        f"₹ {lunch_sum:,.2f}",
        f"₹ {snacks_sum:,.2f}",
        f"₹ {dinner_sum:,.2f}",
        fig_trend,
        heat_fig,
        table_data
    )

# ---------------------------
# DayCode KPIs & comparison (sidebar + graph)
# ---------------------------
@app.callback(
    Output("daycode-cards", "children"),
    Output("daycode-compare", "figure"),
    Input('region-drop', 'value'),
    Input('month-drop', 'value'),
    Input('refresh-btn', 'n_clicks'),
    prevent_initial_call=False
)
def update_daycode(region_vals, month_val, refresh_clicks):
    dff = df.copy()
    if region_vals:
        dff = dff[dff['Region'].isin(region_vals)]
    if month_val:
        dff = dff[dff['Month'] == month_val]

    codes = ['WD', 'WF', 'WE']
    rows = []
    for code in codes:
        g = dff[dff['DayCode'] == code]
        net = g['NetSaleAdj'].sum()
        bills = g['No Of Bills'].sum()
        aov = compute_aov(g)
        adt = compute_adt(g)
        ads = compute_ads(g)
        rows.append({'code': code, 'net': net, 'bills': bills, 'AOV': aov, 'ADT': adt, 'ADS': ads})

    cards = []
    for r in rows:
        txt = [
            html.Div(f"Day Code: {r['code']}", style={'fontSize': '12px', 'color': '#6c757d'}),
            html.Div(f"Net Sale: ₹ {r['net']:,.2f}", style={'fontWeight': '700'}),
            html.Div(f"Bills: {int(r['bills'])}", style={'fontSize': '12px'}),
            html.Div(f"AOV: ₹ {r['AOV']:.2f}", style={'fontSize': '12px'}),
            html.Div(f"ADT: {r['ADT']:.1f}", style={'fontSize': '12px'}),
            html.Div(f"ADS: ₹ {r['ADS']:.2f}", style={'fontSize': '12px'})
        ]
        cards.append(
            dbc.Card(
                dbc.CardBody(txt),
                className='kpi-animate',
                style={'minHeight': '120px', 'marginBottom': '8px'}
            )
        )

    if not dff.empty:
        comp = dff.groupby(['DayCode', 'SourceParticular'], as_index=False)['NetSaleAdj'].sum().rename(columns={'NetSaleAdj': 'Net Sale'})
        fig = px.bar(
            comp,
            x='DayCode',
            y='Net Sale',
            color='SourceParticular',
            title='Day Code: Net Sale by SourceParticular',
            barmode='group'
        )
    else:
        fig = px.bar(
            pd.DataFrame({'DayCode': ['No data'], 'Net Sale': [0]}),
            x='DayCode',
            y='Net Sale',
            title='Day Code Comparison'
        )

    return cards, fig

# ---------------------------
# Run server
# ---------------------------
if __name__ == '__main__':
    print("Starting DSR dashboard on http://127.0.0.1:8050")
    app.run(debug=True, port=8050)
