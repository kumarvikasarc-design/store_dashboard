# =====================================================================
# feedback_page.py — Coffee Island Feedback Dashboard
# PART A — Imports, Config, Store Map, Date Utilities, Loaders
# =====================================================================

# =========================
# IMPORTS
# =========================
import os
import io
import re
import urllib
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import dash
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from dash import html, dcc, Input, Output, State, dash_table
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import no_update
import warnings
from sqlalchemy.exc import SAWarning
warnings.filterwarnings("ignore", category=SAWarning)
# Email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase   # ✅ REQUIRED
from email import encoders

# =========================
# CONFIGURATION
# =========================
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=coffee_island_analytics;"
    "Trusted_Connection=yes;"
)

ENGINE = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

from functools import lru_cache

@lru_cache(maxsize=1)
def get_master_data():
    return build_combined_complaints()

def parse_emails(val):
    if not val:
        return []
    return [e.strip() for e in str(val).split(",") if e.strip()]

def load_email_config():
    df = pd.read_sql(
        "SELECT config_key, config_value FROM dbo.email_config",
        ENGINE
    )
    return dict(zip(df["config_key"], df["config_value"]))


EMAIL_CONFIG = load_email_config()

EMAIL_FROM = EMAIL_CONFIG.get("EMAIL_FROM", "noreply@coffeeisland.com")
EMAIL_PASS = EMAIL_CONFIG.get("EMAIL_PASS", "")

EMAIL_TO   = parse_emails(EMAIL_CONFIG.get("LEADERSHIP_TO"))
EMAIL_CC   = parse_emails(EMAIL_CONFIG.get("LEADERSHIP_CC"))
EMAIL_BCC  = parse_emails(EMAIL_CONFIG.get("LEADERSHIP_BCC"))

OPS_TO_EMAILS  = parse_emails(EMAIL_CONFIG.get("OPS_TO"))
OPS_CC_EMAILS  = parse_emails(EMAIL_CONFIG.get("OPS_CC"))
OPS_BCC_EMAILS = parse_emails(EMAIL_CONFIG.get("OPS_BCC"))

#EMAIL_SUBJECT = f"Coffee Island – Feedback Report ({date_display})"

# Exact NPS column text (DO NOT CHANGE)
NPS_COL = "How likely are you to recommend Coffee Island to a friend ?"

YESTERDAY = date.today() - timedelta(days=1)

# ==========================================================
# STRICT NPS CLASSIFICATION (SINGLE SOURCE OF TRUTH)
# ==========================================================

def rating_to_nps_type(r):
    if r in [5, 4]:
        return "Promoter"
    if r == 3:
        return "Passive"
    if r in [2, 1]:
        return "Detractor"
    return "Unknown"

# ==========================================================
# NPS GROUPS (BASED ON NUMERIC RATING)
# ==========================================================
PROMOTERS  = {5, 4}
PASSIVES   = {3}
DETRACTORS = {2, 1}


# =========================
# OUTLET NORMALIZATION
# =========================
def normalize_outlet(name):
    if not name or pd.isna(name):
        return ""
    return (
        str(name).lower()
        .replace("|", " ")
        .replace("-", " ")
        .replace("_", " ")
        .replace(".", " ")
        .replace("ii", "2")
        .replace("  ", " ")
        .strip()
    )

def to_date(x):
    return x.date() if hasattr(x, "date") else x

# ==========================================================
# MASTER CALLBACK INITIALIZER (REQUIRED)
# ==========================================================
def init_callbacks(app):
    register_cascade_callbacks(app)
    register_filter_controller(app)
    register_dashboard_callbacks(app)
    register_email_callback(app)
    register_email_modal_callbacks(app)
  
# =========================
# STORE MAP LOADER
# =========================
def load_store_map():

    query = """
        SELECT 
            Store_Id,
            Outlet_Name      AS [Outlet Name],
            Brand            AS [Brand],
            Region           AS [Region],
            State            AS [State],
            City             AS [City],
            Store_Type       AS [Type],
            Zomato_Id        AS [Zomato Id],
            Swiggy_Id        AS [Swiggy Id],
            Email_Id         AS [StoreEmail],
            Area_Manager_Email AS [ManagerEmail],
            Area_Manager     AS [Area Manager],
            Status,
            Opening_Date,
            Google_Business_Name
        FROM dbo.stores_master
    """

    df = pd.read_sql(query, ENGINE)

    # ---------------- CLEAN ----------------
    df["Outlet Name"] = df["Outlet Name"].astype(str).str.strip()
    df["Outlet_norm"] = df["Outlet Name"].apply(normalize_outlet)

    df["StoreEmail"] = df["StoreEmail"].fillna("").astype(str).str.strip()
    df["ManagerEmail"] = df["ManagerEmail"].fillna("").astype(str).str.strip()

    return df

STORE_MAP_DF = load_store_map()

# ==================================================
# ZOMATO ID → OUTLET NAME MAP (SQL-Based)
# ==================================================
ZOMATO_ID_MAP = {
    str(row["Zomato Id"]).strip(): row["Outlet Name"]
    for _, row in STORE_MAP_DF.iterrows()
    if pd.notna(row["Zomato Id"])
}

def resolve_outlet_from_zomato_id(zid):
    if not zid or pd.isna(zid):
        return ""
    return ZOMATO_ID_MAP.get(str(zid).strip(), "")

# ==========================================================
# BUILD COMBINED COMPLAINTS (MASTER DATASET)
# ==========================================================
def build_combined_complaints():

    query = """
        SELECT
            created_date,
            source,
            outlet_name,
            outlet_norm,
            rating,
            category,
            comment,
            reply,
            customer_name,
            restaurant_id
        FROM dbo.feedback_raw
        WHERE created_date IS NOT NULL
    """

    df = pd.read_sql(query, ENGINE)

    if df.empty:
        return pd.DataFrame()

    # ---------------- STANDARDIZE ----------------
    df["Created Date"] = pd.to_datetime(df["created_date"], errors="coerce")
    # ---------------- SOURCE STANDARDIZATION ----------------
    df["Source"] = (
        df["source"]
        .astype(str)
        .str.strip()
    )

    # 🔥 Map DB values to Dashboard values
    SOURCE_MAP = {
        "Website": "Feedback Form",   # ✅ IMPORTANT FIX
        "Google": "Google",
        "Swiggy": "Swiggy",
        "Zomato": "Zomato",
        "Z-District": "Z-District"
    }

    df["Source"] = df["Source"].map(SOURCE_MAP).fillna(df["Source"])

    # Default outlet name
    df["Outlet Name"] = df["outlet_name"].astype(str).str.strip()

    
    # 🔥 Build normalized Google map
    GOOGLE_MAP = {
        normalize_outlet(str(row["Google_Business_Name"])): row["Outlet Name"]
        for _, row in STORE_MAP_DF.iterrows()
        if pd.notna(row.get("Google_Business_Name"))
    }

    # 🔥 Apply mapping safely
    google_mask = df["Source"] == "Google"

    df.loc[google_mask, "Outlet Name"] = (
        df.loc[google_mask, "Outlet Name"]
        .astype(str)
        .apply(normalize_outlet)
        .map(GOOGLE_MAP)
        .fillna(df.loc[google_mask, "Outlet Name"])
    )

    # 🔥 FIX: If Source is Zomato → use restaurant_id
    # 🔥 Correct Zomato Mapping
    df.loc[df["Source"] == "Zomato", "Outlet Name"] = (
        df.loc[df["Source"] == "Zomato", "restaurant_id"]
        .astype(str)
        .apply(resolve_outlet_from_zomato_id)
    )
    
    # 🔥 Always recompute norm from final Outlet Name
    
    # 1️⃣ Clean outlet name
    df["Outlet Name"] = df["Outlet Name"].astype(str).str.strip()

    # 2️⃣ First normalize
    df["Outlet_norm"] = df["Outlet Name"].apply(normalize_outlet)

    # 3️⃣ Map to master outlet name
    df["Outlet Name"] = df["Outlet_norm"].map(OUTLET_NORM_MAP).fillna(df["Outlet Name"])

    # 4️⃣ 🔥 IMPORTANT — Recalculate norm AFTER mapping
    df["Outlet_norm"] = df["Outlet Name"].apply(normalize_outlet)

    df["Rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["Category"] = df["category"]
    df["Comment"] = df["comment"]
    df["Reply"] = df["reply"]
    df["Customer Name"] = df["customer_name"]

    df["Zomato Id"] = df["restaurant_id"]

    df["Is_Aggregator"] = df["Source"].isin(
        ["Swiggy", "Zomato", "Google", "Z-District"]
    )

    return df[
        [
            "Created Date",
            "Source",
            "Outlet Name",
            "Outlet_norm",
            "Rating",
            "Category",
            "Comment",
            "Reply",
            "Customer Name",
            "Zomato Id",
            "Is_Aggregator",
        ]
    ]


def load_quick_feedback_data():

    query = """
        SELECT
            created_date,
            outlet_name,
            price_satisfaction,
            taste_of_food,
            cleanliness,
            coffee_aroma,
            staff_friendliness,
            order_accuracy,
            speed_of_service,
            overall_satisfaction
        FROM dbo.quick_feedback
        WHERE created_date IS NOT NULL
    """

    df = pd.read_sql(query, ENGINE)

    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")

    return df

def load_quick_feedback_summary():

    query = """
        SELECT 'Price Satisfaction' AS Metric,
               AVG(price_satisfaction) AS avg_score,
               COUNT(price_satisfaction) AS response_count
        FROM dbo.quick_feedback

        UNION ALL

        SELECT 'Taste of Food',
               AVG(taste_of_food),
               COUNT(taste_of_food)
        FROM dbo.quick_feedback

        UNION ALL

        SELECT 'Cleanliness',
               AVG(cleanliness),
               COUNT(cleanliness)
        FROM dbo.quick_feedback

        UNION ALL

        SELECT 'Coffee Aroma',
               AVG(coffee_aroma),
               COUNT(coffee_aroma)
        FROM dbo.quick_feedback

        UNION ALL

        SELECT 'Staff Friendliness',
               AVG(staff_friendliness),
               COUNT(staff_friendliness)
        FROM dbo.quick_feedback

        UNION ALL

        SELECT 'Order Accuracy',
               AVG(order_accuracy),
               COUNT(order_accuracy)
        FROM dbo.quick_feedback

        UNION ALL

        SELECT 'Speed of Service',
               AVG(speed_of_service),
               COUNT(speed_of_service)
        FROM dbo.quick_feedback

        UNION ALL

        SELECT 'Overall Satisfaction',
               AVG(overall_satisfaction),
               COUNT(overall_satisfaction)
        FROM dbo.quick_feedback
    """
    return pd.read_sql(query, ENGINE)

def normalize_outlet_columns(df):
    """
    Clean Outlet Name column after merges.
    Removes Outlet Name_x / Outlet Name_y safely.
    """

    if df is None or df.empty:
        return df

    df = df.copy()

    # Prefer _x if exists
    if "Outlet Name_x" in df.columns:
        df["Outlet Name"] = df["Outlet Name_x"]

    elif "Outlet Name_y" in df.columns:
        df["Outlet Name"] = df["Outlet Name_y"]

    # Drop duplicate columns
    df = df.drop(
        columns=[c for c in ["Outlet Name_x", "Outlet Name_y"] if c in df.columns],
        errors="ignore"
    )

    # Fallback if still missing
    if "Outlet Name" not in df.columns and "Outlet_norm" in df.columns:
        df["Outlet Name"] = (
            df["Outlet_norm"]
            .astype(str)
            .str.replace("_", " ")
            .str.title()
        )

    return df

# Build normalized outlet map
OUTLET_NORM_MAP = {
    normalize_outlet(row["Outlet Name"]): row["Outlet Name"]
    for _, row in STORE_MAP_DF.iterrows()
}

def get_store_map():
    return load_store_map()

#---------------------
# Store wise Email Map
def get_store_emails(outlet_name):

    if not outlet_name:
        return [], []

    norm = normalize_outlet(outlet_name)

    store_row = STORE_MAP_DF[
        STORE_MAP_DF["Outlet_norm"] == norm
    ]

    if store_row.empty:
        return [], []

    store_emails = parse_emails(store_row.iloc[0]["StoreEmail"])
    manager_emails = parse_emails(store_row.iloc[0]["ManagerEmail"])

    return store_emails, manager_emails

def format_ddmmyyyy(d):
    """
    Safely formats a single date or timestamp to DD-MM-YYYY
    Accepts:
    - datetime.date
    - datetime.datetime
    - pandas.Timestamp
    - string
    """
    if d is None or pd.isna(d):
        return ""

    try:
        return pd.to_datetime(d, errors="coerce").strftime("%d-%m-%Y")
    except Exception:
        return ""

def parse_ui_date(val):
    if not val:
        return None
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None

def get_default_date_range(df):
    """
    Uses COMBINED data (Created Date).
    Auto-selects earliest → latest available date.
    """
    if df is None or df.empty or "Created Date" not in df:
        return None, None

    valid = df["Created Date"].dropna()
    if valid.empty:
        return None, None

    return valid.min().date(), valid.max().date()

def clean_date_input(d):
    if isinstance(d, str) and " to " in d:
        d = d.split(" to ")[1].strip()
    return pd.to_datetime(d, dayfirst=True, errors="coerce")


def opts(values):
    if values is None:
        return []
    return [
        {"label": str(v), "value": str(v)}
        for v in sorted(set(values))
        if pd.notna(v) and str(v).strip()
    ]

# Insight Generator Function

def generate_store_insight(store, g):
    """
    g = dataframe filtered for ONE store
    Returns human-readable insight text
    """

    promoters  = (g["NPS_Type"] == "Promoter").sum()
    passives   = (g["NPS_Type"] == "Passive").sum()
    detractors = (g["NPS_Type"] == "Detractor").sum()

    total = promoters + passives + detractors
    nps = ((promoters - detractors) / total) * 100 if total else 0

    comments = " ".join(g["Comment"].fillna("").astype(str)).lower()

    issues = []
    if any(k in comments for k in ["slow", "delay", "late"]):
        issues.append("service delays")
    if any(k in comments for k in ["cold", "taste", "coffee", "watery"]):
        issues.append("coffee quality")
    if any(k in comments for k in ["rude", "staff", "behavior"]):
        issues.append("staff behavior")
    if any(k in comments for k in ["dirty", "clean", "hygiene"]):
        issues.append("cleanliness")

    # ---- Insight Text ----
    if nps >= 70:
        return (
            f"🟢 Strong performance with NPS {nps:.1f}. "
            f"High promoter count indicates consistent customer satisfaction."
        )

    if nps >= 40:
        msg = (
            f"🟡 Moderate NPS ({nps:.1f}). "
            f"Some detractors present; opportunity to improve experience."
        )
        if issues:
            msg += f" Key concerns: {', '.join(set(issues))}."
        return msg

    msg = (
        f"🔴 Low NPS ({nps:.1f}). "
        f"High detractor share impacting store perception."
    )
    if issues:
        msg += f" Repeated complaints around {', '.join(set(issues))}."
    msg += " Immediate corrective action recommended."
    return msg

# Build Store-Wise Insight Table

def build_store_insights(df_all):
    df_all = ensure_outlet_column(df_all, "build_store_insights")

    rows = []
    for store, g in df_all.groupby("Outlet Name", dropna=False):
        insight = generate_store_insight(store, g)
        rows.append({
            "Outlet Name": store,
            "Insight": insight
        })

    return pd.DataFrame(rows)

# Risk Scoring Logic (Single Source of Truth)

def compute_store_risk_score(g):
    """
    Higher score = higher risk
    """

    promoters  = (g["NPS_Type"] == "Promoter").sum()
    detractors = (g["NPS_Type"] == "Detractor").sum()
    total = len(g)

    if total == 0:
        return 0

    nps = ((promoters - detractors) / total) * 100

    comments = " ".join(g["Comment"].fillna("").astype(str)).lower()

    keyword_hits = sum(
        k in comments for k in
        ["slow", "delay", "cold", "rude", "dirty", "bad", "worst", "refund"]
    )

    # 🔥 Risk formula
    risk = (
        (100 - max(nps, 0)) * 0.6 +        # low NPS
        (detractors / total) * 100 * 0.3 + # detractor weight
        keyword_hits * 2                   # sentiment signals
    )

    return round(risk, 1)

# Build Top Risk Stores Table

def build_top_risk_stores(df_all, top_n=3):
    df_all = ensure_outlet_column(df_all, "build_top_risk_stores")

    rows = []
    for store, g in df_all.groupby("Outlet Name", dropna=False):
        risk = compute_store_risk_score(g)
        insight = generate_store_insight(store, g)

        rows.append({
            "Outlet Name": store,
            "Risk Score": risk,
            "Insight": insight
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    return out.sort_values("Risk Score", ascending=False).head(top_n)

def build_month_options_from_df(df):
    """
    Build Month dropdown options from Created Date column.
    SQL-safe version.
    """
    if df is None or df.empty or "Created Date" not in df.columns:
        return []

    dates = pd.to_datetime(df["Created Date"], errors="coerce").dropna()

    if dates.empty:
        return []

    months = (
        dates.dt.to_period("M")
        .drop_duplicates()
        .sort_values()
    )

    return [
        {
            "label": m.strftime("%b %Y"),
            "value": m.strftime("%Y-%m")
        }
        for m in months
    ]

# ==========================================================
# PUBLIC LAYOUT EXPORT (REQUIRED BY main_app.py)
# ==========================================================
def get_layout():
    return html.Div([

        html.H2(
            "Coffee Island — Customer Feedback Dashboard",
            style={
                "fontFamily": "Times New Roman",
                "fontWeight": "bold",
                "color": "#0A66C2",
                "marginBottom": "15px"
            }
        ),
        
        # ================= FILTER PANEL =================
        dbc.Card([
            dbc.CardBody([

                dbc.Row([

                    dbc.Col(
                        dcc.Dropdown(
                            id="f_brand",
                            options=opts(STORE_MAP_DF["Brand"]),
                            placeholder="Brand",
                            clearable=True
                        ), md=2
                    ),

                    dbc.Col(
                        dcc.Dropdown(
                            id="f_region",
                            placeholder="Region",
                            clearable=True
                        ), md=2
                    ),

                    dbc.Col(
                        dcc.Dropdown(
                            id="f_state",
                            placeholder="State",
                            clearable=True
                        ), md=2
                    ),

                    dbc.Col(
                        dcc.Dropdown(
                            id="f_city",
                            placeholder="City",
                            clearable=True
                        ), md=2
                    ),

                    dbc.Col(
                        dcc.Dropdown(
                            id="f_type",
                            placeholder="Type",
                            clearable=True
                        ), md=2
                    ),

                    dbc.Col(
                        dcc.Dropdown(
                            id="f_outlet",
                            placeholder="Outlet",
                            clearable=True
                        ), md=2
                    ),

                ], className="mb-2"),

                # ================= DATE CONTROLS =================
                dbc.Row([

                    # Date Range Picker
                    dbc.Col(
                        dcc.DatePickerSingle(
                            id="date_from",
                            display_format="DD-MM-YYYY",
                            date=DEFAULT_FROM,
                            clearable=True
                        ),
                        md=2
                    ),

                    dbc.Col(
                        dcc.DatePickerSingle(
                            id="date_to",
                            display_format="DD-MM-YYYY",
                            date=YESTERDAY,
                            clearable=True
                        ),
                        md=2
                    ),# Month Selector
                    dbc.Col(
                        dcc.Dropdown(
                            id="month_filter",
                            options=build_month_options_from_df(INIT_DF_ALL),  # 🔥 FIX
                            placeholder="Select Month (MMM YYYY)",
                            clearable=True
                        ),
                        md=3
                    ),
                                                                               
                    # Quick Range Buttons
                    dbc.Col(
                        dbc.ButtonGroup([
                            dbc.Button("Yesterday", id="qr_yesterday", color="info",className="me-1", n_clicks=0, outline=True),
                            dbc.Button("Last 7 Days", id="qr_7", color="info",className="me-1", n_clicks=0, outline=True),
                            dbc.Button("Last 30 Days", id="qr_30", color="info",className="me-1", n_clicks=0, outline=True),
                            dbc.Button("Last 90 Days", id="qr_90", color="info",className="me-1", n_clicks=0, outline=True),
                        ]),
                        md=5
                    ),

                ], className="mb-3"),

                dbc.Row([

                    dbc.Col(
                        dcc.Dropdown(
                            id="f_source",
                                options=[
                                    {"label": "ALL", "value": "ALL"},
                                    {"label": "Feedback Form", "value": "Feedback Form"},
                                    {"label": "Google", "value": "Google"},
                                    {"label": "Zomato", "value": "Zomato"},
                                    {"label": "Z-District", "value": "Z-District"},
                                    {"label": "Swiggy", "value": "Swiggy"},
                                ],
                            value="ALL",
                            clearable=False
                        ), md=3
                    ),

                ], className="mb-3"),

                dbc.Row([

                    dbc.Col(
                        dbc.Button(
                            "Apply Filters",
                            id="apply_btn",
                            color="primary",
                            className="me-2"
                        ), md="auto"
                    ),

                    dbc.Col(
                        dbc.Button(
                            "Send Email Report",
                            id="send_email_btn",
                            color="success"
                        ), md="auto"
                    ),

                 
                    dbc.Col(
                        dbc.Button(
                            "Reset Filters",
                            id="reset_filters_btn",
                            color="secondary",
                            outline=True
                        ),
                        md="auto"
                    ),
                ]),

            ])
             ], className="mb-3"),

        # ================= OUTPUT AREA =================
        html.Div(id="feedback_tabs"),
                # ================= SEND EMAIL MODAL (✅ PUT HERE) =================
        dbc.Modal(
            [
                dbc.ModalHeader("✉️ Send Email Confirmation"),
                dbc.ModalBody(
                    [
                        html.P("Add a message to include in the email (max 300 characters):"),

                        dcc.Textarea(
                            id="email_custom_message",
                            placeholder="Type your message here...",
                            maxLength=300,
                            style={"width": "100%", "height": "110px"},
                        ),

                        html.Div(
                            id="char_counter",
                            style={
                                "textAlign": "right",
                                "fontSize": "12px",
                                "color": "#666",
                                "marginTop": "4px",
                            }
                        ),

                        html.Hr(),

                        html.P("📄 Preview:", style={"fontWeight": "bold"}),
                        html.Div(
                            id="email_preview",
                            style={
                                "border": "1px solid #ddd",
                                "padding": "10px",
                                "minHeight": "60px",
                                "backgroundColor": "#fafafa",
                                "fontSize": "14px",
                            }
                        ),
                    ]
                ),
                dbc.ModalFooter(
                    [
                        dbc.Button("Cancel", id="cancel_send_email", color="secondary", className="me-2"),
                        dbc.Button("OK & Send", id="confirm_send_email", color="success"),
                    ]
                ),
            ],
            id="send_email_modal",
            is_open=False,
            centered=True,
        ),

    ], style={
        "padding": "15px",
        "fontFamily": "Times New Roman"
    })

def styled_table(df, header_color):
    return dash_table.DataTable(
        data=df.to_dict("records"),
        columns=[{"name": c, "id": c} for c in df.columns],
        page_size=15,
        filter_action="native",
        sort_action="native",

        style_header={
            "backgroundColor": header_color,
            "color": "white",
            "fontWeight": "bold",
            "textAlign": "center",
            "fontFamily": "Times New Roman"
        },

        style_cell={
            "whiteSpace": "normal",
            "textAlign": "left",
            "fontFamily": "Times New Roman"
        },

        style_data_conditional=[
            {"if": {"column_type": "numeric"}, "textAlign": "center"},
            {"if": {"filter_query": "{Rating} <= 2"}, "backgroundColor": "#FDECEA"},
            {"if": {"filter_query": "{Rating} = 3"}, "backgroundColor": "#FFF8E1"},
            {"if": {"filter_query": "{Rating} >= 4"}, "backgroundColor": "#E8F5E9"},
        ]
    )

REQUIRED_COLS = {"Created Date", "Source", "Outlet Name", "Rating"}

def assert_schema(df):
    required = {"Created Date", "Source", "Outlet Name", "Rating"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
# =========================
# TEXT / SENTIMENT HELPERS
# =========================
def extract_comment(row):
    c1 = str(row.get("Comments", "")).strip()
    c2 = str(row.get("Free Text", "")).strip()
    c3 = str(row.get("Tell us more", "")).strip()
    return f"{c1} {c2} {c3}".strip()

def classify_sentiment(row):
    text = (
        str(row.get("Experience", "")) +
        str(row.get("Food Quality", "")) +
        str(row.get("Ambience", ""))
    ).lower()

    if any(k in text for k in ["poor", "bad", "very poor", "not good"]):
        return "Negative"
    if any(k in text for k in ["average", "ok", "maybe"]):
        return "Neutral"
    return "Positive"

def apply_month_filter(df, month_val):
    if not month_val:
        return df

    try:
        start = pd.to_datetime(month_val + "-01")
        end = start + pd.offsets.MonthEnd(1)
    except Exception:
        return df

    return df[
        (df["Created Date"] >= start) &
        (df["Created Date"] <= end + pd.Timedelta(days=1))
    ]

def severity(text):
    t = str(text).lower()

    if any(k in t for k in ["hair", "cockroach", "bug", "dirty", "stale", "poison"]):
        return "Critical"
    if any(k in t for k in ["cold", "delay", "slow", "rude", "refund", "wrong"]):
        return "Major"
    if any(k in t for k in ["average", "ok", "maybe"]):
        return "Minor"
    return "Low"
# ==================================================
# ZOMATO ID → OUTLET NAME (REVERSE MAP)
# ==================================================
def build_store_source_avg_rating(df_all):
    """
    Builds Store × Source Average Rating table
    """
    if df_all is None or df_all.empty:
        return pd.DataFrame(
            columns=["Outlet Name", "Source", "Average Rating", "Responses"]
        )

    df = ensure_outlet_column(df_all, "avg_rating")

    out = (
        df.dropna(subset=["Rating"])
          .groupby(["Outlet Name", "Source"], dropna=False)
          .agg(
              **{
                  "Average Rating": ("Rating", lambda x: round(x.mean(), 2)),
                  "Responses": ("Rating", "count")
              }
          )
          .reset_index()
          .sort_values(["Outlet Name", "Source"])
    )

    return out

def build_avg_rating_email_table(df):
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    # ---------- GUARANTEE Outlet Name ----------
    if "Outlet Name" not in df.columns:
        if "Outlet_norm" in df.columns:
            df["Outlet Name"] = df["Outlet_norm"]
        else:
            return pd.DataFrame()

    # ---------- AVERAGE RATING PIVOT ----------
    pivot = (
        df.pivot_table(
            index="Outlet Name",
            columns="Source",
            values="Rating",
            aggfunc="mean"
        )
        .round(2)
        .reset_index()
    )

    rating_cols = ["Google", "Feedback Form", "Swiggy", "Zomato", "Z-District"]

    for c in rating_cols:
        if c not in pivot.columns:
            pivot[c] = ""

    # ---------- MIN RATING (ONLY ONE COLUMN) ----------
    pivot["Min Rating"] = (
        pivot[rating_cols]
        .replace("", np.nan)
        .apply(pd.to_numeric, errors="coerce")
        .min(axis=1)
        .round(2)
    )

    # ---------- SORT ----------
    pivot = pivot.sort_values("Min Rating", ascending=True)

    # ---------- GLOBAL AVERAGE ----------
    global_row = {
        "Outlet Name": "⭐ National Average",
        "Google": round(df.loc[df["Source"] == "Google", "Rating"].mean(), 2),
        "Feedback Form": round(df.loc[df["Source"] == "Feedback Form", "Rating"].mean(), 2),
        "Swiggy": round(df.loc[df["Source"] == "Swiggy", "Rating"].mean(), 2),
        "Zomato": round(df.loc[df["Source"] == "Zomato", "Rating"].mean(), 2),
        "Z-District": round(
            df.loc[df["Source"] == "Z-District", "Rating"].mean(), 2
        ),
        "Min Rating": ""
    }

    pivot = pd.concat([pivot, pd.DataFrame([global_row])], ignore_index=True)

    # ---------- NaN → BLANK ----------
    pivot[rating_cols + ["Min Rating"]] = pivot[rating_cols + ["Min Rating"]].where(
        pivot[rating_cols + ["Min Rating"]].notna(), ""
    )

    return pivot[["Outlet Name"] + rating_cols + ["Min Rating"]]

def build_low_rating_alert_html(low_df, date_display):

    table_html = df_to_html(
        low_df[
            ["Outlet Name", "Google", "Feedback Form", "Swiggy", "Zomato", "Z-District"]
        ],
        table_type="default"
    )

    return f"""
    <html>
    <body style="font-family:Times New Roman;">
        <h2 style="color:#B00020;">
            🚨 LOW RATING ALERT (Below 4.0)
        </h2>

        <p>
            The following stores have received <b>low customer ratings</b>
            during <b>{date_display}</b>.
        </p>

        {table_html}

        <p style="margin-top:15px;color:#555;">
            ⚠️ Immediate review and corrective action recommended.
        </p>
    </body>
    </html>
    """

def send_low_rating_alert_email(
    recipients,
    low_df,
    date_display
):
    if low_df.empty:
        return

    html_body = build_low_rating_alert_html(low_df, date_display)

    msg = MIMEMultipart("alternative")
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = f"🚨 LOW RATING ALERT – Immediate Action Required ({date_display})"

    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP("smtp.gmail.com", 587) as s:
        s.starttls()
        s.login(EMAIL_FROM, EMAIL_PASS)
        s.sendmail(EMAIL_FROM, recipients, msg.as_string())

    print("🚨 LOW rating alert mail sent")


#Source Outlet Summary
def build_source_wise_feedback_count_excel(df_all):
    """
    Builds outlet × source feedback count for Excel attachment
    GUARANTEES Outlet Name exists
    """
    if df_all is None or df_all.empty:
        return pd.DataFrame()

    # 🔒 HARD GUARANTEE
    df = ensure_outlet_column(df_all, "source_wise_feedback_count").copy()

    pivot = (
        df.pivot_table(
            index="Outlet Name",
            columns="Source",
            values="Rating",
            aggfunc="count",
            fill_value=0
        )
        .reset_index()
    )

    # Ensure all sources exist
    for c in ["Google", "Feedback Form", "Swiggy", "Zomato", "Z-District"]:
        if c not in pivot.columns:
            pivot[c] = 0

    pivot["Total"] = pivot[
        ["Google", "Feedback Form", "Swiggy", "Zomato", "Z-District"]
    ].sum(axis=1)

    return pivot[
        ["Outlet Name", "Google", "Feedback Form", "Swiggy", "Zomato", "Z-District", "Total"]
    ]

def build_source_outlet_summary(df_all):
    """
    Builds Outlet × Source feedback count table
    GUARANTEES Outlet Name exists
    """

    if df_all is None or df_all.empty:
        return pd.DataFrame(columns=[
            "Outlet Name", "Google", "Feedback Form", "Swiggy", "Zomato", "Z-District"
        ])

    # 🔒 ABSOLUTE SAFETY
    df = ensure_outlet_column(df_all, "source_summary:input")

    # Normalize source
    df["Source"] = df["Source"].astype(str).str.strip()

    # Replace blanks
    df["Outlet Name"] = (
        df["Outlet Name"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    df.loc[df["Outlet Name"] == "", "Outlet Name"] = "UNKNOWN OUTLET"

    # ---------- GROUP ----------
    summary = (
        df
        .groupby(["Outlet Name", "Source"], dropna=False)
        .size()
        .unstack(fill_value=0)
    )

    summary.columns.name = None
    summary = summary.reset_index()

    # ---------- FORCE ALL SOURCES ----------
    for col in ["Google", "Feedback Form", "Swiggy", "Zomato", "Z-District"]:
        if col not in summary.columns:
            summary[col] = 0

    summary = summary[
        ["Outlet Name", "Google", "Feedback Form", "Swiggy", "Zomato", "Z-District"]
    ]

    return summary.sort_values("Outlet Name").reset_index(drop=True)
               
def build_ops_excel_attachment(
    df_all,
    store_summary_df,
    source_count_df,
    avg_rating_df,
    store_nps_df,
    neg_df,
    neu_df,
    pos_df,
    date_display
):
    buffer = io.BytesIO()

    # Columns to drop for ALL Excel outputs
    remove_cols = [
        "Outlet_norm",
        "Category",
        "Zomato Id",
        "Is_Aggregator",
        "Created Date Display",
    ]

    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:

        # ================= SHEET 1: ALL FEEDBACK =================
        df_export = clean_excel_df(safe_excel_df(df_all))

        # REMOVE technical columns
        df_export = df_export.drop(
            columns=[c for c in remove_cols if c in df_export.columns],
            errors="ignore"
        )

        df_export.to_excel(writer, index=False, sheet_name="All_Feedback")
        apply_excel_formatting(writer, df_export, "All_Feedback")

        # ================= SHEET 2: STORE SUMMARY =================
        ss = clean_excel_df(safe_excel_df(store_summary_df))
        ss.to_excel(writer, index=False, sheet_name="Store_Summary")
        apply_excel_formatting(writer, ss, "Store_Summary")

        # ================= SHEET 3: SOURCE COUNT =================
        if source_count_df is not None and not source_count_df.empty:
            sc = clean_excel_df(source_count_df)
            sc.to_excel(writer, index=False, sheet_name="Source_Wise_Feedback_Count")
            apply_excel_formatting(writer, sc, "Source_Wise_Feedback_Count")

        # ================= SHEET 4: AVG RATING =================
        if avg_rating_df is not None and not avg_rating_df.empty:
            ar = clean_excel_df(avg_rating_df)
            for c in ["Google", "Feedback Form", "Swiggy", "Zomato", "Z-District"]:
                if c in ar.columns:
                    ar[c] = pd.to_numeric(ar[c], errors="coerce")
            ar.to_excel(writer, index=False, sheet_name="Average_Rating_Store_Source")
            apply_excel_formatting(writer, ar, "Average_Rating_Store_Source")
            apply_avg_rating_conditional_formatting(writer, ar, "Average_Rating_Store_Source")

        # ================= SHEET 5: STORE NPS =================
        sn = clean_excel_df(safe_excel_df(store_nps_df))

        if not sn.empty and "Total" in sn.columns:
            total_row = {
                "Outlet Name": "TOTAL",
                "Promoters": int(sn["Promoters"].sum()),
                "Passives": int(sn["Passives"].sum()),
                "Detractors": int(sn["Detractors"].sum()),
                "Total": int(sn["Total"].sum()),
                "NPS": round(((sn["Promoters"].sum() - sn["Detractors"].sum()) / sn["Total"].sum() * 100)
                             if sn["Total"].sum() else 0, 1)
            }
            sn = pd.concat([sn, pd.DataFrame([total_row])], ignore_index=True)

        sn.to_excel(writer, index=False, sheet_name="Store_NPS")
        apply_excel_formatting(writer, sn, "Store_NPS")

        # ================= SHEET 6/7/8: SENTIMENT SHEETS =================
        for df_s, name, sentiment in [
            (neg_df, "Negative", "Negative"),
            (neu_df, "Neutral",  "Neutral"),
            (pos_df, "Positive", "Positive"),
        ]:
            sdf = clean_excel_df(safe_excel_df(df_s))
            sdf["Sentiment"] = sentiment
            sdf = sdf.drop(columns=[c for c in remove_cols if c in sdf.columns], errors="ignore")
            sdf.to_excel(writer, index=False, sheet_name=name)
            apply_excel_formatting(writer, sdf, name)

    buffer.seek(0)

    part = MIMEBase(
        "application",
        "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    part.set_payload(buffer.read())
    encoders.encode_base64(part)
    return part

def build_area_manager_excel_attachment(
    df_all,
    store_summary_df,
    source_count_df,
    avg_rating_df,
    store_nps_df,
    neg_df,
    neu_df,
    pos_df,
    date_display
):
    buffer = io.BytesIO()

    # Columns to remove for Area Manager Excel
    remove_cols = [
        "Outlet Name_x",
        "Outlet Name_y",
        "Store Id",
        "Region",
        "Area Manager",
        "StoreEmail",
        "ManagerEmail",
        "Outlet_norm",
        "Category",
        "Zomato Id",
        "Is_Aggregator",
        "Created Date Display"
    ]

    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:

        # ---------------- SHEET 1: CLEAN FEEDBACK ----------------
        df_export = clean_excel_df(safe_excel_df(df_all))

        df_export = df_export.drop(
            columns=[c for c in remove_cols if c in df_export.columns],
            errors="ignore"
        )

        df_export.to_excel(writer, index=False, sheet_name="Area_Feedback")
        apply_excel_formatting(writer, df_export, "Area_Feedback")

        # ---------------- SHEET 2: STORE SUMMARY ----------------
        ss = clean_excel_df(safe_excel_df(store_summary_df))
        ss.to_excel(writer, index=False, sheet_name="Store_Summary")
        apply_excel_formatting(writer, ss, "Store_Summary")

        # ---------------- SHEET 3: SOURCE COUNT ----------------
        if source_count_df is not None and not source_count_df.empty:
            sc = clean_excel_df(source_count_df)
            sc.to_excel(writer, index=False, sheet_name="Source_Summary")
            apply_excel_formatting(writer, sc, "Source_Summary")

        if avg_rating_df is not None and not avg_rating_df.empty:
            avg_rating_clean = clean_excel_df(avg_rating_df)
            avg_rating_clean.to_excel(writer, index=False, sheet_name="Avg_Rating_Details")
            apply_excel_formatting(writer, avg_rating_clean, "Avg_Rating_Details")
            
        if store_nps_df is not None and not store_nps_df.empty:
            sn = clean_excel_df(store_nps_df)
            sn.to_excel(writer, index=False, sheet_name="Store_NPS")
            apply_excel_formatting(writer, sn, "Store_NPS")
            
        # ---------------- SHEET 4: AREA MANAGER OVERALL RATING ----------------
        overall_rating = round(df_all["Rating"].mean(), 2)
        total_reviews = len(df_all)

        positive_count = (df_all["Sentiment"] == "Positive").sum()
        neutral_count  = (df_all["Sentiment"] == "Neutral").sum()
        negative_count = (df_all["Sentiment"] == "Negative").sum()

        promoters  = (df_all["NPS_Type"] == "Promoter").sum()
        passives   = (df_all["NPS_Type"] == "Passive").sum()
        detractors = (df_all["NPS_Type"] == "Detractor").sum()

        if total_reviews > 0:
            nps = round(((promoters - detractors) / total_reviews) * 100, 1)
        else:
            nps = 0

        def rating_status(r):
            if r >= 4.0: return "Excellent"
            if r >= 3.0: return "Good"
            if r >= 2.0: return "Average"
            if r > 0:   return "Poor"
            return "No Reviews"

        status = rating_status(overall_rating)

        summary_df = pd.DataFrame({
            "Metric": [
                "Overall Rating",
                "Rating Status",
                "Total Reviews",
                "Positive Reviews",
                "Neutral Reviews",
                "Negative Reviews",
                "Promoters",
                "Passives",
                "Detractors",
                "NPS Score"
            ],
            "Value": [
                overall_rating,
                status,
                total_reviews,
                positive_count,
                neutral_count,
                negative_count,
                promoters,
                passives,
                detractors,
                nps
            ]
        })

        summary_df.to_excel(writer, index=False, sheet_name="Area_Manager_Overall_Rating")
        apply_excel_formatting(writer, summary_df, "Area_Manager_Overall_Rating")

        # ---------------- SHEET 5: POSITIVE ----------------
        pos = clean_excel_df(pos_df)
        pos = pos.drop(columns=[c for c in remove_cols if c in pos.columns], errors="ignore")
        pos.to_excel(writer, index=False, sheet_name="Positive")
        apply_excel_formatting(writer, pos, "Positive")

        # ---------------- SHEET 6: NEUTRAL ----------------
        neu = clean_excel_df(neu_df)
        neu = neu.drop(columns=[c for c in remove_cols if c in neu.columns], errors="ignore")
        neu.to_excel(writer, index=False, sheet_name="Neutral")
        apply_excel_formatting(writer, neu, "Neutral")

        # ---------------- SHEET 7: NEGATIVE ----------------
        neg = clean_excel_df(neg_df)
        neg = neg.drop(columns=[c for c in remove_cols if c in neg.columns], errors="ignore")
        neg.to_excel(writer, index=False, sheet_name="Negative")
        apply_excel_formatting(writer, neg, "Negative")

    buffer.seek(0)

    part = MIMEBase(
        "application",
        "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    part.set_payload(buffer.read())
    encoders.encode_base64(part)
    return part



def apply_avg_rating_conditional_formatting(writer, df, sheet_name):
    """
    Apply rating color rules to Average_Rating_Store_Source sheet

    🔴 < 4.0
    🟡 4.0 – 4.49
    🟢 ≥ 4.5
    """
    workbook  = writer.book
    worksheet = writer.sheets[sheet_name]

    rating_cols = ["Google", "Feedback Form", "Swiggy", "Zomato", "Z-District"]

    green_fmt  = workbook.add_format({"bg_color": "#C8E6C9"})
    yellow_fmt = workbook.add_format({"bg_color": "#FFF9C4"})
    red_fmt    = workbook.add_format({"bg_color": "#FFCDD2"})

    for col in rating_cols:
        if col not in df.columns:
            continue

        c = df.columns.get_loc(col)

        # 🔴 < 4.0
        worksheet.conditional_format(
            1, c, len(df), c,
            {
                "type": "cell",
                "criteria": "<",
                "value": 4,
                "format": red_fmt
            }
        )

        # 🟡 4.0 – 4.49
        worksheet.conditional_format(
            1, c, len(df), c,
            {
                "type": "cell",
                "criteria": "between",
                "minimum": 4,
                "maximum": 4.49,
                "format": yellow_fmt
            }
        )

        # 🟢 ≥ 4.5
        worksheet.conditional_format(
            1, c, len(df), c,
            {
                "type": "cell",
                "criteria": ">=",
                "value": 4.5,
                "format": green_fmt
            }
        )

def apply_excel_formatting(writer, df, sheet_name):
    workbook  = writer.book
    worksheet = writer.sheets[sheet_name]

    # ---------- FORMATS ----------
    header_fmt = workbook.add_format({
        "bold": True,
        "text_wrap": True,
        "align": "center",
        "valign": "middle",
        "border": 1,
        "font_name": "Times New Roman",
        "font_size": 11,
        "bg_color": "#0A66C2",
        "font_color": "white",
    })

    cell_fmt = workbook.add_format({
        "border": 1,
        "font_name": "Times New Roman",
        "font_size": 10,
    })

    center_fmt = workbook.add_format({
        "border": 1,
        "align": "center",
        "font_name": "Times New Roman",
        "font_size": 10,
    })

    # ---------- HEADER ----------
    for col_num, col_name in enumerate(df.columns):
        worksheet.write(0, col_num, col_name, header_fmt)

    # ---------- COLUMN WIDTH ----------
    for i, col in enumerate(df.columns):
        max_len = max(
            [len(str(col))] +
            [len(str(v)) for v in df[col].astype(str).values[:200]]
        )
        worksheet.set_column(i, i, min(max_len + 3, 40), cell_fmt)

    # ---------- FREEZE HEADER ----------
    worksheet.freeze_panes(1, 0)

    # ---------- CONDITIONAL FORMATTING ----------
    if "Rating" in df.columns:
        rating_col = df.columns.get_loc("Rating")
        worksheet.conditional_format(1, rating_col, len(df), rating_col, {
            "type": "cell",
            "criteria": ">=",
            "value": 4,
            "format": workbook.add_format({"bg_color": "#C8E6C9"})
        })
        worksheet.conditional_format(1, rating_col, len(df), rating_col, {
            "type": "cell",
            "criteria": "==",
            "value": 3,
            "format": workbook.add_format({"bg_color": "#FFF9C4"})
        })
        worksheet.conditional_format(1, rating_col, len(df), rating_col, {
            "type": "cell",
            "criteria": "<=",
            "value": 2,
            "format": workbook.add_format({"bg_color": "#FFCDD2"})
        })

    if "Sentiment" in df.columns:
        s_col = df.columns.get_loc("Sentiment")
        worksheet.conditional_format(1, s_col, len(df), s_col, {
            "type": "text",
            "criteria": "containing",
            "value": "Positive",
            "format": workbook.add_format({"bg_color": "#C8E6C9"})
        })
        worksheet.conditional_format(1, s_col, len(df), s_col, {
            "type": "text",
            "criteria": "containing",
            "value": "Neutral",
            "format": workbook.add_format({"bg_color": "#FFF9C4"})
        })
        worksheet.conditional_format(1, s_col, len(df), s_col, {
            "type": "text",
            "criteria": "containing",
            "value": "Negative",
            "format": workbook.add_format({"bg_color": "#FFCDD2"})
        })

    if "NPS" in df.columns:
        nps_col = df.columns.get_loc("NPS")
        worksheet.conditional_format(1, nps_col, len(df), nps_col, {
            "type": "cell",
            "criteria": ">=",
            "value": 50,
            "format": workbook.add_format({"bg_color": "#C8E6C9"})
        })
        worksheet.conditional_format(1, nps_col, len(df), nps_col, {
            "type": "cell",
            "criteria": "between",
            "minimum": 0,
            "maximum": 49,
            "format": workbook.add_format({"bg_color": "#FFF9C4"})
        })
        worksheet.conditional_format(1, nps_col, len(df), nps_col, {
            "type": "cell",
            "criteria": "<",
            "value": 0,
            "format": workbook.add_format({"bg_color": "#FFCDD2"})
        })

# =========================
# WEBSITE FEEDBACK LOADER (DETAIL + QUICK)
# =========================
INIT_DF_ALL = get_master_data()
MIN_DATE, MAX_DATE = get_default_date_range(INIT_DF_ALL)

# Optional fallbacks (safe)
DEFAULT_FROM = MIN_DATE
DEFAULT_TO   = MAX_DATE

# ==========================================================
# QUICK FEEDBACK SUMMARY CALCULATOR
# ==========================================================
def compute_qf_summary(df2):
    if df2.empty:
        return pd.DataFrame(
            columns=["Metric", "Average Score (1–5)", "Response Count", "Total Responses"]
        )

    rename_map = {
        "Rate satisfaction with the overall price paid ?": "Price Satisfaction",
        "Rate Taste of Food ?": "Taste of Food",
        "Cafe Cleanliness": "Cleanliness",
        "Did you smell the coffee aroma ?": "Coffee Aroma",
        "Staff friendliness": "Staff Friendliness",
        "Order accuracy": "Order Accuracy",
        "Speed of service": "Speed of Service",
        "Overall satisfaction": "Overall Satisfaction"
    }

    df = df2.rename(columns=rename_map)

    rows = []
    for col in rename_map.values():
        if col in df.columns:
            scores = pd.to_numeric(df[col], errors="coerce")
            count = scores.notna().sum()

            if count == 0:
                continue

            rows.append({
                "Metric": col,
                "Average Score (1–5)": round(scores.mean(), 2),
                "Response Count": int(count),
                "Total Responses": int(count)   # ✅ EMAIL FIX
            })

    return pd.DataFrame(rows)

def qf_kpi(label, value):

    try:
        score = float(value)
    except:
        score = 0

    if score >= 4:
        color = "#28a745"   # green
    elif score >= 3:
        color = "#ffc107"   # yellow
    else:
        color = "#dc3545"   # red

    return html.Div([

        html.Div(label, style={
            "fontSize": "13px",
            "color": "#555",
            "marginBottom": "6px"
        }),

        html.Div(f"{score:.2f}", style={
            "fontSize": "26px",
            "fontWeight": "bold",
            "color": color
        }),

    ], style={
        "border": f"2px solid {color}",
        "borderRadius": "10px",
        "padding": "12px",
        "minWidth": "160px",
        "textAlign": "center",
        "margin": "8px",
        "background": "white",
        "fontFamily": "Times New Roman",
        "boxShadow": "0 2px 6px rgba(0,0,0,0.08)"
    })

def build_qf_tab(df2):

    if df2 is None or df2.empty:
        return html.Div(
            "No quick feedback data available.",
            style={"color": "red", "fontFamily": "Times New Roman"}
        )

    df2 = df2.copy()

    metric_map = {
        "price_satisfaction": "Price Satisfaction",
        "taste_of_food": "Taste of Food",
        "cleanliness": "Cleanliness",
        "coffee_aroma": "Coffee Aroma",
        "staff_friendliness": "Staff Friendliness",
        "order_accuracy": "Order Accuracy",
        "speed_of_service": "Speed of Service",
        "overall_satisfaction": "Overall Satisfaction"
    }

    rows = []
    kpi_cards = []

    for col, label in metric_map.items():

        if col not in df2.columns:
            continue

        scores = pd.to_numeric(df2[col], errors="coerce")
        count = scores.notna().sum()

        if count == 0:
            continue

        avg_score = round(scores.mean(), 2)

        # Build KPI card for each metric
        kpi_cards.append(qf_kpi(label, avg_score))

        rows.append({
            "Metric": label,
            "Average Score (1–5)": avg_score,
            "Response Count": int(count),
            "Total Responses": int(count)
        })

    summary_df = pd.DataFrame(rows)

    # ================= KPI ROW =================
    kpi_row = html.Div(
        kpi_cards,
        style={
            "display": "flex",
            "flexWrap": "wrap",
            "marginBottom": "25px"
        }
    )

    # ================= TABLE =================
    summary_table = dash_table.DataTable(
        data=summary_df.to_dict("records"),
        columns=[{"name": c, "id": c} for c in summary_df.columns],
        style_header={
            "backgroundColor": "#0A66C2",
            "color": "white",
            "fontWeight": "bold"
        },
        style_cell={
            "textAlign": "left",
            "fontFamily": "Times New Roman"
        },
        page_size=15,
    )

    # ================= TREND =================
    trend_chart = html.Div()

    trend = compute_qf_trend(df2)

    if trend is not None and not trend.empty:

        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=list(trend.index),
            y=list(trend.values),
            mode="lines+markers",
            name="Overall Satisfaction"
        ))

        fig.update_layout(
            title="Daily Overall Satisfaction Trend",
            height=380
        )

        trend_chart = dcc.Graph(figure=fig)

    return html.Div([
        html.H3("Quick Feedback — KPI Summary",
                style={"color": "#0A66C2"}),

        kpi_row,

        html.H3("Quick Feedback Summary Table",
                style={"color": "#0A66C2"}),

        summary_table,

        html.Br(),

        html.H3("Daily Trend",
                style={"color": "#0A66C2"}),

        trend_chart
    ])



# ==========================================================
# STORE-WISE NPS TABLE BUILDER
# ==========================================================
def build_store_nps_table(df_all):
    df = ensure_outlet_column(df_all, "build_store_nps_table")

    P = df["NPS_Type"] == "Promoter"
    Pa = df["NPS_Type"] == "Passive"
    D = df["NPS_Type"] == "Detractor"

    summary = (
        df.assign(
            Promoters=P.astype(int),
            Passives=Pa.astype(int),
            Detractors=D.astype(int),
        )
        .groupby("Outlet Name", dropna=False)[
            ["Promoters", "Passives", "Detractors"]
        ]
        .sum()
        .reset_index()
    )

    summary["Total"] = (
        summary["Promoters"]
        + summary["Passives"]
        + summary["Detractors"]
    )

    summary["NPS"] = (
        (summary["Promoters"] - summary["Detractors"])
        / summary["Total"].replace(0, pd.NA)
        * 100
    ).round(1).fillna(0)

    return summary

# ==========================================================
# WEEKLY NPS TREND
# ==========================================================
def compute_weekly_nps(df_all):
    df = df_all.copy()

    df["Week"] = df["Created Date"].dt.to_period("W").astype(str)

    weekly = (
        df.groupby("Week")
        .agg(
            promoters=("NPS_Type", lambda x: (x == "Promoter").sum()),
            detractors=("NPS_Type", lambda x: (x == "Detractor").sum()),
            total=("NPS_Type", "count"),
        )
    )

    weekly["NPS"] = (
        (weekly["promoters"] - weekly["detractors"])
        / weekly["total"].replace(0, np.nan)
        * 100
    ).fillna(0)

    return weekly["NPS"]

# ==========================================================
# MONTHLY NPS TREND
# ==========================================================
def compute_monthly_nps(df):
    if df is None or df.empty or "Created Date" not in df or "NPS_Type" not in df:
        return None

    df = df.dropna(subset=["Created Date"]).copy()
    df["Month"] = df["Created Date"].dt.to_period("M").apply(lambda r: r.start_time)

    data = {}
    for m, g in df.groupby("Month"):
        promoters  = (g["NPS_Type"] == "Promoter").sum()
        detractors = (g["NPS_Type"] == "Detractor").sum()
        total = len(g)

        data[m] = ((promoters - detractors) / total) * 100 if total else 0

    return pd.Series(data).sort_index()

# ==========================================================
# QUICK FEEDBACK TREND (OVERALL SATISFACTION)
# ==========================================================
def compute_qf_trend(df2):

    if df2 is None or df2.empty:
        return None

    df = df2.copy()

    # Detect date column safely
    date_col = None
    for c in df.columns:
        if c.lower() in ["created_date", "created date", "date"]:
            date_col = c
            break

    if not date_col:
        return None

    if "overall_satisfaction" not in df.columns:
        return None

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df["score"] = pd.to_numeric(df["overall_satisfaction"], errors="coerce")

    trend = (
        df.dropna(subset=[date_col, "score"])
          .groupby(df[date_col].dt.date)["score"]
          .mean()
    )

    return trend if not trend.empty else None



# ==========================================================
# MATPLOTLIB FIGURE → INLINE IMAGE BYTES
# ==========================================================
def fig_to_bytes():
    """
    Convert current matplotlib figure to PNG bytes.
    MUST NOT touch any DataFrame.
    """
    buf = io.BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format="png", bbox_inches="tight")
    plt.close()
    buf.seek(0)
    return buf.read()

# ==========================================================
# SEND EMAIL (HTML + INLINE IMAGES)
# ==========================================================
def send_email_html(subject, html_body, images):

    msg = MIMEMultipart("related")
    msg["From"] = EMAIL_FROM

    to_emails  = parse_emails(EMAIL_TO)
    cc_emails  = parse_emails(EMAIL_CC)
    bcc_emails = parse_emails(EMAIL_BCC)
    
    msg["To"] = ", ".join(to_emails)
    msg["Cc"] = ", ".join(cc_emails)
    msg["Subject"] = subject

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(html_body, "html"))
    msg.attach(alt)

    for cid, img_bytes in images.items():
        img = MIMEImage(img_bytes)
        img.add_header("Content-ID", f"<{cid}>")
        img.add_header("Content-Disposition", "inline")
        msg.attach(img)

    recipients = to_emails + cc_emails + bcc_emails

    with smtplib.SMTP("smtp.gmail.com", 587) as s:
        s.starttls()
        s.login(EMAIL_FROM, EMAIL_PASS)
        s.sendmail(EMAIL_FROM, recipients, msg.as_string())
       
def safe_excel_df(df):
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
   
def send_ops_email_html(
    subject,
    html_body,
    images,
    ops_excel_part
):

    ops_to  = OPS_TO_EMAILS
    ops_cc  = OPS_CC_EMAILS
    ops_bcc = OPS_BCC_EMAILS


    if not ops_to:
        print("⚠️ OPS_TO empty — OPS mail skipped")
        return

    msg = MIMEMultipart("related")
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(ops_to)
    msg["Cc"] = ", ".join(ops_cc)
    msg["Bcc"] = ", ".join(ops_bcc)
    msg["Subject"] = subject
    msg["Reply-To"] = EMAIL_FROM

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(html_body, "html"))
    msg.attach(alt)

    # ---------- Inline images (same as leadership) ----------
    for cid, img_bytes in images.items():
        img = MIMEImage(img_bytes)
        img.add_header("Content-ID", f"<{cid}>")
        img.add_header("Content-Disposition", "inline")
        msg.attach(img)

    # ---------- EXTRA EXCEL ATTACHMENT ----------
    #ops_excel = build_ops_excel_attachment(df_all, store_summary_df, source_count_df, avg_rating_df, store_nps_df, neg_df, pos_df, date_display)
    msg.attach(ops_excel_part)

    recipients = ops_to + ops_cc + ops_bcc

    print("📨 OPS MAIL SENT TO:", recipients)

    with smtplib.SMTP("smtp.gmail.com", 587) as s:
        s.starttls()
        s.login(EMAIL_FROM, EMAIL_PASS)
        s.sendmail(EMAIL_FROM, recipients, msg.as_string())

def ensure_outlet_column(df, context=""):
    df = df.copy()
    df.columns = df.columns.str.strip()

    # already exists → good
    if "Outlet Name" in df.columns:
        return df

    # try to infer from similar names
    for col in df.columns:
        normalized = col.lower().replace("_", " ").strip()
        if normalized == "outlet name":
            df.rename(columns={col: "Outlet Name"}, inplace=True)
            print(f"🟢 [{context}] Renamed '{col}' → 'Outlet Name'")
            return df

    # fallback: derive from Outlet_norm
    if "Outlet_norm" in df.columns:
        df["Outlet Name"] = df["Outlet_norm"].str.replace("_", " ").str.title()
        print(f"🟡 [{context}] Derived 'Outlet Name' from Outlet_norm")
        return df

    # hard fail (should never happen)
    print(f"🔴 [{context}] Outlet Name missing")
    print("Available columns:", df.columns.tolist())
    return df

def build_area_html(
    df_all,
    store_summary_df,
    source_summary_df,
    pos_df,
    neu_df,
    neg_df,
    store_insight_df,
    top_risk_df,
    date_display,
    avg_rating_df,
    include_global_lifetime=True   # <<--- ADD DEFAULT HERE
):

    return build_email_html(
        kpi={
            "total": len(df_all),
            "P": (df_all["NPS_Type"] == "Promoter").sum(),
            "Pa": (df_all["NPS_Type"] == "Passive").sum(),
            "D": (df_all["NPS_Type"] == "Detractor").sum(),
            "NPS": (
                ((df_all["NPS_Type"] == "Promoter").sum()
                - (df_all["NPS_Type"] == "Detractor").sum())
                / len(df_all) * 100
                if len(df_all) else 0
            )
        },
        store_summary_df=store_summary_df,
        source_summary_df=source_summary_df,
        pos_df=pos_df,
        neu_df=neu_df,
        neg_df=neg_df,
        store_insight_df=store_insight_df,
        top_risk_df=top_risk_df,
        weekly_cid=None,
        monthly_cid=None,
        qf_summary_df=pd.DataFrame(),
        qf_cid=None,
        store_nps_df=build_store_nps_table(df_all),
        date_display=date_display,
        avg_rating_df=avg_rating_df   # ✅ THIS IS THE FIX
    )

def safe_filter_by_outlet(df, outlet_name):
    if df is None or df.empty:
        return df

    df = df.copy()

    # Normalize column name
    if "Outlet Name" not in df.columns:
        if "Outlet Name_x" in df.columns:
            df["Outlet Name"] = df["Outlet Name_x"]
        elif "Outlet Name_y" in df.columns:
            df["Outlet Name"] = df["Outlet Name_y"]
        elif "Outlet_norm" in df.columns:
            df["Outlet Name"] = df["Outlet_norm"].str.replace("_", " ").str.title()
        else:
            return df  # nothing we can do

    return df[df["Outlet Name"] == outlet_name]

#===============================================
#UNIFIED “CUSTOMER FIELD” NORMALIZER (CRITICAL)
#===============================================
def enrich_customer_name(df):

    if df is None or df.empty:
        return df

    # 🚫 If Source not present, DO NOTHING
    if "Source" not in df.columns or "Customer Name" not in df.columns:
        return df

    mask = (df["Source"] == "Zomato") & (df["Customer Name"] == "")

    if "Order ID" in df.columns:
        df.loc[mask, "Customer Name"] = (
            df.loc[mask, "Order ID"].fillna("").astype(str)
        )

    return df

def clean_excel_df(df):
    if df is None or df.empty:
        return df

    # 🔒 ONLY enrich if complaint-level columns exist
    required = {"Source", "Customer Name", "Comment"}
    if not required.issubset(df.columns):
        return df

    df = df.copy()
    df = enrich_customer_name(df)
    if "Created Date" in df.columns:
        df["Created Date"] = df["Created Date"].apply(format_ddmmyyyy)
    return df

def prepare_complaint_table(df):
    cols = [
        "Outlet Name",
        "Source",
        "Rating",
        "Comment",
        "Created Date",
        "Customer Name"
    ]

    df = df.copy()
    df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")

    return (
        df[cols]
        .sort_values("Created Date", ascending=False)
        .reset_index(drop=True)
    )

def build_avg_rating_lifetime_email_html(df_all, till_date, allowed_outlets=None):
    """
    MAIL ONLY
    Lifetime Average Rating (Store × Source)

    DEFINITIVE RULE (LOCKED):
    National Average = mean of per-outlet lifetime averages (per source)

    This guarantees:
    - Overall Average Rating == Store×Source National Average
    - Stable across date filters
    """

    if df_all is None or df_all.empty:
        return "<i>No rating data available</i>"

    df = df_all.copy()

    # ===== FILTER FOR AREA MANAGER ONLY =====
    if allowed_outlets is not None:
        allowed_outlets = set(str(x).strip() for x in allowed_outlets)
        df = df[df["Outlet Name"].isin(allowed_outlets)]

    # 🔒 IMPORTANT: STOP HERE IF NO DATA
    if df.empty:
        return "<i>No lifetime rating data available</i>"

    # ---------------- CLEAN & NORMALIZE ----------------
    df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")
    df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")

    SOURCE_MAP = {
        "google": "Google",
        "feedback form": "Feedback Form",
        "zomato": "Zomato",
        "swiggy": "Swiggy",
        "z-district": "Z-District",
    }

    df["Source"] = (
        df["Source"]
        .astype(str)
        .str.strip()
        .str.lower()
        .map(SOURCE_MAP)
    )

    #df = df.dropna(subset=["Created Date", "Rating", "Source"])
    # ✅ NEW (LIFETIME SAFE)
    df = df.dropna(subset=["Rating", "Source"])

    # Lifetime cut-off (inclusive)
    # ---- HANDLE DATE RANGE INPUT ("start to end") ----
    #if isinstance(till_date, str) and " to " in till_date:
     #   till_date = till_date.split(" to ")[1].strip()

    # ---- PARSE END DATE SAFELY ----
    #till_dt = pd.to_datetime(till_date, dayfirst=True, errors="coerce")
    #if pd.isna(till_dt):
     #   raise ValueError(f"Invalid till_date: {till_date}")

    # End-of-day adjustment
    #till_dt = till_dt + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    #df = df[df["Created Date"] <= till_dt]

    sources = ["Google", "Feedback Form", "Swiggy", "Zomato", "Z-District"]

    def rating_color(val):
        if val > 4:
            return "green"
        elif val >= 3:
            return "orange"
        else:
            return "red"

    outlet_rows = []

    # ---------------- PER-OUTLET LIFETIME AVG ----------------
    for outlet, g_outlet in df.groupby("Outlet Name", dropna=False):
        row = {"Outlet Name": outlet}
        min_vals = []

        for src in sources:
            g_src = g_outlet[g_outlet["Source"] == src]
            if g_src.empty:
                row[src] = ""
                continue

            avg = round(g_src["Rating"].mean(), 2)
            row[src] = avg
            min_vals.append(avg)

        row["Min Rating"] = round(min(min_vals), 2) if min_vals else ""
        outlet_rows.append(row)

    # ---------------- GLOBAL AVERAGE (MATCH STORE × SOURCE) ----------------
    global_row = {"Outlet Name": "National Average"}

    for src in sources:
        vals = [
            r[src]
            for r in outlet_rows
            if r.get("Outlet Name") != "National Average" and r.get(src) != ""
        ]
        global_row[src] = round(sum(vals) / len(vals), 2) if vals else ""

    global_row["Min Rating"] = ""
    outlet_rows.append(global_row)

    # ---------------- BUILD HTML ----------------
    html_rows = ""

    for r in outlet_rows:
        html_rows += "<tr>"
        html_rows += f"<td class='outlet'>{r['Outlet Name']}</td>"

        for src in sources:
            val = r.get(src, "")
            if val == "":
                html_rows += "<td></td>"
            else:
                html_rows += (
                    f"<td style='color:{rating_color(val)}; font-weight:bold;'>"
                    f"{val}</td>"
                )

        min_val = r.get("Min Rating", "")
        if min_val != "":
            html_rows += (
                f"<td style='color:{rating_color(min_val)}; font-weight:bold;'>"
                f"{min_val}</td>"
            )
        else:
            html_rows += "<td></td>"

        html_rows += "</tr>"

    return f"""
    <table class="avg-rating-table">
        <thead>
            <tr>
                <th colspan="7" style="text-align:middle;">
                    Overall Average Rating (Lifetime — Store-weighted)
                </th>
            </tr>
            <tr>
                <th>Outlet Name</th>
                <th>Google</th>
                <th>Feedback Form</th>
                <th>Swiggy</th>
                <th>Zomato</th>
                <th>Z-District</th>
                <th>Min Rating</th>
            </tr>
        </thead>
        <tbody>
            {html_rows}
        </tbody>
    </table>
    """
        
def build_filtered_html(
    df_all,
    store_summary_df,
    source_summary_df,
    pos_df,
    neu_df,
    neg_df,
    store_insight_df,
    top_risk_df,
    date_display
):
    # KPIs
    P = (df_all["NPS_Type"] == "Promoter").sum()
    Pa = (df_all["NPS_Type"] == "Passive").sum()
    D = (df_all["NPS_Type"] == "Detractor").sum()
    T = P + Pa + D

    kpi = {
        "total": T,
        "P": P,
        "Pa": Pa,
        "D": D,
        "NPS": ((P - D) / T) * 100 if T else 0
    }

    return build_email_html(
        kpi=kpi,
        store_summary_df=store_summary_df,
        source_summary_df=source_summary_df,
        pos_df=pos_df,
        neu_df=neu_df,
        neg_df=neg_df,
        store_insight_df=store_insight_df,
        top_risk_df=top_risk_df,
        weekly_cid=None,
        monthly_cid=None,
        qf_summary_df=pd.DataFrame(),
        qf_cid=None,
        store_nps_df=build_store_nps_table(df_all),
        date_display=date_display
    )

#============================================
#STORE / AREA MANAGER EXCEL BUILDER (FINAL)
#============================================
def build_store_or_area_excel(
    buffer,
    all_df,
    summary_df,
    nps_df,
    neg_df,
    pos_df
):
    """
    Final safe Excel builder:
    - Removes *_x / *_y columns
    - Guarantees Outlet Name
    - Prevents blank sheets
    """

    def clean_df(df):
        if df is None or df.empty:
            return None

        df = normalize_outlet_columns(df)

        # Remove internal system columns
        drop_cols = {
            "Outlet_norm", "Zomato Id", "Store Id",
            "Region", "Area Manager", "StoreEmail", "ManagerEmail"
        }
        df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

        return df

    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:

        # ---------------- ALL FEEDBACK ----------------
        df1 = clean_df(all_df)
        if df1 is not None and not df1.empty:
            df1.to_excel(writer, sheet_name="All_Feedback", index=False)

        # ---------------- STORE SUMMARY ----------------
        df2 = clean_df(summary_df)
        if df2 is not None and not df2.empty:
            df2.to_excel(writer, sheet_name="Store_Summary", index=False)

        # ---------------- STORE NPS ----------------
        df3 = clean_df(nps_df)
        if df3 is not None and not df3.empty:
            df3.to_excel(writer, sheet_name="Store_NPS", index=False)

        # ---------------- NEGATIVE ----------------
        df4 = clean_df(neg_df)
        if df4 is not None and not df4.empty:
            df4.to_excel(writer, sheet_name="Negative", index=False)

        # ---------------- POSITIVE ----------------
        df5 = clean_df(pos_df)
        if df5 is not None and not df5.empty:
            df5.to_excel(writer, sheet_name="Positive", index=False)

#==================================
#Store Wise Email 
#============================

def send_store_wise_emails(
    df_all,
    store_summary_df,
    store_nps_df,
    neg_df,
    pos_df,
    date_display,
    custom_message_block=""   # 👈 ADD
):
    merged = df_all.merge(
        STORE_MAP_DF[["Outlet_norm", "StoreEmail", "ManagerEmail"]],
        on="Outlet_norm",
        how="left"
    )

    merged = ensure_outlet_column(merged, "store_email")

    for outlet_norm, g in merged.groupby("Outlet_norm"):

        store_name = g["Outlet Name"].iloc[0]
        store_email = g["StoreEmail"].iloc[0]
        manager_email = g["ManagerEmail"].iloc[0]

        if not store_email:
            continue

        # ✅ FILTERED HTML
        html_body = custom_message_block + build_filtered_html(
            df_all=g,
            store_summary_df=safe_filter_by_outlet(store_summary_df, store_name),
            source_summary_df=build_source_outlet_summary(g),
            pos_df=safe_filter_by_outlet(pos_df, store_name),
            neu_df=g[g["Sentiment"] == "Neutral"],
            neg_df=safe_filter_by_outlet(neg_df, store_name),
            store_insight_df=build_store_insights(g),
            top_risk_df=build_top_risk_stores(g),
            date_display=date_display
        )

        # ✅ Excel
        buffer = io.BytesIO()
        build_store_or_area_excel(
            buffer,
            g,
            safe_filter_by_outlet(store_summary_df, store_name),
            safe_filter_by_outlet(store_nps_df, store_name),
            safe_filter_by_outlet(neg_df, store_name),
            safe_filter_by_outlet(pos_df, store_name),
        )

        buffer.seek(0)

        part = MIMEBase(
            "application",
            "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        part.set_payload(buffer.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f'attachment; filename="{store_name}_Feedback.xlsx"'
        )

        msg = MIMEMultipart("related")
        msg["From"] = EMAIL_FROM
        msg["To"] = store_email
        if manager_email:
            msg["Cc"] = manager_email
        msg["Subject"] = f"Store Feedback – {store_name} {date_display}"

        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(html_body, "html"))
        msg.attach(alt)
        msg.attach(part)

        with smtplib.SMTP("smtp.gmail.com", 587) as s:
            s.starttls()
            s.login(EMAIL_FROM, EMAIL_PASS)
            to_emails = [
                e.strip()
                for e in [store_email, manager_email]
                if e and str(e).strip()
            ]
            if not to_emails:
                continue

            s.sendmail(EMAIL_FROM, to_emails, msg.as_string())
           
#=========================================
#Area Manager wise Email
#=====================================

def send_area_manager_emails(
    df_all,
    store_summary_df,
    store_nps_df,
    neg_df,
    pos_df,
    date_display,
    custom_message_block=""   # 👈 ADD
):
    df_all = ensure_outlet_column(df_all, "send_area_manager_emails")
    # 🔒 TRUE LIFETIME DATA (ignore UI filters completely)
    df_lifetime = get_master_data().copy()

    merged = df_all.merge(
        STORE_MAP_DF[["Outlet_norm", "StoreEmail", "ManagerEmail"]],
        on="Outlet_norm",
        how="left"
    )

    # ---------- ENSURE CLEAN OUTLET NAME COLUMN ----------
    if "Outlet Name_x" in merged.columns:
        merged["Outlet Name"] = merged["Outlet Name_x"]
    elif "Outlet Name" in merged.columns:
        merged["Outlet Name"] = merged["Outlet Name"]
    elif "Outlet Name_y" in merged.columns:
        merged["Outlet Name"] = merged["Outlet Name_y"]
    else:
        print("❌ ERROR: No Outlet Name column in merged data")
        print("Merged Columns:", merged.columns.tolist())
        return

    for manager_email, g in merged.groupby("ManagerEmail"):

        if not manager_email or g.empty:
            continue

        # ================= AREA MANAGER NAME =================
        manager_name = (
            g["Area Manager"]
            .dropna()
            .astype(str)
            .iloc[0]
            if "Area Manager" in g.columns and g["Area Manager"].notna().any()
            else "Area_Manager"
        )
        manager_name_safe = (
            manager_name.strip()
            .replace(" ", "_")
            .replace("/", "_")
        )

        # ================= FILTER DATA =================
        outlet_names = g["Outlet Name"].dropna().unique()
        if len(outlet_names) == 0:
            continue

        store_summary_f = store_summary_df[
            store_summary_df["Outlet Name"].isin(outlet_names)
        ]

        store_nps_f = store_nps_df[
            store_nps_df["Outlet Name"].isin(outlet_names)
        ]

        neg_f = neg_df[neg_df["Outlet Name"].isin(outlet_names)]
        pos_f = pos_df[pos_df["Outlet Name"].isin(outlet_names)]


        # ================= BUILD HTML =================
        avg_rating_df = build_avg_rating_email_table(g)
        
        allowed = g["Outlet Name"].unique().tolist()

        lifetime_block = build_avg_rating_lifetime_email_html(
            df_all=df_lifetime,        # ✅ always full lifetime
            till_date="Till Date",
            allowed_outlets=allowed
        )


        html_body = (
            custom_message_block
            + lifetime_block
            + "<br><br>"
            + build_area_html(
                df_all=g,
                store_summary_df=store_summary_f,
                source_summary_df=build_source_outlet_summary(g),
                pos_df=pos_f,
                neu_df=g[g["Sentiment"] == "Neutral"],
                neg_df=neg_f,
                store_insight_df=build_store_insights(g),
                top_risk_df=build_top_risk_stores(g),
                date_display=date_display,
                avg_rating_df=avg_rating_df,
                include_global_lifetime=False
            )
        )
        # ================= BUILD EXCEL (8 SHEETS) =================
        source_count_df = build_source_wise_feedback_count_excel(g)

        neu_df_excel = g[g["Sentiment"] == "Neutral"].copy()
        neg_df_excel = g[g["Sentiment"] == "Negative"].copy()
        pos_df_excel = g[g["Sentiment"] == "Positive"].copy()

        area_excel = build_area_manager_excel_attachment(
            df_all=g,
            store_summary_df=store_summary_f,
            source_count_df=source_count_df,
            avg_rating_df=avg_rating_df,
            store_nps_df=store_nps_f,
            neg_df=neg_df_excel,
            neu_df=neu_df_excel,
            pos_df=pos_df_excel,
            date_display=date_display
        )
        
        area_excel.add_header(
            "Content-Disposition",
            f'attachment; filename="Area_wise_Stores_Feedback__{date_display}.xlsx"'
        )

        # ================= SEND EMAIL =================
        msg = MIMEMultipart("related")
        msg["From"] = EMAIL_FROM
        msg["To"] = manager_email
        msg["Subject"] = f"Area Feedback Report ({date_display})"

        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(html_body, "html"))
        msg.attach(alt)
        msg.attach(area_excel)

        with smtplib.SMTP("smtp.gmail.com", 587) as s:
            s.starttls()
            s.login(EMAIL_FROM, EMAIL_PASS)
            s.sendmail(EMAIL_FROM, [manager_email], msg.as_string())

        print(f"✅ Area email sent → {manager_email}")

# KPI COMPUTATION (ADD ONCE)
def compute_dashboard_kpis(df):
    if df.empty:
        return {}

    total = len(df)

    # ---------- Rating ----------
    avg_rating = round(df["Rating"].mean(), 2)

    rating_dist = (
        df["Rating"]
        .dropna()
        .astype(int)
        .value_counts()
        .reindex([5, 4, 3, 2, 1], fill_value=0)
    )

    rated_total = df["Rating"].notna().sum()
    rating_pct = round((rating_dist / rated_total) * 100, 2) if rated_total else rating_dist

    # ---------- NPS ----------
    promoters  = (df["Rating"].isin([4, 5])).sum()
    passives   = (df["Rating"] == 3).sum()
    detractors = (df["Rating"].isin([1, 2])).sum()

    nps = round(((promoters - detractors) / total) * 100, 2) if total else 0

    # ---------- Reply ----------
    replied = df["Reply"].fillna("").str.strip() != ""
    replied_c = replied.sum()
    not_replied_c = total - replied_c

    # ---------- Text ----------
    has_text = df["Comment"].fillna("").str.strip() != ""
    text_c = has_text.sum()
    no_text_c = total - text_c

    return {
        "total": total,
        "avg_rating": avg_rating,
        "rating_dist": rating_dist,
        "rating_pct": rating_pct,
        "nps": nps,
        "promoters_pct": round((promoters / total) * 100, 2),
        "passives_pct": round((passives / total) * 100, 2),
        "detractors_pct": round((detractors / total) * 100, 2),
        "replied_c": replied_c,
        "replied_pct": round((replied_c / total) * 100, 2),
        "not_replied_c": not_replied_c,
        "not_replied_pct": round((not_replied_c / total) * 100, 2),
        "text_c": text_c,
        "text_pct": round((text_c / total) * 100, 2),
        "no_text_c": no_text_c,
        "no_text_pct": round((no_text_c / total) * 100, 2),
    }

# KPI CARD UI (REUSABLE)
def kpi_card(title, value, subtitle=None, color="#0A66C2"):
    return html.Div([
        html.Div(title, style={
            "fontSize": "13px",
            "color": "#555",
            "marginBottom": "6px"
        }),
        html.Div(value, style={
            "fontSize": "28px",
            "fontWeight": "bold",
            "color": color
        }),
        html.Div(subtitle or "", style={
            "fontSize": "12px",
            "color": "#777",
            "marginTop": "4px"
        })
    ], style={
        "border": f"2px solid {color}",
        "borderRadius": "12px",
        "padding": "14px",
        "minWidth": "210px",
        "background": "white",
        "fontFamily": "Times New Roman",
        "boxShadow": "0 3px 10px rgba(0,0,0,0.08)"
    })

def get_running_month_dates():
    today = pd.Timestamp.today().normalize()
    start = today.replace(day=1)
    return start.date(), today.date()

def apply_multi_filter(df, column, values):
    if not values or column not in df.columns:
        return df
    return df[df[column].isin(values)]
# Callback

def register_cascade_callbacks(app):

    if getattr(app, "_cascade_callbacks_registered", False):
        return
    app._cascade_callbacks_registered = True

    @app.callback(
        Output("f_brand", "options"),
        Output("f_region", "options"),
        Output("f_state", "options"),
        Output("f_city", "options"),
        Output("f_type", "options"),
        Output("f_outlet", "options"),

        Input("f_brand", "value"),
        Input("f_region", "value"),
        Input("f_state", "value"),
        Input("f_city", "value"),
        Input("f_type", "value"),
    )
    def cascade_filters(brand, region, state, city, typ):
        df = STORE_MAP_DF.copy()

        if brand:
            if isinstance(brand, str):
                brand = [brand]
            df = df[df["Brand"].isin(brand)]
        if region:
            if isinstance(region, str):
                region = [region]
            df = df[df["Region"].isin(region)]
        if state:
            if isinstance(state, str):
                state = [state]
            df = df[df["State"].isin(state)]
        if city:
            if isinstance(city, str):
                city = [city]
            df = df[df["City"].isin(city)]
        if typ:
            if isinstance(typ, str):
                typ = [typ]
            df = df[df["Type"].isin(typ)]

        def make_opts(col):
            return [{"label": v, "value": v} for v in sorted(df[col].dropna().unique())]

        return (
            make_opts("Brand"),
            make_opts("Region"),
            make_opts("State"),
            make_opts("City"),
            make_opts("Type"),
            make_opts("Outlet Name"),
        )

def register_filter_controller(app):
    if getattr(app, "_filter_controller_registered", False):
        return
    app._filter_controller_registered = True

    @app.callback(
        # -------- FILTER VALUES --------
        Output("f_brand", "value"),
        Output("f_region", "value"),
        Output("f_state", "value"),
        Output("f_city", "value"),
        Output("f_type", "value"),
        Output("f_outlet", "value"),
        Output("f_source", "value"),

        # -------- DATE --------
        Output("date_from", "date"),
        Output("date_to", "date"),

        # -------- MONTH FILTER (SOLE OWNER) --------
        Output("month_filter", "value"),
        Output("month_filter", "options", allow_duplicate=True),


        # -------- TRIGGERS --------
        Input("reset_filters_btn", "n_clicks"),
        Input("qr_yesterday", "n_clicks"),
        Input("qr_7", "n_clicks"),
        Input("qr_30", "n_clicks"),
        Input("qr_90", "n_clicks"),
        Input("month_filter", "value"),
        Input("date_from", "date"),
        Input("date_to", "date"),

        prevent_initial_call=True
    )
    def control_filters(
        reset_click, y, d7, d30, d90,
        month_val, start, end
    ):
        ctx = dash.callback_context.triggered_id
        today = pd.Timestamp.today().normalize()

        def build_month_options(start, end):
            if not start or not end:
                return []
            months = pd.period_range(pd.to_datetime(start), pd.to_datetime(end), freq="M")
            return [{"label": m.strftime("%b %Y"), "value": m.strftime("%Y-%m")} for m in months]

        # RESET
        if ctx == "reset_filters_btn":
            return (
                None, None, None, None, None, None,
                "ALL",
                MIN_DATE, MAX_DATE,
                None,
                build_month_options_from_df(INIT_DF_ALL)
            )

        # QUICK RANGES
        if ctx == "qr_yesterday":
            d = today - pd.Timedelta(days=1)
            return (no_update, no_update, no_update, no_update, no_update, no_update,
                    no_update, d.date(), d.date(), None, build_month_options(d, d))

        if ctx == "qr_7":
            s, e = today - pd.Timedelta(days=7), YESTERDAY
            return (no_update, no_update, no_update, no_update, no_update, no_update,
                    no_update, to_date(s), to_date(e), None, build_month_options(to_date(s), to_date(e)))


        if ctx == "qr_30":
            s, e = today - pd.Timedelta(days=29), today
            return (no_update, no_update, no_update, no_update, no_update, no_update,
                    no_update, to_date(s), to_date(e), None, build_month_options(to_date(s), to_date(e)))


        if ctx == "qr_90":
            s, e = today - pd.Timedelta(days=89), today
            return (no_update, no_update, no_update, no_update, no_update, no_update,
                    no_update, to_date(s), to_date(e), None, build_month_options(to_date(s), to_date(e)))


        # MONTH SELECTION
        if ctx == "month_filter" and month_val:
            try:
                s = pd.to_datetime(month_val + "-01")
            except Exception:
                raise PreventUpdate

            e = s + pd.offsets.MonthEnd(1)
            return (no_update, no_update, no_update, no_update, no_update, no_update,
                    no_update, s.date(), e.date(), month_val, build_month_options(s, e))

        # DATE PICKER CHANGE
        if ctx in ("date_from", "date_to"):
            if not start or not end:
                start, end = get_running_month_dates()
            return (no_update, no_update, no_update, no_update, no_update, no_update,
                    no_update, start, end, month_val, build_month_options(start, end))

        raise PreventUpdate
    
def apply_date_filter(df, start, end):

    if not start and not end:
        return df

    df = df.copy()
    df["Created Date"] = pd.to_datetime(df["Created Date"], errors="coerce")

    if start:
        df = df[df["Created Date"] >= pd.to_datetime(start)]

    if end:
        df = df[df["Created Date"] <= pd.to_datetime(end) + pd.Timedelta(days=1)]

    return df


# -------------------------------------------------
# SAFE STORE FILTER RESOLVER (USED BY DASHBOARD & EMAIL)
# -------------------------------------------------
def resolve_allowed_outlets(
    brand=None,
    region=None,
    state=None,
    city=None,
    type_=None,
    store=None
):
    """
    Returns allowed Outlet_norm values.

    RULES:
    - If NO filters selected → all outlets
    - If filters selected but no match → empty set
    - Website rows (empty Outlet_norm) are NOT auto-included here
    """

    sm = STORE_MAP_DF.copy()

    filters_applied = any([brand, region, state, city, type_, store])

    if brand:
        sm = sm[sm["Brand"].isin([brand] if isinstance(brand,str) else brand)]
    if region:
        sm = sm[sm["Region"] == region]
    if state:
        sm = sm[sm["State"] == state]
    if city:
        sm = sm[sm["City"] == city]
    if type_:
        sm = sm[sm["Type"] == type_]
    if store:
        sm = sm[sm["Outlet Name"] == store]

    allowed = set(sm["Outlet_norm"].dropna())

    # ✅ ONLY when NO filters are applied
    if not filters_applied:
        allowed = set(STORE_MAP_DF["Outlet_norm"].dropna())

    return allowed

# =====================================================================
# PART C — DASHBOARD CALLBACKS (CLEAN + STABLE)
# =====================================================================

def register_dashboard_callbacks(app):

    @app.callback(
        Output("feedback_tabs", "children"),
        Input("apply_btn", "n_clicks"),

        State("f_brand", "value"),
        State("f_region", "value"),
        State("f_state", "value"),
        State("f_city", "value"),
        State("f_type", "value"),
        State("f_outlet", "value"),
        State("f_source", "value"),
        State("date_from", "date"),
        State("date_to", "date"),
        State("month_filter", "value"),
        prevent_initial_call=True   # 🔥 IMPORTANT
    )
    def refresh_dashboard(
        n_clicks,
        brand, region, state, city, type_, store, source,
        date_from, date_to,
        month_val
    ):

           # ---------- SAFETY ----------
        if not n_clicks:
            raise PreventUpdate    
        # --------------------------------------------------
        # 1️⃣ LOAD MASTER DATA
        # --------------------------------------------------
        df_all = get_master_data().copy()
        assert_schema(df_all)

        if df_all is None or df_all.empty:
            return html.Div("No feedback data available.", style={"color": "red"})
        
       # --------------------------------------------------
        # LOAD QUICK FEEDBACK (WEBSITE ONLY)
        # --------------------------------------------------
        
        df_qf = load_quick_feedback_data()
        # --------------------------------------------------
        # 2️⃣ RESOLVE ALLOWED OUTLETS (BEFORE DATE FILTER)
        # --------------------------------------------------
        allowed = resolve_allowed_outlets(
            brand=brand,
            region=region,
            state=state,
            city=city,
            type_=type_,
            store=store
        )

       # --------------------------------------------------
        # 🔥 SAFE OUTLET FILTER (DO NOT DROP GOOGLE / ZOMATO)
        # --------------------------------------------------

        if allowed:
            df_all = df_all[df_all["Outlet_norm"].isin(allowed)]

        # 🔥 3️⃣ MONTH FILTER (HIGHEST PRIORITY)
        # 🔥 MONTH HAS PRIORITY
        if month_val:
            df_all = apply_month_filter(df_all, month_val)
        else:
            df_all = apply_date_filter(df_all, date_from, date_to)

        # --------------------------------------------------
        # APPLY SAME FILTER TO QUICK FEEDBACK
        # --------------------------------------------------

        df_qf = df_qf.rename(columns={"created_date": "Created Date"})

        if month_val:
            df_qf = apply_month_filter(df_qf, month_val)
        else:
            df_qf = apply_date_filter(df_qf, date_from, date_to)

                # --------------------------------------------------
        # 4️⃣ SOURCE FILTER
        # --------------------------------------------------
        if source and source != "ALL":
            df_all = df_all[df_all["Source"] == source]
                    
        if df_all.empty:
            return html.Div("No data for selected date range.", style={"color": "red"})

        df_all["Sentiment"] = df_all["Rating"].apply(
            lambda r: "Negative" if r in (1, 2)
            else "Neutral" if r == 3
            else "Positive" if r in (4, 5)
            else None
        )

        # --------------------------------------------------
        # 5️⃣ DERIVED DATAFRAMES
        # --------------------------------------------------
        #df_web = df_all[df_all["Source"] == "Feedback Form"].copy()
        neg_df = df_all[df_all["Sentiment"] == "Negative"]
        neu_df = df_all[df_all["Sentiment"] == "Neutral"]
        pos_df = df_all[df_all["Sentiment"] == "Positive"]

            # --------------------------------------------------
        # 6️⃣ NPS CALCULATION
        # --------------------------------------------------
        df_all["NPS_Type"] = df_all["Rating"].apply(rating_to_nps_type)

        P  = (df_all["NPS_Type"] == "Promoter").sum()
        Pa = (df_all["NPS_Type"] == "Passive").sum()
        D  = (df_all["NPS_Type"] == "Detractor").sum()
        T  = P + Pa + D
        NPS = ((P - D) / T) * 100 if T else 0

        # 🔒 FINAL HARD GUARANTEE BEFORE ANY SUMMARY
        df_all = ensure_outlet_column(df_all, "send_report:before_source_summary")

       # --------------------------------------------------
        # 7️⃣ SOURCE × OUTLET SUMMARY
        # --------------------------------------------------
        source_summary_df = build_source_outlet_summary(df_all)
        
        source_summary_table = dash_table.DataTable(
            data=source_summary_df.to_dict("records"),
            columns=[
                {"name": "Outlet Name", "id": "Outlet Name"},
                {"name": "Google", "id": "Google"},
                {"name": "Feedback Form", "id": "Feedback Form"},
                {"name": "Swiggy", "id": "Swiggy"},
                {"name": "Zomato", "id": "Zomato"},
                {"name": "Z-District", "id": "Z-District"},
            ],
            page_size=15,
            sort_action="native",
            filter_action="native",
            style_header={
                "backgroundColor": "#0A66C2",
                "color": "white",
                "fontWeight": "bold",
                "textAlign": "center",
                "fontFamily": "Times New Roman"
            },
            style_cell={
                "textAlign": "center",
                "fontFamily": "Times New Roman"
            }
        )

        # --------------------------------------------------
        # 6️⃣ KPI ROW
        # --------------------------------------------------
        def kpi(label, value, bg="white", highlight=False):
            return html.Div([

                # KPI Label
                html.Div(
                    label,
                    style={
                        "fontSize": "13px",
                        "fontWeight": "600",
                        "letterSpacing": "0.4px",
                        "marginBottom": "6px",
                        "color": "white" if bg != "white" else "#444",
                    }
                ),

                # KPI Value
                html.Div(
                    value,
                    style={
                        "fontSize": "30px",
                        "fontWeight": "bold",
                        "color": "white" if bg != "white" else "#0A66C2",
                    }
                )

            ], style={
                "background": bg,
                "border": "3px solid #0A66C2" if highlight else "1.5px solid #dee2e6",
                "borderRadius": "14px",
                "padding": "16px",
                "minWidth": "180px",
                "textAlign": "center",
                "marginRight": "12px",
                "marginBottom": "12px",
                "fontFamily": "Times New Roman",
                "boxShadow": "0 6px 14px rgba(0,0,0,0.10)",
                "transition": "transform 0.2s ease",
            })
        kpi_row = html.Div([

            kpi("Total Responses", T, bg="white"),

            kpi("Promoters", P, bg="#28a745"),     # Green

            kpi("Passives", Pa, bg="#0d6efd"),     # Blue

            kpi("Detractors", D, bg="#dc3545"),    # Red

            kpi(
                "NPS Score",
                f"{NPS:.1f}",
                bg="#ffc107",                      # Yellow
                highlight=True
            ),

        ], style={
            "display": "flex",
            "flexWrap": "wrap",
            "marginBottom": "20px"
        })
        kpi = compute_dashboard_kpis(df_all)

        dashboard_kpis = html.Div([

            #kpi_card(
             #   "Total Reviews",
              #  kpi["total"]
           # ),

            kpi_card(
                "Average Rating",
                f'{kpi["avg_rating"]} ⭐'
            ),

            kpi_card(
                "NPS Score",
                f'{kpi["nps"]}',
                f'Promoters {kpi["promoters_pct"]}% | '
                f'Passives {kpi["passives_pct"]}% | '
                f'Detractors {kpi["detractors_pct"]}%',
                color="#28a745"
            ),

            kpi_card(
                "Replied vs Not Replied",
                f'{kpi["replied_c"]} / {kpi["not_replied_c"]}',
                f'{kpi["replied_pct"]}% Replied',
                color="#198754"
            ),

            kpi_card(
                "Text vs No Text",
                f'{kpi["text_c"]} / {kpi["no_text_c"]}',
                f'{kpi["text_pct"]}% With Text',
                color="#0d6efd"
            ),

            kpi_card(
                "5⭐ Reviews",
                f'{kpi["rating_dist"].get(5,0)}',
                f'{kpi["rating_pct"].get(5,0)}%',
                color="#f4b400"
            ),

        ], style={
            "display": "flex",
            "flexWrap": "wrap",
            "gap": "14px",
            "marginBottom": "20px"
        })

        # --------------------------------------------------
        # 7️⃣ STORE SUMMARY TABLE (COMPLAINTS)
        # --------------------------------------------------
        summary = (
            df_all.groupby(["Outlet Name", "Sentiment"])
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )

        # Ensure all columns exist
        for c in ["Positive", "Neutral", "Negative"]:
            if c not in summary.columns:
                summary[c] = 0

        summary["Total"] = summary["Positive"] + summary["Neutral"] + summary["Negative"]

        summary_table = dash_table.DataTable(
            data=summary.to_dict("records"),
            columns=[{"name": c, "id": c} for c in summary.columns],
            page_size=15,

            style_header={
                "backgroundColor": "#0A66C2",
                "color": "white",
                "fontWeight": "bold",
                "textAlign": "center"
            },

            style_header_conditional=[
                {
                    "if": {"column_id": "Positive"},
                    "backgroundColor": "#2E7D32",
                    "color": "white"
                },
                {
                    "if": {"column_id": "Neutral"},
                    "backgroundColor": "#F9A825",
                    "color": "white"
                },
                {
                    "if": {"column_id": "Negative"},
                    "backgroundColor": "#B00020",
                    "color": "white"
                },
                {
                    "if": {"column_id": "Total"},
                    "backgroundColor": "#424242",
                    "color": "white",
                    "fontWeight": "bold"
                }
            ],

            style_cell={
                "textAlign": "left",
                "fontFamily": "Times New Roman"
            }
        )

        # -------------------------------------------------------------
        # 8️⃣ Store Sentiment Bar Chart
        # -------------------------------------------------------------
        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=summary["Outlet Name"], y=summary["Positive"],
            name="Positive", marker_color="#2E7D32"
        ))
        fig.add_trace(go.Bar(
            x=summary["Outlet Name"], y=summary["Neutral"],
            name="Neutral", marker_color="#F9A825"  
        ))
        fig.add_trace(go.Bar(
            x=summary["Outlet Name"], y=summary["Negative"],
            name="Negative", marker_color="#B00020"
        ))

        fig.update_layout(
            barmode="group",
            title="Store-wise Sentiment Summary",
            height=450,
            margin=dict(l=10, r=10, t=40, b=20)
        )

        store_chart = dcc.Graph(figure=fig)
        
        # --------------------------------------------------
        # 9️⃣ FINAL TABS
        # --------------------------------------------------
        store_nps_dash = build_store_nps_table(df_all)
        top_risk_df = build_top_risk_stores(df_all)
        store_insight_df = build_store_insights(df_all)
        # # --------------------------------------------------
        # # QUICK FEEDBACK KPI + TREND
        # # --------------------------------------------------

        # qf_summary_df = compute_qf_summary(df_qf)
        # qf_trend = compute_qf_trend(df_qf)

        # # ---- KPI (Overall Satisfaction only)
        # overall_score = 0

        # if not qf_summary_df.empty:
        #     row = qf_summary_df[
        #         qf_summary_df["Metric"] == "Overall Satisfaction"
        #     ]
        #     if not row.empty:
        #         overall_score = row["Average Score (1–5)"].iloc[0]

        # qf_kpi_row = html.Div([
        #     qf_kpi("Overall Satisfaction", overall_score)
        # ], style={
        #     "display": "flex",
        #     "marginBottom": "20px"
        # })

        # # ---- Trend Graph
        # qf_trend_graph = html.Div()

        # if qf_trend is not None and not qf_trend.empty:
        #     fig_qf = go.Figure()
        #     fig_qf.add_trace(
        #         go.Scatter(
        #             x=list(qf_trend.index),
        #             y=list(qf_trend.values),
        #             mode="lines+markers"
        #         )
        #     )
        #     fig_qf.update_layout(
        #         title="Quick Feedback Trend (Overall Satisfaction)",
        #         height=350
        #     )
        #     qf_trend_graph = dcc.Graph(figure=fig_qf)

        return dcc.Tabs([

            dcc.Tab(label="Dashboard", children=[
                kpi_row,
                dashboard_kpis,
                html.H4("Store Summary"),
                summary_table,
            ]),
            
            dcc.Tab(label="Source-wise Feedback", children=[
                html.H4("Outlet × Source Feedback Count"),
                source_summary_table
            ]),
            
            dcc.Tab(label="Store NPS", children=[
                dash_table.DataTable(
                    data=store_nps_dash.to_dict("records"),
                    columns=[{"name": c, "id": c} for c in store_nps_dash.columns],
                    sort_action="native",
                    filter_action="native",
                    style_header={
                        "backgroundColor": "#0A66C2",
                        "color": "white",
                        "fontWeight": "bold",
                        "textAlign": "center",
                        "fontFamily": "Times New Roman"
                    },
                    style_cell={
                        "fontFamily": "Times New Roman",
                        "textAlign": "left"
                    }
                )
            ]),

            dcc.Tab(label="Complaints", children=[

                html.H4("Negative Feedback", style={"color": "red"}),
                styled_table(
                    neg_df.drop(columns=["Outlet_norm", "Zomato Id"], errors="ignore"),
                    "#B00020"
                ),

                html.Br(),

                html.H4("Neutral Feedback", style={"color": "blue"}),
                styled_table(
                    neu_df.drop(columns=["Outlet_norm", "Zomato Id"], errors="ignore"),
                    "#F9A825"
                ),

                html.Br(),

                html.H4("Positive Feedback", style={"color": "green"}),
                styled_table(
                    pos_df.drop(columns=["Outlet_norm", "Zomato Id"], errors="ignore"),
                    "#2E7D32"
                ),
            ]),

            dcc.Tab(label="Quick Feedback", children=[
                # qf_kpi_row,
                # qf_trend_graph,
                build_qf_tab(df_qf)
            ]),
  
        # ✅ MOVE HERE
            dcc.Tab(label="🚨 Risk Stores", children=[

                html.H4("Top Risk Stores (Auto-Detected)", style={
                    "color": "#B00020",
                    "fontFamily": "Times New Roman"
                }),

                dash_table.DataTable(
                    data=top_risk_df.to_dict("records"),
                    columns=[
                        {"name": "Outlet Name", "id": "Outlet Name"},
                        {"name": "Risk Score", "id": "Risk Score"},
                        {"name": "Insight", "id": "Insight"},
                    ],
                    page_size=5,
                    style_header={
                        "backgroundColor": "#B00020",
                        "color": "white",
                        "fontWeight": "bold",
                        "textAlign": "center",
                        "fontFamily": "Times New Roman"
                    },
                    style_cell={
                        "whiteSpace": "normal",
                        "textAlign": "left",
                        "fontFamily": "Times New Roman"
                    }
                )
            ]),

            # ✅ MOVE HERE
            dcc.Tab(label="Auto Insights", children=[
                dash_table.DataTable(
                    data=store_insight_df.to_dict("records"),
                    columns=[
                        {"name": "Outlet Name", "id": "Outlet Name"},
                        {"name": "Insight", "id": "Insight"},
                    ],
                    page_size=10,
                    style_header={
                        "backgroundColor": "#0A66C2",
                        "color": "white",
                        "fontWeight": "bold",
                        "textAlign": "center"
                    },
                    style_cell={
                        "whiteSpace": "normal",
                        "textAlign": "left",
                        "fontFamily": "Times New Roman"
                    }
                )
            ])
        ])
        
def with_comment(df):
    """
    Keep rows that have a real comment.
    Used ONLY for email message body tables.
    """
    return df[
        df["Comment"]
        .fillna("")
        .astype(str)
        .str.strip()
        .ne("")
    ]
    
# ==========================================================
# DATAFRAME → HTML TABLE (FOR EMAIL)
# ==========================================================
def df_to_html(df, table_type="default"):
    if df is None or df.empty:
        return "<i>No data available</i>"

    # 🔒 ABSOLUTE ISOLATION
    df = df.copy(deep=True)

    # 🔥 KILL INTERNAL COLUMNS (FINAL SAFETY)
    df = df.drop(columns=[c for c in df.columns if c.startswith("_")], errors="ignore")
    
    # ================= SOURCE SUMMARY TOTAL =================
    if table_type == "source_summary":

        numeric_cols = ["Google", "Feedback Form", "Swiggy", "Zomato", "Z-District"]

        # Ensure numeric columns exist
        for c in numeric_cols:
            if c not in df.columns:
                df[c] = 0

        # Row-wise Total
        if "Total" not in df.columns:
            df["Total"] = df[numeric_cols].sum(axis=1)

        # Column-wise TOTAL row
        total_row = {"Outlet Name": "TOTAL"}
        for c in numeric_cols + ["Total"]:
            total_row[c] = int(df[c].sum())

        df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)

    elif table_type == "store_nps":

        # Store NPS table MUST already contain Total
        if "Total" not in df.columns:
            raise RuntimeError(
                f"store_nps table missing Total. Columns: {df.columns.tolist()}"
            )

        # Add TOTAL row
        total_row = {
            "Outlet Name": "TOTAL",
            "Promoters": int(df["Promoters"].sum()),
            "Passives": int(df["Passives"].sum()),
            "Detractors": int(df["Detractors"].sum()),
            "Total": int(df["Total"].sum()),
            "NPS": round(
                ((df["Promoters"].sum() - df["Detractors"].sum())
                / df["Total"].sum() * 100)
                if df["Total"].sum() else 0,
                1
            ),
        }

        df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)

        # ================= HTML =================
    html = df.to_html(index=False, border=1, escape=False)

    # ================= AVG RATING COLOR =================
    def color_rating(match):
        val = float(match.group(1))
        if val >= 4.5:
            color = "#2E7D32"   # Green
        elif val >= 4.0:
            color = "#F9A825"   # Yellow
        else:
            color = "#B00020"   # Red
        return f"><span style='color:{color};font-weight:bold;'>{val}</span><"

    html = re.sub(
        r">(\\d+(?:\\.\\d+)?)<",
        color_rating,
        html
    )

    def color_rating_cell(match):
        val = float(match.group(1))

        if val >= 4.5:
            bg = "#C8E6C9"   # green
        elif val >= 4.0:
            bg = "#FFF9C4"   # yellow
        else:
            bg = "#FFCDD2"   # red

        return (
            f"<td style='background:{bg};"
            f"border:1px solid #444;"
            f"text-align:center;font-weight:bold;'>{val}</td>"
        )

    # Apply ONLY to numeric rating cells
    html = re.sub(
        r"<td[^>]*>(\d+\.\d+)</td>",
        color_rating_cell,
        html
    )
    
    html = html.replace(
        "<table ",
        "<table style='border-collapse:collapse;"
        "font-family:Times New Roman;font-size:12px;width:100%;' "
    )

    html = html.replace(
        "<th>",
        "<th style='background:#0A66C2;color:white;"
        "padding:6px;border:1px solid #444;text-align:center;'>"
    )

    html = html.replace(
        "<td>",
        "<td style='padding:6px;border:1px solid #444;text-align:left;'>"
    )

    html = html.replace(
        "<td>TOTAL</td>",
        "<td style='font-weight:bold;background:#ECEFF1;'>TOTAL</td>"
    )

   # ---------- HEADER (CENTER) ----------
    html = re.sub(
        r"<th>(.*?)</th>",
        r"<th style='padding:6px;border:1px solid #444;"
        r"text-align:center;font-weight:bold;"
        r"background:#0A66C2;color:white;'>\1</th>",
        html
    )

    # ---------- BODY CELLS (LEFT) ----------
    html = html.replace(
        "<td>",
        "<td style='padding:6px;border:1px solid #444;"
        "text-align:left;'>"
    )

    # Center-align numeric cells
    html = re.sub(
        r"<td style='([^']*)'>(\d+(\.\d+)?)</td>",
        r"<td style='\1;text-align:center;'>\2</td>",
        html
    )

    # ================= STORE SUMMARY COLORS =================
    if table_type == "store_summary":
        html = html.replace("'>Positive<",  "' style='background:#2E7D32;'>Positive<")
        html = html.replace("'>Neutral<",   "' style='background:#F9A825;color:black;'>Neutral<")
        html = html.replace("'>Negative<",  "' style='background:#B00020;'>Negative<")
        html = html.replace("'>Total<",     "' style='background:#000000;'>Total<")

    # ================= STORE NPS COLORS =================
    if table_type == "store_nps":
        html = html.replace("'>Promoters<",  "' style='background:#2E7D32;'>Promoters<")
        html = html.replace("'>Passives<",   "' style='background:#F9A825;color:black;'>Passives<")
        html = html.replace("'>Detractors<", "' style='background:#B00020;'>Detractors<")

    # ================= FEEDBACK TABLES =================
    if table_type == "negative":
        html = html.replace("background:#0A66C2", "background:#B00020")
    elif table_type == "neutral":
        html = html.replace("background:#0A66C2", "background:#F9A825;color:black")
    elif table_type == "positive":
        html = html.replace("background:#0A66C2", "background:#2E7D32")
    
    html = html.replace(
        "<td>TOTAL</td>",
        "<td style='font-weight:bold;background:#ECEFF1;'>TOTAL</td>"
    )

    html = html.replace(
        "<table ",
        "<table style='width:100%;max-width:100%;"
        "border-collapse:collapse;font-size:12px;' "
    )
    html = html.replace(
        "font-size:13px",
        "font-size:12px"
    )

    return html

# ==========================================================
# EMAIL HTML BUILDER
# ==========================================================
def build_email_html(
    kpi, store_summary_df, source_summary_df,
    pos_df, neu_df, neg_df, store_insight_df, top_risk_df,
    weekly_cid, monthly_cid, qf_summary_df, qf_cid,
    store_nps_df, date_display, *, avg_rating_df=None
):
    # ================= HARD SAFETY GUARD =================
    import pandas as pd
    if not isinstance(avg_rating_df, pd.DataFrame):
        avg_rating_df = pd.DataFrame()

    return f"""
    <div style="font-family:Times New Roman;padding:20px;">

        <h2 style="color:#0A66C2;">Coffee Island — Feedback Report</h2>
        <p><b>Date:</b> {date_display}</p>
        <p style="font-size:12px; color:#555; margin-top:10px;">
        <b>Note:</b> Customer details were not provided by the aggregator team.
        Please check the respective dashboard and reply directly to the comment.
        </p>
        <!-- ===================== NPS Summary ===================== -->
        <h3 style='color:#0A66C2;'>NPS Summary</h3>
        <div style='border:2px solid #0A66C2; padding:12px; width:50%; border-radius:8px;'>
            <ul>
                <li>Total Responses: {kpi['total']}</li>
                <li>Promoters: {kpi['P']}</li>
                <li>Passives: {kpi['Pa']}</li>
                <li>Detractors: {kpi['D']}</li>
                <li><b>NPS Score: {kpi['NPS']:.1f}</b></li>
            </ul>
        </div>
        
        <h3 style="color:#0A66C2;">⭐ Average Rating (Store × Source)</h3>
        {df_to_html(avg_rating_df) if not avg_rating_df.empty else "<i>No rating data available</i>"}
             
        <h3>Source-wise Feedback Count</h3>
        {df_to_html(source_summary_df, table_type="source_summary")}     

        <h3>Store-wise NPS</h3>
        {df_to_html(store_nps_df, "store_nps")}
         
        {safe_section("Negative Feedback", neg_df, "negative")}
        {safe_section("Neutral Feedback", neu_df, "neutral")}
        {safe_section("Positive Feedback", pos_df, "positive")}
  
        <h3>Quick Feedback Summary</h3>
        {df_to_html(qf_summary_df)}

        <h3>Quick Feedback Trend</h3>
        {f"<img src='cid:{qf_cid}'>" if qf_cid else ""}

        <h3>Store Summary</h3>
        {df_to_html(store_summary_df, "store_summary")}
        
        <h3 style="color:#0A66C2;">Store-wise Auto Insights</h3>
        {df_to_html(store_insight_df)}

        <h3 style="color:#B00020;">🚨 Top Risk Stores (Immediate Attention)</h3>
        <p style="font-size:13px;">
        Automatically identified based on low NPS, detractor volume,
        and negative feedback patterns.
        </p>

        {df_to_html(top_risk_df)}

        <h3>NPS Trends</h3>
        {f"<img src='cid:{weekly_cid}'><br>" if weekly_cid else ""}
        {f"<img src='cid:{monthly_cid}'>" if monthly_cid else ""}

        <br>
        <i>This is an automated Coffee Island report.</i>
    </div>
    """
def filter_rows_with_comment(df):
    """
    Keep only rows having meaningful comments.
    Used ONLY for email tables.
    """
    return df[
        df["Comment"]
        .fillna("")
        .astype(str)
        .str.strip()
        .ne("")
    ] 

def safe_section(title, df, table_type):
    if df is None or df.empty:
        return ""

    df = df.copy()   # keep ALL ratings

    if df.empty:
        return ""

    color_map = {
        "negative": "#B00020",
        "neutral":  "#F9A825",
        "positive": "#2E7D32"
    }
    
    color = color_map.get(table_type, "#0A66C2")

    return f"""
    <h3 style="color:{color};">{title}</h3>
    {df_to_html(df, table_type)}
    """


def register_email_modal_callbacks(app):

    @app.callback(
        Output("send_email_modal", "is_open"),   # 👈 ONLY OWNER
        Input("send_email_btn", "n_clicks"),
        Input("cancel_send_email", "n_clicks"),
        Input("confirm_send_email", "n_clicks"),
        State("send_email_modal", "is_open"),
        prevent_initial_call=True
    )
    def toggle_send_email_modal(send_click, cancel_click, confirm_click, is_open):
        ctx = dash.callback_context
        trigger = ctx.triggered[0]["prop_id"].split(".")[0]

        if trigger == "send_email_btn":
            return True     # open modal

        if trigger in ("cancel_send_email", "confirm_send_email"):
            return False    # close modal

        return is_open

      
# =====================================================================
# PART D — EMAIL CALLBACKS (CLEAN + DASHBOARD-MATCHING)
# =====================================================================

def register_email_callback(app):

    @app.callback(
        Output("feedback_tabs", "children", allow_duplicate=True),
        Output("email_custom_message", "value"),
        Input("confirm_send_email", "n_clicks"),
        # -------- FILTERS --------
        State("f_brand", "value"),
        State("f_region", "value"),
        State("f_state", "value"),
        State("f_city", "value"),
        State("f_type", "value"),
        State("f_outlet", "value"),
        State("f_source", "value"),
        State("date_from", "date"),
        State("date_to", "date"),
        State("email_custom_message", "value"),

        prevent_initial_call=True
    )
    def send_report(
        n_clicks,
        brand,
        region,
        state,
        city,
        type_,
        store,
        source,
        date_from,
        date_to,
        user_message
    ):

        # =========================================================
        # 1️⃣ LOAD MASTER DATA
        # =========================================================
        df_all = get_master_data().copy()

        if df_all is None or df_all.empty:
            return html.Div(
                "No feedback data available.",
                style={"color": "red"}
            )

        required = {"Created Date", "Source", "Outlet Name", "Rating"}
        missing = required - set(df_all.columns)
        if missing:
            raise RuntimeError(f"Missing required columns: {missing}")

        # Load quick feedback (safe)
        # _, df_qf, _ = load_website_feedback()
        df_qf = df_all[df_all["Source"] == "Feedback Form"].copy()

        # 🔒 NEVER allow Total in df_all
        df_all = df_all.drop(columns=["Total"], errors="ignore")

        df_master = df_all.copy() 
        # =========================================================
        # 2️⃣ RESOLVE ALLOWED OUTLETS
        # =========================================================
        allowed = resolve_allowed_outlets(
            brand=brand,
            region=region,
            state=state,
            city=city,
            type_=type_,
            store=store
        )

        if allowed:
            df_all = df_all[
                df_all["Outlet_norm"].isin(allowed) |
                (df_all["Outlet_norm"] == "")  # website / aggregator safe
            ]
        else:
            # 🔥 If no outlets matched filters, DO NOT kill Swiggy
            df_all = df_all[df_all["Source"] == "Swiggy"]

        if df_all.empty:
            return html.Div(
                "No data for selected filters.",
                style={"color": "red"}
            )

        # =========================================================
        # 3️⃣ SOURCE FILTER
        # =========================================================
        if source and source != "ALL":
            df_all = df_all[df_all["Source"] == source]
            

        # =========================================================
        # 4️⃣ DATE FILTER
        # =========================================================
        df_all = apply_date_filter(df_all, date_from, date_to)

        if df_all.empty:
            return html.Div(
                "No data for selected date range.",
                style={"color": "red"}
            )

        date_display = f"{format_ddmmyyyy(date_from)} to {format_ddmmyyyy(date_to)}"
        email_subject = f"Coffee Island – Feedback Report ({date_display})"
        
        df_all = ensure_outlet_column(df_all, context="send_report:post_filter")

        # =========================================================
        # 5️⃣ SENTIMENT + NPS (SINGLE SOURCE OF TRUTH)
        # =========================================================
        df_all = df_all.copy()

        df_all["Sentiment"] = df_all["Rating"].apply(
            lambda r: "Negative" if r in (1, 2)
            else "Neutral" if r == 3
            else "Positive" if r in (4, 5)
            else None
        )

        df_all["NPS_Type"] = df_all["Rating"].apply(rating_to_nps_type)
        df_all["Comment"] = df_all["Comment"].fillna("")
        df_all["Created Date Display"] = df_all["Created Date"].apply(format_ddmmyyyy)

        # =========================================================
        # 6️⃣ COMPLAINTS SUMMARY (WITH CUSTOMER NAME)
        # =========================================================
        complaint_cols = [
            "Outlet Name",
            "Source",
            "Rating",
            "Comment",
            "Created Date",
            "Customer Name",
        ]

        complaints_df = (
            df_all[complaint_cols]
            .sort_values("Created Date", ascending=False)
        )
        # =========================================================
        # 6️⃣ KPI (EMAIL)
        # =========================================================
        P = (df_all["NPS_Type"] == "Promoter").sum()
        Pa = (df_all["NPS_Type"] == "Passive").sum()
        D = (df_all["NPS_Type"] == "Detractor").sum()
        T = P + Pa + D

        kpi = {
            "total": T,
            "P": P,
            "Pa": Pa,
            "D": D,
            "NPS": ((P - D) / T) * 100 if T else 0
        }

        # =========================================================
        # 7️⃣ SENTIMENT TABLES (EMAIL BODY)
        # =========================================================
        def _prep(df):
            cols = [
                "Outlet Name",
                "Source",
                "Rating",
                "Comment",
                "Created Date Display",
                "Customer Name",
            ]

            return (
                df[cols]
                .rename(columns={"Created Date Display": "Created Date"})
                .sort_values("Created Date", ascending=False)
            )

        neg_df = prepare_complaint_table(df_all[df_all["Sentiment"] == "Negative"])
        neu_df = prepare_complaint_table(df_all[df_all["Sentiment"] == "Neutral"])
        pos_df = prepare_complaint_table(df_all[df_all["Sentiment"] == "Positive"])


        # =========================================================
        # 8️⃣ DERIVED TABLES
        # =========================================================
        store_insight_df = build_store_insights(df_all)
        top_risk_df = build_top_risk_stores(df_all)
        store_nps_df = build_store_nps_table(df_all)

        if "Total" not in store_nps_df.columns:
            raise RuntimeError("store_nps_df missing Total column")

        source_summary_df = build_source_outlet_summary(df_all)
        for c in ["Google", "Feedback Form", "Swiggy", "Zomato", "Z-District"]:
            if c not in source_summary_df.columns:
                source_summary_df[c] = 0

        source_summary_df["Total"] = source_summary_df[
            ["Google", "Feedback Form", "Swiggy", "Zomato", "Z-District"]
        ].sum(axis=1)

        # =========================================================
        # 9️⃣ STORE SUMMARY (BASE)
        # =========================================================
        store_summary_base = (
            df_all
            .groupby(["Outlet Name", "Sentiment"], dropna=False)
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )

        for c in ["Positive", "Neutral", "Negative"]:
            if c not in store_summary_base.columns:
                store_summary_base[c] = 0

        store_summary_base["Total"] = (
            store_summary_base["Positive"]
            + store_summary_base["Neutral"]
            + store_summary_base["Negative"]
        )

        # =========================================================
        # 🔟 QUICK FEEDBACK
        # =========================================================
        df2 = df_qf.copy()
        qf_summary_df = compute_qf_summary(df2)

        # =========================================================
        # 🔟 OVERALL SOURCE-WISE AVG RATING (START → TILL DATE)
        # =========================================================
        
        lifetime_avg_html = build_avg_rating_lifetime_email_html(
            df_master,   # UNFILTERED master data
            date_to
        )
    
        # =========================================================
        # 1️⃣1️⃣ CHARTS
        # =========================================================
        images = {}

        weekly = compute_weekly_nps(df_all)
        if weekly is not None and not weekly.empty:
            plt.figure()
            weekly.plot(marker="o", figsize=(6, 3))
            images["weekly"] = fig_to_bytes()

        qf_trend = compute_qf_trend(df2)
        if qf_trend is not None:
            plt.figure()
            qf_trend.plot(marker="o", figsize=(6, 3))
            images["qftrend"] = fig_to_bytes()

        weekly_cid = "weekly" if "weekly" in images else None
        qf_cid = "qftrend" if "qftrend" in images else None

        # =========================================================
        # 1️⃣2️⃣ EXCEL DATA (OPS + AREA MANAGER)
        # =========================================================

        source_count_df = build_source_wise_feedback_count_excel(df_all)
        avg_rating_df = build_avg_rating_email_table(df_all)
        neu_df_excel    = df_all[df_all["Sentiment"] == "Neutral"].copy()
        neg_df_excel    = df_all[df_all["Sentiment"] == "Negative"].copy()
        pos_df_excel    = df_all[df_all["Sentiment"] == "Positive"].copy()

        # ================= MERGE MANAGER EMAILS (SAFE VIEW) =================
        df_with_manager = df_all.merge(
            STORE_MAP_DF[["Outlet_norm", "ManagerEmail"]].drop_duplicates(),
            on="Outlet_norm",
            how="left"
        )

        alert_df = avg_rating_df.copy()

        alert_df["_min_rating"] = (
            alert_df[["Google", "Feedback Form", "Swiggy", "Zomato", "Z-District"]]
            .replace("", np.nan)
            .apply(pd.to_numeric, errors="coerce")
            .min(axis=1)
        )

        alert_df["_rating_flag"] = alert_df["_min_rating"].apply(
            lambda x: "LOW" if pd.notna(x) and x < 4.0
            else "MEDIUM" if pd.notna(x) and x < 4.5
            else "GOOD"
        )

        low_stores_df = alert_df[alert_df["_rating_flag"] == "LOW"]

        if not low_stores_df.empty:

            low_outlets = (
                low_stores_df["Outlet Name"]
                .str.replace(" ⚠️", "", regex=False)
                .unique()
                .tolist()
            )

            df_with_manager = df_all.merge(
                STORE_MAP_DF[["Outlet_norm", "ManagerEmail"]].drop_duplicates(),
                on="Outlet_norm",
                how="left"
            )


            alert_recipients = (
                df_with_manager.loc[
                    df_with_manager["Outlet Name"].isin(low_outlets),
                    "ManagerEmail"
                ]
                .dropna()
                .unique()
                .tolist()
            )

            alert_recipients = list(set(alert_recipients + OPS_TO_EMAILS))

            if alert_recipients:
                send_low_rating_alert_email(
                    recipients=alert_recipients,
                    low_df=low_stores_df,
                    date_display=date_display
                )

        # =========================================================
        # 🔹 CUSTOM MESSAGE FROM POPUP
        # =========================================================
        user_message = (user_message or "").strip()

        custom_message_block = ""
        if user_message:
            custom_message_block = f"""
            <div style="
                border:1px solid #ccc;
                padding:10px;
                margin-bottom:15px;
                background:#f9f9f9;
            ">
                <b>📌 Message from sender:</b><br>
                {user_message}
            </div>
            """
        # =========================================================
        # 1️⃣3️⃣ BUILD EMAIL BODY
        # =========================================================
        base_html = build_email_html(
            kpi,
            store_summary_base,
            source_summary_df,
            pos_df,
            neu_df,
            neg_df,
            store_insight_df,
            top_risk_df,
            weekly_cid,
            None,
            qf_summary_df,
            qf_cid,
            store_nps_df,
            date_display,
            avg_rating_df=avg_rating_df,  # date-range logic
        )

        html_body = f"""
        {custom_message_block}

        <h3 style="color:#0A66C2;">
        Overall Average Rating
        </h3>

        {lifetime_avg_html}
        <br>

        {base_html}
        """


        # =========================================================
        # 1️⃣4️⃣ SEND EMAILS
        # =========================================================
        # 🔒 GUARANTEE Outlet_norm EXISTS
        if "Outlet_norm" not in df_all.columns:
            df_all["Outlet_norm"] = df_all["Outlet Name"].apply(normalize_outlet)
        # Store-wise emails
        send_store_wise_emails(
            df_all,
            store_summary_base,
            store_nps_df,
            neg_df,
            pos_df,
            date_display,
            html_body
        )

        # Area manager emails
        send_area_manager_emails(
            df_all,
            store_summary_base,
            store_nps_df,
            neg_df,
            pos_df,
            date_display,
            html_body
        )

        # OPS email
        ops_excel = build_ops_excel_attachment(
            df_all=df_all,
            store_summary_df=store_summary_base,
            source_count_df=source_count_df,
            avg_rating_df=avg_rating_df,
            store_nps_df=store_nps_df,
            neg_df=neg_df_excel,
            neu_df=neu_df_excel,
            pos_df=pos_df_excel,
            date_display=date_display
        )
        ops_excel.add_header(
            "Content-Disposition",
            f'attachment; filename="OPS_All_Stores_Feedback_{date_display}.xlsx"'
        )
        send_ops_email_html(
            subject=email_subject,
            html_body=html_body,
            images=images,
            ops_excel_part=ops_excel
        )

        # Leadership email
        send_email_html(
            email_subject,
            html_body,
            images
        )

        # =========================================================
        # ✅ FINAL RETURN
        # =========================================================
        return (
            html.Div(
                "Email report sent successfully.",
                style={
                    "color": "green",
                    "fontFamily": "Times New Roman",
                    "padding": "10px",
                    "fontWeight": "bold"
                }
            ),
            ""       # ✅ clear textarea
        )