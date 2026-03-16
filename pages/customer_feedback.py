import os
import re
import pandas as pd
import smtplib
import logging
import glob
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import io
import base64
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from collections import Counter
import math

# ---------------------------
# Load environment variables
# ---------------------------
load_dotenv(dotenv_path=r"C:\Users\ACER\store_dashboard\pages\Gmail_credentials.env")

sender = os.getenv("EMAIL_SENDER")
app_password = os.getenv("GMAIL_APP_PASSWORD")

# Days range for feedback from .env (default 7 days)
days_range = int(os.getenv("FEEDBACK_DAYS_RANGE", "165"))

# Default manager fallback recipients (comma-separated) for escalations
ESCALATION_RECIPIENTS = [email for email in os.getenv("ESCALATION_RECIPIENTS", "").split(",") if email]
LEADERSHIP_EMAILS = [email for email in os.getenv("LEADERSHIP_TO", "").split(",") if email]

# ---------------------------
# Setup Logging (daily file)
# ---------------------------
today = datetime.now().strftime("%Y-%m-%d")
log_filename = f"feedback_email_{today}.log"

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("=== Daily Feedback Email Run Started ===")

# ---------------------------
# Load feedback data (both sheets)
# ---------------------------
file_path = r"C:\Users\ACER\store_dashboard\SubmissionsExport.xlsx"

df_detailed = pd.read_excel(file_path, sheet_name="Feedback Forms")
df_quick = pd.read_excel(file_path, sheet_name="Quick Feedback Forms")

# ---------------------------
# Apply Date Range Filter (from .env)
# ---------------------------
def apply_date_filter(df, date_col):
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_range)
    return df[(df[date_col] >= start_date) & (df[date_col] <= end_date)]

df_detailed = apply_date_filter(df_detailed, 'Date')
df_quick = apply_date_filter(df_quick, 'Date')

if df_detailed.empty and df_quick.empty:
    logging.info(f"No feedback data found in any sheet for the last {days_range} days. Exiting.")
    raise SystemExit(0)

# ---------------------------
# NPS Calculation (Detailed Feedback)
# ---------------------------
nps_col = "How likely are you to recommend Coffee Island to a friend ?"

def classify_nps(value: str) -> str:
    text = str(value).strip().lower()

    # Exact match lists (must match FULL answer, not substring)
    promoters = [
        "definitely",
        "likely",
        "excellent",
        "good"
    ]

    passives = [
        "maybe",
        "average"
    ]

    detractors = [
        "not likely at all",
        "unlikely",
        "poor",
        "very poor",
        "not at all likely"
    ]

    # exact match only
    if text in promoters:
        return "Promoter"
    if text in passives:
        return "Passive"
    if text in detractors:
        return "Detractor"

    return "Unknown"


if not df_detailed.empty:
    df_detailed["NPS_Type"] = df_detailed[nps_col].apply(classify_nps)
    nps_summary = df_detailed["NPS_Type"].value_counts()
    promoters = nps_summary.get("Promoter", 0)
    passives = nps_summary.get("Passive", 0)
    detractors = nps_summary.get("Detractor", 0)
    total_nps = promoters + passives + detractors

    if total_nps > 0:
        nps_score = ((promoters - detractors) / total_nps) * 100
    else:
        nps_score = 0
else:
    promoters = passives = detractors = total_nps = 0
    nps_score = 0

nps_html = f"""
<b>NPS Summary (for last {days_range} days):</b><br>
Total Responses considered: {total_nps}<br>
Promoters: {promoters}<br>
Passives: {passives}<br>
Detractors: {detractors}<br>
<b>NPS Score:</b> {nps_score:.1f}
<br><br>
"""

# ---------------------------
# Sentiment classification (Positive / Neutral / Negative)
# ---------------------------
def classify_sentiment(row):
    exp = str(row.get('How was your overall experience?', '')).lower()
    food = str(row.get('How was the food quality?', '')).lower()
    amb = str(row.get('How was the ambience?', '')).lower()
    nps = str(row.get('How likely are you to recommend Coffee Island to a friend ?', '')).lower()

    negative_keywords = ["poor", "very poor", "not likely at all", "unlikely"]
    neutral_keywords = ["average", "maybe"]

    # Negative if any field contains negative keywords
    if any(k in exp for k in negative_keywords) or \
       any(k in food for k in negative_keywords) or \
       any(k in amb for k in negative_keywords) or \
       any(k in nps for k in negative_keywords):
        return "Negative"

    # Neutral if any field contains neutral keywords
    if "average" in exp or "average" in food or "average" in amb or "maybe" in nps:
        return "Neutral"

    # Otherwise positive (includes Good/Excellent)
    return "Positive"

if not df_detailed.empty:
    df_detailed['Sentiment'] = df_detailed.apply(classify_sentiment, axis=1)
else:
    df_detailed['Sentiment'] = []

# ---------------------------
# Sentiment sort score (Excellent > Good > Average > Very Poor > Poor)
# ---------------------------
def sentiment_sort_score(row):
    exp = str(row.get('How was your overall experience?', '')).lower()

    if "excellent" in exp:
        return 1
    if "good" in exp:
        return 2
    if "average" in exp:
        return 3
    if "very poor" in exp:
        return 4
    if "poor" in exp:
        return 5

    return 99  # fallback

if not df_detailed.empty:
    df_detailed["sort_score"] = df_detailed.apply(sentiment_sort_score, axis=1)
else:
    df_detailed["sort_score"] = []

# ---------------------------
# Store-wise Summary
# ---------------------------
if not df_detailed.empty and 'Outlet Name' in df_detailed.columns:
    store_summary = df_detailed.groupby(['Outlet Name', 'Sentiment']).size().unstack(fill_value=0)

    for col in ['Positive', 'Neutral', 'Negative']:
        if col not in store_summary.columns:
            store_summary[col] = 0

    store_summary['Total'] = store_summary[['Positive', 'Neutral', 'Negative']].sum(axis=1)
    store_summary = store_summary.sort_values(
        by=["Positive", "Neutral", "Negative"],
        ascending=[False, False, True]
    )
    store_summary = store_summary.reset_index()
else:
    store_summary = pd.DataFrame(columns=['Outlet Name', 'Positive', 'Neutral', 'Negative', 'Total'])

def build_store_summary_html(summary_df):
    if summary_df.empty:
        return "No store-wise feedback available for the selected period.<br><br>"

    html = "<table border='1' style='border-collapse:collapse; text-align:center;'>"
    html += "<tr><th>Store</th><th>Total</th><th>Positive</th><th>Neutral</th><th>Negative</th></tr>"

    for _, row in summary_df.iterrows():
        html += "<tr>"
        html += f"<td>{row['Outlet Name']}</td>"
        html += f"<td>{row['Total']}</td>"
        html += f"<td style='color:green;'>{row['Positive']}</td>"
        html += f"<td style='color:blue;'>{row['Neutral']}</td>"
        html += f"<td style='color:red;'>{row['Negative']}</td>"
        html += "</tr>"

    html += "</table><br><br>"
    return html

store_summary_html = build_store_summary_html(store_summary)

# ---------------------------
# All Positive / Neutral / Negative Feedback (Detailed) with sorting
# ---------------------------
if not df_detailed.empty:
    positive_feedback = df_detailed[df_detailed['Sentiment'] == 'Positive'] \
        .sort_values(["sort_score", "Date"], ascending=[True, False])

    neutral_feedback = df_detailed[df_detailed['Sentiment'] == 'Neutral'] \
        .sort_values(["sort_score", "Date"], ascending=[True, False]) 
    
    negative_feedback = df_detailed[df_detailed['Sentiment'] == 'Negative'] \
        .sort_values(["sort_score", "Date"], ascending=[True, False])
else:
    positive_feedback = neutral_feedback = negative_feedback = pd.DataFrame(
        columns=['Full Name', 'Outlet Name', 'Tell us more', 'Date']
    )

# ---------------------------
# Color-coded HTML tables for detailed feedback
# ---------------------------
def build_html_table(df_subset, row_bg_color):
    if df_subset.empty:
        return "<b>No records found.</b><br><br>"

    html = "<table border='1' style='border-collapse:collapse;'>"
    html += "<tr><th>Customer</th><th>Store</th><th>Comment</th><th>Date</th></tr>"

    for _, row in df_subset.iterrows():
        html += f"<tr style='background-color:{row_bg_color};'>"
        html += f"<td>{row.get('Full Name', '')}</td>"
        html += f"<td>{row.get('Outlet Name', '')}</td>"
        html += f"<td>{row.get('Tell us more', '')}</td>"
        html += f"<td>{row.get('Date', '')}</td>"
        html += "</tr>"

    html += "</table><br><br>"
    return html

positives_html = build_html_table(positive_feedback, "#ccffcc")  # green
neutral_html = build_html_table(neutral_feedback, "#cce0ff")     # blue
negatives_html = build_html_table(negative_feedback, "#ffcccc")  # red

# ---------------------------
# Sentiment per store (stacked bar chart)
# ---------------------------
if not df_detailed.empty:
    sentiment_counts = df_detailed.groupby(['Outlet Name', 'Sentiment']).size().unstack(fill_value=0)

    for col in ['Positive', 'Neutral', 'Negative']:
        if col not in sentiment_counts.columns:
            sentiment_counts[col] = 0

    sentiment_counts = sentiment_counts[['Positive', 'Neutral', 'Negative']]

    plt.figure(figsize=(8, 5))
    sentiment_counts.plot(kind='bar', stacked=True)
    plt.title("Feedback Sentiment per Store")
    plt.ylabel("Number of Feedbacks")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    chart_base64 = base64.b64encode(buf.read()).decode('utf-8')
    chart_html = f'<img src="data:image/png;base64,{chart_base64}" alt="Feedback Chart"/>'
    plt.close()
else:
    chart_html = "No sentiment chart available (no detailed feedback in this period).<br><br>"

# ---------------------------
# Daily Sentiment Trend (Positive / Neutral / Negative)
# ---------------------------
if not df_detailed.empty:
    df_detailed['created_date'] = df_detailed['Date'].dt.date

    trend_counts = df_detailed.groupby(['created_date', 'Sentiment']).size().unstack(fill_value=0)

    for col in ['Positive', 'Neutral', 'Negative']:
        if col not in trend_counts.columns:
            trend_counts[col] = 0

    trend_counts = trend_counts[['Positive', 'Neutral', 'Negative']]

    plt.figure(figsize=(8, 5))
    trend_counts.plot(kind='line', marker='o')
    plt.title("Daily Feedback Sentiment Trend")
    plt.xlabel("Date")
    plt.ylabel("Number of Feedbacks")
    plt.xticks(rotation=45)
    plt.tight_layout()

    buf_trend = io.BytesIO()
    plt.savefig(buf_trend, format='png')
    buf_trend.seek(0)
    trend_chart_base64 = base64.b64encode(buf_trend.read()).decode('utf-8')
    trend_chart_html = f'<img src="data:image/png;base64,{trend_chart_base64}" alt="Trend Chart"/>'
    plt.close()
else:
    trend_chart_html = "No sentiment trend chart available (no detailed feedback in this period).<br><br>"

# ---------------------------
# Quick Feedback Processing
# ---------------------------
quick_rating_cols = [
    "Rate satisfaction with the overall price paid ?",
    "Rate Taste of Food ?",
    "Cafe Cleanliness",
    "Did you smell the coffee aroma ?",
    "Staff friendliness",
    "Order accuracy",
    "Speed of service",
    "Overall satisfaction"
]

rating_map = {
    "excellent": 5,
    "good": 4,
    "average": 3,
    "poor": 2,
    "very poor": 1
}

def convert_to_numeric_rating(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s.isdigit():
        return int(s)
    lower = s.lower()
    for k, v in rating_map.items():
        if k in lower:
            return v
    return None

if not df_quick.empty:
    for col in quick_rating_cols:
        if col in df_quick.columns:
            df_quick[col + "_score"] = df_quick[col].apply(convert_to_numeric_rating)

    avg_dict = {}
    for col in quick_rating_cols:
        score_col = col + "_score"
        if score_col in df_quick.columns:
            avg = df_quick[score_col].dropna().mean()
            avg_dict[col] = avg

    total_quick_responses = len(df_quick)

    if avg_dict:
        quick_html = "<b>Quick Feedback Summary (for last " + str(days_range) + " days):</b><br>"
        quick_html += f"Total Quick Feedback Responses: {total_quick_responses}<br><br>"
        quick_html += "<table border='1' style='border-collapse:collapse; text-align:center;'>"
        quick_html += "<tr><th>Metric</th><th>Average Score (1-5)</th></tr>"
        for metric, avg in avg_dict.items():
            display_metric = metric.replace(" ?", "").strip()
            avg_text = f"{avg:.2f}" if pd.notna(avg) else "N/A"
            quick_html += f"<tr><td>{display_metric}</td><td>{avg_text}</td></tr>"
        quick_html += "</table><br><br>"
    else:
        quick_html = "No valid quick feedback metrics available for this period.<br><br>"

    if "Overall satisfaction_score" in df_quick.columns:
        df_quick['created_date'] = df_quick['Date'].dt.date
        trend_quick = df_quick.groupby('created_date')["Overall satisfaction_score"].mean()

        if not trend_quick.empty:
            plt.figure(figsize=(8, 5))
            trend_quick.plot(kind='line', marker='o')
            plt.title("Daily Quick Feedback – Overall Satisfaction Trend")
            plt.xlabel("Date")
            plt.ylabel("Average Overall Satisfaction (1-5)")
            plt.xticks(rotation=45)
            plt.ylim(1, 5)
            plt.tight_layout()

            buf_qtrend = io.BytesIO()
            plt.savefig(buf_qtrend, format='png')
            buf_qtrend.seek(0)
            qtrend_chart_base64 = base64.b64encode(buf_qtrend.read()).decode('utf-8')
            quick_trend_chart_html = f'<img src="data:image/png;base64,{qtrend_chart_base64}" alt="Quick Feedback Trend Chart"/>'
            plt.close()
        else:
            quick_trend_chart_html = "No quick feedback trend data available.<br><br>"
    else:
        quick_trend_chart_html = "No overall satisfaction quick feedback available.<br><br>"

else:
    avg_dict = {}
    total_quick_responses = 0
    quick_html = f"No quick feedback records found for last {days_range} days.<br><br>"
    quick_trend_chart_html = ""

# ---------------------------
# NPS per Store (new feature)
# ---------------------------
if not df_detailed.empty:

    # Calculate NPS types per store
    nps_store = df_detailed.groupby(['Outlet Name', 'NPS_Type']).size().unstack(fill_value=0)

    # Ensure all columns exist
    for col in ['Promoter', 'Passive', 'Detractor']:
        if col not in nps_store.columns:
            nps_store[col] = 0

    # Calculate totals and NPS score store-wise
    nps_store['Total'] = nps_store[['Promoter', 'Passive', 'Detractor']].sum(axis=1)
    # avoid division by zero by replacing 0 with NaN temporarily
    nps_store['NPS_Score'] = ((nps_store['Promoter'] - nps_store['Detractor']) / 
                              nps_store['Total'].replace(0, pd.NA)) * 100
    # replace NaN with 0 for empty stores
    nps_store['NPS_Score'] = nps_store['NPS_Score'].fillna(0)
    nps_store = nps_store.reset_index()

else:
    nps_store = pd.DataFrame(columns=['Outlet Name','Promoter','Passive','Detractor','Total','NPS_Score'])

# ---------------------------
# Advanced NPS helpers (categories, insights, recommendations, charts)
# ---------------------------
def nps_category(score):
    """Returns NPS category label."""
    try:
        s = float(score)
    except Exception:
        return "Unknown"
    if s >= 70:
        return "🌟 Excellent"
    elif s >= 50:
        return "✅ Good"
    elif s >= 0:
        return "⚠️ Needs Improvement"
    else:
        return "❗ Critical"


def generate_store_insight(row):
    score = row.get("NPS_Score", 0)
    promoters = int(row.get("Promoter", 0))
    detractors = int(row.get("Detractor", 0))
    total = int(row.get("Total", 0))

    if total == 0:
        return "No feedback available."

    if score >= 70:
        return "Customers love this store. High promoter count indicates consistent experience."
    elif score >= 50:
        return "Good performance. Minor improvements can increase promoters."
    elif score >= 0:
        return "Mixed feedback. Increase staff consistency and improve service time."
    else:
        return "Urgent issues detected. High detractors suggest service or product concerns."


def recommended_actions(score):
    try:
        s = float(score)
    except Exception:
        return "No action"

    if s >= 70:
        return "Maintain high service levels. Encourage more reviews."
    elif s >= 50:
        return "Identify opportunities to upgrade ambience or interaction quality."
    elif s >= 0:
        return "Train staff on service standards; review peak-hour performance."
    else:
        return "Immediate intervention required: audit operations, quality, and cleanliness."


def build_store_nps_html_advanced(df):
    if df.empty:
        return "<b>No NPS data store-wise.</b><br><br>"
    df = df.sort_values("NPS_Score", ascending=False).reset_index(drop=True)
    df['Rank'] = df.index + 1

    html = """
    <b style="font-size:16px;">🏆 Store-wise NPS Performance (Advanced Analytics)</b><br>
    <table border='1' style='border-collapse:collapse; text-align:center; width:100%;'>
        <tr style='background-color:#f2f2f2;'>
            <th>Rank</th>
            <th>Store</th>
            <th>NPS</th>
            <th>Category</th>
            <th>Promoters</th>
            <th>Passives</th>
            <th>Detractors</th>
            <th>Insights</th>
            <th>Recommended Action</th>
        </tr>
    """

    for _, row in df.iterrows():
        score = row["NPS_Score"]
        cat = nps_category(score)
        insight = generate_store_insight(row)
        action = recommended_actions(score)

        html += f"""
        <tr>
            <td><b>{row['Rank']}</b></td>
            <td>{row['Outlet Name']}</td>
            <td><b>{score:.1f}</b></td>
            <td>{cat}</td>
            <td style='color:green;'>{int(row['Promoter'])}</td>
            <td style='color:blue;'>{int(row['Passive'])}</td>
            <td style='color:red;'>{int(row['Detractor'])}</td>
            <td>{insight}</td>
            <td><i>{action}</i></td>
        </tr>
        """

    html += "</table><br><br>"
    return html


def build_nps_bar_chart(df):
    if df.empty:
        return ""
    df_sorted = df.sort_values("NPS_Score", ascending=False)

    plt.figure(figsize=(10, 4))
    plt.bar(df_sorted['Outlet Name'], df_sorted['NPS_Score'])
    plt.xticks(rotation=45, ha='right')
    plt.title("Store-wise NPS Ranking")
    plt.ylabel("NPS Score")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    img_b64 = base64.b64encode(buf.read()).decode('utf-8')
    plt.close()

    return f'<img src="data:image/png;base64,{img_b64}" alt="Store-wise NPS Chart"/>'

# ---------------------------
# Complaint Summary (Negative Sentiment Only) + Top Keywords (new)
# ---------------------------
if not df_detailed.empty:
    complaints_df = df_detailed[df_detailed['Sentiment'] == 'Negative'].copy()
    # Use 'Tell us more' and fallback columns to collect text
    text_cols = ['Tell us more', 'write here .....', 'Please select', 'Tell us more.']  # try several variants
    # combine several fields if present
    def collect_text(row):
        parts = []
        for c in text_cols:
            if c in row and pd.notna(row[c]):
                parts.append(str(row[c]))
        if not parts and 'Tell us more' in row and pd.notna(row['Tell us more']):
            parts.append(str(row['Tell us more']))
        return " ".join(parts).strip()

    complaints_df['complaint_text'] = complaints_df.apply(collect_text, axis=1)

    # Complaints per store
    complaint_store_summary = complaints_df.groupby("Outlet Name").size().reset_index(name="Complaints")

    # Build complaints HTML
    def build_complaint_html(df):
        if df.empty:
            return "<b>No complaints recorded in this period.</b><br><br>"

        html = "<b>Complaint Summary (Negative Feedback):</b><br>"
        html += "<table border='1' style='border-collapse:collapse; text-align:center;'>"
        html += "<tr><th>Store</th><th>Complaint Count</th></tr>"

        for _, row in df.iterrows():
            html += "<tr>"
            html += f"<td>{row['Outlet Name']}</td>"
            html += f"<td style='color:red;'><b>{row['Complaints']}</b></td>"
            html += "</tr>"

        html += "</table><br><br>"
        return html

    complaint_html = build_complaint_html(complaint_store_summary)

    # ---------------------------
    # Top complaint keywords (simple extraction)
    # ---------------------------
    STOPWORDS = set([
        "the","and","is","it","to","a","i","of","was","for","not","that","with","this","but","be","we","are","have",
        "on","so","too","very","in","my","they","you","would","had","have","at","as","our","your","me","its","or"
    ])

    def extract_keywords(texts, top_n=20):
        all_text = " ".join([t for t in texts if isinstance(t, str) and t.strip()])
        cleaned = re.sub(r'[^a-zA-Z0-9\s]', ' ', all_text.lower())
        tokens = [t for t in cleaned.split() if len(t) > 2 and t not in STOPWORDS and not t.isdigit()]
        counts = Counter(tokens)
        return counts.most_common(top_n)

    complaint_texts = complaints_df['complaint_text'].fillna("").tolist()
    top_keywords = extract_keywords(complaint_texts, top_n=20)

    if top_keywords:
        keyword_html = "<b>Top Complaint Keywords:</b><br>"
        keyword_html += "<table border='1' style='border-collapse:collapse; text-align:center;'>"
        keyword_html += "<tr><th>Keyword</th><th>Count</th></tr>"
        for k, v in top_keywords:
            keyword_html += f"<tr><td>{k}</td><td>{v}</td></tr>"
        keyword_html += "</table><br><br>"
    else:
        keyword_html = "<b>No complaint keywords found.</b><br><br>"

else:
    complaint_html = "<b>No complaints available.</b><br><br>"
    keyword_html = "<b>No complaint keywords available.</b><br><br>"
    complaints_df = pd.DataFrame(columns=['Outlet Name','complaint_text'])

# ---------------------------
# Complaint heatmap per store (new)
# ---------------------------
def build_complaint_heatmap(df):
    if df.empty or not top_keywords:
        return "No complaint heatmap available.<br><br>"

    keywords = [k for k, _ in top_keywords[:10]]  # top 10 keywords
    stores = df['Outlet Name'].unique().tolist()
    data = []
    for s in stores:
        texts = " ".join(df[df['Outlet Name'] == s]['complaint_text'].fillna("").tolist()).lower()
        row_counts = [texts.count(k) for k in keywords]
        data.append(row_counts)

    mat = pd.DataFrame(data, index=stores, columns=keywords)

    plt.figure(figsize=(max(6, len(keywords)*0.8), max(4, len(stores)*0.3)))
    plt.imshow(mat.values, aspect='auto')
    plt.colorbar(label='Count')
    plt.yticks(range(len(stores)), stores)
    plt.xticks(range(len(keywords)), keywords, rotation=45, ha='right')
    plt.title("Complaint Heatmap by Store (top keywords)")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    img_b64 = base64.b64encode(buf.read()).decode('utf-8')
    plt.close()
    return f'<img src="data:image/png;base64,{img_b64}" alt="Complaint Heatmap"/>'

if not df_detailed.empty:
    complaint_heatmap_html = build_complaint_heatmap(complaints_df)
else:
    complaint_heatmap_html = "No complaint heatmap available.<br><br>"

# ---------------------------
# NPS Trend (weekly & monthly) (new)
# ---------------------------
def compute_nps_from_counts(df_counts):
    df = df_counts.copy()

    for col in ['Promoter', 'Passive', 'Detractor']:
        if col not in df.columns:
            df[col] = 0

    df['Total'] = df[['Promoter', 'Passive', 'Detractor']].sum(axis=1)
    df['NPS'] = ((df['Promoter'] - df['Detractor']) /
                 df['Total'].replace(0, pd.NA)) * 100

    df['NPS'] = df['NPS'].fillna(0).infer_objects(copy=False).astype(float)

    return df['NPS']

if not df_detailed.empty:
    df_detailed['created_date'] = df_detailed['Date'].dt.date
    df_detailed['week_start'] = df_detailed['Date'].dt.to_period('W').apply(lambda r: r.start_time.date())
    df_detailed['month'] = df_detailed['Date'].dt.to_period('M').apply(lambda r: r.start_time.date())

    weekly_counts = df_detailed.groupby(['week_start','NPS_Type']).size().unstack(fill_value=0)
    weekly_nps = compute_nps_from_counts(weekly_counts)

    monthly_counts = df_detailed.groupby(['month','NPS_Type']).size().unstack(fill_value=0)
    monthly_nps = compute_nps_from_counts(monthly_counts)

    # Weekly trend message (latest vs previous)
    nps_trend_msg = "Not enough data"
    try:
        if len(weekly_nps) >= 2:
            latest_week = float(weekly_nps.iloc[-1])
            prev_week = float(weekly_nps.iloc[-2])
            diff = latest_week - prev_week
            trend_arrow = "⬆️" if diff > 0 else ("⬇️" if diff < 0 else "➡️")
            nps_trend_msg = f"{latest_week:.1f} ({trend_arrow} {diff:+.1f})"
    except Exception:
        nps_trend_msg = "Not enough data"

    # Create weekly chart
    if not weekly_nps.empty:
        plt.figure(figsize=(8,4))
        weekly_nps.plot(marker='o')
        plt.title("Weekly NPS Trend")
        plt.xlabel("Week Start")
        plt.ylabel("NPS")
        plt.xticks(rotation=45)
        plt.tight_layout()
        buf_w = io.BytesIO()
        plt.savefig(buf_w, format='png')
        buf_w.seek(0)
        weekly_chart_b64 = base64.b64encode(buf_w.read()).decode('utf-8')
        weekly_chart_html = f'<img src="data:image/png;base64,{weekly_chart_b64}" alt="Weekly NPS Trend"/>'
        plt.close()
    else:
        weekly_chart_html = "No weekly NPS data available.<br><br>"

    # create monthly chart
    if not monthly_nps.empty:
        plt.figure(figsize=(8,4))
        monthly_nps.plot(marker='o')
        plt.title("Monthly NPS Trend")
        plt.xlabel("Month")
        plt.ylabel("NPS")
        plt.xticks(rotation=45)
        plt.tight_layout()
        buf_m = io.BytesIO()
        plt.savefig(buf_m, format='png')
        buf_m.seek(0)
        monthly_chart_b64 = base64.b64encode(buf_m.read()).decode('utf-8')
        monthly_chart_html = f'<img src="data:image/png;base64,{monthly_chart_b64}" alt="Monthly NPS Trend"/>'
        plt.close()
    else:
        monthly_chart_html = "No monthly NPS data available.<br><br>"
else:
    weekly_chart_html = monthly_chart_html = "No NPS trend data available.<br><br>"
    nps_trend_msg = "No data"

# ---------------------------
# Manager notification when store NPS < 0 (new)
# ---------------------------
def slugify_store_name(name: str):
    return re.sub(r'[^A-Z0-9]', '_', str(name).upper())

def send_manager_notification(store_row):
    store = store_row['Outlet Name']
    nps_val = float(store_row['NPS_Score'])
    slug = slugify_store_name(store)
    manager_env_key = f"MANAGER_{slug}"
    manager_email = os.getenv(manager_env_key, "")
    recipients = [r for r in manager_email.split(",") if r]
    if not recipients:
        recipients = ESCALATION_RECIPIENTS or LEADERSHIP_EMAILS or [sender]

    subject = f"URGENT: Store NPS Alert - {store} (NPS {nps_val:.1f})"
    body = f"""
    Hi Team,<br><br>
    The NPS for <b>{store}</b> is <b>{nps_val:.1f}</b> which is below 0. Please investigate immediately.<br><br>
    Promoters: {int(store_row.get('Promoter',0))}<br>
    Detractors: {int(store_row.get('Detractor',0))}<br>
    Total Responses: {int(store_row.get('Total',0))}<br><br>
    Recent negative comments (up to 5):<br>
    <ul>
    """
    recent_comments = complaints_df[complaints_df['Outlet Name'] == store].sort_values('Date', ascending=False)
    for _, r in recent_comments.head(5).iterrows():
        txt = re.sub(r'[\r\n]+', ' ', str(r.get('complaint_text',''))).strip()
        if txt:
            body += f"<li>{txt}</li>"

    body += "</ul><br>Regards,<br>Automation Script"

    try:
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender, app_password)
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        logging.info(f"Sent manager alert for {store} to {recipients}")
    except Exception as e:
        logging.error(f"Failed to send manager alert for {store}: {str(e)}")

# Collect list of stores with NPS < 0
stores_below_zero = []
for _, row in nps_store.iterrows():
    if row.get('NPS_Score', 0) < 0:
        stores_below_zero.append(row)
# send notifications
for r in stores_below_zero:
    try:
        send_manager_notification(r)
    except Exception as e:
        logging.error(f"Error while notifying manager for store {r.get('Outlet Name')}: {e}")

# ---------------------------
# Build advanced NPS visuals to include in email
# ---------------------------
nps_bar_chart_html = build_nps_bar_chart(nps_store) if not nps_store.empty else ""
store_nps_html_advanced = build_store_nps_html_advanced(nps_store) if not nps_store.empty else ""

# ---------------------------
# PDF Summary Generation (unchanged except we add some new lines info)
# ---------------------------
pdf_filename = f"Feedback_Summary_{datetime.now().strftime('%Y-%m-%d')}.pdf"

def generate_summary_pdf(filename, nps_score, promoters, passives, detractors,
                         total_nps, days_range, avg_dict, total_quick_responses, store_summary):
    c = canvas.Canvas(filename, pagesize=A4)
    width, height = A4

    y = height - 50
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, y, "Customer Feedback Summary - Coffee Island")
    y -= 30

    c.setFont("Helvetica", 11)
    c.drawString(50, y, f"Period: Last {days_range} days")
    y -= 20

    # NPS
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y, "NPS Summary (Detailed Feedback):")
    y -= 20

    c.setFont("Helvetica", 11)
    c.drawString(60, y, f"Total Responses: {total_nps}")
    y -= 15
    c.drawString(60, y, f"Promoters: {promoters}")
    y -= 15
    c.drawString(60, y, f"Passives: {passives}")
    y -= 15
    c.drawString(60, y, f"Detractors: {detractors}")
    y -= 15
    c.drawString(60, y, f"NPS Score: {nps_score:.1f}")
    y -= 25

    # Sentiment totals
    if not store_summary.empty:
        c.setFont("Helvetica-Bold", 12)
        c.drawString(50, y, "Overall Sentiment (Detailed Feedback):")
        y -= 20
        c.setFont("Helvetica", 11)
        c.drawString(60, y, f"Positive: {store_summary['Positive'].sum()}")
        y -= 15
        c.drawString(60, y, f"Neutral: {store_summary['Neutral'].sum()}")
        y -= 15
        c.drawString(60, y, f"Negative: {store_summary['Negative'].sum()}")
        y -= 25

    # Quick feedback
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y, "Quick Feedback Summary:")
    y -= 20

    c.setFont("Helvetica", 11)
    c.drawString(60, y, f"Total Quick Feedback Responses: {total_quick_responses}")
    y -= 20

    if avg_dict:
        c.setFont("Helvetica-Bold", 11)
        c.drawString(60, y, "Average Ratings (1-5):")
        y -= 15
        c.setFont("Helvetica", 11)
        for metric, avg in avg_dict.items():
            if y < 80:
                c.showPage()
                y = height - 50
                c.setFont("Helvetica", 11)
            display_metric = metric.replace(" ?", "").strip()
            avg_text = f"{avg:.2f}" if pd.notna(avg) else "N/A"
            c.drawString(70, y, f"{display_metric}: {avg_text}")
            y -= 15
    else:
        c.drawString(60, y, "No quick feedback rating data available.")
        y -= 15

    # Note about complaint keywords
    y -= 20
    c.setFont("Helvetica-Bold", 11)
    c.drawString(50, y, "Top Complaint Keywords (from negative feedback):")
    y -= 15
    c.setFont("Helvetica", 10)
    for k, v in top_keywords[:10]:
        if y < 80:
            c.showPage()
            y = height - 50
            c.setFont("Helvetica", 10)
        c.drawString(60, y, f"{k}: {v}")
        y -= 12

    y -= 20
    c.setFont("Helvetica-Bold", 11)
    c.drawString(50, y, "Note: Detailed comments and charts are available in the email body.")
    c.showPage()
    c.save()

generate_summary_pdf(
    pdf_filename,
    nps_score,
    promoters,
    passives,
    detractors,
    total_nps,
    days_range,
    avg_dict,
    total_quick_responses,
    store_summary
)

# ---------------------------
# Email sending function (update email body to include new parts)
# ---------------------------
def send_group_email(group_name, subject_suffix):
    try:
        to_recipients = os.getenv(f"{group_name}_TO", "")
        cc_recipients = os.getenv(f"{group_name}_CC", "")
        bcc_recipients = os.getenv(f"{group_name}_BCC", "")

        to_recipients = [email for email in to_recipients.split(",") if email]
        cc_recipients = [email for email in cc_recipients.split(",") if email]
        bcc_recipients = [email for email in bcc_recipients.split(",") if email]

        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = ", ".join(to_recipients)
        msg['Cc'] = ", ".join(cc_recipients)
        msg['Subject'] = f"Customer Feedback Report – Coffee Island ({subject_suffix})"

        body = f"""
        Hi Team,<br><br>
        Here are the latest customer feedback highlights for the last <b>{days_range}</b> days.<br><br>

        {nps_html}

        <b>Store-wise Summary (Detailed Feedback):</b><br>
        {store_summary_html}<br><br>

        <b>Store-wise NPS Score (Advanced):</b><br>
        {store_nps_html_advanced}<br>

        <b>Weekly NPS Movement:</b> {nps_trend_msg}<br><br>

        <b>NPS Ranking Chart:</b><br>
        {nps_bar_chart_html}<br><br>

        <b>Complaint Summary:</b><br>
        {complaint_html}<br>

        {keyword_html}

        <b>Complaint Heatmap (top keywords):</b><br>
        {complaint_heatmap_html}<br><br>

        <b>All Positive Feedback (Green):</b><br>
        {positives_html}
        
        <b>All Neutral Feedback (Blue):</b><br>
        {neutral_html}

        <b>All Negative Feedback (Red):</b><br>
        {negatives_html}

        <b>Feedback Sentiment per Store (Stacked Bar):</b><br>
        {chart_html}<br><br>

        <b>Daily Feedback Sentiment Trend:</b><br>
        {trend_chart_html}<br><br>

        <hr>
        <b>Quick Feedback Summary (from 'Quick Feedback Forms'):</b><br><br>
        {quick_html}

        <b>Quick Feedback – Daily Overall Satisfaction Trend:</b><br>
        {quick_trend_chart_html}<br><br>

        <b>Weekly NPS Trend:</b><br>
        {weekly_chart_html}<br><br>

        <b>Monthly NPS Trend:</b><br>
        {monthly_chart_html}<br><br>

        Regards,<br>
        Vikash
        """
        msg.attach(MIMEText(body, 'html'))

        if os.path.exists(pdf_filename):
            with open(pdf_filename, "rb") as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(pdf_filename)}"')
            msg.attach(part)

        all_recipients = to_recipients + cc_recipients + bcc_recipients

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender, app_password)
        server.sendmail(sender, all_recipients, msg.as_string())
        server.quit()

        logging.info(f"Email sent successfully to {group_name} group: {all_recipients}")

    except Exception as e:
        logging.error(f"Failed to send email to {group_name} group: {str(e)}")
        raise

# ---------------------------
# Error notification function
# ---------------------------
def notify_error(errors, log_file):
    try:
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = sender
        msg['Subject'] = "Feedback Email Automation Error"

        body = f"""
        Hi Vikash,<br><br>
        The daily feedback email run encountered errors:<br><br>
        <ul>
        {''.join([f'<li>{err}</li>' for err in errors])}
        </ul><br>
        Please see the attached log file for full details.<br><br>
        Regards,<br>
        Automation Script
        """
        msg.attach(MIMEText(body, 'html'))

        if os.path.exists(log_file):
            with open(log_file, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(log_file)}')
            msg.attach(part)

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender, app_password)
        server.sendmail(sender, [sender], msg.as_string())
        server.quit()

        logging.info("Error notification email with log file sent to self.")

    except Exception as e:
        logging.error(f"Failed to send error notification: {str(e)}")

# ---------------------------
# Cleanup old logs
# ---------------------------
def cleanup_old_logs(log_dir=".", days_to_keep=7):
    cutoff_date = datetime.now() - timedelta(days=days_to_keep)
    for log_file in glob.glob(os.path.join(log_dir, "feedback_email_*.log")):
        try:
            date_str = os.path.basename(log_file).replace("feedback_email_", "").replace(".log", "")
            log_date = datetime.strptime(date_str, "%Y-%m-%d")
            if log_date < cutoff_date:
                os.remove(log_file)
                logging.info(f"Deleted old log file: {log_file}")
        except Exception as e:
            logging.warning(f"Could not parse log file date for {log_file}: {str(e)}")

# ---------------------------
# Main run
# ---------------------------
errors = []

try:
    send_group_email("LEADERSHIP", "Leadership Team")
except Exception as e:
    errors.append(f"Leadership group failed: {str(e)}")

try:
    send_group_email("OPS", "Operations Team")
except Exception as e:
    errors.append(f"Ops group failed: {str(e)}")

if errors:
    logging.error("=== Daily Feedback Email Run Completed WITH ERRORS ===")
    for err in errors:
        logging.error(err)
    notify_error(errors, log_filename)
else:
    logging.info("=== Daily Feedback Email Run Completed SUCCESSFULLY ===")

cleanup_old_logs(log_dir=".", days_to_keep=7)
