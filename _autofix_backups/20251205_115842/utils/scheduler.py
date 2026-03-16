import os
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

from .data_loader import load_expiry
from .export_excel import df_to_excel_bytes      # <-- CORRECTED
from .email_sender import send_report_email      # <-- CORRECTED

EXPORT_NAME = "Daily_Expiry_Risk.xlsx"


def run_daily_expiry_job():
    df = load_expiry()
    df = df[df["Expiry Parsed"].notna()]

    # High-risk only
    today = pd.Timestamp.today().normalize()
    df["Days Left"] = (df["Expiry Parsed"] - today).dt.days
    df = df[df["Days Left"] <= 7]

    if df.empty:
        return

    # Export file: Generate Excel bytes
    excel_bytes = df_to_excel_bytes(
        df,
        sheet_name="Expiry Risk",
        # highlight_rules argument removed as it is not supported by df_to_excel_bytes
    )

    # Email file: Send the bytes as an attachment
    send_report_email(
        subject="Daily Expiry Risk Report",
        body="Attached is the daily expiry risk report.",
        to_emails=["manager@coffeeisland.com"], # <-- List of emails required
        attachments=[(
            EXPORT_NAME,
            excel_bytes,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )], # <-- Updated to correct attachment format (bytes)
    )


def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_daily_expiry_job, 'cron', hour=8, minute=0)
    scheduler.start()