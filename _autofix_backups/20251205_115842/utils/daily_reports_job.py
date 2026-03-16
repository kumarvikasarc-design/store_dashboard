# daily_reports_job.py
#
# Standalone job: generates & emails COGS + Expiry + Item Risk daily
# Run via Windows Task Scheduler at 08:00

from pages.expiry_cogs_page import _filter_by_date_range, _build_master_rm, _build_item_risk_df
from .export_excel import df_to_excel_bytes
from .export_pdf import df_to_pdf_bytes
from .email_sender import send_report_email # <-- CORRECTED import name


def run_daily_job():
    # No date or outlet filter → full period
    filtered = _filter_by_date_range(None, None)
    master_rm = _build_master_rm(filtered)
    variance_cogs = filtered["variance_cogs"]

    expiry_df = master_rm
    cogs_df = variance_cogs
    risk_df = _build_item_risk_df(filtered, master_rm)

    cogs_xlsx = df_to_excel_bytes(cogs_df, sheet_name="COGS", money_cols=["COGS_Amount"])
    expiry_xlsx = df_to_excel_bytes(expiry_df, sheet_name="Expiry", money_cols=["Expiry Amount", "COGS_Amount"])
    risk_xlsx = df_to_excel_bytes(risk_df, sheet_name="ItemRisk", money_cols=["Net Sale"])

    cogs_pdf = df_to_pdf_bytes(cogs_df, title="COGS Report")
    expiry_pdf = df_to_pdf_bytes(expiry_df, title="Expiry Report")
    risk_pdf = df_to_pdf_bytes(risk_df, title="Item Risk Report")

    attachments = [
        ("cogs.xlsx", cogs_xlsx, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        ("expiry.xlsx", expiry_xlsx, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        ("item_risk.xlsx", risk_xlsx, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        ("cogs.pdf", cogs_pdf, "application/pdf"),
        ("expiry.pdf", expiry_pdf, "application/pdf"),
        ("item_risk.pdf", risk_pdf, "application/pdf"),
    ]

    to_emails = ["you@example.com"]  # change

    send_report_email(
        subject="Coffee Island – Daily COGS & Expiry Reports",
        body="Automated 8AM report.",
        to_emails=to_emails,
        attachments=attachments,
    )


if __name__ == "__main__":
    run_daily_job()