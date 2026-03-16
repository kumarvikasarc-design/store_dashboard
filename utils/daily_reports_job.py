# daily_reports_job.py
#
# Standalone job: generates & emails COGS + Expiry + Item Risk daily
# Run via Windows Task Scheduler at 08:00


# utils/daily_reports_job.py

import time
from datetime import datetime

def run_now():
    print("=== DAILY JOB STARTED ===")
    print("Time:", datetime.now())

    # simulate work
    time.sleep(2)

    print("Daily reports job finished successfully.")
