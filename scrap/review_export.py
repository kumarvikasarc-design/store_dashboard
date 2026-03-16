# review_export.py
import os
import pandas as pd
from datetime import datetime

EXPORT_DIR = r"C:\Users\ACER\store_dashboard\exports\zomato_reviews"


def export_reviews(reviews, file_format="csv"):
    """
    reviews     : list of dicts
    file_format : csv | xlsx | json
    """

    # ✅ HARD STOP ONLY WITH MESSAGE — NO EXCEPTION
    if not reviews:
        print("⚠️ No reviews found — export skipped")
        return None

    os.makedirs(EXPORT_DIR, exist_ok=True)

    df = pd.DataFrame(reviews)

    if df.empty:
        print("⚠️ Reviews DataFrame empty — export skipped")
        return None

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(
        EXPORT_DIR,
        f"zomato_reviews_{ts}.{file_format}"
    )

    if file_format == "csv":
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
    elif file_format == "xlsx":
        df.to_excel(file_path, index=False)
    elif file_format == "json":
        df.to_json(file_path, orient="records", force_ascii=False, indent=2)
    else:
        raise ValueError("Invalid export format")

    print(f"✅ Reviews exported → {file_path}")
    return file_path
