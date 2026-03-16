import requests
import pandas as pd
from datetime import datetime
import os

# ==============================
# CONFIGURATION
# ==============================
API_KEY = os.getenv("AIzaSyCwnf6IzcjWjJE45_ROBlISTfsECoxgxbQ")
#API_KEY = "AIzaSyCwnf6IzcjWjJE45_ROBlISTfsECoxgxbQ"
PLACE_ID = "10866130483095140789"

OUTPUT_CSV   = "google_reviews.csv"
OUTPUT_EXCEL = "google_reviews.xlsx"

# ==============================
# FETCH GOOGLE REVIEWS
# ==============================
def fetch_google_reviews(api_key, place_id):
    url = (
        "https://maps.googleapis.com/maps/api/place/details/json"
        f"?place_id={place_id}"
        "&fields=name,rating,reviews"
        f"&key={api_key}"
    )

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "result" not in data or "reviews" not in data["result"]:
        return []

    reviews = []
    for r in data["result"]["reviews"]:
        reviews.append({
            "Date": datetime.fromtimestamp(r.get("time")).strftime("%Y-%m-%d"),
            "Source": "Google",
            "Outlet": data["result"].get("name", ""),
            "Rating": r.get("rating"),
            "Review": r.get("text"),
            "Customer": r.get("author_name"),
            "Language": r.get("language"),
            "Review_Time_UTC": r.get("time"),
            "Imported_At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    return reviews

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    print("Fetching Google reviews...")

    reviews_data = fetch_google_reviews(API_KEY, PLACE_ID)

    if not reviews_data:
        print("No reviews found.")
        exit()

    df = pd.DataFrame(reviews_data)

    # Save files
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    df.to_excel(OUTPUT_EXCEL, index=False)

    print(f"Saved {len(df)} reviews")
    print(f"CSV   → {OUTPUT_CSV}")
    print(f"Excel → {OUTPUT_EXCEL}")
