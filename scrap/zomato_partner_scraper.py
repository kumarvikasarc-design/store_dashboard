import time
import logging
import pandas as pd
import re
from typing import List, Optional
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

from zomato_cookie_store import load_cookies
from review_export import export_reviews


# ---------------------------
# Helpers
# ---------------------------

def parse_rating(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    m = re.search(r"(\d+(\.\d+)?)", text)
    return float(m.group(1)) if m else None


def parse_relative_date(text: Optional[str]):
    if not text:
        return None

    text = text.lower().strip()
    today = datetime.today().date()

    if "today" in text:
        return today
    if "yesterday" in text:
        return today - timedelta(days=1)

    m = re.search(r"(\d+)\s+day", text)
    if m:
        return today - timedelta(days=int(m.group(1)))

    return None


# ---------------------------
# Selenium Driver
# ---------------------------

def create_driver():
    opts = Options()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--start-maximized")

    driver = webdriver.Chrome(options=opts)
    return driver


# ---------------------------
# Main Scraper
# ---------------------------

def scrape_partner_reviews_selenium(
    res_ids: List[int],
    stores_db_path: str,
    max_scrolls: int = 20
) -> pd.DataFrame:

    cookies = load_cookies()
    if not cookies:
        raise RuntimeError("❌ Cookies missing or expired. Login again.")

    driver = create_driver()
    records = []

    try:
        # Open base page
        driver.get("https://www.zomato.com/partners/onlineordering/reviews/")
        time.sleep(5)

        # Inject cookies
        for k, v in cookies.items():
            driver.add_cookie({
                "name": k,
                "value": v,
                "domain": ".zomato.com",
                "path": "/"
            })

        driver.refresh()
        time.sleep(5)

        for res_id in res_ids:
            logging.info(f"🏪 Scraping resId={res_id}")
            driver.get(
                f"https://www.zomato.com/partners/onlineordering/reviews/?resId={res_id}"
            )
            time.sleep(5)

            # Scroll to load reviews
            for _ in range(max_scrolls):
                driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);"
                )
                time.sleep(1.5)

            review_cards = driver.find_elements(
                By.CSS_SELECTOR,
                "div.css-1p7qahj"
            )

            logging.info(f"Found {len(review_cards)} review cards")

            for card in review_cards:
                try:
                    name = card.find_element(By.CSS_SELECTOR, "div.css-e2fpqw").text
                except Exception:
                    name = None

                try:
                    rating = parse_rating(
                        card.find_element(By.CSS_SELECTOR, "span.css-1vnryd").text
                    )
                except Exception:
                    rating = None

                try:
                    comment = card.find_element(By.CSS_SELECTOR, "div.css-1lx0hjg").text
                except Exception:
                    comment = None

                try:
                    meta = card.find_element(By.CSS_SELECTOR, "div.css-1vxa13o").text
                    parts = [p.strip() for p in meta.split("•")]
                    order_id = parts[0].replace("Order ID:", "").strip()
                    review_date = parse_relative_date(parts[1]) if len(parts) > 1 else None
                except Exception:
                    order_id, review_date = None, None

                records.append({
                    "Zomato_Id": str(res_id),
                    "Reviewer_Name": name,
                    "Rating": rating,
                    "Comments": comment,
                    "Order_ID": order_id,
                    "Review_Date": review_date
                })

    finally:
        driver.quit()

    if not records:
        return pd.DataFrame()

    df_reviews = pd.DataFrame(records)

    # ---------------------------
    # Merge with Active Stores
    # ---------------------------

    df_stores = pd.read_csv(stores_db_path)
    df_stores["Zomato Id"] = df_stores["Zomato Id"].astype(str).str.strip()
    df_stores["Status"] = df_stores["Status"].astype(str).str.lower().str.strip()

    df_active = df_stores[df_stores["Status"] == "active"]

    df_final = df_reviews.merge(
        df_active[["Zomato Id", "Outlet Name"]],
        left_on="Zomato_Id",
        right_on="Zomato Id",
        how="inner"
    ).drop(columns="Zomato Id")

    df_final = df_final.drop_duplicates(
        subset=["Zomato_Id", "Order_ID", "Reviewer_Name"],
        keep="first"
    )

    return df_final[[
        "Outlet Name",
        "Reviewer_Name",
        "Rating",
        "Comments",
        "Order_ID",
        "Review_Date",
        "Zomato_Id"
    ]]


# ---------------------------
# Runner
# ---------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    STORE_DB = r"C:\Users\ACER\store_dashboard\stores_db.csv"
    RES_IDS = [123456, 654321]   # <-- replace with real Zomato IDs

    df = scrape_partner_reviews_selenium(
        res_ids=RES_IDS,
        stores_db_path=STORE_DB
    )

    if df.empty:
        print("⚠️ No reviews scraped")
    else:
        export_reviews(df.to_dict("records"), file_format="csv")
