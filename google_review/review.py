import time
import os
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException

# ------------- CONFIG -------------

OUTPUT_FOLDER = r"C:\Users\ACER\store_dashboard\feedback\google"
OUTPUT_CSV = "coffee_island_reviews.csv"

# Use your existing Chrome profile (already logged in)
CHROME_USER_DATA_DIR = r"C:\Users\ACER\AppData\Local\Google\Chrome\User Data"
CHROME_PROFILE_DIR = "Default"  # change if you use another profile

# Fill these with your actual Google Maps URLs for each store
LOCATIONS = [
    {
        "store_code": "CI-AMANORA",
        "business_name": "Coffee Island | AMANORA",
        "maps_url": "https://www.google.com/maps/place/..."  # TODO: replace
    },
    {
        "store_code": "CI-KURLA",
        "business_name": "Coffee Island | Phoenix Marketcity | Kurla",
        "maps_url": "https://www.google.com/maps/place/..."  # TODO: replace
    },
    # Add more locations here
]

SCROLL_PAUSE = 1.0   # seconds between scrolls
MAX_SCROLLS = 100     # safety limit per location


# ------------- BROWSER SETUP -------------

def get_driver():
    options = webdriver.ChromeOptions()

    # Use your existing Chrome profile
    options.add_argument(f"--user-data-dir={CHROME_USER_DATA_DIR}")
    options.add_argument(f"--profile-directory={CHROME_PROFILE_DIR}")

    # Stability flags (required for Chrome 115+)
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--start-maximized")

    # ✅ Prevent Chrome crash when using a real profile
    options.add_argument("--disable-features=OptimizationGuideModelDownloading")

    # ✅ Force ChromeDriver to match installed Chrome
    service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=options)
    return driver


# ------------- HELPERS -------------

def open_reviews_panel(driver):
    """Click the 'All reviews' button on the Google Maps place page."""
    time.sleep(3)  # wait for page to load basic UI

    try:
        # Try direct 'All reviews' button
        buttons = driver.find_elements(By.XPATH, "//button[contains(., 'reviews')]")
        for btn in buttons:
            if "review" in btn.text.lower():
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(3)
                return
    except NoSuchElementException:
        pass

    # Fallback: try the 'Reviews' tab if present
    try:
        reviews_tab = driver.find_element(By.XPATH, "//button[@data-tab-id='reviews']")
        driver.execute_script("arguments[0].click();", reviews_tab)
        time.sleep(3)
    except NoSuchElementException:
        print("⚠ Could not find 'All reviews' or 'Reviews' tab.")
        return


def scroll_reviews(driver):
    """Scroll the reviews panel to load more reviews."""
    time.sleep(2)

    try:
        scrollable_div = driver.find_element(By.XPATH, "//div[@role='region']")
    except NoSuchElementException:
        # try fallback: main scroll if dialog not detected
        scrollable_div = driver.find_element(By.TAG_NAME, "body")

    last_height = driver.execute_script("return arguments[0].scrollHeight;", scrollable_div)

    for _ in range(MAX_SCROLLS):
        driver.execute_script("arguments[0].scrollTo(0, arguments[0].scrollHeight);", scrollable_div)
        time.sleep(SCROLL_PAUSE)
        new_height = driver.execute_script("return arguments[0].scrollHeight;", scrollable_div)
        if new_height == last_height:
            break
        last_height = new_height


def parse_reviews(driver, store_code, business_name):
    """
    Parse all visible reviews from the current page.
    Selectors are based on current Google Maps structure and may need adjustment
    if Google changes the UI.
    """
    reviews_data = []

    # Reviews are usually in a container with role='article'
    review_cards = driver.find_elements(By.XPATH, "//div[@role='article']")

    for card in review_cards:
        try:
            # Reviewer name
            try:
                reviewer_name = card.find_element(By.XPATH, ".//div[contains(@class, 'd4r55')]").text
            except NoSuchElementException:
                reviewer_name = None

            # Rating (aria-label like "5 stars")
            try:
                star_el = card.find_element(By.XPATH, ".//span[contains(@aria-label, 'star')]")
                rating_text = star_el.get_attribute("aria-label")  # e.g. "5 stars"
                rating = rating_text.split(" ")[0]
            except NoSuchElementException:
                rating = None

            # Review date
            try:
                date_el = card.find_element(By.XPATH, ".//span[contains(@class, 'rsqaWe')]")
                review_date = date_el.text
            except NoSuchElementException:
                review_date = None

            # Review text
            try:
                comment_el = card.find_element(By.XPATH, ".//span[@class='wiI7pd']")
                comment = comment_el.text
            except NoSuchElementException:
                # sometimes long reviews use a different span
                try:
                    comment_el = card.find_element(By.XPATH, ".//span[contains(@class, 'wiI7pd')]")
                    comment = comment_el.text
                except NoSuchElementException:
                    comment = None

            # Reply text (if owner replied)
            try:
                reply_el = card.find_element(By.XPATH, ".//span[contains(., 'Response from the owner')]/following::span[1]")
                reply_text = reply_el.text
            except NoSuchElementException:
                reply_text = None

            reviews_data.append({
                "store_code": store_code,
                "Business Name": business_name,
                "Reviewer Name": reviewer_name,
                "Rating": rating,
                "Comments": comment,
                "Reply": reply_text,
                "Review Date": review_date,
            })
        except Exception as e:
            print(f"⚠ Error parsing one review card: {e}")
            continue

    return reviews_data


# ------------- MAIN -------------

def main():
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    driver = get_driver()

    all_reviews = []

    try:
        for loc in LOCATIONS:
            store_code = loc["store_code"]
            business_name = loc["business_name"]
            url = loc["maps_url"]

            print(f"🔹 Processing: {store_code} - {business_name}")
            driver.get(url)
            time.sleep(5)

            open_reviews_panel(driver)
            scroll_reviews(driver)

            loc_reviews = parse_reviews(driver, store_code, business_name)
            print(f"   → Found {len(loc_reviews)} reviews")
            all_reviews.extend(loc_reviews)

        # Export combined CSV
        df = pd.DataFrame(all_reviews)
        output_path = os.path.join(OUTPUT_FOLDER, OUTPUT_CSV)
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"\n✅ Export completed successfully!")
        print(f"Saved at: {output_path}")

    finally:
        print("Closing browser...")
        driver.quit()


if __name__ == "__main__":
    main()