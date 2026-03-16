import time
import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup

# Launch Chrome
driver = webdriver.Chrome()

base_url = "https://www.zomato.com/ncr/coffee-island-sushant-lok-gurgaon/reviews?page={}&sort=dd&filter=reviews-dining"

dates = []
reviewer_names = []
ratings = []
comments = []

# Loop through multiple pages
for page in range(1, 11):  # adjust range for number of pages you want
    url = base_url.format(page)
    driver.get(url)
    time.sleep(3)  # wait for page to load

    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Find all review blocks
    review_blocks = soup.find_all("div", {"class": "sc-1q7bklc-10"})
    for idx, block in enumerate(review_blocks):
        
        # Reviewer name
        name_tag = block.find_previous("p", {"class": "sc-1hez2tp-0 sc-lenlpJ dCAQIv"})
        reviewer_names.append(name_tag.get_text(strip=True) if name_tag else None)

        # Rating
        rating_tag = block.find("div", {"class": "sc-1q7bklc-1"})
        ratings.append(rating_tag.get_text(strip=True) if rating_tag else None)

        # Date
        date_tag = block.find_previous("p", {"class": "sc-1hez2tp-0 fKvqMN time-stamp"})
        dates.append(date_tag.get_text(strip=True) if date_tag else None)

        # Comments
        comment_tag = block.find_next("p", {"class": "sc-1hez2tp-0 sc-hfLElm hreYiP"})
        comments.append(comment_tag.get_text(strip=True) if comment_tag else None)

driver.quit()

# Build DataFrame
df = pd.DataFrame({
    "Date": dates,
    "Reviewer_name": reviewer_names,
    "Rating": ratings,
    "Comments": comments
})

# Save to CSV
output_path = r"C:\Users\ACER\store_dashboard\scrap\zomato_reviews.csv"
df.to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"Saved {len(df)} reviews to {output_path}")