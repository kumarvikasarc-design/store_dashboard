import os
import re
import mailbox
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

input_folder = r"C:\Users\ACER\store_dashboard\zomato_district_review"
output_folder = r"C:\Users\ACER\store_dashboard\feedback\zomato_district"

# Ensure output folder exists
os.makedirs(output_folder, exist_ok=True)

# Outlet name conversion mapping
outlet_mapping = {
    "Coffee Island, Mundhwa": "Coffee Island - Amanora Mall",
    "Coffee Island, Pune, NIBM Road": "Coffee Island - Tribeca Pune",
    "Coffee Island, NIBM Road": "Coffee Island - Tribeca Pune",
    "Coffee Island, Kurla": "Coffee Island Phoenix Market City - Mumbai",
    "Coffee Island, Churchgate": "Coffee Island Eros Churchgate - Mumbai",
    "Coffee Island Beyond, Greater Kailash 2 (GK2)": "Coffee Island Beyond GK II",
    "Coffee Island, Netaji Subhash Place": "Coffee Island NSP - New Delhi",
    "Coffee Island, Sushant Lok": "Coffee Island HQ-27",
    "Coffee Island, Sector 65": "Coffee Island AIPL",
    "Coffee Island Beyond, Ghatkopar West": "Coffee Island Beyond R City",
}

# Loop all .mbox files in input folder
for file in os.listdir(input_folder):
    if not file.lower().endswith(".mbox"):
        continue

    mbox_path = os.path.join(input_folder, file)
    mbox = mailbox.mbox(mbox_path)

    rows = []

    for msg in mbox:
        subject = msg.get("Subject", "")
        subject = subject.replace("\r", "").replace("\n", " ").strip()

        # Only process review mails
        if "New Review" not in subject:
            continue

        # Extract Outlet + Customer
        try:
            part = subject.split("New Review for", 1)[1].strip()
            original_outlet, customer = part.split(" by ", 1)
            original_outlet = original_outlet.strip(" ,")
            customer = customer.strip()
        except:
            original_outlet = ""
            customer = ""
            
        # Extract HTML body
        body_html = ""
        if msg.is_multipart():
            for part_msg in msg.walk():
                if part_msg.get_content_type() == "text/html":
                    try:
                        body_html = part_msg.get_payload(decode=True).decode(errors="ignore")
                    except:
                        pass
                    break
        else:
            try:
                body_html = msg.get_payload(decode=True).decode(errors="ignore")
            except:
                body_html = ""

        soup = BeautifulSoup(body_html, "html.parser")
        text = soup.get_text(" ", strip=True)

        # Extract Rating (only first number)
        rating_match = re.search(r"Rating[:\s]+(\d)\s*/\s*5", text)
        rating = rating_match.group(1) if rating_match else ""

    # MULTI-PATTERN COMMENT EXTRACTION
        comment = ""

        # Pattern 1: Outlet: Comment Rating
        pattern1 = re.escape(original_outlet) + r"[:]\s*(.*?)\s*Rating"
        m1 = re.search(pattern1, text, flags=re.IGNORECASE | re.DOTALL)

        # Pattern 2: Outlet Comment Rating (no colon)
        pattern2 = re.escape(original_outlet) + r"\s+(.*?)\s*Rating"
        m2 = re.search(pattern2, text, flags=re.IGNORECASE | re.DOTALL)

        # Pattern 3: Anything before Rating
        pattern3 = r"Hello!.*?\:\s*(.*?)\s*Rating"
        m3 = re.search(pattern3, text, flags=re.IGNORECASE | re.DOTALL)

        if m1:
            comment = m1.group(1).strip()
        elif m2:
            comment = m2.group(1).strip()
        elif m3:
            comment = m3.group(1).strip()
        else:
            comment = ""

        # Apply outlet mapping AFTER extracting comment
        outlet = outlet_mapping.get(original_outlet, original_outlet)

        # Convert date
        date_raw = msg.get("Date", "")
        try:
            dt = datetime.strptime(date_raw[:25], "%a, %d %b %Y %H:%M:%S")
            date_clean = dt.strftime("%d-%b-%Y")
        except:
            date_clean = date_raw
            
        rows.append({
            "Customer": customer,
            "Date": date_clean,
            "Rating": rating,
            "Comments": comment,
            "Outlet Name": outlet   
        })

    # Final DataFrame
    df = pd.DataFrame(rows)

    # Output file name based on input file
    output_csv = os.path.join(
        output_folder,
        os.path.splitext(file)[0] + "_cleaned.csv"
    )

    # Save CSV
    df.to_csv(output_csv, index=False, encoding="utf-8")

print("Processing complete! CSV files saved in:", output_folder)
