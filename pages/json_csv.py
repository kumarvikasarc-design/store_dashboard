import os
import json
import pandas as pd
from datetime import datetime
import re

# ✅ Convert ISO date → DD-MM-YYYY
def format_date(date_str):
    if not date_str:
        return ""
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", ""))
        return dt.strftime("%d-%m-%Y")
    except:
        return ""

# ✅ Staff names (expand anytime)
STAFF_NAMES = ["Rohit", "Bhakti", "Yuvraj", "Rewa", "Anuj"]

# ✅ Words that should NEVER be treated as names
STOPWORDS = {
    "Any","But","Spanish Bombon","Amanora","Amazing","Great","Coffee Island","Definitely",
    "Disgusting","Do","Extremely","He","Instagram","No","Our","RS",
    "Spanish","Bombon","Thank","The","They","This","Told","We","Went","Your",
    "Can","It","Ordered","Considering","However","Thank","Thanks","Very",
    "Food","Service","Place","Ambience","Staff","Manager","Owner","Bill",
    "Taste","Quality","Quantity","Experience","Restaurant","Cafe","Shop",
    "Freddo Cappuccino","You","wasn","Google","Original","Tanslated","chai",
    "Really","The Korean","Its","Unreasonable","Visited","Good","Refreshing",
    "Also Great","Loved","People","She","Stop","Delayed",
}

# ✅ Extract staff names ONLY from comment

def extract_staff(comment):
    
    text = comment.strip()
    found = set()

    # ✅ Step 1: Manual staff names
    for name in STAFF_NAMES:
        if name.lower() in text.lower():
            found.add(name)

    # ✅ Step 2: Auto-detect capitalized names (single or two-word)
    auto_names = re.findall(r"\b[A-Z][a-z]{2,}(?:\s[A-Z][a-z]{2,})?\b", text)

    for n in auto_names:
        if n not in STAFF_NAMES and n not in STOPWORDS:
            found.add(n)

    return ", ".join(sorted(found)) if found else ""

# ✅ Root folder containing MANY folders, each with its own data.json
root_folder = r"C:\Users\ACER\store_dashboard\google_review_json"

# ✅ Output folder
output_folder = r"C:\Users\ACER\store_dashboard\g_reviews"
os.makedirs(output_folder, exist_ok=True)

all_rows = []

# ✅ Walk through each folder inside google_review_json
for folder in os.listdir(root_folder):

    folder_path = os.path.join(root_folder, folder)

    if not os.path.isdir(folder_path):
        continue

    # ✅ Store ID = folder name
    store_id = folder

    # ✅ Read business details from data.json inside this folder
    business_file = os.path.join(folder_path, "data.json")

    Business_name = folder  # fallback

    if os.path.exists(business_file):
        try:
            with open(business_file, "r", encoding="utf-8") as bf:
                business_data = json.load(bf)

            Business_name = business_data.get("title", folder)

        except json.JSONDecodeError:
            print(f"❌ Error reading data.json in: {folder_path}")

    # ✅ Loop through review JSON files inside this folder
    for file in os.listdir(folder_path):
        if file.endswith(".json") and file != "data.json":
            file_path = os.path.join(folder_path, file)

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError:
                print(f"❌ JSON error in file: {file_path}")
                continue

            reviews = data.get("reviews", [])

            for r in reviews:
                comment = r.get("comment", "")
                reply_obj = r.get("reviewReply", {})
                reply = reply_obj.get("comment", "")

                # ✅ Extract reply date
                reply_date = reply_obj.get("updateTime") or reply_obj.get("createTime") or ""
                reply_date = format_date(reply_date)

                review_id = r.get("name", "")

                all_rows.append({
                    "Store ID": store_id,
                    "Business Name": Business_name,
                    "Customer Name": r.get("reviewer", {}).get("displayName"),
                    "Rating": r.get("starRating"),
                    "Comment": comment,
                    "Reply": reply,
                    "Reply Date": reply_date,
                    "Staff Mentioned": extract_staff(comment),
                    "Created Date": format_date(r.get("createTime")),
                    "Updated Date": format_date(r.get("updateTime")),
                    "Review ID": review_id,
                    "Source File": file
                })

# ✅ Convert to DataFrame
df = pd.DataFrame(all_rows)

# ✅ Remove duplicate reviews
df.drop_duplicates(subset=["Review ID"], inplace=True)

# ✅ Delete all old CSV files before creating new one
for old_file in os.listdir(output_folder):
    if old_file.endswith(".csv"):
        try:
            os.remove(os.path.join(output_folder, old_file))
        except:
            pass

# ✅ Create new CSV
timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M")
output_csv = os.path.join(output_folder, f"google_reviews_{timestamp}.csv")

df.to_csv(output_csv, index=False, encoding="utf-8")

print("✅ New CSV created (old files removed):", output_csv)