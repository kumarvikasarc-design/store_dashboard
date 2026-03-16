import os
import pandas as pd
import re

INPUT_DIR  = r"C:\Users\ACER\store_dashboard\district"
OUTPUT_DIR = r"C:\Users\ACER\store_dashboard\feedback\zomato_district"
os.makedirs(OUTPUT_DIR, exist_ok=True)

output_name = input("📄 Enter output CSV file name (without .csv): ").strip()
output_csv = os.path.join(OUTPUT_DIR, f"{output_name}.csv")

# ❌ Text to ignore completely
IGNORE_TEXT = {
    "view review details",
    "verified visit",
    "1 comment",
    "1 comments"
}

rows = []

for file in os.listdir(INPUT_DIR):

    # 🚫 Skip temp / hidden files
    if file.startswith("~$"):
        continue

    if not file.lower().endswith((".xlsx", ".xls", ".csv")):
        continue

    path = os.path.join(INPUT_DIR, file)
    print(f"🔄 Processing: {file}")

    # ✅ Correct reader
    if file.lower().endswith(".csv"):
        df = pd.read_csv(path, header=None)
    else:
        df = pd.read_excel(path, header=None)

    # Clean values
    values = []
    for v in df.iloc[:, 0].dropna():
        text = str(v).strip()
        text_lower = text.lower()

        # Skip junk rows
        if (
            not text
            or text_lower in IGNORE_TEXT
            or re.match(r"\d+\s*comment[s]?", text_lower)
        ):
            continue

        values.append(text)

    review = {}

    for v in values:

        # 📅 Date
        if re.search(r"\d{4}-\d{2}-\d{2}", v):
            review["Created Date"] = v

        # ⭐ Rating
        elif v.isdigit() and v in {"1", "2", "3", "4", "5"}:
            review["Rating"] = int(v)

        # 👤 Customer name (first text)
        elif "Customer Name" not in review:
            review["Customer Name"] = v

        # 💬 Comment
        else:
            review["Comment"] = v

        # ✅ Save only complete reviews
        if {"Created Date", "Customer Name", "Rating", "Comment"} <= review.keys():
            review["Outlet Name"] = os.path.splitext(file)[0]
            rows.append(review)
            review = {}

# ===============================
# EXPORT
# ===============================
df_final = pd.DataFrame(rows)

if df_final.empty:
    print("❌ No valid reviews extracted")
else:
    df_final.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"\n✅ CLEAN CSV EXPORTED SUCCESSFULLY:\n{output_csv}")
