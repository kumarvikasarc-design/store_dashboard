import os
import pandas as pd
import urllib
from sqlalchemy import create_engine
from datetime import datetime
import hashlib
from sqlalchemy import text
import warnings
from sqlalchemy.exc import SAWarning
warnings.filterwarnings("ignore", category=SAWarning)

# =========================================
# DATABASE CONNECTION
# =========================================

SQL_CONN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=coffee_island_analytics;"
    "Trusted_Connection=yes;"
)

params = urllib.parse.quote_plus(SQL_CONN)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True
)

# =========================================
# BASE PATH
# =========================================

BASE_PATH = r"c:\Users\ACER\store_dashboard\feedback"

# =========================================
# COMMON CLEAN FUNCTION
# =========================================
def safe_read_csv(path):
    for enc in ["utf-8", "cp1252", "latin1"]:
        try:
            return pd.read_csv(path, dtype=str, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("All encodings failed")

def clean_common(df, source):
    df = df.copy()

    df["source"] = source
    df["outlet_norm"] = df["outlet_name"].astype(str).str.lower().str.strip()

    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    df["created_date"] = (
    pd.to_datetime(df["created_date"], errors="coerce", dayfirst=True)
    )

    df["updated_date"] = (
        pd.to_datetime(df.get("updated_date"), errors="coerce")
    )

    df["category"] = df["rating"].apply(
        lambda x: "Positive" if x >= 4
        else "Neutral" if x == 3
        else "Negative" if x <= 2
        else None
    )

    # ---- Ensure optional columns exist ----
    if "review_id" not in df.columns:
        df["review_id"] = ""

    if "zomato_order_id" not in df.columns:
        df["zomato_order_id"] = ""
        
    # ---- Generate Hash ----
    df["review_hash"] = (
        df["source"].fillna("") +
        df["review_id"].fillna("") +
        df["zomato_order_id"].fillna("") +
        df["outlet_norm"].fillna("") +
        df["created_date"].astype(str).fillna("") +
        df["comment"].fillna("")
    ).apply(lambda x: hashlib.sha256(x.encode("utf-8")).hexdigest())

 
    return df

def convert_qf(series):
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .map({
            "excellent": 5,
            "good": 4,
            "average": 3,
            "poor": 2,
            "very poor": 1,
        })
    )

# =========================================
# GOOGLE IMPORT
# =========================================

def import_google():
    path = os.path.join(BASE_PATH, "google")
    frames = []
    
    for file in os.listdir(path):
        if file.endswith(".csv"):
            df = safe_read_csv(os.path.join(path, file))

            # 🔥 Normalize column names
            df.columns = df.columns.str.strip()
            
            # ✅ Fix rating conversion BEFORE dataframe creation
            df["Rating"] = (
                df["Rating"]
                .replace({
                    "FIVE": 5,
                    "FOUR": 4,
                    "THREE": 3,
                    "TWO": 2,
                    "ONE": 1
                })
                .infer_objects(copy=False)
            )

            df2 = pd.DataFrame({
                "created_date": df["Created Date"],
                "updated_date": df["Updated Date"],
                "outlet_name": df["Business Name"],
                "rating": df["Rating"],
                "comment": df["Comment"],
                "reply": df["Reply"],
                "customer_name": df["Customer Name"],
                "review_id": df["Review ID"],
                "raw_source_file": file
            })

            frames.append(clean_common(df2, "Google"))

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# =========================================
# SWIGGY IMPORT
# =========================================

def import_swiggy():
    path = os.path.join(BASE_PATH, "swiggy")
    frames = []
    
    for file in os.listdir(path):
        if file.endswith(".csv"):
            df = safe_read_csv(os.path.join(path, file))
            
            # 🔥 Normalize column names
            df.columns = df.columns.str.strip()
            
            df2 = pd.DataFrame({
                "created_date": df["Created Date"],
                "outlet_name": df["Outlet Name"],
                "rating": df["Rating"],
                "comment": df["Comment"],
                "customer_name": df["Customer Name"],
                "restaurant_id": df.get("Restaurant ID"),
                "raw_source_file": file
            })

            frames.append(clean_common(df2, "Swiggy"))

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# =========================================
# ZOMATO IMPORT
# =========================================

def import_zomato():
    path = os.path.join(BASE_PATH, "zomato")
    frames = []

    for file in os.listdir(path):
        if file.endswith(".csv"):
            df = safe_read_csv(os.path.join(path, file))

            # 🔥 Normalize column names
            df.columns = df.columns.str.strip()

            df2 = pd.DataFrame({
                "created_date": df.get("Order Placed At"),
                "outlet_name": df.get("Restaurant name"),
                "rating": df.get("Rating"),
                "comment": df.get("Review"),
                "zomato_order_id": df.get("Order ID"),
                "restaurant_id": df.get("Restaurant ID"),  # Safe
                "raw_source_file": file
            })

            frames.append(clean_common(df2, "Zomato"))

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# =========================================
# Z-DISTRICT IMPORT
# =========================================

def import_zdistrict():
    path = os.path.join(BASE_PATH, "zomato_district")
    frames = []
    
    for file in os.listdir(path):
        if file.endswith(".csv"):
            df = safe_read_csv(os.path.join(path, file))

            # 🔥 Normalize column names
            df.columns = df.columns.str.strip()
            
            df2 = pd.DataFrame({
                "created_date": df["Created Date"],
                "outlet_name": df["Outlet Name"],
                "rating": df["Rating"],
                "comment": df["Comment"],
                "customer_name": df["Customer Name"],
                "raw_source_file": file
            })

            frames.append(clean_common(df2, "Z-District"))

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# =========================================
# WEBSITE IMPORT
# =========================================

def import_website():
    path = os.path.join(BASE_PATH, "website")
    frames = []
    quick_frames = []
    
    rating_map = {
        "Excellent": 5,
        "Definitely": 5,
        "very good" : 5,
        "Good": 4,
        "Likely": 4,
        "Maybe": 3,
        "Average": 3,
        "Poor": 2,
        "Very poor": 1,
        "Very Poor": 1,
        "Unlikely": 2,
        "Not Likely At All": 1
    }

    for file in os.listdir(path):
        if file.endswith(".xlsx"):

            excel_path = os.path.join(path, file)
            xl = pd.ExcelFile(excel_path)

            for sheet in xl.sheet_names:

                df = pd.read_excel(xl, sheet_name=sheet, dtype=str)
                df.columns = df.columns.str.strip()

                # ---------------------------------------
                # Detect Rating Column Automatically
                # ---------------------------------------
                if "Overall satisfaction" in df.columns:
                    rating_col = "Overall satisfaction"

                elif "How likely are you to recommend Coffee Island to a friend ?" in df.columns:
                    rating_col = "How likely are you to recommend Coffee Island to a friend ?"

                else:
                    print(f"⚠ Rating column not found in {file} - {sheet}")
                    continue

                df2 = pd.DataFrame({
                    "created_date": df.get("Date"),
                    "outlet_name": df.get("Outlet Name"),
                    "rating": df[rating_col].replace(rating_map).infer_objects(copy=False),
                    "comment": df.get("Tell us more"),
                    "customer_name": df.get("Customer Name"),
                    "customer_phone": df.get("Phone No:") if "Phone No:" in df.columns else df.get("Phone"),
                    "review_id": df.get("ID", "").astype(str),
                    "raw_source_file": file
                })

                frames.append(clean_common(df2, "Website"))

        # ==========================
        # SHEET 2 → Quick Feedback
        # ==========================
        if file.endswith(".xlsx") and "Quick Feedback Forms" in xl.sheet_names:

            df_qf = pd.read_excel(xl, sheet_name="Quick Feedback Forms", dtype=str)
            df_qf.columns = df_qf.columns.str.strip()

            # 🔎 Detect date column automatically
            date_col = None
            for col in df_qf.columns:
                if "date" in col.lower():
                    date_col = col
                    break

            if date_col is None:
                print(f"⚠ No date column found in {file}")
                print("Columns available:", df_qf.columns.tolist())
                return pd.DataFrame()

            qf = pd.DataFrame({
                    "created_date": pd.to_datetime(df_qf[date_col], errors="coerce"),
                    "outlet_name": df_qf["Outlet Name"],
                    "price_satisfaction": convert_qf(df_qf["Rate satisfaction with the overall price paid ?"]),
                    "taste_of_food": convert_qf(df_qf["Rate Taste of Food ?"]),
                    "cleanliness": convert_qf(df_qf["Cafe Cleanliness"]),
                    "coffee_aroma": convert_qf(df_qf["Did you smell the coffee aroma ?"]),
                    "staff_friendliness": convert_qf(df_qf["Staff friendliness"]),
                    "order_accuracy": convert_qf(df_qf["Order accuracy"]),
                    "speed_of_service": convert_qf(df_qf["Speed of service"]),
                    "overall_satisfaction": convert_qf(df_qf["Overall satisfaction"]),
                    "raw_source_file": file
                })

            qf["outlet_norm"] = qf["outlet_name"].astype(str).str.lower().str.strip()
            # 🔐 Duplicate hash for quick feedback
            qf["qf_hash"] = (
                qf["outlet_norm"].fillna("") +
                qf["created_date"].astype(str).fillna("") +
                qf["overall_satisfaction"].astype(str).fillna("")
            ).apply(lambda x: hashlib.sha256(x.encode("utf-8")).hexdigest())

            quick_frames.append(qf)

    # ---------------------------------
    # Insert Quick Feedback into SQL
    # ---------------------------------
    if quick_frames:
        df_quick_all = pd.concat(quick_frames, ignore_index=True)
        df_quick_all = df_quick_all.drop_duplicates(subset=["qf_hash"])

        # ---------------------------------
        # SAFE INSERT QUICK FEEDBACK
        # ---------------------------------
        temp_qf = "##temp_quick_feedback"

        df_quick_all.to_sql(
            temp_qf,
            engine,
            if_exists="replace",
            index=False,
            chunksize=1000
        )

        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO quick_feedback (
                    created_date,
                    outlet_name,
                    outlet_norm,
                    price_satisfaction,
                    taste_of_food,
                    cleanliness,
                    coffee_aroma,
                    staff_friendliness,
                    order_accuracy,
                    speed_of_service,
                    overall_satisfaction,
                    raw_source_file,
                    inserted_at,
                    qf_hash
                )
                SELECT
                    t.created_date,
                    t.outlet_name,
                    t.outlet_norm,
                    t.price_satisfaction,
                    t.taste_of_food,
                    t.cleanliness,
                    t.coffee_aroma,
                    t.staff_friendliness,
                    t.order_accuracy,
                    t.speed_of_service,
                    t.overall_satisfaction,
                    t.raw_source_file,
                    GETDATE(),
                    t.qf_hash
                FROM ##temp_quick_feedback t
                LEFT JOIN quick_feedback q
                    ON t.qf_hash = q.qf_hash
                WHERE q.qf_hash IS NULL
            """))


    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# =========================================
# MASTER RUNNER
# =========================================

def main():
    print("🔄 Starting daily feedback import...")

    df_all = pd.concat([
        import_google(),
        import_swiggy(),
        import_zomato(),
        import_zdistrict(),
        import_website()
    ], ignore_index=True)

    if df_all.empty:
        print("⚠ No files found.")
        return

    print(f"📦 Total records found: {len(df_all)}")

    # Remove duplicates inside current batch
    df_all = df_all.drop_duplicates(subset=["review_hash"])

    print(f"🆕 Unique records in batch: {len(df_all)}")

    # -----------------------------------------
    # 🔧 Ensure All Required Columns Exist
    # -----------------------------------------
    required_cols = [
        "created_date","updated_date","source","outlet_name",
        "outlet_norm","rating","category","comment","reply",
        "customer_name","customer_phone","review_id",
        "zomato_order_id","restaurant_id","raw_source_file",
        "review_hash"
    ]

    for col in required_cols:
        if col not in df_all.columns:
            df_all[col] = None

    df_all = df_all[required_cols]

    # -----------------------------------------
    # STEP 1: Upload to Temporary Table
    # -----------------------------------------
    temp_table = "##temp_feedback_upload"

    df_all.to_sql(
        temp_table,
        engine,
        if_exists="replace",
        index=False,
        chunksize=1000
    )

    # -----------------------------------------
    # STEP 2: Insert Only New Records
    # -----------------------------------------
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO feedback_raw (
                created_date,
                updated_date,
                source,
                outlet_name,
                outlet_norm,
                rating,
                category,
                comment,
                reply,
                customer_name,
                customer_phone,
                review_id,
                zomato_order_id,
                restaurant_id,
                raw_source_file,
                inserted_at,
                review_hash
            )
            SELECT
                t.created_date,
                t.updated_date,
                t.source,
                t.outlet_name,
                t.outlet_norm,
                t.rating,
                t.category,
                t.comment,
                t.reply,
                t.customer_name,
                t.customer_phone,
                t.review_id,
                t.zomato_order_id,
                t.restaurant_id,
                t.raw_source_file,
                GETDATE(),
                t.review_hash
            FROM ##temp_feedback_upload t
            LEFT JOIN feedback_raw f
                ON t.review_hash = f.review_hash
            WHERE f.review_hash IS NULL
        """))

    print("✅ Incremental import completed successfully.")

if __name__ == "__main__":
    main()