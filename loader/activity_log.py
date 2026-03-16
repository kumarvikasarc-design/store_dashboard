import pandas as pd
from sqlalchemy import create_engine
import warnings
from sqlalchemy.exc import SAWarning
import pyodbc

warnings.filterwarnings("ignore", category=SAWarning)

# -----------------------------------
# CONFIG
# -----------------------------------
CSV_FILE = r"C:\Users\ACER\Desktop\Item_Out_Of_Stock(2026-03-15--2026-03-15).csv"

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=coffee_island_analytics;"
    "Trusted_Connection=yes;"
)
print("✅ ODBC connected successfully")

engine = create_engine(
    "mssql+pyodbc://@localhost\\SQLEXPRESS/coffee_island_analytics"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)

TABLE_NAME = "activity_log"


# -----------------------------------
# 1. Extract Generated On
# -----------------------------------
meta_row = pd.read_csv(CSV_FILE, nrows=1, header=None).iloc[0, 0]
generated_on = pd.to_datetime(meta_row.split(":", 1)[1].strip())

# -----------------------------------
# 2. Read actual table (header row = 1)
# -----------------------------------
df = pd.read_csv(
    CSV_FILE,
    header=1,
    dtype=str,
    keep_default_na=False,
    na_values=[]
)

# -----------------------------------
# 3. Normalize column names
# -----------------------------------
df.columns = (
    df.columns
      .str.strip()
      .str.lower()
      .str.replace(" ", "_")
)

# -----------------------------------
# 4. Add generated_on to all rows
# -----------------------------------
df["generated_on"] = generated_on

# -----------------------------------
# 5. Parse datetime fields safely
# -----------------------------------
# Dates
# df["from_date"] = pd.to_datetime(
#     df["from_date"],
#     format="%d-%m-%Y",
#     errors="coerce"
# ).dt.date

# df["to_date"] = pd.to_datetime(
#     df["to_date"],
#     format="%d-%m-%Y",
#     errors="coerce"
# ).dt.date

# # Times
# df["from_time"] = pd.to_datetime(
#     df["from_time"],
#     format="%I:%M:%S %p",
#     errors="coerce"
# ).dt.time

# df["to_time"] = pd.to_datetime(
#     df["to_time"],
#     format="%I:%M:%S %p",
#     errors="coerce"
# ).dt.time

# # Activity time
# df["activity_time"] = pd.to_datetime(
#     df["activity_time"].str.strip(),
#     errors="coerce"
# )
# Clean dash values
df = df.fillna("-")

# Strip spaces
for col in ["activity_time","from_date","from_time","to_date","to_time"]:
    df[col] = df[col].str.strip()

# -----------------------------------
# 6. Reorder columns for SQL
# -----------------------------------
if "user" in df.columns:
    df = df.rename(columns={"user": "activity_user"})
elif "activity_user" not in df.columns:
    df["activity_user"] = None   # keep NULL for missing

df = df[
    [
        "generated_on",
        "deployment_name",
        "activity_time",
        "tab",
        "partner_names",
        "item_name",
        "item_type",
        "activity",
        "activity_user",
        "from_date",
        "from_time",
        "to_date",
        "to_time",
    ]
]

# -----------------------------------
# 7. Dash fill only for text columns
# -----------------------------------
text_cols = ["deployment_name", "tab", "partner_names", "item_name", "item_type", "activity", "activity_user"]
df[text_cols] = df[text_cols].fillna("-")


# -----------------------------------
# 8. Insert into DB
# -----------------------------------
df.to_sql(
    TABLE_NAME,
    engine,
    schema="dbo",
    if_exists="append",
    index=False,
    method=None
)

print(f"✅ Loaded {len(df)} rows into {TABLE_NAME}")