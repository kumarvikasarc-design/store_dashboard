import pandas as pd
from sqlalchemy import create_engine, text
import os
import warnings
from sqlalchemy.exc import SAWarning

warnings.filterwarnings("ignore", category=SAWarning)

BASE_DIR = r"C:\Users\ACER\store_dashboard"
ITEM_DIR = os.path.join(BASE_DIR, "item_source")

engine = create_engine(
    "mssql+pyodbc://@localhost\\SQLEXPRESS/coffee_island_analytics"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)

# Already imported files
with engine.connect() as conn:
    imported_files = conn.execute(
        text("SELECT file_name FROM file_import_log")
    ).fetchall()

imported_files = {row[0] for row in imported_files}

csv_files = [
    f for f in os.listdir(ITEM_DIR)
    if f.lower().endswith(".csv") and f not in imported_files
]

if not csv_files:
    print("⚠️ No new files to import")
    exit()

for file in csv_files:
    file_path = os.path.join(ITEM_DIR, file)
    print(f"📥 Importing: {file}")

    df = pd.read_csv(file_path, dayfirst=True)
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    if len(df.columns) != 18:
        raise ValueError(f"Unexpected column count: {len(df.columns)} → {df.columns}")

    df.columns = [
        "deployment_name", "date", "source", "tab_name", "item_code",
        "section", "super_category_name", "category_name", "item_name",
        "item_qty", "total_qty", "rate", "subtotal", "discount",
        "net_amount", "total_tax", "gross_amount", "bills"
    ]

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Remove GRAND TOTAL
    df = df[~df["deployment_name"].str.upper().eq("GRAND TOTAL")]

    # ✅ ALWAYS NULL section (SQL controls it)
    df["section"] = None

    df.to_sql(
        "menu_mix_raw",
        engine,
        if_exists="append",
        index=False,
        chunksize=500
    )

    # ✅ AUTO MAP AFTER INSERT
    with engine.begin() as conn:
        conn.execute(text("EXEC dbo.sp_auto_map_menu_section"))

    # Log file
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO file_import_log (file_name) VALUES (:file)"),
            {"file": file}
        )

print("✅ Import completed (duplicates skipped)")
