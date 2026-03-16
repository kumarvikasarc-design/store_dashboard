import pandas as pd
from sqlalchemy import create_engine
import urllib

csv_path = r"C:\Users\ACER\store_dashboard\stores_db.csv"

df = pd.read_csv(csv_path)

# ---- Column rename to DB-safe names ----
df.rename(columns={
    "Store Id": "Store_Id",
    "Outlet Name": "Outlet_Name",
    "Type": "Store_Type",
    "Email id": "Email_Id",
    "Area Manager": "Area_Manager",
    "Area Manager Email Id": "Area_Manager_Email",
    "Opening Date": "Opening_Date",
    "Zomato Id": "Zomato_Id",
    "Swiggy Id": "Swiggy_Id",
}, inplace=True)

# ---- Fix date ----
df["Opening_Date"] = pd.to_datetime(
    df["Opening_Date"],
    dayfirst=True,
    errors="coerce"
)

# ---- SQL Server connection via SQLAlchemy ----
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=coffee_island_analytics;"
    "Trusted_Connection=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# ---- LOAD TO SQL ----
df.to_sql(
    name="stores_master",
    con=engine,
    schema="dbo",
    if_exists="replace",   # use "append" later
    index=False,
    method="multi"
)

print("✅ stores_master imported successfully")
