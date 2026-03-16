from sqlalchemy import create_engine, text
import warnings
from sqlalchemy.exc import SAWarning

warnings.filterwarnings("ignore", category=SAWarning)
engine = create_engine(
    "mssql+pyodbc://@localhost\\SQLEXPRESS/coffee_island_analytics"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)

with engine.connect() as conn:
    print(conn.execute(text("SELECT DB_NAME()")).fetchone())
