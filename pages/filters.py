import pandas as pd
from db_connection import engine

def get_filter_data():

    q = """
    SELECT 
    Brand,
    State,
    Region,
    City,
    Store_Type,
    Outlet_Name
    FROM stores_master
    WHERE status='Active'
    """

    return pd.read_sql(q, engine)
