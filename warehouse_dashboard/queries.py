import pandas as pd
from db_connection import engine

# ============================================
# 🔥 MAIN STOCK VIEW (LIVE STOCK)
# ============================================
def get_live_stock():
    query = """
    SELECT 
        i.item_id,
        i.item_name,
        w.warehouse_name,

        ISNULL(os.opening_qty,0)
        + ISNULL(se.total_entry,0)
        - ISNULL(ind.total_indent,0)
        - ISNULL(ws.total_wastage,0)
        - ISNULL(ex.total_expiry,0)
        + (ISNULL(ind.total_indent,0) - ISNULL(cons.total_consumption,0))
        AS available_stock

    FROM dbo.stockitem_master i
    CROSS JOIN dbo.warehouse_master w

    LEFT JOIN (
        SELECT warehouse_id,item_id,SUM(opening_qty) opening_qty
        FROM dbo.warehouse_opening_stock
        GROUP BY warehouse_id,item_id
    ) os ON os.item_id=i.item_id AND os.warehouse_id=w.warehouse_id

    LEFT JOIN (
        SELECT warehouse_id,item_id,SUM(qty) total_entry
        FROM dbo.warehouse_stockentry
        GROUP BY warehouse_id,item_id
    ) se ON se.item_id=i.item_id AND se.warehouse_id=w.warehouse_id

    LEFT JOIN (
        SELECT warehouse_id,item_id,SUM(qty) total_indent
        FROM dbo.warehouse_indent
        GROUP BY warehouse_id,item_id
    ) ind ON ind.item_id=i.item_id AND ind.warehouse_id=w.warehouse_id

    LEFT JOIN (
        SELECT item_id,SUM(qty) total_consumption
        FROM dbo.outlet_consumption
        GROUP BY item_id
    ) cons ON cons.item_id=i.item_id

    LEFT JOIN (
        SELECT warehouse_id,item_id,SUM(qty) total_wastage
        FROM dbo.warehouse_wastage
        GROUP BY warehouse_id,item_id
    ) ws ON ws.item_id=i.item_id AND ws.warehouse_id=w.warehouse_id

    LEFT JOIN (
        SELECT warehouse_id,item_id,SUM(qty) total_expiry
        FROM dbo.warehouse_item_expiry
        GROUP BY warehouse_id,item_id
    ) ex ON ex.item_id=i.item_id AND ex.warehouse_id=w.warehouse_id
    """

    return pd.read_sql(query, engine)


# ============================================
# 🔥 EXPIRY DATA
# ============================================
def get_expiry():
    return pd.read_sql("""
    SELECT *
    FROM warehouse_item_expiry
    """, engine)


# ============================================
# 🔥 CONSUMPTION
# ============================================
def get_consumption():
    return pd.read_sql("""
    SELECT outlet_name,item_id,qty,date
    FROM outlet_consumption
    """, engine)
