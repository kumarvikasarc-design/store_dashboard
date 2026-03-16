STOCK_SUMMARY_QUERY = """
WITH Opening AS (
    SELECT 
        Warehouse AS warehouse,
        NULL AS item_id,
        SUM(Opening_Stock) AS qty
    FROM warehouse_opening_stock
    GROUP BY Warehouse
),

StockEntry AS (
    SELECT
        warehouse,
        item_id,
        SUM(qty_in) AS qty
    FROM warehouse_stockentry
    GROUP BY warehouse, item_id
),

Indent AS (
    SELECT
        warehouse,
        item_id,
        SUM(indent_qty) AS qty
    FROM warehouse_indent
    GROUP BY warehouse, item_id
),

Consumption AS (
    SELECT
        store_name AS warehouse,
        item_id,
        SUM(consumption_qty) AS qty
    FROM outlet_consumption
    GROUP BY store_name, item_id
),

Wastage AS (
    SELECT
        warehouse,
        item_id,
        SUM(qty) AS qty
    FROM warehouse_wastage
    GROUP BY warehouse, item_id
)

SELECT 
    se.warehouse,
    se.item_id,
    SUM(se.qty) 
    - (ISNULL(i.qty,0) - ISNULL(c.qty,0))
    - ISNULL(w.qty,0) AS Available_Qty
FROM StockEntry se
LEFT JOIN Indent i ON se.item_id = i.item_id AND se.warehouse = i.warehouse
LEFT JOIN Consumption c ON se.item_id = c.item_id
LEFT JOIN Wastage w ON se.item_id = w.item_id AND se.warehouse = w.warehouse
GROUP BY se.warehouse, se.item_id, i.qty, c.qty, w.qty
"""
# ===============================================
# MASTER STOCK CALCULATION (FINAL FORMULA)
# ===============================================

WAREHOUSE_STOCK_SUMMARY = """

WITH stock_in AS (
    SELECT 
        warehouse,
        item_id,
        SUM(qty_in) AS stock_in_qty,
        SUM(total_amount) AS stock_in_value
    FROM warehouse_stockentry
    GROUP BY warehouse, item_id
),

opening AS (
    SELECT
        warehouse,
        item_name,
        SUM(opening_stock) AS opening_qty
    FROM warehouse_opening_stock
    GROUP BY warehouse, item_name
),

indent AS (
    SELECT
        warehouse,
        item_id,
        SUM(indent_qty) AS indent_qty,
        SUM(total_amount) AS indent_value
    FROM warehouse_indent
    GROUP BY warehouse, item_id
),

consumption AS (
    SELECT
        store_name AS warehouse,
        item_id,
        SUM(consumption_qty) AS consumption_qty
    FROM outlet_consumption
    GROUP BY store_name, item_id
),

wastage AS (
    SELECT
        warehouse,
        item_id,
        SUM(qty) AS wastage_qty,
        SUM(amount) AS wastage_value
    FROM warehouse_wastage
    GROUP BY warehouse, item_id
)

SELECT 
    se.warehouse,
    m.item_name,
    m.category_name,
    m.base_uom,
    m.tax_rate,
    se.stock_in_qty,
    se.stock_in_value,

    ISNULL(i.indent_qty,0) AS indent_qty,
    ISNULL(i.indent_value,0) AS indent_value,

    ISNULL(c.consumption_qty,0) AS outlet_consumption,
    ISNULL(w.wastage_qty,0) AS wastage_qty,

    -- FINAL STOCK FORMULA
    (
        se.stock_in_qty 
        - ((ISNULL(i.indent_qty,0) - ISNULL(c.consumption_qty,0)) 
        + ISNULL(w.wastage_qty,0))
    ) AS available_qty,

    (
        se.stock_in_value 
        - ((ISNULL(i.indent_value,0)) 
        + ISNULL(w.wastage_value,0))
    ) AS available_value

FROM stock_in se
LEFT JOIN stockitem_master m ON se.item_id = m.item_id
LEFT JOIN indent i ON se.item_id = i.item_id AND se.warehouse=i.warehouse
LEFT JOIN consumption c ON se.item_id = c.item_id
LEFT JOIN wastage w ON se.item_id = w.item_id AND se.warehouse=w.warehouse

ORDER BY se.warehouse, m.item_name
"""
LEDGER_QUERY = """
SELECT 
entry_date AS date,
warehouse,
item_name,
uom,
qty_in AS stock_in,
0 AS stock_out
FROM warehouse_stockentry

UNION ALL

SELECT 
indent_date,
warehouse,
item_name,
uom,
0,
indent_qty
FROM warehouse_indent

UNION ALL

SELECT
wastage_date,
warehouse,
item_name,
uom,
0,
qty
FROM warehouse_wastage

ORDER BY date
"""
AGING_QUERY = """
SELECT 
item_name,
warehouse,
MAX(entry_date) AS last_movement,
SUM(qty_in - qty_out) AS closing_stock
FROM warehouse_ledger_view
GROUP BY item_name, warehouse
"""
