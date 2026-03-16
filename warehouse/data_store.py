import pandas as pd
import glob

# ===============================
# CONSTANTS
# ===============================
COL_DATE = "date"
COL_WAREHOUSE = "Warehouse"
COL_OUTLET = "Outlet"
COL_ITEM = "Item"
COL_CATEGORY = "Category"

ENTRY_PATH = r"C:\Users\ACER\store_dashboard\inventory\warehouse_stockentry"
INDENT_PATH = r"C:\Users\ACER\store_dashboard\inventory\Indent_report"


# ===============================
# LOADERS
# ===============================
def load_entry():
    files = glob.glob(f"{ENTRY_PATH}/*.csv")
    dfs = []
    for f in files:
        df = pd.read_csv(f, engine="python", on_bad_lines="skip")
        df.columns = df.columns.str.strip()
        df["date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
        df["qty_in"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
        df["qty_out"] = 0
        df["Warehouse"] = df["Warehouse"]
        df["Outlet"] = None
        df["Item"] = df["Item Name"]
        df["Category"] = df["Category Name"]
        df["source"] = "ENTRY"
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def load_indent():
    files = glob.glob(f"{INDENT_PATH}/*.csv")
    dfs = []

    for f in files:
        raw = pd.read_csv(f, header=None, engine="python", on_bad_lines="skip")
        header_row = None

        for i in range(min(40, len(raw))):
            row = raw.iloc[i].astype(str).str.lower()
            if "item name" in row.values and "received qty" in row.values:
                header_row = i
                break

        if header_row is None:
            continue

        df = pd.read_csv(f, header=header_row, engine="python")
        df.columns = df.columns.str.strip()

        df["date"] = pd.to_datetime(df["Received Date"], errors="coerce", dayfirst=True)
        df["qty_in"] = 0
        df["qty_out"] = pd.to_numeric(df["Received Qty"], errors="coerce").fillna(0)
        df["Warehouse"] = df["Supplier"]
        df["Outlet"] = df["Receiver"]
        df["Item"] = df["Item Name"]
        df["Category"] = df["Category Name"]
        df["source"] = "INDENT"

        dfs.append(df)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ===============================
# FINAL DATAFRAME
# ===============================
entry_df = load_entry()
indent_df = load_indent()

tx_df = pd.concat([entry_df, indent_df], ignore_index=True)

tx_df = tx_df[
    [COL_DATE, COL_WAREHOUSE, COL_OUTLET, COL_ITEM, COL_CATEGORY, "qty_in", "qty_out", "source"]
]
