# storage/repository.py

from storage.models import Store
from sqlalchemy import select
from datetime import datetime
import pandas as pd

def add_store(db, data):
    store = Store(
        store_id=data["store_id"],
        outlet_name=data["outlet_name"],
        region=data["region"],
        city=data["city"],
        type=data["type"],
        area_manager=data["area_manager"],
        opening_date=datetime.strptime(data["opening_date"], "%Y-%m-%d"),
        status=data["status"]
    )
    db.add(store)
    db.commit()
    return True

def get_all_stores(db):
    result = db.execute(select(Store)).scalars().all()
    return [r.__dict__ for r in result]

def delete_store(db, store_id):
    record = db.execute(
        select(Store).where(Store.store_id == store_id)
    ).scalar_one_or_none()

    if record:
        db.delete(record)
        db.commit()
        return True
    return False

def update_store(db, store_id, updated):
    record = db.execute(
        select(Store).where(Store.store_id == store_id)
    ).scalar_one_or_none()

    if not record:
        return False

    for key, value in updated.items():
        if key == "opening_date":
            value = datetime.strptime(value, "%Y-%m-%d")
        setattr(record, key, value)

    db.commit()
    return True

def import_excel(db, filepath):
    df = pd.read_excel(filepath)
    for _, row in df.iterrows():
        data = {
            "store_id": int(row["Store Id"]),
            "outlet_name": row["Outlet Name"],
            "region": row["Region"],
            "city": row["City"],
            "type": row["Type"],
            "area_manager": row["Area Manager"],
            "opening_date": str(row["Opening Date"].date()),
            "status": row["Status"]
        }
        add_store(db, data)
    return True
