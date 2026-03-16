# storage/storage_firebase.py
import firebase_admin
from firebase_admin import credentials, firestore
from config import CONFIG
import threading

_db = None
_lock = threading.Lock()

def init_firebase():
    global _db
    with _lock:
        if _db is None:
            cred_path = CONFIG.get("FIREBASE_CREDENTIALS_JSON")
            if not cred_path:
                raise RuntimeError("FIREBASE_CREDENTIALS_JSON is not set in CONFIG")
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred)
            _db = firestore.client()
    return _db

def get_collection_ref():
    db = init_firebase()
    return db.collection("stores")

def get_all_stores():
    docs = get_collection_ref().stream()
    out = []
    for d in docs:
        data = d.to_dict()
        out.append({
            "Store ID": int(data.get("Store ID")),
            "Outlet Name": data.get("Outlet Name",""),
            "Region": data.get("Region",""),
            "City": data.get("City",""),
            "Type": data.get("Type",""),
            "Area Manager": data.get("Area Manager",""),
            "Opening Date": data.get("Opening Date",""),
            "Status": data.get("Status","")
        })
    out.sort(key=lambda r: r["Store ID"])
    return out

def create_store(row):
    ref = get_collection_ref().document(str(int(row["Store ID"])))
    ref.set(row)
    return row

def update_store(store_id, row):
    ref = get_collection_ref().document(str(int(store_id)))
    if not ref.get().exists:
        return None
    ref.update(row)
    return row

def delete_store(store_id):
    ref = get_collection_ref().document(str(int(store_id)))
    if not ref.get().exists:
        return False
    ref.delete()
    return True
