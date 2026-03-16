from config import CONFIG

if CONFIG.get("BACKEND", "sql") == "firebase":
    from storage.storage_firebase import (
        get_all_stores,
        create_store,
        update_store,
        delete_store
    )
else:
    from storage.storage_sql import get_db
    from storage.repository import (
        get_all_stores,
        create_store,
        update_store,
        delete_store
    )
