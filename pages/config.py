# ---------------------------------------------
# GLOBAL APP CONFIGURATION
# ---------------------------------------------
CONFIG = {
    "APP_NAME": "Store Information Dashboard",
    "VERSION": "1.0.0",

    # -----------------------------------------
    # BACKEND OPTIONS: sql / firebase
    # -----------------------------------------
    "BACKEND": "sql",   # change to "firebase" if needed

    "DB": {
        "ENGINE": "mysql",
        "HOST": "localhost",
        "USER": "root",
        "PASSWORD": "",
        "DATABASE": "store_dashboard",
        "PORT": 3306,
    },

    # Firebase (only used if BACKEND = "firebase")
    "FIREBASE_CREDENTIALS_JSON": "firebase_key.json",
}

# ---------------------------------------------
# Build SQLAlchemy Connection String
# ---------------------------------------------
db = CONFIG["DB"]

CONFIG["SQLALCHEMY_DATABASE_URI"] = (
    f"{db['ENGINE']}+pymysql://{db['USER']}:{db['PASSWORD']}"
    f"@{db['HOST']}:{db['PORT']}/{db['DATABASE']}"
)

CONFIG["UPLOAD_FOLDER"] = "uploads"
CONFIG["ALLOWED_EXTENSIONS"] = ["xlsx", "xls"]

CONFIG["REGIONS"] = ["North", "South", "East", "West"]
CONFIG["STATUS"] = ["Active", "Inactive", "Closed"]
