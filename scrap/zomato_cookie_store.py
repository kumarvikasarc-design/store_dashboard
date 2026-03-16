# zomato_cookie_store.py
import json
import os
import time

COOKIE_FILE = os.path.join(
    os.path.dirname(__file__),
    "zomato_cookies.json"
)

COOKIE_TTL_HOURS = 12  # ✅ increase TTL


def save_cookies(cookies: dict):
    payload = {
        "saved_at": int(time.time()),
        "cookies": cookies
    }
    with open(COOKIE_FILE, "w") as f:
        json.dump(payload, f)


def load_cookies():
    if not os.path.exists(COOKIE_FILE):
        return None

    with open(COOKIE_FILE) as f:
        payload = json.load(f)

    saved_at = payload.get("saved_at", 0)
    age_hours = (time.time() - saved_at) / 3600

    if age_hours > COOKIE_TTL_HOURS:
        return None

    cookies = payload.get("cookies")
    return cookies if cookies else None
