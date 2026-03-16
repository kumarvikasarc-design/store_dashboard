import os
import json
from datetime import datetime

LOG_FILE = os.path.join(os.path.dirname(__file__), "..", "login_logs.json")

MAX_LOGS_PER_USER = 20   # keep only latest 20 entries


def _load_logs():
    if not os.path.exists(LOG_FILE):
        return {}
    try:
        with open(LOG_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_logs(data):
    with open(LOG_FILE, "w") as f:
        json.dump(data, f, indent=2)


def record_login(username, ip, user_agent):
    logs = _load_logs()

    event = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ip": ip or "unknown",
        "browser": user_agent or "unknown"
    }

    if username not in logs:
        logs[username] = []

    logs[username].insert(0, event)
    logs[username] = logs[username][:MAX_LOGS_PER_USER]  # keep only last 20

    _save_logs(logs)


def get_user_logs(username):
    logs = _load_logs()
    return logs.get(username, [])
