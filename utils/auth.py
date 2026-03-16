"""
utils/auth.py

User auth utilities for Coffee Island dashboards.

Features:
- File-backed user store (users.json)
- Password hashing (bcrypt)
- JWT token generation and verification (pyjwt)
- Admin-only user creation via create_user()
- Reset password using security question
- Change password
- Login logging (login_logs.json)
- Utility APIs expected by main_app.py
"""

import os
import json
from datetime import datetime, timezone
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, List

import bcrypt
import jwt

# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")  # all runtime files kept here
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR, exist_ok=True)

USERS_FILE = os.path.join(DATA_DIR, "users.json")
LOGIN_LOGS_FILE = os.path.join(DATA_DIR, "login_logs.json")

# Secret / JWT config - override with env variable in production
JWT_SECRET = os.environ.get("COFFEE_ISLAND_JWT_SECRET", "please-change-this-secret")
JWT_ALG = "HS256"

# Password policy
MIN_PASSWORD_LEN = 8

# Defaults
DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_ADMIN_PASSWORD = "Admin@123"  # please change after first run

# -------------------------
# Helpers: file I/O
# -------------------------
def _read_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _write_json(path: str, data: dict):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)

# -------------------------
# Users store structure
# {
#   "<username>": {
#       "username": "<username>",
#       "password_hash": "<bcrypt>",
#       "role": "admin|manager|viewer",
#       "is_active": true,
#       "security_question": "...",
#       "security_answer_hash": "<bcrypt>",
#       "created_at": "<iso>",
#       "last_password_change": "<iso>"
#   },
#   ...
# }
# -------------------------
def _load_users() -> Dict[str, dict]:
    data = _read_json(USERS_FILE)
    if not isinstance(data, dict):
        return {}
    return data

def _save_users(users: Dict[str, dict]):
    _write_json(USERS_FILE, users)

def _load_logs() -> List[dict]:
    data = _read_json(LOGIN_LOGS_FILE)
    if not isinstance(data, list):
        return []
    return data

def _save_logs(logs: List[dict]):
    _write_json(LOGIN_LOGS_FILE, logs)

# -------------------------
# Password / security helpers
# -------------------------
def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

def _check_password(password: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False

def _validate_password_policy(password: str) -> Tuple[bool, str]:
    if not password or len(password) < MIN_PASSWORD_LEN:
        return False, f"Password must be at least {MIN_PASSWORD_LEN} characters."
    # require uppercase, lowercase, digit, special
    import re
    if not re.search(r"[A-Z]", password):
        return False, "Password must contain at least one uppercase letter."
    if not re.search(r"[a-z]", password):
        return False, "Password must contain at least one lowercase letter."
    if not re.search(r"[0-9]", password):
        return False, "Password must contain at least one digit."
    if not re.search(r"[!@#$%^&*(),.?\":{}|<>_\-+=/\\\[\]]", password):
        return False, "Password must contain at least one special character."
    return True, "OK"

# -------------------------
# Bootstrapping: ensure admin exists
# -------------------------
def _ensure_admin_exists():
    users = _load_users()
    if DEFAULT_ADMIN_USERNAME not in users:
        # Create default admin (only if no admin exists)
        pwd_hash = _hash_password(DEFAULT_ADMIN_PASSWORD)
        users[DEFAULT_ADMIN_USERNAME] = {
            "username": DEFAULT_ADMIN_USERNAME,
            "password_hash": pwd_hash,
            "role": "admin",
            "is_active": True,
            "security_question": "What is your organisation name?",
            "security_answer_hash": _hash_password("coffeeisland"),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "last_password_change": datetime.now(timezone.utc).isoformat(),
        }
        _save_users(users)

# ensure on import
_ensure_admin_exists()

# -------------------------
# Public API
# -------------------------
def authenticate_user(username: str, password: str) -> Optional[str]:
    """
    Validate username/password. If ok and active -> returns role string.
    Also logs the attempt in login logs.
    """
    username = (username or "").strip()
    if not username or not password:
        _log_login_attempt(username, success=False, reason="empty_credentials")
        return None

    users = _load_users()
    u = users.get(username)
    if not u:
        _log_login_attempt(username, success=False, reason="user_not_found")
        return None
    if not u.get("is_active", True):
        _log_login_attempt(username, success=False, reason="inactive")
        return None

    if not _check_password(password, u.get("password_hash", "")):
        _log_login_attempt(username, success=False, reason="bad_password")
        return None

    # success
    _log_login_attempt(username, success=True, reason="ok")
    # update last login time
    u["last_login"] = datetime.now(timezone.utc).isoformat()
    users[username] = u
    _save_users(users)
    return u.get("role", "viewer")

def generate_token(username: str, role: str, ttl_hours: int = 12) -> str:
    now = datetime.now()
    payload = {
        "sub": username,
        "role": role,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(hours=ttl_hours)).timestamp()),
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)
    # pyjwt >=2 returns str; earlier returns bytes - ensure str
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token

def verify_token(token: str) -> Optional[dict]:
    if not token:
        return None
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except Exception:
        return None

def create_user(username: str, password: str, role: str, security_question: str = "", security_answer: str = "") -> Tuple[bool, str]:
    """
    Admin-only user creation should call this (main_app checks admin).
    Returns (success, message).
    """
    username = (username or "").strip()
    if not username:
        return False, "Username is required."
    users = _load_users()
    if username in users:
        return False, "Username already exists."

    valid, msg = _validate_password_policy(password)
    if not valid:
        return False, msg

    pwd_hash = _hash_password(password)
    ans_hash = _hash_password(security_answer or "") if security_answer else ""
    users[username] = {
        "username": username,
        "password_hash": pwd_hash,
        "role": role or "viewer",
        "is_active": True,
        "security_question": security_question or "",
        "security_answer_hash": ans_hash,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "last_password_change": datetime.now(timezone.utc).isoformat(),
    }
    _save_users(users)
    return True, "User created successfully."

def get_users_for_display() -> Dict[str, dict]:
    """Return a simple tabular view as list-like pandas-friendly structure (list of dicts)."""
    users = _load_users()
    # to keep callers same as earlier code: produce list-of-dicts or a small pandas-like structure if they wrap
    out = []
    for u in users.values():
        out.append({
            "username": u["username"],
            "role": u.get("role", "viewer"),
            "is_active": u.get("is_active", True),
            "created_at": u.get("created_at"),
            "last_login": u.get("last_login"),
        })
    # return as list (the main_app previously expected df.to_dict('records'), but we give list)
    # it's fine for DataTable.data
    return out

def set_user_active(username: str, active: bool) -> bool:
    users = _load_users()
    u = users.get(username)
    if not u:
        return False
    u["is_active"] = bool(active)
    users[username] = u
    _save_users(users)
    return True

def delete_user(username: str) -> bool:
    users = _load_users()
    if username not in users:
        return False
    del users[username]
    _save_users(users)
    return True

def update_user_role(username: str, new_role: str) -> bool:
    users = _load_users()
    u = users.get(username)
    if not u:
        return False
    u["role"] = new_role
    users[username] = u
    _save_users(users)
    return True

# -------------------------
# Password reset using security question
# -------------------------
def get_security_question(username: str) -> Optional[str]:
    users = _load_users()
    u = users.get(username)
    if not u:
        return None
    return u.get("security_question", "")

def reset_password_with_answer(username: str, answer: str, new_password: str) -> Tuple[bool, str]:
    users = _load_users()
    u = users.get(username)
    if not u:
        return False, "User not found."
    stored = u.get("security_answer_hash", "")
    if not stored:
        return False, "No security question configured for this user."
    if not _check_password(answer or "", stored):
        return False, "Security answer incorrect."
    valid, msg = _validate_password_policy(new_password)
    if not valid:
        return False, msg
    u["password_hash"] = _hash_password(new_password)
    u["last_password_change"] = datetime.now(timezone.utc).isoformat()
    users[username] = u
    _save_users(users)
    return True, "Password reset successfully."

def change_password(username: str, old_password: str, new_password: str) -> Tuple[bool, str]:
    users = _load_users()
    u = users.get(username)
    if not u:
        return False, "User not found."
    if not _check_password(old_password or "", u.get("password_hash", "")):
        return False, "Old password is incorrect."
    valid, msg = _validate_password_policy(new_password)
    if not valid:
        return False, msg
    u["password_hash"] = _hash_password(new_password)
    u["last_password_change"] = datetime.now(timezone.utc).isoformat()
    users[username] = u
    _save_users(users)
    return True, "Password changed successfully."

# -------------------------
# Login logs
# -------------------------
def _log_login_attempt(username: str, success: bool, reason: str = ""):
    logs = _load_logs()
    logs.append({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "username": username,
        "success": bool(success),
        "reason": reason,
    })
    # keep logs size bounded (e.g. last 5000)
    if len(logs) > 5000:
        logs = logs[-5000:]
    _save_logs(logs)

def get_login_logs(limit: int = 1000) -> List[dict]:
    logs = _load_logs()
    return logs[-limit:][::-1]  # newest first

def get_recent_login_activity(username: str, limit: int = 50) -> List[dict]:
    logs = _load_logs()
    res = [l for l in logs if l.get("username") == username]
    return res[-limit:][::-1]

# -------------------------
# Utility: safe admin-runner (used by main_app)
# -------------------------
def is_admin_user(username: str) -> bool:
    users = _load_users()
    u = users.get(username)
    return bool(u and u.get("role") == "admin")

# End of utils/auth.py
