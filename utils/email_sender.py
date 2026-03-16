# utils/email_sender.py
import os
import smtplib
import base64
import json
import time
import requests
from email.message import EmailMessage
from typing import List, Tuple, Optional

# ============================================================
# CONFIG
# ============================================================

GOOGLE_OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"

# Required if OAuth2 is enabled
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN")

# Fallback to normal SMTP login
SMTP_USER = os.environ.get("COGS_SMTP_USER")
SMTP_PASS = os.environ.get("COGS_SMTP_PASS")
SMTP_SERVER = os.environ.get("COGS_SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("COGS_SMTP_PORT", 587))


# ============================================================
# HELPER — Get Gmail XOAUTH2 token
# ============================================================
def _get_oauth2_access_token() -> Optional[str]:
    """Return a fresh Gmail OAuth2 access token, or None if OAuth is not configured."""
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        return None  # No OAuth config ⇒ skip

    data = {
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": GOOGLE_REFRESH_TOKEN,
        "grant_type": "refresh_token",
    }

    try:
        r = requests.post(GOOGLE_OAUTH_TOKEN_URL, data=data)
        if r.status_code != 200:
            print("OAuth token refresh failed:", r.text)
            return None
        resp = r.json()
        return resp.get("access_token")
    except Exception as e:
        print("OAuth Exception:", e)
        return None


# ============================================================
# HELPER — Construct XOAUTH2 auth string
# ============================================================
def _generate_oauth2_string(email: str, access_token: str) -> str:
    auth_string = f"user={email}\1auth=Bearer {access_token}\1\1"
    return base64.b64encode(auth_string.encode()).decode()


# ============================================================
# MAIN EMAIL SENDER
# ============================================================
def send_email_with_attachments(
    subject: str,
    body_text: str,
    to_addrs: List[str],
    attachments: List[Tuple[str, bytes, str]] = None,
    cc_addrs: Optional[List[str]] = None,
    bcc_addrs: Optional[List[str]] = None,
    body_html: Optional[str] = None,
):
    """
    Send email using Gmail OAuth2 (if configured) or fallback SMTP username/password.
    Attachments: list of (filename, file_bytes, mime_type)
    """
    msg = EmailMessage()
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(to_addrs)

    if cc_addrs:
        msg["Cc"] = ", ".join(cc_addrs)
    if bcc_addrs:
        # BCC is not added to visible headers automatically, but used during send
        pass

    msg["Subject"] = subject

    # Add HTML and plain text multipart
    if body_html:
        msg.set_content(body_text)
        msg.add_alternative(body_html, subtype="html")
    else:
        msg.set_content(body_text)

    # Attach files
    if attachments:
        for fname, bts, mime in attachments:
            maintype, subtype = mime.split("/", 1)
            msg.add_attachment(bts, maintype=maintype, subtype=subtype, filename=fname)

    all_recipients = list(to_addrs)
    if cc_addrs:
        all_recipients.extend(cc_addrs)
    if bcc_addrs:
        all_recipients.extend(bcc_addrs)

    # ========= Try OAuth2 first ==========
    access_token = _get_oauth2_access_token()

    try:
        if access_token:
            auth_string = _generate_oauth2_string(SMTP_USER, access_token)

            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.ehlo()

                smtp.docmd("AUTH", "XOAUTH2 " + auth_string)
                smtp.send_message(msg, to_addrs=all_recipients)
                return "Email sent using Gmail OAuth2"

        # ========= Fall back to normal SMTP login ==========
        if not SMTP_PASS:
            raise RuntimeError(
                "SMTP password missing and OAuth2 not configured. Cannot send email."
            )

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(SMTP_USER, SMTP_PASS)
            smtp.send_message(msg, to_addrs=all_recipients)
            return "Email sent using SMTP username/password"

    except Exception as e:
        raise RuntimeError(f"Failed to send email: {e}")
