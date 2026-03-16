import smtplib

EMAIL = "vikash.k@vitaxnova.com"
PASS = "gxvfbzlxgykdhqsy"

try:
    s = smtplib.SMTP("smtp.gmail.com", 587)
    s.starttls()
    s.login(EMAIL, PASS)
    print("LOGIN SUCCESS ✅")
except Exception as e:
    print("ERROR ❌", e)
