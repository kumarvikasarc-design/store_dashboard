import smtplib

EMAIL_FROM = "vikash.k@vitaxnova.com"
EMAIL_PASS = "gxvfbzlxgykdhqsy"

try:
    with smtplib.SMTP("smtp.gmail.com", 587) as s:
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login(EMAIL_FROM, EMAIL_PASS)
        print("Login successful")
except Exception as e:
    print("Login failed:", e)