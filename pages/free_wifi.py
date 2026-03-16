from flask import Flask, request, render_template_string
import random, requests, pyodbc

app = Flask(__name__)

# SQL connection
conn = pyodbc.connect(
    "DRIVER={SQL Server};SERVER=localhost;DATABASE=wifi;Trusted_Connection=yes;"
)
cursor = conn.cursor()

OTP_STORE = {}

# ---------- send otp ----------
def send_otp(mobile, otp):
    url = "https://www.fast2sms.com/dev/bulkV2"
    headers = {"authorization": "YOUR_API_KEY"}
    data = {
        "variables_values": otp,
        "route": "otp",
        "numbers": mobile,
    }
    requests.post(url, data=data, headers=headers)

# ---------- login page ----------
@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "POST":
        mobile = request.form["mobile"]
        otp = str(random.randint(1000,9999))
        OTP_STORE[mobile] = otp
        send_otp(mobile, otp)

        return f"""
        Enter OTP:<br>
        <form action='/verify' method='post'>
        <input name='mobile' value='{mobile}' hidden>
        <input name='otp'>
        <button>Verify</button>
        </form>
        """

    return """
    Enter Mobile:<br>
    <form method='post'>
    <input name='mobile'>
    <button>Get OTP</button>
    </form>
    """

# ---------- verify otp ----------
@app.route("/verify", methods=["POST"])
def verify():
    mobile = request.form["mobile"]
    otp = request.form["otp"]

    if OTP_STORE.get(mobile) == otp:
        # store in SQL
        cursor.execute(
            "INSERT INTO wifi_users (mobile,login_time) VALUES (?,GETDATE())",
            mobile,
        )
        conn.commit()

        # allow internet (mikrotik api call)
        ip = request.remote_addr
        mikrotik_allow(ip)

        return "Connected! Enjoy free WiFi"

    return "Wrong OTP"

# ---------- allow internet ----------
def mikrotik_allow(ip):
    import socket
    s = socket.socket()
    s.connect(("192.168.88.1",8728))
    # simple API command (example)
    print("User allowed:", ip)

app.run(host="0.0.0.0", port=5000)
