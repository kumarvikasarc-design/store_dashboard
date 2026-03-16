import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Load feedback file into DataFrame
df = pd.read_excel(r"C:\Users\ACER\store_dashboard\SubmissionsExport.xlsx")

# Filter negative feedback
negative_feedback = df[
    (df["How was your overall experience?"].str.contains("Poor", case=False)) |
    (df["How likely are you to recommend Coffee Island to a friend ?"].str.contains("Not Likely", case=False))
]
latest_negatives = negative_feedback.sort_values("created at", ascending=False).head(5)

# Filter positive feedback
positive_feedback = df[
    (df["How was your overall experience?"].str.contains("Excellent", case=False)) &
    (df["How likely are you to recommend Coffee Island to a friend ?"].str.contains("Definitely", case=False))
]
latest_positives = positive_feedback.sort_values("created at", ascending=False).head(5)

# Build HTML table for negatives (red)
negatives_html = "<table border='1' style='border-collapse:collapse;'>"
negatives_html += "<tr><th>Customer</th><th>Store</th><th>Comment</th><th>Date</th></tr>"
for _, row in latest_negatives.iterrows():
    negatives_html += f"<tr>"
    negatives_html += f"<td>{row['Full Name:']}</td>"
    negatives_html += f"<td>{row['Stores']}</td>"
    negatives_html += f"<td style='color:red;'>{row['Tell us more']}</td>"
    negatives_html += f"<td>{row['created at']}</td>"
    negatives_html += "</tr>"
negatives_html += "</table>"

# Build HTML table for positives (green)
positives_html = "<table border='1' style='border-collapse:collapse;'>"
positives_html += "<tr><th>Customer</th><th>Store</th><th>Comment</th><th>Date</th></tr>"
for _, row in latest_positives.iterrows():
    positives_html += f"<tr>"
    positives_html += f"<td>{row['Full Name:']}</td>"
    positives_html += f"<td>{row['Stores']}</td>"
    positives_html += f"<td style='color:green;'>{row['Tell us more']}</td>"
    positives_html += f"<td>{row['created at']}</td>"
    positives_html += "</tr>"
positives_html += "</table>"

# Email setup
sender = "vikash.k@vitaxnova.com"
receiver = "samit.k@vitaxnova.com"
app_password = "Believe2024*"   # ⚠️ Use Gmail App Password, not your normal password

msg = MIMEMultipart()
msg['From'] = sender
msg['To'] = receiver
msg['Subject'] = "Customer Feedback Report – Coffee Island"

# Body with only negatives + positives
body = f"""
Hi Team,<br><br>
Here are the latest customer feedback highlights.<br><br>

<b>Latest 5 Negative Comments (Highlighted in Red):</b><br>
{negatives_html}<br><br>

<b>Latest 5 Positive Comments (Highlighted in Green):</b><br>
{positives_html}<br><br>

Regards,<br>
Vikash
"""
msg.attach(MIMEText(body, 'html'))

# Send email via Gmail SMTP
server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login(sender, "gxvf bzlx gykd hqsy")   # ✅ Use App Password
server.send_message(msg)
server.quit()