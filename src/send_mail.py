import os
import smtplib
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

def send_email(subject, body_html):
    sender = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASS")
    recipient = os.getenv("SMTP_RECIPIENT")

    if not sender or not password or not recipient:
        raise ValueError("SMTP_USER, SMTP_PASS, or SMTP_RECIPIENT environment variable is missing.")

    msg = MIMEMultipart("related")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    msg_alt = MIMEMultipart("alternative")
    msg.attach(msg_alt)

    msg_alt.attach(MIMEText("This email requires HTML support.", "plain"))
    msg_alt.attach(MIMEText(body_html, "html"))

    logo_path = Path("images/logo_vives.png")
    if logo_path.exists():
        with open(logo_path, 'rb') as img:
            logo_data = img.read()
            image = MIMEImage(logo_data)
            image.add_header('Content-ID', '<viveslogo>')
            msg.attach(image)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, [recipient], msg.as_string())
        print("Email successfully sent.")
    except Exception as e:
        print(f"Error sending email: {e}")

if __name__ == "__main__":
    try:
        with open("data/predicted_anomalies_latest.json", encoding="utf-8") as f:
            anomalies = json.load(f)
        count = len(anomalies)
    except Exception as e:
        print(f"Failed to load anomaly data: {e}")
        count = "unknown (error reading file)"

    dashboard_url = os.getenv("DASHBOARD_URL", "https://vivesnetdetect.streamlit.app/")
    elastic_url = os.getenv("ELASTICSEARCH_URL", "https://uat.elastic.vives.cloud:5601/app/r/s/Ok4A0")

    html_body = f"""
    <html>
    <body>
        <p><img src='cid:viveslogo' alt='VIVES Logo' style='height: 40px;'><br><br>
        <b>{count} anomalies</b> were detected in the latest batch.<br><br>
        View details in:<br>
        <a href="{dashboard_url}">Streamlit anomaly dashboard</a><br>
        <a href="{elastic_url}">Elasticsearch interface</a><br><br>
        This is an automated message.</p>
    </body>
    </html>
    """

    send_email(
        subject="VIVES: Anomaly detection scan completed",
        body_html=html_body
    )