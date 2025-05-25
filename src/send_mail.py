"""
Script: send_mail.py
Author: Moussa El Bazioui and Laurens Rasschaert
Project: Bachelorproef — Data-driven anomaly detection on network logs

Purpose:
This script sends an email alert when new anomalies have been detected.
It reads the predicted anomalies JSON and checks how many logs were flagged.
If anomalies are found it sends an e-mail with links to dashboards.

This is the final step of the automated batch run.
"""
import os
import smtplib
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from dotenv import load_dotenv
from pathlib import Path

# Load credentials and config
load_dotenv()

def send_email(subject, body_html):
    sender = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASS")
    recipient = os.getenv("SMTP_RECIPIENT")

    if not sender or not password or not recipient:
        raise ValueError("SMTP_USER, SMTP_PASS, or SMTP_RECIPIENT environment variable is missing.")

    # Compose the email
    msg = MIMEMultipart("related")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    msg_alt = MIMEMultipart("alternative")
    msg.attach(msg_alt)

    msg_alt.attach(MIMEText("This email requires HTML support.", "plain"))
    msg_alt.attach(MIMEText(body_html, "html"))

    # Attach VIVES logo
    logo_path = Path(__file__).resolve().parent.parent / "images" / "logo_vives.png"
    if logo_path.exists():
        with open(logo_path, 'rb') as img:
            logo_data = img.read()
            image = MIMEImage(logo_data)
            image.add_header('Content-ID', '<viveslogo>')
            msg.attach(image)

    # Send the email
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, [recipient], msg.as_string())
        print("Email successfully sent.")
    except Exception as e:
        print(f"Error sending email: {e}")

if __name__ == "__main__":
    anomaly_file = Path("/home/runner/work/PoC_Test/data/predicted_anomalies_latest.json")
    # Don’t try to send mail if file isn’t there
    if not anomaly_file.exists():
        print("Anomaly file does not exist. Email will not be sent.")
        exit(0)

    # Load the JSON and count anomalies
    try:
        with anomaly_file.open(encoding="utf-8") as f:
            anomalies = json.load(f)
        count = len(anomalies)
        if count == 0:
            print("No anomalies found. Email will not be sent.")
            exit(0)
    except Exception as e:
        print(f"Failed to load anomaly data: {e}")
        exit(1)

    # URLs for UI interfaces
    dashboard_url = os.getenv("DASHBOARD_URL", "https://vivesnetdetect.streamlit.app/")
    elastic_url = os.getenv("ELASTICSEARCH_URL", "https://uat.elastic.vives.cloud:5601/app/r/s/Ok4A0")

    # Email body
    html_body = f"""
    <html>
    <body>
        <p><img src='cid:viveslogo' alt='VIVES Logo' style='height: 40px;'><br><br>
        Dear colleague<br><br>
        During an automated and AI-powered anomaly detection run there were <b>{count} anomalies</b> detected in the latest batch.<br><br>
        View details in:
        <a href="{dashboard_url}">Streamlit anomaly dashboard</a><br>
        <a href="{elastic_url}">Elasticsearch interface</a><br><br>
        This is an automated message.</p>
    </body>
    </html>
    """

    send_email(
        subject="VIVES alert: Anomalies detected in networklogs.",
        body_html=html_body
    )
