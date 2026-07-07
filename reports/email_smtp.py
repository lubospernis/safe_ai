"""Shared Gmail SMTP send helper for the newsletter scripts.

Required environment variables:
  GMAIL_ADDRESS   — the Gmail address to send from (e.g. yourname@gmail.com)
  GMAIL_16CHAR     — a Gmail App Password (16 characters, no spaces) — NOT your
                     normal Gmail password. Generate one at
                     https://myaccount.google.com/apppasswords (requires 2FA enabled).
"""

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587


def send_email(to_address: str, subject: str, html: str) -> None:
    """Send a single HTML email via Gmail SMTP (STARTTLS). Raises on failure —
    callers are expected to catch per-recipient so one failure doesn't abort a batch."""
    gmail_address = os.environ["GMAIL_ADDRESS"]
    gmail_app_password = os.environ["GMAIL_16CHAR"]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = gmail_address
    msg["To"] = to_address
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(gmail_address, gmail_app_password)
        server.sendmail(gmail_address, [to_address], msg.as_string())
