import smtplib
# Here are the email package modules we'll need.
from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import os

load_dotenv("../.env")

# Mailjet SMTP server credentials
MAILJET_API_KEY = os.environ["MAILJET_API_KEY"]

MAILJET_SECRET_KEY = os.environ["MAILJET_SECRET_KEY"]
MAILJET_SMTP_SERVER = 'in-v3.mailjet.com'
MAILJET_SMTP_PORT = 587

# Email details
sender_email = os.environ["FROM_EMAIL"]
recipient_email = os.environ["TO_EMAIL"]
subject = "Data Email"
body = "This is a test email sent through Mailjet SMTP in Python."

# Create the email message
message = MIMEMultipart()
message['From'] = sender_email
message['To'] = recipient_email
message['Subject'] = subject
message.attach(MIMEText(body, 'plain'))

try:
    # Create an SMTP session
    smtp_server = smtplib.SMTP(MAILJET_SMTP_SERVER, MAILJET_SMTP_PORT)
    smtp_server.starttls()  # Secure the connection
    smtp_server.login(MAILJET_API_KEY, MAILJET_SECRET_KEY)  # Login with Mailjet API credentials

    # Send the email
    smtp_server.sendmail(sender_email, recipient_email, message.as_string())
    print("Email sent successfully!")

    # Close the SMTP session
    smtp_server.quit()
except Exception as e:
    print(f"Error occurred while sending email: {e}")