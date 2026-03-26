# config_paths.py
from pathlib import Path
import os



COMPANY_NAME = "MY COMPANY"

# === PATHS ===
PATHS = {
    "oantostock":Path(r"path\to\OAN to STOCK\dir"),
    "purchdata": Path(r"path\to\database.sqlite"),
    "catalogs": Path(r"path\to\catalog\dir"),
    "reports": Path(r"path\to\reports\dir"),
    "pdspadds": Path(r"path\to\PDSP\dir"),
}

# === JDBC CONFIG ===
JDBC = {
    "class": "",
    "url": "",
    "jar": r"",
    "user": "",
    "password": "",
}

# === EMAIL SETTINGS

GMAIL_CREDS = {
    "DEFAULT_TO":["myemail@company.com"], #  default recipient(s) for gmail notifications
    "GMAIL_USER":"mygmail@gmail.com",     #  your Gmail address ; must create app password and set it as environmental variable
    "GMAIL_APP_PASSWORD":os.getenv("GMAIL_APP_PASSWORD"),  # load from env variable
}

CONTACTS = {
    "manager_first_name": "Name",
    "manager_email": "myboss@company.com",
    "buyer_email": "buyer@company.com",
    "user_email": "youremail@company.com",
    "email_signoff_ln_1": "Your Name | Your Title",
    "email_signoff_ln_2": "Your Company ",
    "yardi_email": "yardi_contact@yardi.com",
}

