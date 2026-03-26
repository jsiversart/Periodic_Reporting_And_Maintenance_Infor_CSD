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
    "analyst":Path(r"path\to\analyst\aka\general\dir"),
    "osapprovaldata": Path(r"path\to\osapprovaldata\dir"),
    "threshold_reports": Path(r"path\to\threshold_reports\dir"),
    "thresholdignores": Path(r"path\to\thresholdignores_list.csv"),
    "icspr_reports": Path(r"path\to\icspr_reports\dir"),
    "icspr_ignores": Path(r"path\to\icspr_ignores_list.csv"),
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

