#outlook_drafter.py

import os
from email.message import EmailMessage
from pathlib import Path
from typing import List, Optional

def create_email_draft(
    to: str,
    cc: Optional[str],
    bcc: Optional[str],
    subject: str,
    body: str,
    attachments: Optional[List[str]] = None,
    save_path: Optional[str] = None
) -> str:
    """
    Create an email draft as a .eml file, properly formatted for Outlook.
    
    Args:
        to: Comma-separated string of recipients
        cc: Comma-separated CC
        bcc: Comma-separated BCC
        subject: Email subject line
        body: Email body (plain text)
        attachments: List of full paths to attach
        save_path: Where to save the .eml file; if None, uses current folder + subject.eml
    
    Returns:
        Full path to saved .eml file
    """
    msg = EmailMessage()
    msg["To"] = to
    if cc:
        msg["Cc"] = cc
    if bcc:
        msg["Bcc"] = bcc
    msg["Subject"] = subject
    
    # Clean line breaks and strip leading/trailing spaces
    clean_body = "\r\n".join([line.strip() for line in body.strip().splitlines()])
    msg.set_content(clean_body)
    
    # Attach files
    if attachments:
        for file in attachments:
            path = Path(file)
            if not path.exists():
                continue
            maintype, subtype = ("application", "octet-stream")
            if path.suffix.lower() in [".xlsx", ".xls"]:
                subtype = "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif path.suffix.lower() == ".csv":
                subtype = "csv"
            with open(path, "rb") as f:
                msg.add_attachment(f.read(),
                                   maintype=maintype,
                                   subtype=subtype,
                                   filename=path.name)
    
    # Save to .eml
    if save_path is None:
        safe_subject = "".join(c if c.isalnum() else "_" for c in subject)
        save_path = os.path.join(os.getcwd(), f"{safe_subject}.eml")
    with open(save_path, "wb") as f:
        f.write(bytes(msg))
    
    return save_path
