# mid_month_runner.py
import sys
from pathlib import Path

# --- Ensure repo root is in Python path ---

repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))

from datetime import datetime
from core.config import PATHS, JDBC, CONTACTS
from core.notifier import send_email
from core.outlook_drafter import create_email_draft
from etl.etl_utils import remote_to_csv, remote_scalar
from sql_queries.mid_month_SQL import WG_ICSPE_NEEDED_SQL, FROZ_COUNT_SQL, CORE_ISSUE_CHECK_SQL, ZERO_BASE_LIST_SQL



# --- Runner Functions ---

today_str = datetime.now().strftime('%m%d%y')


def notify():
    print("SQL BEING SENT:", repr(FROZ_COUNT_SQL))
    froz_count = remote_scalar(JDBC, FROZ_COUNT_SQL)
    SUBJECT ="Biweekly Runner Notification"
    #logic based email inclusions

    logical_email_lines = ""
    
    if froz_count > 350:
        logical_email_lines = "- Run SAAMM from [froz fix template] (froztype N-N), update so frosenmmyy/frozentype = blank, frozenmos = 0 "

    BODY=f"""Runner complete. Please check the analyst folder for outputs and email drafts.
        
        To Do:

        - Send drafts to {CONTACTS["manager_first_name"]}: White Goods ICSPE Needed
        - Review Core Issue Check Report and update as needed
        - Review Zero Base / List Check Report and update as needed
        {logical_email_lines}
        """

    send_email(
        subject=SUBJECT,
        body=BODY)

# def run_icsw_adds_check():
#     print("Running ICSW Adds Check Report...")
#     file_name = f"ICSW_Adds_{datetime.now().strftime('%m%d%y')}"
#     remote_to_csv(file_name, JDBC, ICSW_ADDS_CHECK_SQL, "ICSW_Adds_Check", PATHS["analyst"],log_items=None)

def run_zero_base_list_check():
    print("Running Zero Base / List Check Report...")
    file_name = f"Zero_base_list_check_{datetime.now().strftime('%m%d%y')}"
    remote_to_csv(file_name, JDBC, ZERO_BASE_LIST_SQL, "Zero_base_list_check", PATHS["analyst"],log_items=None)

def run_core_issue_check():
    print("Running Core Issue Check Report...")
    file_name = f"Core_issue_check_{datetime.now().strftime('%m%d%y')}"
    remote_to_csv(file_name, JDBC, CORE_ISSUE_CHECK_SQL, "Core_issue_check", PATHS["analyst"],log_items=None)

def run_white_goods_report():
    print("Running white goods report...")

    # Step 1: Build report
    file_name = f"White_Goods_ICSPE_Needed_{datetime.now().strftime('%m%d%y')}"
    filepath = remote_to_csv(file_name, JDBC, WG_ICSPE_NEEDED_SQL, "White_Goods_ICSPE_Needed", PATHS["analyst"],log_items=None)

    # Step 2: Generate email draft
    email_body = f"""
    Good morning {CONTACTS["manager_first_name"]},

    See attached, thanks! 

    {CONTACTS["email_signoff_ln_1"]}
    {CONTACTS["email_signoff_ln_2"]}
    """
    draft_path = create_email_draft(
        to=CONTACTS["manager_email"],
        cc="",
        bcc=CONTACTS["user_email"],
        subject=f"White Goods ICSPE Record Needed, {today_str}",
        body=email_body,
        attachments=[filepath],
        save_path=f"{PATHS['analyst']}/White Goods ICSPE Needed {today_str}.eml"
    )

    print(f"Draft saved: {draft_path}")
    print(f"Report saved: {filepath}")
    print("white goods report script complete")


# --- Main entry point ---
if __name__ == "__main__":
    print("Starting mid month runner")
    run_zero_base_list_check()
    run_core_issue_check()
    run_white_goods_report()
    notify()
    print("Mid month runner complete")
