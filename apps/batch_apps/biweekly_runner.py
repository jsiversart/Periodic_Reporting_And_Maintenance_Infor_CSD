# biweekly_runner.py


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
from etl.etl_utils import remote_to_csv
from sql_queries.Biweekly_SQL import SUPERSEDING_OAN_SQL, TRANE_ORDER_XREF_SQL, COST_ADJ_SQL
from apps.mass_maint_apps.PDSP_UPDATES import pdsp_updates



# --- Runner Functions ---

today_str = datetime.now().strftime('%m%d%y')

def notify():
    SUBJECT ="Biweekly Runner Notification"
    BODY=f"""Runner complete. Please check the analyst folder for outputs and email drafts.
        
        To Do:

        - Send draft to {CONTACTS["manager_first_name"]}: Superseding OAN Report
        - Modify Trane Order Xref to apply formatting and barcodes, then review output and send to Melissa, Jimmy, and QC
        - Review Cost Adj Report 
        - Process PDSP updates (in PDSP subfolder of working directory)
        """

    send_email(
        subject=SUBJECT,
        body=BODY)

def run_PDSP_Updates():
    print("Running PDSP updates...")
    pdsp_updates()

def run_superseding_oan_report():
    print("Running superseding OAN report...")

    # Step 1: Build report
    file_name = f"Superseding_OAN_Report{datetime.now().strftime('%m%d%y')}"
    filepath = PATHS["oantostock"] / f"{file_name}.csv"
    remote_to_csv(file_name, JDBC, SUPERSEDING_OAN_SQL, "Superseding_OAN_Report", PATHS["oantostock"],log_items=None)

    # Step 2: Generate email draft
    email_body = f"""
    Good morning {CONTACTS["manager_first_name"]},

    Please review this list and let me know which of these should be changed to stock.

    Thanks, 

    {CONTACTS["email_signoff_ln_1"]}
    {CONTACTS["email_signoff_ln_2"]}
    """
    draft_path = create_email_draft(
        to=CONTACTS["manager_email"],
        cc="",
        bcc=CONTACTS["user_email"],
        subject=f"OAN high usage products that supersede something, {today_str}",
        body=email_body,
        attachments=[filepath],
        save_path=f"{PATHS['analyst']}/Sup to OAN {today_str}.eml"
    )

    print(f"Draft saved: {draft_path}")
    print(f"Report saved: {filepath}")
    print("superseding OAN script complete")

def run_cost_adj_report():
    print("Running Cost Adj Report...")
    file_name = f"CostAdjReport{datetime.now().strftime('%m%d%y')}"
    remote_to_csv(file_name, JDBC, COST_ADJ_SQL, "CostAdjReport", PATHS["analyst"],log_items=None)

def run_trane_order_xref():
    print("Running Trane Order Xref Report...")
    file_name = f"Trane_Order_Xref_{datetime.now().strftime('%m%d%y')}"
    remote_to_csv(file_name, JDBC, TRANE_ORDER_XREF_SQL, "Trane_Order_Xref", PATHS["analyst"],log_items=None)


# --- Main entry point ---
if __name__ == "__main__":
    print("Starting biweekly runner")
    run_PDSP_Updates()
    run_cost_adj_report()
    run_trane_order_xref()
    run_superseding_oan_report()
    notify()
    print("Biweekly runner complete")
