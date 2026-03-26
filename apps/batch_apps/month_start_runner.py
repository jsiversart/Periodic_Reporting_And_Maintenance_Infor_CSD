# month_start_runner.py


import sys
from pathlib import Path


#  --- Ensure repo root is in Python path ---

repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))

from datetime import datetime
from core.config import PATHS, JDBC, CONTACTS
from core.notifier import send_email
from core.outlook_drafter import create_email_draft
from etl.etl_utils import load_list, remote_to_csv, sqlite_to_file
from sql_queries.month_start_SQL import PRICE_CHANGE_QUERY, TWENTYFIVE_THRESHOLD_QUERY, FIFTY_THRESHOLD_QUERY, FIVE_TRANE_THRESHOLD_QUERY, BRANCH_THRESHOLD_QUERY, ICSPR_UPLOAD_READY_QUERY
from etl.csdusage_table_fill import fill_usage_table
from etl.price_change_history_etl_sqlite import main as run_price_change_history_etl
from apps.reporting_apps.month_end_vendor_reporting import create_vendor_reports
from apps.reporting_apps.OPs_Catalog_Generator import generate_ops_catalog
from apps.reporting_apps.Yardi_Catalog_Generator import generate_yardi_catalog




# import etl.etl_utils
# print("Using remote_to_csv from:", etl.etl_utils.__file__)
# print("Function params:", etl.etl_utils.remote_to_csv.__code__.co_varnames)


# --- Runner Functions ---

today_str = datetime.now().strftime('%m%d%y')

def notify():
    SUBJECT ="Month Start Runner Notification"
    #logic based email inclusions
    logical_email_lines = ""
    BODY=f"""Runner complete. Please check the analyst folder for outputs and email drafts.
        
        To Do:

        - Send ICSPR report draft to {CONTACTS["manager_first_name"]}
        - Send price change report draft to {CONTACTS["manager_first_name"]}
        - Send threshold email drafts (2) to {CONTACTS["manager_first_name"]} / buyer
        - Share vendor reports with vendors
        - Send Yardi email draft to Rep
        - Upload OPs catalog (posts to Analyst Files\Working directory\Catalogs)

        {logical_email_lines}
        """

    send_email(
        subject=SUBJECT,
        body=BODY)

def run_icspr_report():
    print("Running ICSPR report...")

    # Load the ignore list
    icspr_ignore_prod_list = load_list(PATHS["icspr_ignores"], "prod")
    
    # Build only the inner placeholders for parameterized query
    # SQL template already wraps with: AND usge.prod NOT IN ({placeholders})
    if icspr_ignore_prod_list:
        placeholders = ",".join(["?"] * len(icspr_ignore_prod_list))
    else:
        placeholders = "'__NO_MATCH__'"  # dummy value — valid SQL, never matches a real prod

    today_str = datetime.now().strftime('%m%d%y')

    reports_to_email = [
        ("ICSPR_Report", ICSPR_UPLOAD_READY_QUERY, icspr_ignore_prod_list),
    ]

    # Keep track of file paths to attach
    attachments_main = []

    for title, query_template, param_list in reports_to_email:
        query = query_template.format(placeholders=placeholders)
        file_name = f"{title}_{today_str}"
        filepath = f"{PATHS['icspr_reports']}/{file_name}.csv"
        remote_to_csv(
            title=file_name,
            JDBC=JDBC,
            query=query,
            table_name=title,
            output_folder=PATHS["icspr_reports"],
            log_items=None,
            params=param_list
        )
        print(f"DEBUG: {title} returned filepath: {filepath}")  
        attachments_main.append(filepath)

    # Generate email draft for report
    email_body_main = F"""
                Good morning {CONTACTS["manager_first_name"]},

                Please see the attached ICSPR report.  Let me know what to update and/or exclude from future reporting.

                When I hear back from you, I will add any exceptions to my exception list (icspr_report_ignore_list.csv), and use the Mass Maintenance template "ICSPR TEMPLATE" to load in new records.

                Thanks,

                {CONTACTS["email_signoff_ln_1"]}
                {CONTACTS["email_signoff_ln_2"]}
                """
    draft_path_main = create_email_draft(
        to=CONTACTS["manager_email"],
        cc="",
        bcc="",
        subject=f"ICSPR Report {today_str}",
        body=email_body_main,
        attachments=attachments_main,
        save_path=f"{PATHS['analyst']}/ICSPR_Report_{today_str}.eml"
    )
    print(f"Main ICSPR email draft saved: {draft_path_main}")

   
def run_threshold_reports():
    print("Running threshold report...")

    # Load the ignore list
    threshold_ignore_prod_list = load_list(PATHS["thresholdignores"], "prod")
    
    # Build only the inner placeholders for parameterized query
    # SQL template already wraps with: AND usge.prod NOT IN ({placeholders})
    if threshold_ignore_prod_list:
        placeholders = ",".join(["?"] * len(threshold_ignore_prod_list))
    else:
        placeholders = "'__NO_MATCH__'"  # dummy value — valid SQL, never matches a real prod

    today_str = datetime.now().strftime('%m%d%y')

    reports_to_email = [
        ("25_threshold_report", TWENTYFIVE_THRESHOLD_QUERY, threshold_ignore_prod_list),
        ("50_threshold_report", FIFTY_THRESHOLD_QUERY, threshold_ignore_prod_list),
        ("05_Trane_OAN_threshold_report", FIVE_TRANE_THRESHOLD_QUERY, None)
    ]
    branch_report = ("Branch_threshold_report", BRANCH_THRESHOLD_QUERY, None)

    # Keep track of file paths to attach
    attachments_main = []

    for title, query_template, param_list in reports_to_email:
        query = query_template.format(placeholders=placeholders)
        file_name = f"{title}_{today_str}"
        filepath = f"{PATHS['threshold_reports']}/{file_name}.csv"
        remote_to_csv(
            title=file_name,
            JDBC=JDBC,
            query=query,
            table_name=title,
            output_folder=PATHS["threshold_reports"],
            log_items=None,
            params=param_list
        )
        print(f"DEBUG: {title} returned filepath: {filepath}")  # <-- add this
        attachments_main.append(filepath)

    # Generate email draft for 25, 50, and 5 reports
    email_body_main = """
                Good morning team,

                Please see the attached Threshold reports.  Let me know what to update and/or exclude from future reporting.

                Thanks,

                Julian Sivers | Purchasing Analyst
                D&L Parts Company
                """
    draft_path_main = create_email_draft(
        to=CONTACTS["manager_email"],
        cc=CONTACTS["buyer_email"],
        bcc=CONTACTS["user_email"],
        subject=f"Threshold Reports (25, 50, 05 Trane) {today_str}",
        body=email_body_main,
        attachments=attachments_main,
        save_path=f"{PATHS['analyst']}/Threshold_Reports_{today_str}.eml"
    )
    print(f"Main threshold email draft saved: {draft_path_main}")

    # Step 2: Generate Branch report separately
    branch_title, branch_query_template, _ = branch_report
    branch_query = branch_query_template.format(placeholders=placeholders)
    branch_file_name = f"{branch_title}_{today_str}"
    branch_filepath = f"{PATHS['threshold_reports']}/{branch_file_name}.csv"
    remote_to_csv(
        title=branch_file_name,
        JDBC=JDBC,
        query=branch_query,
        table_name=branch_title,
        output_folder=PATHS["threshold_reports"],
        log_items=None,
        params=None
    )

    email_body_branch = f"""
            Good morning team,

            Please see attached, I will update once I receive the cleaned up version of this sheet.

            Thanks,

            Julian Sivers | Purchasing Analyst
            D&L Parts Company
            """
    draft_path_branch = create_email_draft(
        to=CONTACTS["manager_email"],
        cc="",
        bcc=CONTACTS["user_email"],
        subject=f"Branch Threshold Report {today_str}",
        body=email_body_branch,
        attachments=[branch_filepath],
        save_path=f"{PATHS['analyst']}/Branch_Threshold_Report_{today_str}.eml"
    )
    print(f"Branch email draft saved: {draft_path_branch}")

def price_change_etl_and_reporting():
    print("Running price change ETL and reporting...")
    
    run_price_change_history_etl()

    today_str = datetime.now().strftime('%m%d%y')

    reports_to_email = [
        ("price_change_summary_vendor_period_category", PRICE_CHANGE_QUERY, None),
    ]
    # Keep track of file paths to attach
    attachments_main = []

    for title, query, param_list in reports_to_email:
        file_name = f"{title}_{today_str}"
        filepath = f"{PATHS['analyst']}/{file_name}.csv"
        sqlite_to_file(
            title=file_name,
            db_path=PATHS["purchdata"],
            query=query,
            table_name=title,
            output_folder=PATHS["analyst"],
            log_items=None,
            params=param_list,
            output_type = "xlsx"
        )
        print(f"DEBUG: {title} returned filepath: {filepath}") 
        attachments_main.append(filepath)

    # Generate email draft for 25, 50, and 5 reports
    email_body_main = F"""
                Good morning {CONTACTS["manager_first_name"]},

                Please see the attached for your monthly price change summary report. 

                Thanks,

                {CONTACTS["email_signoff_ln_1"]}
                {CONTACTS["email_signoff_ln_2"]}
                """
    draft_path_main = create_email_draft(
        to=CONTACTS["manager_email"],
        cc="",
        bcc=CONTACTS["user_email"],
        subject=f"Price Change Report {today_str}",
        body=email_body_main,
        attachments=attachments_main,
        save_path=f"{PATHS['analyst']}/Price_Change_Report_{today_str}.eml"
    )
    print(f"Main price change report email draft saved: {draft_path_main}")


# --- Main entry point ---
if __name__ == "__main__":
    print("Starting month start runner")
    run_icspr_report()
    run_threshold_reports()
    create_vendor_reports()
    fill_usage_table()
    price_change_etl_and_reporting() 
    generate_ops_catalog() 
    generate_yardi_catalog()
    notify()
    print("Month start runner complete")
