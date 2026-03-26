# weekly_runner.py


import sys
from pathlib import Path

# --- Ensure repo root is in Python path ---

repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))


from core.config import PATHS, JDBC
from etl.etl_utils import remote_to_csv
from apps.mass_maint_apps.weekly_simple_saamm_email import simple_saamm
from sql_queries.OSApprovalData_SQL import OSAPPROVAL_QUERY




# --- Runner Functions ---

from datetime import datetime

def run_Simple_SAAM():
    print("Running Simple SAAM...")
    simple_saamm()


def run_OSApprovalData():
    print("Running OS Approval Data...")
    file_name = f"OSApprovalData{datetime.now().strftime('%m%d%y')}"
    remote_to_csv(file_name, JDBC, OSAPPROVAL_QUERY, file_name, PATHS["osapprovaldata"])

# --- Main entry point ---
if __name__ == "__main__":
    print("Starting weekly runner")
    run_Simple_SAAM()
    run_OSApprovalData()
    print("Weekly runner complete")
