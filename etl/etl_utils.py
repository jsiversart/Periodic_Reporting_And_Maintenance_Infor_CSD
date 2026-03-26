# etl_utils.py
# --- Ensure repo root is in Python path ---

import sys
from pathlib import Path

# Automatically find the repo root (assumes 'core' folder is in the root)
repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))

import pandas as pd
from pathlib import Path
from typing import List
from pathlib import Path
import os
import csv
import jaydebeapi
from datetime import datetime
import sqlite3

def remote_to_csv(
    title,
    JDBC,
    query,
    table_name,
    output_folder,
    log_items=None,
    params=None
):
    """
    Connects to a remote DB via JDBC, runs a query, writes results to CSV,
    and logs specified info to both console and a text file.

    Params:
        JDBC: JDBC connection dict {class, url, user, password, jar}
        query: SQL query to run
        table_name: logical name for the extract (used for filenames)
        output_folder: directory to save CSV + log file
        log_items: list of (label, value) tuples to add to log and console
    """


    os.makedirs(output_folder, exist_ok=True)

    # Output paths
    csv_path = os.path.join(output_folder, f"{title}.csv")
    log_path = os.path.join(output_folder, f"{title}_log.txt")

    log_lines = []

    def log(msg):
        """Write to console and buffer the message for log file."""
        print(msg)
        log_lines.append(msg)

    conn_remote = None

    try:
        log(f"🔌 Connecting to remote DB for '{table_name}'...")
        conn_remote = jaydebeapi.connect(
            JDBC["class"],
            JDBC["url"],
            [JDBC["user"], JDBC["password"]],
            JDBC["jar"]
        )
        cursor = conn_remote.cursor()

        log("▶️ Running remote query...")
        if params is not None:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        if not results:
            log(f"⚠️ No data returned for {table_name}.")
            return

        # --- Write CSV ---
        log("💾 Writing CSV...")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(results)

        log(f"✅ CSV written: {csv_path}")
        log(f"📦 Rows exported: {len(results):,}")

        # Additional user-specified metadata
        if log_items:
            log("📄 Additional info:")
            for label, value in log_items:
                log(f"   - {label}: {value}")

    except Exception as e:
        log(f"❌ Error processing {title}: {e}")
        raise

    finally:
        try:
            if conn_remote:
                conn_remote.close()
        except:
            pass

        log(f"🔒 Remote connection closed for {title}")

        # --- Write log file ---
        with open(log_path, "w", encoding="utf-8") as f:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"Log created {ts}\n\n")
            for line in log_lines:
                f.write(line + "\n")

        print(f"📝 Log written to: {log_path}")
        print("Done.")
        return csv_path
    
def sqlite_to_file(
    title,
    db_path,
    query,
    table_name,
    output_folder,
    log_items=None,
    params=None,
    output_type="csv"   
):
    """
    Connects to a local SQLite DB, runs a query, writes results to CSV or Excel,
    and logs specified info to both console and a text file.

    Params:
        db_path: path to .db / .sqlite file
        query: SQL query to run
        table_name: logical name for the extract (used for filenames)
        output_folder: directory to save output + log file
        log_items: list of (label, value) tuples to add to log and console
        output_type: "csv" or "excel"
    """


    os.makedirs(output_folder, exist_ok=True)

    # Determine output path
    if output_type == "xlsx":
        file_path = os.path.join(output_folder, f"{title}.xlsx")
    else:
        file_path = os.path.join(output_folder, f"{title}.csv")

    log_path = os.path.join(output_folder, f"{title}_log.txt")

    log_lines = []

    def log(msg):
        print(msg)
        log_lines.append(msg)

    conn = None

    try:
        log(f"🔌 Connecting to SQLite DB for '{table_name}'...")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        log("▶️ Running query...")
        if params is not None:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        if not results:
            log(f"⚠️ No data returned for {table_name}.")
            return

        # --- Write Output ---
        log(f"💾 Writing {output_type.upper()}...")

        if output_type == "xlsx":
            from openpyxl import Workbook

            wb = Workbook()
            ws = wb.active
            ws.title = "Data"

            ws.append(columns)
            for row in results:
                ws.append(row)

            # --- Excel usability enhancements ---
            ws.auto_filter.ref = ws.dimensions
            ws.freeze_panes = "A2"

            wb.save(file_path)

        else:  # default CSV
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(results)

        log(f"✅ File written: {file_path}")
        log(f"📦 Rows exported: {len(results):,}")

        # Additional user-specified metadata
        if log_items:
            log("📄 Additional info:")
            for label, value in log_items:
                log(f"   - {label}: {value}")

    except Exception as e:
        log(f"❌ Error processing {title}: {e}")
        raise

    finally:
        try:
            if conn:
                conn.close()
        except:
            pass

        log(f"🔒 SQLite connection closed for {table_name}")

        # --- Write log file ---
        with open(log_path, "w", encoding="utf-8") as f:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"Log created {ts}\n\n")
            for line in log_lines:
                f.write(line + "\n")

        print(f"📝 Log written to: {log_path}")
        print("Done.")
        return file_path

def remote_scalar(
    JDBC,
    query,
    params=None
):
    """
    Executes a SQL query against a remote DB via JDBC
    and returns a single scalar value.

    Intended for queries like:
        SELECT COUNT(*)
        SELECT MAX(date)
        SELECT price
    """



    conn_remote = None

    try:
        conn_remote = jaydebeapi.connect(
            JDBC["class"],
            JDBC["url"],
            [JDBC["user"], JDBC["password"]],
            JDBC["jar"]
        )

        cursor = conn_remote.cursor()

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        result = cursor.fetchone()

        if result is None:
            return None

        return result[0]  # return first column of first row

    finally:
        if conn_remote:
            conn_remote.close()

def load_list(file_path: str, col_name: str = None) -> List[str]:
    """
    Load a list of strings from a text, CSV, or Excel file.

    Args:
        file_path: Path to the file.
        col_name: Required for CSV/Excel, ignored for TXT.

    Returns:
        List of strings (lines or column values)
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    ext = path.suffix.lower()
    
    if ext == ".txt":
        with open(path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
        return lines

    elif ext == ".csv":
        if not col_name:
            raise ValueError("col_name must be specified for CSV")
        df = pd.read_csv(path)
        if col_name not in df.columns:
            raise ValueError(f"Column '{col_name}' not found in {file_path}")
        return df[col_name].dropna().astype(str).tolist()

    elif ext in [".xls", ".xlsx"]:
        if not col_name:
            raise ValueError("col_name must be specified for Excel")
        df = pd.read_excel(path)
        if col_name not in df.columns:
            raise ValueError(f"Column '{col_name}' not found in {file_path}")
        return df[col_name].dropna().astype(str).tolist()

    else:
        raise ValueError(f"Unsupported file extension: {ext}")


