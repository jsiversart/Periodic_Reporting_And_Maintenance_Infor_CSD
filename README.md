# Periodic Reporting & Maintenance — Infor CloudSuite Distribution

Python automation suite that drives recurring reporting and data maintenance workflows for Infor CSD (CloudSuite Distribution). Four cadence-based runner scripts orchestrate everything from weekly SAAMM checks to full month-start reporting packages — pulling from a live ERP via JDBC, transforming data through a local SQLite layer, generating formatted Excel outputs, drafting Outlook emails, and sending Gmail notifications.

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Runners](#runners)
- [Apps & Modules](#apps--modules)
- [Setup](#setup)
- [Configuration](#configuration)
- [Requirements](#requirements)

---

## Overview

Each runner corresponds to a maintenance cadence. Running a script triggers all associated reports, ETL refreshes, catalog exports, and email drafts automatically. The user's remaining work is review and send — not data gathering.

```
weekly_runner.py       →  Run weekly (SAAMM checks, OS approval data)
biweekly_runner.py     →  Run every two weeks (PDSP, cost adj, superseding OAN)
mid_month_runner.py    →  Run mid-month (white goods, core issues, zero base/list)
month_start_runner.py  →  Run at month start (thresholds, ICSPR, price change, catalogs)
```

### Key Technical Patterns

- **JDBC → SQLite bridge**: Heavy ERP queries run over JDBC (Infor Compass); results are cached in a local SQLite database for fast local computation and audit queries (`csdusage_table_fill.py`, `OPs_Catalog_Generator.py`, `Yardi_Catalog_Generator.py`).
- **Parameterized exception lists**: Threshold and ICSPR queries accept a dynamically-built `NOT IN` clause loaded from CSV ignore lists, so new exclusions never require SQL edits.
- **`.eml` draft generation**: `outlook_drafter.py` builds properly-formatted email drafts with attachments that open directly in Outlook — no copy-paste required.
- **Gmail SMTP notifications**: `notifier.py` sends a completion summary after each runner, including context-sensitive to-do reminders.

---

## Project Structure

```
Periodic_Reporting_And_Maintenance_Infor_CSD/
│
├── core/                              # Shared utilities
│   ├── config.py                      # ← you create this from config_sample.py
│   ├── config_sample.py               # Template — copy and fill in your values
│   ├── notifier.py                    # Gmail SMTP notification utility
│   └── outlook_drafter.py            # Outlook .eml draft builder
│
├── etl/                               # Data extraction and loading
│   ├── etl_utils.py                   # JDBC/SQLite query runners, file exporters, list loader
│   ├── csdusage_table_fill.py         # Refreshes csdusage SQLite table from Compass
│   └── price_change_history_etl_sqlite.py  # Price change ETL → SQLite
│
├── sql_queries/                       # All SQL query definitions
│   ├── month_start_SQL.py             # Threshold, ICSPR, price change queries
│   ├── Biweekly_SQL.py                # Superseding OAN, Trane xref, cost adj queries
│   ├── mid_month_SQL.py               # White goods ICSPE, frozen count, core issues, zero base/list
│   └── OSApprovalData_SQL.py          # OS Approval report query
│
├── apps/
│   ├── mass_maint_apps/
│   │   ├── PDSP_UPDATES.py            # PDSP add/remove report generation
│   │   └── weekly_simple_saamm_email.py  # Simple recurring SAAMM checks via email
│   └── reporting_apps/
│       ├── month_end_vendor_reporting.py  # Vendor PO/inventory reports (Excel, formatted)
│       ├── OPs_Catalog_Generator.py   # OPs customer catalog → Excel template
│       └── Yardi_Catalog_Generator.py # Yardi item master + price list → CSV + email draft
│
├── weekly_runner.py
├── biweekly_runner.py
├── mid_month_runner.py
└── month_start_runner.py
```

---

## Runners

### `weekly_runner.py` — Weekly
Runs every week. Sends a Simple SAAMM email with results of standing data-quality checks (core avg cost, arppushfl, pricetype, $0 avgcost items), and pulls the latest OS Approval data report.

| Function | What it does |
|----------|-------------|
| `run_Simple_SAAM()` | Runs 5 standing SQL checks against Compass; emails results as a .txt attachment |
| `run_OSApprovalData()` | Exports OS approval data (product × warehouse QOH, status, ARP warehouse matrix) to CSV |

---

### `biweekly_runner.py` — Every Two Weeks
Processes PDSP pricing records, runs cost adjustment and Trane order cross-reference reports, identifies high-usage OAN products that supersede something, and sends completion notification.

| Function | What it does |
|----------|-------------|
| `run_PDSP_Updates()` | Generates PDSP_ADDS and PDSP_REMOVES files with processing notes |
| `run_cost_adj_report()` | Exports cost adjustment report to CSV |
| `run_trane_order_xref()` | Exports Trane order cross-reference for branch review |
| `run_superseding_oan_report()` | Identifies OAN products with usage that supersede something; creates report + Outlook draft |
| `notify()` | Sends Gmail summary with to-do checklist |

---

### `mid_month_runner.py` — Mid-Month
Runs mid-month checks. Conditionally adds a frozen-type SAAMM reminder to the notification email based on a live frozen-count query.

| Function | What it does |
|----------|-------------|
| `run_zero_base_list_check()` | Finds active products with $0 base or list price |
| `run_core_issue_check()` | Identifies core product setup issues (prodtype, implied/dirty core, descriptions) |
| `run_white_goods_report()` | Finds equipment products missing required state ICSPE records; creates report + Outlook draft |
| `notify()` | Sends Gmail summary; includes frozen SAAMM reminder if frozen count > 350 |

---

### `month_start_runner.py` — Month Start
The most comprehensive runner. Produces threshold reports, ICSPR report, vendor catalogs, price change analysis, and vendor inventory/PO reports — all with Outlook drafts ready to send.

| Function | What it does |
|----------|-------------|
| `run_icspr_report()` | Identifies products needing ICSPR (Freon restriction) records; creates report + Outlook draft |
| `run_threshold_reports()` | Runs threshold reports for WH25, WH50, WH05 Trane, and branch; creates two Outlook drafts |
| `create_vendor_reports()` | Generates formatted Excel PO and inventory reports for a specific vendor |
| `fill_usage_table()` | Refreshes the `csdusage` SQLite table with 12-month usage data from Compass |
| `price_change_etl_and_reporting()` | Runs price change ETL then generates vendor × period × category price analytics report |
| `generate_ops_catalog()` | Builds OPs customer catalog by querying Compass → SQLite → Excel template |
| `generate_yardi_catalog()` | Builds Yardi item master CSV + price list CSV + Outlook draft to Yardi contact |
| `notify()` | Sends Gmail summary with full to-do checklist |

---

## Apps & Modules

### `etl/etl_utils.py`
Core ETL utility library. Key functions:

| Function | Description |
|----------|-------------|
| `remote_to_csv(title, JDBC, query, ...)` | Connects via JDBC, runs query, writes CSV + log |
| `sqlite_to_file(title, db_path, query, ...)` | Queries SQLite, writes CSV or formatted Excel |
| `remote_scalar(JDBC, query)` | Returns a single scalar value from a remote JDBC query |
| `load_list(file_path, col_name)` | Loads a list from .txt, .csv, or .xlsx for use as query parameters |

### `core/outlook_drafter.py`
Builds `.eml` files with attachments that open in Outlook as ready-to-send drafts. Handles Excel, CSV, and generic binary attachments.

### `core/notifier.py`
Lightweight Gmail SMTP wrapper. Supports HTML body, multiple recipients, CC/BCC, and file attachments.

### `apps/mass_maint_apps/weekly_simple_saamm_email.py`
Runs 5 standing data-quality SQL queries against Compass and emails results as a formatted .txt attachment. Designed for recurring review without opening the ERP.

### `apps/mass_maint_apps/PDSP_UPDATES.py`
Generates two PDSP (customer-specific pricing) files: adds (products meeting criteria not yet priced) and removes (priced products whose replacement cost has dropped below threshold). Each file includes embedded processing instructions.

### `apps/reporting_apps/month_end_vendor_reporting.py`
Generates vendor-formatted PO sales and inventory snapshot Excel reports with header styling, auto-fit columns, and auto-filter.

### `apps/reporting_apps/OPs_Catalog_Generator.py`
Two-step catalog: queries Compass for product/price/category data → loads into SQLite → joins with locally-maintained image data → pastes into a pre-formatted Excel template starting at row 20.

### `apps/reporting_apps/Yardi_Catalog_Generator.py`
Three-step Yardi catalog: Compass → SQLite (step 1 item master) → SQLite join with images (step 2 full record) → CSV export. Also generates a monthly price list CSV and creates an Outlook draft to the Yardi contact.

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/jsiversart/Periodic_Reporting_And_Maintenance_Infor_CSD.git
cd Periodic_Reporting_And_Maintenance_Infor_CSD
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure

Copy the sample config and fill in your values:

```bash
cp core/config_sample.py core/config.py
```

Edit `core/config.py` — see [Configuration](#configuration) below for all required keys.

Set your Gmail App Password as an environment variable:

```bash
# Windows
set GMAIL_APP_PASSWORD=your_app_password_here

# Mac/Linux
export GMAIL_APP_PASSWORD=your_app_password_here
```

> `config.py` is listed in `.gitignore` and will never be committed.

### 4. Run a runner

```bash
python weekly_runner.py
python biweekly_runner.py
python mid_month_runner.py
python month_start_runner.py
```

---

## Configuration

All configurable values live in `core/config.py` (created from `config_sample.py`). The full set of required keys:

### PATHS

| Key | Used by | Description |
|-----|---------|-------------|
| `purchdata` | All SQLite apps | Path to local SQLite database |
| `analyst` | Most runners | Root output folder for reports and email drafts |
| `reports` | `month_end_vendor_reporting` | Folder for vendor Excel reports |
| `catalogs` | OPs/Yardi generators | Folder for catalog outputs and templates |
| `oantostock` | `biweekly_runner` | Folder for OAN-to-stock superseding report |
| `pdspadds` | `PDSP_UPDATES` | Folder for PDSP add files |
| `pdsprems` | `PDSP_UPDATES` | Folder for PDSP remove files |
| `osapprovaldata` | `weekly_runner` | Folder for OS approval data exports |
| `threshold_reports` | `month_start_runner` | Folder for threshold report CSVs |
| `thresholdignores` | `month_start_runner` | Path to threshold exception list (CSV with `prod` column) |
| `icspr_reports` | `month_start_runner` | Folder for ICSPR report CSVs |
| `icspr_ignores` | `month_start_runner` | Path to ICSPR exception list (CSV with `prod` column) |

### JDBC

Standard JDBC connection parameters for your Infor Compass instance.

### GMAIL_CREDS

| Key | Description |
|-----|-------------|
| `DEFAULT_TO` | Default recipient list (used when `to_addrs` is not specified) |
| `GMAIL_USER` | Your Gmail address |
| `GMAIL_APP_PASSWORD` | Loaded from `GMAIL_APP_PASSWORD` env variable — never hardcode |

### CONTACTS

| Key | Description |
|-----|-------------|
| `manager_first_name` | Used in email body greetings |
| `manager_email` | Primary recipient for most report drafts |
| `buyer_email` | CC on threshold emails |
| `user_email` | BCC on outgoing drafts; primary recipient for internal notifications |
| `email_signoff_ln_1` | First line of email signature |
| `email_signoff_ln_2` | Second line of email signature |
| `yardi_email` | Recipient for monthly Yardi catalog email |

### Other

| Key | Description |
|-----|-------------|
| `COMPANY_NAME` | Company name string embedded in vendor report SQL |

---

## Requirements

```
pandas
openpyxl
jaydebeapi
JPype1
```

> `smtplib`, `sqlite3`, `pathlib`, `csv`, `shutil`, `glob`, `argparse`, and `email` are all Python standard library — no install needed.

A `requirements.txt` is included in the repo root.
