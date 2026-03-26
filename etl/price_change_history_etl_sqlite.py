
"""
PRICE_CHANGE_HISTORY_ETL_SQLITE.PY
------------------------------------------------------------

OVERVIEW
--------
This script builds and maintains a local SQLite analytics database
used for price change tracking and realized savings analysis.

It pulls historical pricing data from Infor (via JDBC),
loads structured Excel logs, and stores everything in a
normalized SQLite schema using UPSERT logic.

All loads are safe to re-run and are idempotent.


TABLES MAINTAINED
-----------------

1) price_change_history
   Source: Infor (allvariations('icsw'))
   Grain: (prod, whse, arpvendno, period_month)
   Purpose:
     - Stores derived monthly starting and ending pricing
     - Captures cost, replacement cost, base price, list price,
       status, and company rank at month boundaries
     - Excludes current open month
     - Used for historical pricing trend and margin analysis

2) announced_price_changes
   Source: Excel log (Announced_Price_Changes_Log.xlsx)
   Grain: (vendor_no, announce_date, effective_date)
   Purpose:
     - Tracks vendor-announced price changes
     - Stores effective month and notes
     - Supports forward-looking cost impact analysis

3) savings_pos
   Source: Excel log (Savings_POs_Log.xlsx)
   Grain: (po)
   Purpose:
     - Maintains controlled list of POs tied to savings initiatives
     - Categorizes each PO by savings_type
     - Drives downstream PO-level realized savings analysis

4) savings_po_lines
   Source: Infor (poel)
   Grain: (pono, posuf, shipprod)
   Purpose:
     - Stores received PO line details for POs in savings_pos
     - Includes qty received and invoice cost
     - Enables realized savings calculations at line level
"""

# TODO (perf):
# - Currently recomputes all historical months every run
# - Easy speedup later: read MAX(period) from SQLite
# - Filter remote query to period_month > last_loaded_period
# - Always exclude current (open) month
# → Turns job into “last closed month only” incremental load

# Automatically find the repo root (assumes 'core' folder is in the root)
import sys
from pathlib import Path


repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))


import os
import sqlite3
import jpype
import jaydebeapi
import pandas as pd
from datetime import datetime
from core.config import PATHS, JDBC

# -----------------------------
# JVM / JDBC Utilities
# -----------------------------

def ensure_jvm(jdbc_jar_path: str):
    """Start JVM once for JDBC access."""
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=[jdbc_jar_path])


def connect_remote(JDBC: dict):
    """Open JDBC connection to remote DB."""
    return jaydebeapi.connect(
        JDBC["class"],
        JDBC["url"],
        [JDBC["user"], JDBC["password"]],
        JDBC["jar"]
    )


# -----------------------------
# Data normalization
# -----------------------------

def normalize(value):
    """
    Convert JDBC / Java objects into SQLite-safe Python primitives.
    """
    if value is None:
        return None

    try:
        # Java numeric types (BigInteger, Long, etc.)
        if hasattr(value, "longValue"):
            return int(value.longValue())

        # Java timestamps / dates → ISO strings
        if hasattr(value, "toString"):
            return str(value.toString())

    except Exception:
        pass

    return value


# -----------------------------
# SQLite schema management
# -----------------------------

def create_schema(conn: sqlite3.Connection):
    """
    Create the price history table and indexes if they do not exist.
    """
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_change_history (
            prod TEXT NOT NULL,
            whse TEXT NOT NULL,
            arpvendno INTEGER,
            period DATE NOT NULL,

            avgcost_starting REAL,
            replcost_starting REAL,
            baseprice_starting REAL,
            listprice_starting REAL,
            statustype_starting TEXT,
            companyrank_starting TEXT,

            avgcost_ending REAL,
            replcost_ending REAL,
            baseprice_ending REAL,
            listprice_ending REAL,
            statustype_ending TEXT,
            companyrank_ending TEXT,

            last_sync_ts TEXT DEFAULT CURRENT_TIMESTAMP,

            PRIMARY KEY (prod, whse, arpvendno, period)
        );
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_icsw_ph_prod
        ON price_change_history(prod);
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_icsw_ph_period
        ON price_change_history(period);
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_icsw_ph_prod_period
        ON price_change_history(prod, period);
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_icsw_ph_prod_arpvendno_period
        ON price_change_history(prod, whse, arpvendno, period);
    """)

    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_icsw_ph_vendor
    ON price_change_history(arpvendno);
    """)

    conn.commit()


# -----------------------------
# UPSERT logic
# -----------------------------

def upsert_rows(conn: sqlite3.Connection, rows: list):
    """
    Insert or update rows into SQLite using natural key.
    """
    sql = """
        INSERT INTO price_change_history (
    prod, whse, arpvendno, period,
    avgcost_starting, replcost_starting, baseprice_starting, listprice_starting,
    statustype_starting, companyrank_starting,
    avgcost_ending, replcost_ending, baseprice_ending, listprice_ending,
    statustype_ending, companyrank_ending
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(prod, whse, arpvendno, period) DO NOTHING;"""

    cursor = conn.cursor()
    cursor.executemany(sql, rows)
    conn.commit()

# -----------------------------
# Excel ETL routine
# -----------------------------

def create_announced_price_changes_schema(conn: sqlite3.Connection):
    """
    Create table for announced price changes if not exists.
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS announced_price_changes (
            vendor_no INTEGER NOT NULL,
            announce_date DATE NOT NULL,
            effective_date DATE NOT NULL,
            effective_mo DATE NOT NULL,
            notes TEXT,
            last_sync_ts TEXT DEFAULT CURRENT_TIMESTAMP,

            PRIMARY KEY (vendor_no, announce_date, effective_date)
        );
    """)
    conn.commit()


def upsert_announced_price_changes(conn: sqlite3.Connection, rows: list):
    """
    Insert or update announced_price_changes.
    Updates notes if row exists and notes changed.
    """
    sql = """
    INSERT INTO announced_price_changes (
        vendor_no, announce_date, effective_date, effective_mo, notes
    ) VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(vendor_no, announce_date, effective_date) 
    DO UPDATE SET 
        notes = excluded.notes,
        last_sync_ts = CURRENT_TIMESTAMP
    WHERE notes IS NOT excluded.notes;
    """
    cursor = conn.cursor()
    cursor.executemany(sql, rows)
    conn.commit()


def sync_announced_price_changes_from_excel(sqlite_db_path: str, excel_path: str):
    """
    Load announced price changes from Excel and upsert into SQLite.
    """
    print(f"📥 Loading announced price changes from {excel_path} ...")
    df = pd.read_excel(excel_path, dtype={"vendor_no": int, "notes": str})
    
    # Normalize dates
    df["announce_date"] = pd.to_datetime(df["announce_date"]).dt.date
    df["effective_date"] = pd.to_datetime(df["effective_date"]).dt.date
    df["effective_mo"] = df["effective_date"].apply(lambda d: d.replace(day=1))

    # Prepare tuples for SQLite
    rows = [
        (row.vendor_no, row.announce_date, row.effective_date, row.effective_mo, row.notes)
        for row in df.itertuples(index=False)
    ]

    sqlite_conn = sqlite3.connect(sqlite_db_path)
    try:
        sqlite_conn.execute("PRAGMA journal_mode = WAL;")
        sqlite_conn.execute("PRAGMA synchronous = NORMAL;")
        sqlite_conn.execute("PRAGMA temp_store = MEMORY;")

        create_announced_price_changes_schema(sqlite_conn)
        upsert_announced_price_changes(sqlite_conn, rows)
        print(f"✅ Announced price changes synced: {len(rows):,} rows")
    finally:
        sqlite_conn.close()

# -----------------------------
# SQLite schema + UPSERT for logged savings POs
# -----------------------------

def create_savings_pos_schema(conn: sqlite3.Connection):
    """
    Create the table for logged savings POs if it doesn't exist.
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS savings_pos (
            po INTEGER NOT NULL,
            savings_type TEXT,
            last_sync_ts TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (po)
        );
    """)
    conn.commit()


def upsert_savings_pos(conn: sqlite3.Connection, rows: list):
    """
    Insert or update logged savings POs.
    Updates savings_type if PO already exists and value changed.
    """
    sql = """
        INSERT INTO savings_pos (po, savings_type)
        VALUES (?, ?)
        ON CONFLICT(po) DO UPDATE SET
            savings_type = excluded.savings_type,
            last_sync_ts = CURRENT_TIMESTAMP
        WHERE savings_type IS NOT excluded.savings_type;
    """
    cursor = conn.cursor()
    cursor.executemany(sql, rows)
    conn.commit()


def sync_savings_pos_from_excel(sqlite_db_path: str, excel_path: str):
    """
    Load logged savings POs from Excel and upsert into SQLite.
    Expected columns: po, savings_type
    """
    print(f"📥 Loading logged savings POs from {excel_path} ...")

    df = pd.read_excel(excel_path, dtype={"po": float, "savings_type": str})

    # Drop completely blank rows
    df = df.dropna(subset=["po"])

    # Ensure PO is integer
    df["po"] = df["po"].astype(int)

    # Optional: strip whitespace from savings_type
    df["savings_type"] = df["savings_type"].astype(str).str.strip()

    rows = [
        (row.po, row.savings_type)
        for row in df.itertuples(index=False)
    ]

    sqlite_conn = sqlite3.connect(sqlite_db_path)
    try:
        sqlite_conn.execute("PRAGMA journal_mode = WAL;")
        sqlite_conn.execute("PRAGMA synchronous = NORMAL;")
        sqlite_conn.execute("PRAGMA temp_store = MEMORY;")

        create_savings_pos_schema(sqlite_conn)
        upsert_savings_pos(sqlite_conn, rows)

        print(f"✅ Logged savings POs synced: {len(rows):,} rows")

    finally:
        sqlite_conn.close()


# -----------------------------
# SQLite schema + UPSERT for savings PO lines
# -----------------------------

def create_savings_po_lines_schema(conn: sqlite3.Connection):
    """
    Create table for savings PO lines if not exists.
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS savings_po_lines (
            pono INTEGER NOT NULL,
            vendno INTEGER,
            ackdt TEXT,
            posuf INTEGER NOT NULL,
            shipprod TEXT NOT NULL,
            qtyrcv REAL,
            invcost REAL,
            unitconv REAL,                 -- NEW
            last_sync_ts TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (pono, posuf, shipprod)
        );
    """)
    conn.commit()


def upsert_savings_po_lines(conn: sqlite3.Connection, rows: list):
    """
    Insert or update savings PO lines.
    """
    sql = """
        INSERT INTO savings_po_lines (
            pono, vendno, ackdt, posuf, shipprod, qtyrcv, invcost, unitconv
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(pono, posuf, shipprod) DO UPDATE SET
            vendno = excluded.vendno,
            ackdt = excluded.ackdt,
            qtyrcv = excluded.qtyrcv,
            invcost = excluded.invcost,
            unitconv = excluded.unitconv,
            last_sync_ts = CURRENT_TIMESTAMP;
    """
    cursor = conn.cursor()
    cursor.executemany(sql, rows)
    conn.commit()


def get_savings_po_list(sqlite_db_path: str):
    """
    Fetch list of POs from local savings_pos table.
    """
    conn = sqlite3.connect(sqlite_db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT po FROM savings_pos;")
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()

def sync_savings_po_lines(JDBC: dict, sqlite_db_path: str):
    """
    Pull PO line detail for savings POs and store locally.
    """

    po_list = get_savings_po_list(sqlite_db_path)


    if not po_list:
        print("⚠️ No savings POs found. Skipping PO line sync.")
        return

    ensure_jvm(JDBC["jar"])
    remote_conn = None
    sqlite_conn = None

    try:
        print("🔌 Connecting to remote DB...")
        remote_conn = connect_remote(JDBC)
        remote_cur = remote_conn.cursor()

        # Build IN clause safely
        po_string = ",".join(str(int(po)) for po in po_list)

        # print(f"PO count: {len(po_list)}")
        # print(f"PO string preview: {po_string[:200]}")


        query = f"""
            SELECT 
                l.pono,
                h.vendno,
                h.ackdt,
                l.posuf,
                l.shipprod,
                l.qtyrcv,
                l.invcost,
                l.unitconv
            FROM poel l
            LEFT JOIN poeh h
                ON h.pono = l.pono
                AND h.cono = 1
                AND h.posuf = 0
            WHERE l.pono IN ({po_string})
            AND l.cono = 1
            AND l.invcost > 0
            AND l.qtyrcv > 0
        """


        print("▶️ Running savings PO lines query...")
        remote_cur.execute(query)

        print("🗄️ Preparing local SQLite...")
        sqlite_conn = sqlite3.connect(sqlite_db_path)
        sqlite_conn.execute("PRAGMA journal_mode = WAL;")
        sqlite_conn.execute("PRAGMA synchronous = NORMAL;")
        sqlite_conn.execute("PRAGMA temp_store = MEMORY;")

        create_savings_po_lines_schema(sqlite_conn)

        BATCH_SIZE = 5_000
        buffer = []
        total = 0

        print("📥 Streaming savings PO lines...")
        while True:
            rows = remote_cur.fetchmany(BATCH_SIZE)
            if not rows:
                break

            for row in rows:
                normalized = [normalize(v) for v in row]

                # Force ackdt to ISO YYYY-MM-DD if present
                if normalized[2]:
                    normalized[2] = str(normalized[2])[:10]

                buffer.append(tuple(normalized))

            upsert_savings_po_lines(sqlite_conn, buffer)
            total += len(buffer)
            print(f"  → {total:,} rows processed")
            buffer.clear()

        print(f"✅ Savings PO lines sync complete: {total:,} rows")

    finally:
        if remote_conn:
            remote_conn.close()
        if sqlite_conn:
            sqlite_conn.close()


# -----------------------------
# Main ETL routine
# -----------------------------

def sync_icsw_price_history(JDBC: dict, sqlite_db_path: str, query: str):
    # old = def sync_icsw_price_history(JDBC: dict, sqlite_db_path: str, query: str, last_loaded_period: str):
    ensure_jvm(JDBC["jar"])
    os.makedirs(os.path.dirname(sqlite_db_path), exist_ok=True)
    
    remote_conn = None
    sqlite_conn = None

    try:
        print("🔌 Connecting to remote DB...")
        remote_conn = connect_remote(JDBC)
        remote_cur = remote_conn.cursor()

        print("▶️ Running query...")
        # Pass last_loaded_period as a parameter
        remote_cur.execute(query)
        # old : remote_cur.execute(query, [last_loaded_period])

        print("🗄️ Preparing local SQLite...")
        sqlite_conn = sqlite3.connect(sqlite_db_path)
        sqlite_conn.execute("PRAGMA journal_mode = WAL;")
        sqlite_conn.execute("PRAGMA synchronous = NORMAL;")
        sqlite_conn.execute("PRAGMA temp_store = MEMORY;")

        create_schema(sqlite_conn)

        BATCH_SIZE = 10_000
        buffer = []
        total = 0

        print("📥 Streaming rows...")
        while True:
            rows = remote_cur.fetchmany(BATCH_SIZE)
            if not rows:
                break

            for row in rows:
                buffer.append(tuple(normalize(v) for v in row))

            upsert_rows(sqlite_conn, buffer)
            total += len(buffer)
            print(f"  → {total:,} rows processed")
            buffer.clear()

        print(f"✅ Sync complete: {total:,} rows")

    finally:
        if remote_conn:
            remote_conn.close()
        if sqlite_conn:
            sqlite_conn.close()



# -----------------------------
# QUERY
# -----------------------------
PRICE_CHANGE_HIST_QUERY = """
WITH base AS (
    SELECT
        prod,
        whse,
        arpvendno,
        avgcost,
        replcost,
        baseprice,
        listprice,
        statustype,
        companyrank,
        transdt,
        CAST(
    SUBSTRING(transdt, 1, 7) || '-01'
    AS DATE
) AS period_month
    FROM infor.allvariations('icsw')
    WHERE cono = 1
    AND whse = '25'
    AND CAST(SUBSTRING(transdt, 1, 7) || '-01' AS DATE)
    < DATE_TRUNC('month', CURRENT_DATE)
    --AND CAST(SUBSTRING(transdt, 1, 7) || '-01' AS DATE) > ?
),
ending_period AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY prod, whse, arpvendno, period_month
               ORDER BY transdt DESC
           ) AS rn
    FROM base
),
starting_period AS (
    SELECT
        m.prod,
        m.whse,
        m.arpvendno,
        m.period_month,
        b.avgcost  AS avgcost_starting,
        b.replcost AS replcost_starting,
        b.baseprice AS baseprice_starting,
        b.listprice AS listprice_starting,
        b.statustype AS statustype_starting,
        b.companyrank AS companyrank_starting,
        ROW_NUMBER() OVER (
            PARTITION BY m.prod, m.whse, m.arpvendno, m.period_month
            ORDER BY b.transdt DESC
        ) AS rn
    FROM (
        SELECT DISTINCT prod, whse, arpvendno, period_month
        FROM base
    ) m
    LEFT JOIN base b
        ON b.prod = m.prod
       AND b.whse = m.whse
       AND b.arpvendno = m.arpvendno
       AND b.transdt < m.period_month
)
SELECT
    d.prod,
    d.whse,
    d.arpvendno, --vendor at period end
    d.period_month AS period,
    /* starting period */
    b.avgcost_starting,
    b.replcost_starting,
    b.baseprice_starting,
    b.listprice_starting,
    b.statustype_starting,
    b.companyrank_starting,
    /* ending period */
    d.avgcost    AS avgcost_ending,
    d.replcost   AS replcost_ending,
    d.baseprice  AS baseprice_ending,
    d.listprice  AS listprice_ending,
    d.statustype AS statustype_ending,
    d.companyrank AS companyrank_ending
FROM ending_period d
LEFT JOIN starting_period b
    ON b.prod = d.prod
   AND b.whse = d.whse
   AND b.period_month = d.period_month
   AND b.rn = 1
WHERE d.rn = 1
AND b.avgcost_starting IS NOT NULL
ORDER BY d.prod, d.period_month;
"""

def main():
    # -----------------------------
    # MAIN
    # -----------------------------

    # #FETCH LAST_LOADED_PERIOD

    print("starting price change history ETL...")
    # sqlite_conn = sqlite3.connect(PATHS["purchdata"])
    # cur = sqlite_conn.cursor()
    # cur.execute("SELECT MAX(period) FROM price_change_history;")
    # last_loaded_period = cur.fetchone()[0]  # e.g., '2026-01-01'
    # sqlite_conn.close()

    # #print(f"Starting ETL from last loaded period: {last_loaded_period}") #I cant actually do this it messes up the starting price

    sync_icsw_price_history(
        JDBC=JDBC,
        sqlite_db_path=PATHS["purchdata"],
        query=PRICE_CHANGE_HIST_QUERY
        # ,
        # last_loaded_period=last_loaded_period
    )


    ANNOUNCED_CHANGES_FILE = PATHS["announced_price_changes_excel"]
    SAVINGS_PO_FILE = PATHS["logged_savings_pos_excel"]

    sync_announced_price_changes_from_excel(
        sqlite_db_path=PATHS["purchdata"],
        excel_path=ANNOUNCED_CHANGES_FILE
    )

    sync_savings_pos_from_excel(
        sqlite_db_path=PATHS["purchdata"],
        excel_path=SAVINGS_PO_FILE)

    sync_savings_po_lines(
        JDBC=JDBC,
        sqlite_db_path=PATHS["purchdata"]
    )

    print("Price change history ETL complete.")

main()