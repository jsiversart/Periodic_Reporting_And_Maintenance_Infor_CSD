def fill_usage_table():

    # --- Ensure repo root is in Python path ---
    import sys
    from pathlib import Path

    # Automatically find the repo root (assumes 'core' folder is in the root)
    repo_root = Path(__file__).resolve().parent
    while not (repo_root / "core").exists() and repo_root.parent != repo_root:
        repo_root = repo_root.parent

    sys.path.insert(0, str(repo_root))

    import jaydebeapi
    import pandas as pd
    import sqlite3
    from datetime import datetime
    import calendar
    from core.config import PATHS, JDBC

    # === CONFIGURATION ===

    WAREHOUSE = "25"
    SQLITE_PATH = PATHS["purchdata"]
    TARGET_TABLE = "csdusage"

    # === SQL QUERY ===
    icswu_query = f"""WITH date_m AS (
        SELECT MONTH(MAX(transdt)) AS month_max_transdt
        FROM default.icswu
    )
    SELECT 
        w.prod,
        m.vendprod,
        m.arpvendno,
        w.whse as WAREHOUSE,
        w.normusage_2 AS usage_1_mo_back,
        w.normusage_3 AS usage_2_mo_back,
        w.normusage_4 AS usage_3_mo_back,
        w.normusage_5 AS usage_4_mo_back,
        w.normusage_6 AS usage_5_mo_back,
        w.normusage_7 AS usage_6_mo_back,
        w.normusage_8 AS usage_7_mo_back,
        w.normusage_9 AS usage_8_mo_back,
        w.normusage_10 AS usage_9_mo_back,
        w.normusage_11 AS usage_10_mo_back,
        w.normusage_12 AS usage_11_mo_back,
        w.normusage_13 AS usage_12_mo_back,
        CASE 
            WHEN d.month_max_transdt = 1 THEN CAST((w.normusage_11 + w.normusage_10 + w.normusage_9 + w.normusage_8 + w.normusage_7 + w.normusage_6) AS INT)
            WHEN d.month_max_transdt = 2 THEN CAST((w.normusage_12 + w.normusage_11 + w.normusage_10 + w.normusage_9 + w.normusage_8 + w.normusage_7) AS INT)
            WHEN d.month_max_transdt = 3 THEN CAST((w.normusage_13 + w.normusage_12 + w.normusage_11 + w.normusage_10 + w.normusage_9 + w.normusage_8) AS INT)
            WHEN d.month_max_transdt = 4 THEN CAST((w.normusage_2 + w.normusage_13 + w.normusage_12 + w.normusage_11 + w.normusage_10 + w.normusage_9) AS INT)
            WHEN d.month_max_transdt = 5 THEN CAST((w.normusage_3 + w.normusage_2 + w.normusage_13 + w.normusage_12 + w.normusage_11 + w.normusage_10) AS INT)
            WHEN d.month_max_transdt = 6 THEN CAST((w.normusage_4 + w.normusage_3 + w.normusage_2 + w.normusage_13 + w.normusage_12 + w.normusage_11) AS INT)
            WHEN d.month_max_transdt = 7 THEN CAST((w.normusage_5 + w.normusage_4 + w.normusage_3 + w.normusage_2 + w.normusage_13 + w.normusage_12) AS INT)
            WHEN d.month_max_transdt = 8 THEN CAST((w.normusage_6 + w.normusage_5 + w.normusage_4 + w.normusage_3 + w.normusage_2 + w.normusage_13) AS INT)
            WHEN d.month_max_transdt = 9 THEN CAST((w.normusage_7 + w.normusage_6 + w.normusage_5 + w.normusage_4 + w.normusage_3 + w.normusage_2) AS INT)
            WHEN d.month_max_transdt = 10 THEN CAST((w.normusage_8 + w.normusage_7 + w.normusage_6 + w.normusage_5 + w.normusage_4 + w.normusage_3) AS INT)
            WHEN d.month_max_transdt = 11 THEN CAST((w.normusage_9 + w.normusage_8 + w.normusage_7 + w.normusage_6 + w.normusage_5 + w.normusage_4) AS INT)
            WHEN d.month_max_transdt = 12 THEN CAST((w.normusage_10 + w.normusage_9 + w.normusage_8 + w.normusage_7 + w.normusage_6 + w.normusage_5) AS INT)
        END AS Total_Summer_Usage,
        CASE 
            WHEN d.month_max_transdt = 1 THEN CAST((w.normusage_13 + w.normusage_12 + w.normusage_5 + w.normusage_4 + w.normusage_3 + w.normusage_2) AS INT)
            WHEN d.month_max_transdt = 2 THEN CAST((w.normusage_2 + w.normusage_13 + w.normusage_6 + w.normusage_5 + w.normusage_4 + w.normusage_3) AS INT)
            WHEN d.month_max_transdt = 3 THEN CAST((w.normusage_3 + w.normusage_2 + w.normusage_7 + w.normusage_6 + w.normusage_5 + w.normusage_4) AS INT)
            WHEN d.month_max_transdt = 4 THEN CAST((w.normusage_4 + w.normusage_3 + w.normusage_8 + w.normusage_7 + w.normusage_6 + w.normusage_5) AS INT)
            WHEN d.month_max_transdt = 5 THEN CAST((w.normusage_5 + w.normusage_4 + w.normusage_9 + w.normusage_8 + w.normusage_7 + w.normusage_6) AS INT)
            WHEN d.month_max_transdt = 6 THEN CAST((w.normusage_6 + w.normusage_5 + w.normusage_10 + w.normusage_9 + w.normusage_8 + w.normusage_7) AS INT)
            WHEN d.month_max_transdt = 7 THEN CAST((w.normusage_7 + w.normusage_6 + w.normusage_11 + w.normusage_10 + w.normusage_9 + w.normusage_8) AS INT)
            WHEN d.month_max_transdt = 8 THEN CAST((w.normusage_8 + w.normusage_7 + w.normusage_12 + w.normusage_11 + w.normusage_10 + w.normusage_9) AS INT)
            WHEN d.month_max_transdt = 9 THEN CAST((w.normusage_9 + w.normusage_8 + w.normusage_13 + w.normusage_12 + w.normusage_11 + w.normusage_10) AS INT)
            WHEN d.month_max_transdt = 10 THEN CAST((w.normusage_10 + w.normusage_9 + w.normusage_2 + w.normusage_13 + w.normusage_12 + w.normusage_11) AS INT)
            WHEN d.month_max_transdt = 11 THEN CAST((w.normusage_11 + w.normusage_10 + w.normusage_3 + w.normusage_2 + w.normusage_13 + w.normusage_12) AS INT)
            WHEN d.month_max_transdt = 12 THEN CAST((w.normusage_12 + w.normusage_11 + w.normusage_4 + w.normusage_3 + w.normusage_2 + w.normusage_13) AS INT)
        END AS Total_Winter_Usage,
        (
            (CASE WHEN  w.normusage_2 > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN  w.normusage_3 > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN  w.normusage_4 > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN  w.normusage_5 > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN  w.normusage_6 > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN  w.normusage_7 > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN  w.normusage_8 > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN  w.normusage_9 > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN  w.normusage_10 > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN  w.normusage_11 > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN  w.normusage_12 > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN  w.normusage_13 > 0 THEN 1 ELSE 0 END) ) AS mths_w_usge_count,
        CAST((w.normusage_2 + w.normusage_3 + w.normusage_4 + w.normusage_5 + w.normusage_6 + w.normusage_7 + w.normusage_8 + w.normusage_9 + w.normusage_10 + w.normusage_11 + w.normusage_12 + w.normusage_13) AS INT) AS Total_Usage_12_Mo
    FROM 
        default.icswu w
    CROSS JOIN 
        date_m d
    INNER JOIN default.icsw m on m.cono = 1 and m.prod = w.prod and m.whse = '{WAREHOUSE}'
    Where w.cono = 1 and w.whse in ('25','50');
    """

    # === STEP 1: Run Query from External DB ===
    print("Connecting to external database...")
    conn = jaydebeapi.connect(
        JDBC["class"],
        JDBC["url"],
        [JDBC["user"], JDBC["password"]],
        JDBC["jar"]
    )
    curs = conn.cursor()
    curs.execute(icswu_query)

    columns = [desc[0] for desc in curs.description]
    rows = curs.fetchall()
    df = pd.DataFrame(rows, columns=columns)
    curs.close()
    conn.close()
    print("Query complete.")

    # === STEP 2: Rename usage columns by month ===
    today = datetime.today()
    month_list = []

    # month offset: normusage_2 = 1 month ago
    for i in range(1, 13):
        month_offset = (today.month - i - 1) % 12 + 1
        year_offset = (today.month - i - 1) // 12
        month_abbr = calendar.month_abbr[month_offset]
        label = f"{month_abbr} {str(today.year + year_offset)[-2:]} Usage"
        month_list.append(label)

    rename_map = {
        f"usage_{i}_mo_back": month_list[i - 1]
        for i in range(1, 13)
    }

    df.rename(columns=rename_map, inplace=True)


    # === STEP 3: Export to SQLite ===
    try:
        print(f"Exporting to SQLite table '{TARGET_TABLE}'...")
        with sqlite3.connect(SQLITE_PATH) as sqlite_conn:
            df.to_sql(TARGET_TABLE, sqlite_conn, if_exists='replace', index=False)
        print(f"Successfully wrote {len(df)} rows to {TARGET_TABLE}.")
        print(f"Usage table fill complete.")
    except Exception as e:
        print(f"Error writing to SQLite: {e}")
        
