# weekly_simple_saamm_email.py

def simple_saamm():
    """Main function to run simple SAAMM email process."""

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
    from core.notifier import send_email
    from core.config import PATHS, JDBC, CONTACTS
    from tempfile import NamedTemporaryFile
    import jaydebeapi
    #import sqlite3 #FOR SQLITE
    

    # === CONFIGURATION ===
    #DB_PATH = PATHS["purchdata"] #FOR SQLITE
    EMAIL_SUBJECT = "Simple SAAMM Updates"
    RECIPIENTS = [CONTACTS["user_email"]]

    # === DEFINE YOUR QUERIES ===
    QUERIES = [
        # 1️⃣ CORE AVG COST TO 55
        ("-- CORE AVG COST TO 55 [SAAMM = ADJ AVGCOST CORES TEMPLATE]", """select distinct prod from default.icsw where (prod LIKE 'IC%') AND 
            prod not in ('TNK50','ICTNK50','DCTNK50','TNKEXCHANGE','ICTNKEXCHANGE','DCTNKEXCHANGE','MCT','ICMCT','DCMCT','40','IC40','DC40','20','IC20','DC20','B','ICB','DCB','MC','ICMC','DCMC','N20','ICN20','DCN20','MC20','ICMC20','DCMC20','60','IC60','DC60') 
            AND (arpvendno = '360' AND avgcost <> 55) AND cono = 1"""),

        # 2️⃣ CORE AVG COST TO 60
        ("-- CORE AVG COST TO 60 [SAAMM = ADJ AVGCOST CORES TEMPLATE]", """select distinct prod from default.icsw 
            where 
                (prod LIKE 'IC%') 
                AND 
                prod not in 
                    ('TNK50','ICTNK50','DCTNK50','TNKEXCHANGE','ICTNKEXCHANGE','DCTNKEXCHANGE','MCT','ICMCT','DCMCT','40','IC40','DC40','20','IC20',
                    'DC20','B','ICB','DCB','MC','ICMC','DCMC','N20','ICN20','DCN20','MC20','ICMC20','DCMC20','60','IC60','DC60') 
                AND (arpvendno = '825' AND avgcost <> 60)
                AND cono = 1"""),

        # 3️⃣ ARPPUSHFL TO TRUE
        ("-- ARPPUSHFL TO TRUE [SAAMM = ARPPUSHFL TEMPLATE]", """select distinct prod
            from default.icsw
            where cono = 1 and upper(statustype) = 'S' and arppushfl = 'false'
            """),
        
        # 4 PRICETYPE TO DEAL
        ("--PRICETYPE TO DEAL [SAAMM = PRICETYPE TEMPLATE]","""select distinct w.prod from default.icsw w 
        left join default.icsp p on w.prod= p.prod and p.cono = 1 where w.cono = 1 and w.pricetype <> 'DEAL' and p.statustype <> 'L' and p.prodtype in ('S','R')"""),

        # 5 AVGCOST TO MATCH REPLCOST, ITEMS W $0 AVGCOST AND (QUANTITY ON HAND OR SET UP IN PAST 30 DAYS)
        ("-- AVGCOST TO MATCH REPLCOST, **IN SAAMM MUST COPY REPLCOST OVER TO AVGCOST**, [SAAMM = AVGCOST 2 REPLCOST TEMPLATE]", 
        """WITH first_seen AS (
                SELECT
                    prod,
                    whse,
                    MIN(transdt) AS first_transdt
                FROM infor.allvariations('icsw')
                WHERE cono = 1
                GROUP BY prod, whse
            )
            SELECT DISTINCT c.prod
            FROM default.icsw c
            JOIN first_seen f
                ON f.prod = c.prod
            WHERE c.cono = 1
            AND c.avgcost = 0
            AND c.prod NOT IN ('WHITE-GOODS-FEE-$2-$3')
            AND (
                    c.qtyonhand > 0
                    OR (
                        c.qtyonhand = 0
                        AND CAST(f.first_transdt AS DATE) >= DATEADD(day, -30, CURRENT_DATE)
                    )
                )""")
    ]



    def run_queries_and_email():
        """Run SQL queries, write results to file, and email safely."""
        
        print("🔗 Connecting to remote JDBC database...")

        # print(f"🔗 Connecting to database: {DB_PATH}") #FOR SQLITE

        conn_remote = jaydebeapi.connect(
        JDBC["class"],
        JDBC["url"],
        [JDBC["user"], JDBC["password"]],
        JDBC["jar"]
        )
        
        # cursor_remote = conn_remote.cursor() #FOR SQLITE
        # conn = sqlite3.connect(DB_PATH) #FOR SQLITE
        body_parts = []

        for comment, query in QUERIES:
            try:
                df = pd.read_sql_query(query, conn_remote)
                if not df.empty:
                    # Flatten results into a comma-separated string
                    flat_results = ",".join(df.iloc[:, 0].astype(str).tolist())
                    csv_text = flat_results
                else:
                    csv_text = "(no results)"
                section = f"{comment}\n{csv_text}\n"
                body_parts.append(section)
                print(f"✅ Ran query: {comment}")
            except Exception as e:
                body_parts.append(f"{comment}\n⚠️ Error running query: {e}\n")
                print(f"❌ Error in query {comment}: {e}")

        conn_remote.close()

        # --- Write results to temp file ---
        tmpfile = NamedTemporaryFile(delete=False, suffix=".txt", mode="w", encoding="utf-8")
        tmpfile.write("\n\n".join(body_parts))
        tmpfile.close()

        # --- Compose short, safe email body ---
        body_text = (
            "Hello,\n\n"
            "Attached are the results from today's Simple Mass Maintenance checks.\n"
            "Each section includes the query label and any product codes returned.\n\n"
            "If a section says '(no results)', that means no discrepancies were found.\n\n"
            "— Purchasing Automation Bot"
        )

        # --- Send the email ---
        send_email(
            subject=EMAIL_SUBJECT,
            body=body_text,
            to_addrs=RECIPIENTS,
            attachments=[tmpfile.name]
        )

        print(f"📧 Email sent successfully with attachment: {tmpfile.name}")


    run_queries_and_email()
        
        