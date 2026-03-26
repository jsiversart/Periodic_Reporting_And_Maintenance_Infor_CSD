# PDSP updates.py

def pdsp_updates():
    print("Running PDSP updates...")
    # --- Ensure repo root is in Python path ---
    import sys
    from pathlib import Path

    # Automatically find the repo root (assumes 'core' folder is in the root)
    repo_root = Path(__file__).resolve().parent
    while not (repo_root / "core").exists() and repo_root.parent != repo_root:
        repo_root = repo_root.parent

    sys.path.insert(0, str(repo_root))


    from core.config import PATHS, JDBC
    from etl.etl_utils import remote_to_csv




    addfolder = PATHS["pdspadds"]
    addnotes = [
            ("Processing Instructions", "PDSP ADDS"),
            ("Step 1", "Load export file via SAAFT"),
            ("Step 2", "Update via DCDCC- includes headers")
        ]
    addquery = """	WITH cust_and_prodline AS (SELECT 
        distinct i.prodline, i.vendno, p.custno
        FROM default.pdsc p
        JOIN default.icsl i
        ON p.cono = i.cono
        WHERE p.cono = 1
        AND i.vendno <> 999
        AND p.levelcd = 2
        AND RIGHT(p.prod, LENGTH(CAST(i.vendno AS int) || i.prodline)) = CAST(i.vendno AS int) || i.prodline
        AND (CAST(p.enddt AS DATE) > CURRENT_DATE() OR p.enddt is null)
        AND p.statustype = 'True'
        AND (p.custno not in (18000, 17471, 24882, 34262, 18924)) -- specific customer exclusions per Caleb; primarily friendly competitors
        AND (
            CASE 
                -- Customer-specific vendor rules; only case statements resulting in 1 included
                WHEN p.custno in (56817, 57641) THEN 
                    CASE WHEN i.vendno IN (190, 298, 680) THEN 1 
                        WHEN i.vendno = 784 AND i.prodline = 'AFTMK' THEN 1
                    ELSE 0 END
                WHEN p.custno = 91485 THEN
                    CASE WHEN i.vendno = 825 THEN 1
                    WHEN i.vendno = 797 AND i.prodline in ('WCEP','WCLK','WCMSDC','WCPT') THEN 1
                    ELSE 0 END
                -- Default for all others
                ELSE 1
            END = 1
        ) -- case statment filters for specific multi-point exclusions defined by Caleb
        ), -- Exisitng cust and prodline pdsp records; excluding exceptions
        rankedrows AS (SELECT -- rankedrows is misnomer, as Caleb and I decided we could limit to 25 items w usage
            prod, arpvendno, replcost, prodline
            FROM default.icsw WHERE cono = '1' AND whse = '25' AND usagerate > 0 AND prodline IN (SELECT prodline FROM cust_and_prodline)) -- ICSW data as needed
        SELECT 
        1 AS levelcd,
        '' AS pdlevelty,
        c.custno AS custno,
        '' AS custtype,
        '' AS jobno,
        r.prod AS prod,
        '' AS vendno,
        '' AS units,
        '' AS whse,
        date_format(CURRENT_DATE, 'MM/dd/yy') AS startdt,
        '' AS enddt,
        '' AS statustype,
        '' AS refer,
        '' AS promofl,
        '' AS termsdiscfl,
        '' AS termspct,
        '' AS commtype,
        '' AS prctype,
        '' AS pricesheet,
        '' AS price_effectivedate,
        'C' AS priceonty,
        '' AS qtytype,
        '' AS qtybreakty,
        '' AS contractno,
        '' AS modifiernm,
        '' AS modifierrebfl,
        132 AS prcmult1,
        '' AS prcmult2,
        '' AS prcmult3,
        '' AS prcmult4,
        '' AS prcmult5,
        '' AS prcmult6,
        '' AS prcmult7,
        '' AS prcmult8,
        '' AS prcmult9,
        '' AS prcdisc1,
        '' AS prcdisc2,
        '' AS prcdisc3,
        '' AS prcdisc4,
        '' AS prcdisc5,
        '' AS prcdisc6,
        '' AS prcdisc7,
        '' AS prcdisc8,
        '' AS prcdisc9,
        '' AS qtybrk1,
        '' AS qtybrk2,
        '' AS qtybrk3,
        '' AS qtybrk4,
        '' AS qtybrk5,
        '' AS qtybrk6,
        '' AS qtybrk7,
        '' AS qtybrk8,
        '' AS minqty,
        '' AS maxqty,
        '' AS actqty,
        '' AS qtyyymm,
        '' AS maxqtytype,
        '' AS hardmaxqtyfl,
        '' AS pround,
        '' AS ptarget,
        '' AS pexactrnd,
        '' AS hardpricefl,
        '' AS lastuseddt,
        '' AS ovrridepctup,
        '' AS ovrridepctdown,
        '' AS quotefl,
        '' AS quoteno,
        '' AS slchgdt,
        '' AS user1,
        '' AS user2,
        '' AS user3,
        '' AS user4,
        '' AS user5,
        '' AS user6,
        '' AS user7,
        '' AS user8,
        '' AS user9,
        '' AS prod,
        '' AS costbasedon,
        '' AS costmult,
        '' AS costtype
        FROM rankedrows r
        INNER JOIN cust_and_prodline c 
            ON c.prodline = r.prodline 
            AND c.vendno = r.arpvendno
        LEFT JOIN default.pdsc p 
            ON p.cono = 1
            AND p.levelcd = 1
            AND p.custno = c.custno
            AND p.prod = r.prod
        WHERE 
            (p.prod is null or (CAST(p.enddt AS DATE) < CURRENT_DATE()) or p.statustype = 'False')
            AND r.replcost >= 75"""



    removefolder = PATHS["pdsprems"]
    removenotes = [
            ("Processing Instructions", "PDSP REMOVES"),
            ("Step 1", "Go to PDEM and create a new set, pull all price records for customer/prod"),
            ("Step 2", "Select active only, only update exixting, skip next page, run immediate"),
            ("Step 3", "Open set, select all, click ... , export to excel, pull update,price discount record, end date, and status, export."),
            ("Step 4", "Open file.  xlookup with PDSP_REMOVES to find removal records.  Set removal records with update = Yes, end date = today (mm/dd/yyyy), status = Inactive"),
            ("Step 5", "Save as excel file, upload, review, and update.")
        ]
    removequery = """WITH cust_and_prodline AS (SELECT 
        distinct i.prodline, i.vendno, p.custno
        FROM default.pdsc p
        JOIN default.icsl i
        ON p.cono = i.cono
        WHERE p.cono = 1
        AND i.vendno <> 999
        AND p.levelcd = 2
        AND RIGHT(p.prod, LENGTH(CAST(i.vendno AS int) || i.prodline)) = CAST(i.vendno AS int) || i.prodline
        AND statustype = 'True'
        AND (CAST(p.enddt AS DATE) > CURRENT_DATE() OR p.enddt is null)), -- Exisitng cust and prodline pdsp records
    rankedrows AS (SELECT
        prod, arpvendno, replcost, prodline, ROW_NUMBER() OVER (PARTITION BY prod ORDER BY CASE WHEN whse = '25' THEN 0 ELSE 1 END) AS RowNum
        FROM default.icsw WHERE cono = '1' AND prodline IN (SELECT prodline FROM cust_and_prodline)) -- ICSW data as needed
    SELECT p.pdrecno, 'Yes' as "update"
    --p.custno, p.prod, r.prodline, r.arpvendno, r.replcost 
    FROM default.pdsc p
        LEFT JOIN rankedrows r on r.RowNum = 1 and p.prod = r.prod 
        INNER JOIN cust_and_prodline c on c.prodline = r.prodline AND c.custno = p.custno and c.vendno = r.arpvendno
        WHERE 
        cono = 1
        AND levelcd = 1
        AND statustype = 'True'
        AND (CAST(enddt AS DATE) > CURRENT_DATE() OR enddt is null)
        AND r.replcost < 75
        AND prod not in (SELECT prod FROM default.pdsc WHERE cono = 1 AND custno = 0 AND upper(qtybreakty) = 'P' AND (whse IS NULL OR whse = '') AND statustype = 'True' AND (CAST(enddt AS DATE) > CURRENT_DATE() OR enddt is null))"""



    remote_to_csv(
        "PDSP_ADDS",
        JDBC,
        addquery,
        "ICSW",
        addfolder,
        addnotes
    )

    remote_to_csv(
        "PDSP_REMOVES",
        JDBC,
        removequery,
        "ICSW",
        removefolder,
        removenotes
    )
