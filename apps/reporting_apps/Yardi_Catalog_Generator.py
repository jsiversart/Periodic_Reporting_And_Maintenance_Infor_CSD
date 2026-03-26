# Yardi Catalog Generator.py


import sys
from pathlib import Path

 # --- Ensure repo root is in Python path ---
repo_root = Path(__file__).resolve().parent
while not (repo_root / "core").exists() and repo_root.parent != repo_root:
    repo_root = repo_root.parent

sys.path.insert(0, str(repo_root))

import jaydebeapi
import pandas as pd
import sqlite3
import os
from datetime import datetime
from pathlib import Path
import csv
from core.config import PATHS, JDBC, CONTACTS
from core.outlook_drafter import create_email_draft

def generate_yardi_catalog():


    # === CONFIGURATION ===

    SQLITE_PATH = PATHS["purchdata"]
    EXPORT_DIR = PATHS["catalogs"]
    TODAY = datetime.today()
    MONTH_STRING = TODAY.strftime("%B")

    # === SQL SCRIPTS ===
    YARDI_STEP_1_SQL = """WITH RankedRows AS (
        SELECT whse, prod, baseprice, listprice, replcost, vendprod, qtyonhand, arpvendno,
            ROW_NUMBER() OVER (PARTITION BY prod ORDER BY CASE WHEN whse = '25' THEN 0 ELSE 1 END) AS RowNum
        FROM default.icsw
        WHERE cono = '1' AND CAST(arpvendno AS INT) < 999 AND vendprod <> '!999!'
    ),
    ProductDetails AS (
        SELECT DISTINCT RankedRows.whse, RankedRows.prod, RankedRows.baseprice, RankedRows.listprice,
                        RankedRows.replcost, RankedRows.vendprod, RankedRows.qtyonhand, RankedRows.arpvendno,
                        icsp.descrip_1, icsp.prodcat, icsp.altprodgrp, icsp.brandcode, icsp.unitsell, icsp.unitstock
        FROM RankedRows
        JOIN default.icsp ON RankedRows.prod = icsp.prod
        WHERE RankedRows.RowNum = 1 AND icsp.cono = 1
    )
    SELECT
        ProductDetails.arpvendno AS SUPPLIER_CODE,
        CASE
            WHEN ProductDetails.altprodgrp = 'APAC' THEN 'Electronics, Appliances, and Batteries|Appliance Repair|Repair Parts'
            WHEN ProductDetails.altprodgrp = 'APDW' THEN 'Electronics, Appliances, and Batteries|Appliances|Dishwashers'
            WHEN ProductDetails.altprodgrp = 'APGB' THEN 'Electronics, Appliances, and Batteries|Appliances|Disposals'
            WHEN ProductDetails.altprodgrp = 'APIM' THEN 'Electronics, Appliances, and Batteries|Appliances|Ice Makers'
            WHEN ProductDetails.altprodgrp = 'APLA' THEN 'Electronics, Appliances, and Batteries|Appliances|Laundry'
            WHEN ProductDetails.altprodgrp = 'APMW' THEN 'Electronics, Appliances, and Batteries|Appliances|Microwaves'
            WHEN ProductDetails.altprodgrp = 'APRH' THEN 'Electronics, Appliances, and Batteries|Appliances|Range Hoods'
            WHEN ProductDetails.altprodgrp = 'APRG' THEN 'Electronics, Appliances, and Batteries|Appliances|Ranges'
            WHEN ProductDetails.altprodgrp = 'APRF' THEN 'Electronics, Appliances, and Batteries|Appliances|Refrigerators'
            WHEN ProductDetails.altprodgrp = 'APRP' THEN 'Electronics, Appliances, and Batteries|Appliance Repair|Repair Parts'
            WHEN ProductDetails.altprodgrp = 'HVMC' THEN 'HVAC and Refrigeration|Replacement Parts|Parts'
            WHEN ProductDetails.altprodgrp = 'HVAF' THEN 'HVAC and Refrigeration|Replacement Parts|Parts'
            WHEN ProductDetails.altprodgrp = 'HVFU' THEN 'HVAC and Refrigeration|Replacement Parts|Parts'
            WHEN ProductDetails.altprodgrp = 'HVMS' THEN 'HVAC and Refrigeration|Replacement Parts|Parts'
            WHEN ProductDetails.altprodgrp = 'HVPU' THEN 'HVAC and Refrigeration|Replacement Parts|Parts'
            WHEN ProductDetails.altprodgrp = 'HVPT' THEN 'HVAC and Refrigeration|Replacement Parts|Parts'
            WHEN ProductDetails.altprodgrp = 'HVRP' THEN 'HVAC and Refrigeration|Replacement Parts|Parts'
            WHEN ProductDetails.altprodgrp = 'HVSS' THEN 'HVAC and Refrigeration|Replacement Parts|Parts'
            WHEN ProductDetails.altprodgrp = 'HVWU' THEN 'HVAC and Refrigeration|Replacement Parts|Parts'
            WHEN ProductDetails.altprodgrp = 'HVCL' THEN 'HVAC and Refrigeration|Replacement Parts|Parts'
            WHEN ProductDetails.altprodgrp = 'HVUV' THEN 'HVAC and Refrigeration|Replacement Parts|Parts'
            WHEN ProductDetails.altprodgrp = 'TACH' THEN 'HVAC and Refrigeration|Replacement Parts|Parts'
            WHEN ProductDetails.altprodgrp = 'TAFI' THEN 'HVAC and Refrigeration|Replacement Parts|Parts'
            WHEN ProductDetails.altprodgrp = 'TATO' THEN 'HVAC and Refrigeration|Replacement Parts|Parts'
            WHEN ProductDetails.altprodgrp = 'WHRP' THEN 'Electronics, Appliances, and Batteries|Appliance Repair|Repair Parts'
            WHEN ProductDetails.altprodgrp = 'WHWH' THEN 'Electronics, Appliances, and Batteries|Appliances|Water Heaters'
            WHEN ProductDetails.altprodgrp = 'EXCL' THEN 'HVAC and Refrigeration|Replacement Parts|Parts'
            ELSE 'Electronics, Appliances, and Batteries|Appliance Repair|Repair Parts'
        END AS CATEGORY,
        tablecode.descrip AS MANUFACTURER,
        ProductDetails.prod AS SUPPLIER_SKU,
        CASE WHEN ProductDetails.vendprod = '' THEN ProductDetails.prod ELSE ProductDetails.vendprod END AS SUPPLIER_PRODUCT_NAME,
        ProductDetails.descrip_1 AS LONG_DESCRIPTION,
        COALESCE(ProductDetails.unitsell, ProductDetails.unitstock) AS UOM,
        COALESCE(ProductDetails.unitsell, ProductDetails.unitstock) AS QTY_UOM,
        CASE WHEN ProductDetails.vendprod = '' THEN ProductDetails.prod ELSE ProductDetails.vendprod END AS MANUFACTURER_SKU
    FROM ProductDetails
    LEFT JOIN (
        SELECT codeval, descrip FROM default.sasta WHERE cono = 1 AND codeiden = 'BC'
    ) tablecode ON ProductDetails.brandcode = tablecode.codeval
    WHERE ProductDetails.vendprod <> '!999!'
    ORDER BY ProductDetails.prod;
    """

    YARDI_STEP_2_SQL = """SELECT
        SUPPLIER_CODE, CATEGORY, MANUFACTURER, SUPPLIER_SKU, SUPPLIER_PRODUCT_NAME,
        LONG_DESCRIPTION, 
        CASE WHEN UOM IS NULL OR TRIM(UOM) = '' THEN 'EA' ELSE UOM END AS UOM,
        CASE WHEN QTY_UOM IS NULL OR TRIM(QTY_UOM) = '' THEN 'EA' ELSE QTY_UOM END AS QTY_UOM,
        MANUFACTURER_SKU,
        '' AS EMPTY, '' AS END_DATE, '' AS ENLARGE_IMAGE,
        COALESCE(d.imageFull1, d.image1, s.StreamFlowImage1, '') AS DETAIL_IMAGE,
        COALESCE(d.imageFull1, d.image1, s.StreamFlowImage1, '') AS THUMBNAIL_IMAGE,
        '' AS CORE_ICON, '' AS PRODUCT_TYPE, '' AS ATT_UPC, '' AS ATT_PROP_65,
        '' AS ATT_HEIGHT, '' AS ATT_DEPTH, '' AS ATT_WIDTH, '' AS ATT_WEIGHT,
        '' AS ATT_CUBE, '' AS ATT_COLOR, '' AS ATT_Hazmat_Description,
        '' AS ATT_Hazmat_Classification, '' AS ATT_Call_For_Pricing,
        '' AS ATT_Freight_Code, '' AS ATT_Purchase_Multiples, '' AS ATT_MSDS_URL,
        '' AS ATT_EcoSource_Green_Product, '' AS ATT_Green_Certifying_Body_1,
        '' AS ATT_ENERGY_STAR, '' AS ATT_GREEN_SEAL_CERTIFIED,
        '' AS ATT_CARPET_AND_RUG_INSTITUTE
    FROM yardiitemmaster y
    LEFT JOIN ddsexport d ON y.SUPPLIER_SKU = d.ID
    LEFT JOIN sfimages s ON y.SUPPLIER_SKU = s."Item ID";
    """

    YARDI_PRICELIST_SQL = """WITH RankedRows AS (
        SELECT whse, prod, baseprice, listprice, replcost, vendprod, qtyonhand,
            ROW_NUMBER() OVER (PARTITION BY prod ORDER BY CASE WHEN whse = '25' THEN 0 ELSE 1 END) AS RowNum
        FROM default.icsw
        WHERE cono = '1' AND vendprod <> '!999!'
    )
    SELECT prod AS "Seller Sku", baseprice AS UNIT_SELLING_PRICE,
        '' AS PRICE_BREAK, '' AS FROM_QTY, '' AS TO_QTY,
        '' AS START_DATE, '' AS END_DATE, '' AS LEAD_TIME_DAYS, '' AS COMP_ALIAS
    FROM RankedRows
    WHERE RowNum = 1
    ORDER BY prod;
    """

    # === RUN PROCESS ===
    print("Connecting via JDBC...")
    conn_remote = jaydebeapi.connect(
        JDBC["class"],
        JDBC["url"],
        [JDBC["user"], JDBC["password"]],
        JDBC["jar"]
    )
    try:
        cursor_remote = conn_remote.cursor()


        print("Running yardi step 1.sql...")
        cursor_remote.execute(YARDI_STEP_1_SQL)
        columns = [desc[0] for desc in cursor_remote.description]
        data = cursor_remote.fetchall()
        step1_df = pd.DataFrame(data, columns=columns)
    finally:
        conn_remote.close()

    # Save to SQLite
    print("Updating SQLite yardiitemmaster table...")
    with sqlite3.connect(SQLITE_PATH) as conn_sqlite:
        step1_df.to_sql("yardiitemmaster", conn_sqlite, if_exists="replace", index=False)

    print("Running yardi step 2.sql on SQLite...")
    with sqlite3.connect(SQLITE_PATH) as conn_sqlite:
        step2_df = pd.read_sql_query(YARDI_STEP_2_SQL, conn_sqlite)

    # Define the export path
    step2_path = os.path.join(EXPORT_DIR, "Yardi Catalog data item master new.csv")

    # Export to CSV with minimal quoting
    step2_df.to_csv(step2_path, index=False, lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
    print(f"Step 2 export saved to: {step2_path}")

    print("Running Yardi PriceList.sql...")
    cursor_remote.execute(YARDI_PRICELIST_SQL)
    price_df = pd.DataFrame(cursor_remote.fetchall(), columns=[desc[0] for desc in cursor_remote.description])
    price_template_path = os.path.join(EXPORT_DIR, "PriceList_Template.csv")
    price_df.to_csv(price_template_path, index=False, lineterminator='\n', quoting=csv.QUOTE_MINIMAL)

    # Rename price template
    month_named_path = os.path.join(EXPORT_DIR, f"{MONTH_STRING} PriceList_Template.csv")
    # Safely overwrite if the destination already exists
    if os.path.exists(month_named_path):
        os.remove(month_named_path)
    os.rename(price_template_path, month_named_path)

    create_email_draft(
        to=CONTACTS["yardi_email"],
        cc="",
        bcc="",
        subject=f"D&L {MONTH_STRING} Yardi Catalog",
        body=f"""Hello,

            Please process the attached Yardi Catalog data, which applies to all clients/catalogs.

            Let me know if you have any issues.

            Thanks, """,
        attachments=[month_named_path],
        save_path=os.path.join(EXPORT_DIR, f"D&L {MONTH_STRING} Yardi Catalog.eml")
    )


    print(f"\n Yardi Catalog file generated to: {EXPORT_DIR}")
