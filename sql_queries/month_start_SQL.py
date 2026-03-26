# USE VARIABLES IN QUERY TO INCLUDE EXCEPTIONS, IF ANY,

TWENTYFIVE_THRESHOLD_QUERY = """
WITH usge as (SELECT prod,
    CAST((normusage_2) AS INT) AS usage_1_mo_back,
    CAST((normusage_3) AS INT) AS usage_2_mo_back,
    CAST((normusage_4) AS INT) AS usage_3_mo_back,
    CAST((normusage_5) AS INT) AS usage_4_mo_back,
    CAST((normusage_6) AS INT) AS usage_5_mo_back,
    CAST((normusage_7) AS INT) AS usage_6_mo_back,
    CAST((normusage_8) AS INT) AS usage_7_mo_back,
    CAST((normusage_9) AS INT) AS usage_8_mo_back,
    CAST((normusage_10) AS INT) AS usage_9_mo_back,
    CAST((normusage_11) AS INT) AS usage_10_mo_back,
   	CAST((normusage_12) AS INT) AS usage_11_mo_back,
    CAST((normusage_13) AS INT) AS usage_12_mo_back,
    CASE 
       	WHEN month_max_transdt = 1 THEN CAST((normusage_11 + normusage_10 + normusage_9 + normusage_8 + normusage_7 + normusage_6) AS INT)
		WHEN month_max_transdt = 2 THEN CAST((normusage_12 + normusage_11 + normusage_10 + normusage_9 + normusage_8 + normusage_7) AS INT)
		WHEN month_max_transdt = 3 THEN CAST((normusage_13 + normusage_12 + normusage_11 + normusage_10 + normusage_9 + normusage_8) AS INT)
		WHEN month_max_transdt = 4 THEN CAST((normusage_2 + normusage_13 + normusage_12 + normusage_11 + normusage_10 + normusage_9) AS INT)
		WHEN month_max_transdt = 5 THEN CAST((normusage_3 + normusage_2 + normusage_13 + normusage_12 + normusage_11 + normusage_10) AS INT)
		WHEN month_max_transdt = 6 THEN CAST((normusage_4 + normusage_3 + normusage_2 + normusage_13 + normusage_12 + normusage_11) AS INT)
		WHEN month_max_transdt = 7 THEN CAST((normusage_5 + normusage_4 + normusage_3 + normusage_2 + normusage_13 + normusage_12) AS INT)
		WHEN month_max_transdt = 8 THEN CAST((normusage_6 + normusage_5 + normusage_4 + normusage_3 + normusage_2 + normusage_13) AS INT)
		WHEN month_max_transdt = 9 THEN CAST((normusage_7 + normusage_6 + normusage_5 + normusage_4 + normusage_3 + normusage_2) AS INT)
		WHEN month_max_transdt = 10 THEN CAST((normusage_8 + normusage_7 + normusage_6 + normusage_5 + normusage_4 + normusage_3) AS INT)
		WHEN month_max_transdt = 11 THEN CAST((normusage_9 + normusage_8 + normusage_7 + normusage_6 + normusage_5 + normusage_4) AS INT)
		WHEN month_max_transdt = 12 THEN CAST((normusage_10 + normusage_9 + normusage_8 + normusage_7 + normusage_6 + normusage_5) AS INT)
	END AS Total_Summer_Usage,
    CASE 
        WHEN month_max_transdt = 1 THEN CAST((normusage_13 + normusage_12 + normusage_5 + normusage_4 + normusage_3 + normusage_2) AS INT)
		WHEN month_max_transdt = 2 THEN CAST((normusage_2 + normusage_13 + normusage_6 + normusage_5 + normusage_4 + normusage_3) AS INT)
		WHEN month_max_transdt = 3 THEN CAST((normusage_3 + normusage_2 + normusage_7 + normusage_6 + normusage_5 + normusage_4) AS INT)
		WHEN month_max_transdt = 4 THEN CAST((normusage_4 + normusage_3 + normusage_8 + normusage_7 + normusage_6 + normusage_5) AS INT)
		WHEN month_max_transdt = 5 THEN CAST((normusage_5 + normusage_4 + normusage_9 + normusage_8 + normusage_7 + normusage_6) AS INT)
		WHEN month_max_transdt = 6 THEN CAST((normusage_6 + normusage_5 + normusage_10 + normusage_9 + normusage_8 + normusage_7) AS INT)
		WHEN month_max_transdt = 7 THEN CAST((normusage_7 + normusage_6 + normusage_11 + normusage_10 + normusage_9 + normusage_8) AS INT)
		WHEN month_max_transdt = 8 THEN CAST((normusage_8 + normusage_7 + normusage_12 + normusage_11 + normusage_10 + normusage_9) AS INT)
		WHEN month_max_transdt = 9 THEN CAST((normusage_9 + normusage_8 + normusage_13 + normusage_12 + normusage_11 + normusage_10) AS INT)
		WHEN month_max_transdt = 10 THEN CAST((normusage_10 + normusage_9 + normusage_2 + normusage_13 + normusage_12 + normusage_11) AS INT)
		WHEN month_max_transdt = 11 THEN CAST((normusage_11 + normusage_10 + normusage_3 + normusage_2 + normusage_13 + normusage_12) AS INT)
		WHEN month_max_transdt = 12 THEN CAST((normusage_12 + normusage_11 + normusage_4 + normusage_3 + normusage_2 + normusage_13) AS INT)
	END AS Total_Winter_Usage,
    CAST((normusage_2 + normusage_3 + normusage_4 + normusage_5 + normusage_6 + normusage_7 + normusage_8 + normusage_9 + normusage_10 + normusage_11 + normusage_12 + normusage_13) AS INT) AS Total_Usage_12_Mo
FROM 
    default.icswu pwhu
CROSS JOIN
    (SELECT month(max(transdt)) AS month_max_transdt FROM default.icswu)
WHERE
    pwhu.whse = '25'
    AND pwhu.cono = 1),
SEASONAL AS (select prod, CASE when arpvendno IN --LIST SEASONAL VENDORS; SOURCE "icspdefaults.xlsx"; SYSTEMIZE IN PYTHON? AND LIST SUPCO CAPACTIOR PART #S
    		(17,25,29,40,45,47,48,51,68,85,99,120,210,215,240,242,245,256,259,317,318,325,340,351,370,371,373,375,400,435,445,470,482,488,501,505,533,535,537,540,543,
    		545,548,550,553,559,560,561,580,620,631,634,635,648,649,660,684,685,688,690,710,751,765,767,772,774,775,776,780,787,788,790,792,794,804,805,815,898)
    		THEN 'Y'
    	WHEN prod IN ('CD15-5X440R','CD20-5X440R','CD25-5X440','CD25-5X440R','CD25-7.5X440R','CD30-3X440R','CD30-5X440','CD30-5X440R','CD30-7.5X440R','CD35-3X440R',
    					'CD35-4X440R','CD35-5X440','CD35-5X440R','CD35-7.5X440','CD35-7.5X440R','CD40-10X440R','CD40-3X440R','CD40-5X440','CD40-5X440R','CD40-7.5X440R',
    					'CD45-10X440R','CD45-3X440R','CD45-5X440','CD45-5X440R','CD45-7.5X440R','CD50-10X440R','CD50-5X440R','CD50-7.5X440R','CD55-10X440R','CD55-5X440R',
    					'CD55-7.5X440R','CD60-10X440R','CD60-5X440R','CD60-7.5X440R','CD65-5X440R','CD70-10X440R','CD70-5X440R','CD70-7.5X440R','CD80-10X440R','CD80-5X440R',
    					'CD80-7.5X440R','CR10X440','CR10X440R','CR12.5X440','CR15X440','CR15X440R','CR20X440','CR20X440R','CR25X440','CR25X440R','CR2X440','CR30X440','CR30X440R',
    					'CR35X370','CR35X440','CR35X440R','CR3X440','CR40X440','CR45X440','CR45X440R','CR4X440','CR50X440','CR50X440R','CR55X440','CR55X440R','CR5X440','CR5X440R',
    					'CR60X440R','CR6X440','CR6X440R','CR7.5X440','CR7.5X440R') THEN 'Y' ELSE 'N'
    	END AS seasonal from default.icsw where cono = 1 and whse = '25') --new 12/4/25 to determine seasonality
SELECT 
    usge.prod AS  "Part Number",
    pwh.arpvendno AS "ARP Vendor",
    prod.descrip_1 AS "Description 1",
    prod.descrip_2 AS "Description 2",
    UPPER(pwh.statustype) AS "Status Type",
    pwh.linept AS "Line Point",
    pwh.orderpt AS "Order Point",
    CAST((pwh.minthreshold) AS INT) AS "Min Threshold",
    CAST((pwh.qtyonhand) AS INT) AS "Qty On Hand",
    round(pwh.usagerate,2) AS "Usage Rate",
	usge.usage_1_mo_back,
	usge.usage_2_mo_back,
	usge.usage_3_mo_back,
	usge.usage_4_mo_back,
	usge.usage_5_mo_back,
	usge.usage_6_mo_back,
	usge.usage_7_mo_back,
	usge.usage_8_mo_back,
	usge.usage_9_mo_back,
	usge.usage_10_mo_back,
	usge.usage_11_mo_back,
	usge.usage_12_mo_back,
	usge.Total_Summer_Usage,
    usge.Total_Winter_Usage,
    usge.Total_Usage_12_Mo,
    pwh.prodline AS "Product Line"
FROM 
    default.icsw pwh
INNER JOIN 
    usge ON pwh.prod = usge.prod
INNER JOIN 
    default.icsp prod ON prod.prod = pwh.prod AND prod.cono = 1
LEFT JOIN
	SEASONAL s on s.prod = pwh.prod
WHERE
    pwh.whse = '25'
    AND pwh.cono = 1
     AND ((prod.descrip_1 not like '%FIP%' OR prod.descrip_2 not like '%FIP%') AND (prod.descrip_1 not like '%DO%' OR prod.descrip_2 not like '%DO%'))
    AND usge.Total_Usage_12_Mo >= 4
    AND (
        (CASE WHEN usge.usage_1_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_2_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_3_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_4_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_5_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_6_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_7_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_8_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_9_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_10_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_11_mo_back > 0 THEN 1 ELSE 0 END) ) >= 4
	--AND (pwh.usagectrl = 'B' OR NOT(usge.Total_Summer_Usage = 0 OR usge.Total_Winter_Usage = 0)) -- old seasonality method, removed 12/4/25
    AND (s.seasonal = 'N' OR (((CASE WHEN usge.usage_11_mo_back > 0 THEN 1 ELSE 0 END) + (CASE WHEN usge.usage_12_mo_back > 0 THEN 1 ELSE 0 END)) > 0)) --new usage filter for seasonal added 12/4/25
	AND RIGHT(pwh.prodline, 2) <> 'LK'
	AND (pwh.linept + pwh.orderpt) = 0
	AND pwh.arpvendno <> 999
	AND pwh.arpvendno <> 560
	AND upper(pwh.ordcalcty) <> 'H' -- exclude human order control
	AND usge.prod NOT IN ({placeholders}) -- exclude specific products per Mark
    ORDER BY "ARP Vendor", "Part Number"
"""

FIFTY_THRESHOLD_QUERY = """
WITH usge as (SELECT prod,
    CAST((normusage_2) AS INT) AS usage_1_mo_back,
    CAST((normusage_3) AS INT) AS usage_2_mo_back,
    CAST((normusage_4) AS INT) AS usage_3_mo_back,
    CAST((normusage_5) AS INT) AS usage_4_mo_back,
    CAST((normusage_6) AS INT) AS usage_5_mo_back,
    CAST((normusage_7) AS INT) AS usage_6_mo_back,
    CAST((normusage_8) AS INT) AS usage_7_mo_back,
    CAST((normusage_9) AS INT) AS usage_8_mo_back,
    CAST((normusage_10) AS INT) AS usage_9_mo_back,
    CAST((normusage_11) AS INT) AS usage_10_mo_back,
   	CAST((normusage_12) AS INT) AS usage_11_mo_back,
    CAST((normusage_13) AS INT) AS usage_12_mo_back,
    CASE 
       	WHEN month_max_transdt = 1 THEN CAST((normusage_11 + normusage_10 + normusage_9 + normusage_8 + normusage_7 + normusage_6) AS INT)
		WHEN month_max_transdt = 2 THEN CAST((normusage_12 + normusage_11 + normusage_10 + normusage_9 + normusage_8 + normusage_7) AS INT)
		WHEN month_max_transdt = 3 THEN CAST((normusage_13 + normusage_12 + normusage_11 + normusage_10 + normusage_9 + normusage_8) AS INT)
		WHEN month_max_transdt = 4 THEN CAST((normusage_2 + normusage_13 + normusage_12 + normusage_11 + normusage_10 + normusage_9) AS INT)
		WHEN month_max_transdt = 5 THEN CAST((normusage_3 + normusage_2 + normusage_13 + normusage_12 + normusage_11 + normusage_10) AS INT)
		WHEN month_max_transdt = 6 THEN CAST((normusage_4 + normusage_3 + normusage_2 + normusage_13 + normusage_12 + normusage_11) AS INT)
		WHEN month_max_transdt = 7 THEN CAST((normusage_5 + normusage_4 + normusage_3 + normusage_2 + normusage_13 + normusage_12) AS INT)
		WHEN month_max_transdt = 8 THEN CAST((normusage_6 + normusage_5 + normusage_4 + normusage_3 + normusage_2 + normusage_13) AS INT)
		WHEN month_max_transdt = 9 THEN CAST((normusage_7 + normusage_6 + normusage_5 + normusage_4 + normusage_3 + normusage_2) AS INT)
		WHEN month_max_transdt = 10 THEN CAST((normusage_8 + normusage_7 + normusage_6 + normusage_5 + normusage_4 + normusage_3) AS INT)
		WHEN month_max_transdt = 11 THEN CAST((normusage_9 + normusage_8 + normusage_7 + normusage_6 + normusage_5 + normusage_4) AS INT)
		WHEN month_max_transdt = 12 THEN CAST((normusage_10 + normusage_9 + normusage_8 + normusage_7 + normusage_6 + normusage_5) AS INT)
	END AS Total_Summer_Usage,
    CASE 
        WHEN month_max_transdt = 1 THEN CAST((normusage_13 + normusage_12 + normusage_5 + normusage_4 + normusage_3 + normusage_2) AS INT)
		WHEN month_max_transdt = 2 THEN CAST((normusage_2 + normusage_13 + normusage_6 + normusage_5 + normusage_4 + normusage_3) AS INT)
		WHEN month_max_transdt = 3 THEN CAST((normusage_3 + normusage_2 + normusage_7 + normusage_6 + normusage_5 + normusage_4) AS INT)
		WHEN month_max_transdt = 4 THEN CAST((normusage_4 + normusage_3 + normusage_8 + normusage_7 + normusage_6 + normusage_5) AS INT)
		WHEN month_max_transdt = 5 THEN CAST((normusage_5 + normusage_4 + normusage_9 + normusage_8 + normusage_7 + normusage_6) AS INT)
		WHEN month_max_transdt = 6 THEN CAST((normusage_6 + normusage_5 + normusage_10 + normusage_9 + normusage_8 + normusage_7) AS INT)
		WHEN month_max_transdt = 7 THEN CAST((normusage_7 + normusage_6 + normusage_11 + normusage_10 + normusage_9 + normusage_8) AS INT)
		WHEN month_max_transdt = 8 THEN CAST((normusage_8 + normusage_7 + normusage_12 + normusage_11 + normusage_10 + normusage_9) AS INT)
		WHEN month_max_transdt = 9 THEN CAST((normusage_9 + normusage_8 + normusage_13 + normusage_12 + normusage_11 + normusage_10) AS INT)
		WHEN month_max_transdt = 10 THEN CAST((normusage_10 + normusage_9 + normusage_2 + normusage_13 + normusage_12 + normusage_11) AS INT)
		WHEN month_max_transdt = 11 THEN CAST((normusage_11 + normusage_10 + normusage_3 + normusage_2 + normusage_13 + normusage_12) AS INT)
		WHEN month_max_transdt = 12 THEN CAST((normusage_12 + normusage_11 + normusage_4 + normusage_3 + normusage_2 + normusage_13) AS INT)
	END AS Total_Winter_Usage,
    CAST((normusage_2 + normusage_3 + normusage_4 + normusage_5 + normusage_6 + normusage_7 + normusage_8 + normusage_9 + normusage_10 + normusage_11 + normusage_12 + normusage_13) AS INT) AS Total_Usage_12_Mo
FROM 
    default.icswu pwhu
CROSS JOIN
    (SELECT month(max(transdt)) AS month_max_transdt FROM default.icswu)
WHERE
    pwhu.whse = '50'
    AND pwhu.cono = 1),
SEASONAL AS (select prod, CASE when arpvendno IN --LIST SEASONAL VENDORS; SOURCE "icspdefaults.xlsx"; SYSTEMIZE IN PYTHON? AND LIST SUPCO CAPACTIOR PART #S
    		(17,25,29,40,45,47,48,51,68,85,99,120,210,215,240,242,245,256,259,317,318,325,340,351,370,371,373,375,400,435,445,470,482,488,501,505,533,535,537,540,543,
    		545,548,550,553,559,560,561,580,620,631,634,635,648,649,660,684,685,688,690,710,751,765,767,772,774,775,776,780,787,788,790,792,794,804,805,815,898)
    		THEN 'Y'
    	WHEN prod IN ('CD15-5X440R','CD20-5X440R','CD25-5X440','CD25-5X440R','CD25-7.5X440R','CD30-3X440R','CD30-5X440','CD30-5X440R','CD30-7.5X440R','CD35-3X440R',
    					'CD35-4X440R','CD35-5X440','CD35-5X440R','CD35-7.5X440','CD35-7.5X440R','CD40-10X440R','CD40-3X440R','CD40-5X440','CD40-5X440R','CD40-7.5X440R',
    					'CD45-10X440R','CD45-3X440R','CD45-5X440','CD45-5X440R','CD45-7.5X440R','CD50-10X440R','CD50-5X440R','CD50-7.5X440R','CD55-10X440R','CD55-5X440R',
    					'CD55-7.5X440R','CD60-10X440R','CD60-5X440R','CD60-7.5X440R','CD65-5X440R','CD70-10X440R','CD70-5X440R','CD70-7.5X440R','CD80-10X440R','CD80-5X440R',
    					'CD80-7.5X440R','CR10X440','CR10X440R','CR12.5X440','CR15X440','CR15X440R','CR20X440','CR20X440R','CR25X440','CR25X440R','CR2X440','CR30X440','CR30X440R',
    					'CR35X370','CR35X440','CR35X440R','CR3X440','CR40X440','CR45X440','CR45X440R','CR4X440','CR50X440','CR50X440R','CR55X440','CR55X440R','CR5X440','CR5X440R',
    					'CR60X440R','CR6X440','CR6X440R','CR7.5X440','CR7.5X440R') THEN 'Y' ELSE 'N'
    	END AS seasonal from default.icsw where cono = 1 and whse = '50')  --new 12/4/25 to determine seasonality
SELECT 
    usge.prod AS "Part Number",
    pwh.arpvendno AS "ARP Vendor",
    prod.descrip_1 AS "Description 1",
    prod.descrip_2 AS "Description 2",
    UPPER(pwh.statustype) AS "Status Type",
    pwh.linept AS "Line Point",
    pwh.orderpt AS "Order Point",
    CAST((pwh.minthreshold) AS INT) AS "Min Threshold",
    CAST((pwh.qtyonhand) AS INT) AS "Qty On Hand",
    ROUND(pwh.usagerate,2) AS "Usage Rate",
	usge.usage_1_mo_back,
	usge.usage_2_mo_back,
	usge.usage_3_mo_back,
	usge.usage_4_mo_back,
	usge.usage_5_mo_back,
	usge.usage_6_mo_back,
	usge.usage_7_mo_back,
	usge.usage_8_mo_back,
	usge.usage_9_mo_back,
	usge.usage_10_mo_back,
	usge.usage_11_mo_back,
	usge.usage_12_mo_back,
	usge.Total_Summer_Usage,
    usge.Total_Winter_Usage,
    usge.Total_Usage_12_Mo,
    pwh.prodline AS "Product Line"
FROM 
    default.icsw pwh
INNER JOIN 
    usge ON pwh.prod = usge.prod
INNER JOIN 
    default.icsp prod ON prod.prod = pwh.prod AND prod.cono = 1
LEFT JOIN
	SEASONAL s on s.prod = pwh.prod
WHERE
    pwh.whse = '50'
    AND pwh.cono = 1
    AND usge.Total_Usage_12_Mo >= 4
    AND (
        (CASE WHEN usge.usage_1_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_2_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_3_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_4_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_5_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_6_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_7_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_8_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_9_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_10_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_11_mo_back > 0 THEN 1 ELSE 0 END) ) >= 4
	--AND (pwh.usagectrl = 'B' OR NOT(usge.Total_Summer_Usage = 0 OR usge.Total_Winter_Usage = 0)) -- old seasonality method, removed 12/4/25
    AND (s.seasonal = 'N' OR (((CASE WHEN usge.usage_11_mo_back > 0 THEN 1 ELSE 0 END) + (CASE WHEN usge.usage_12_mo_back > 0 THEN 1 ELSE 0 END)) > 0)) --new usage filter for seasonal added 12/4/25
	AND RIGHT(pwh.prodline, 2) <> 'LK'
	AND (pwh.linept + pwh.orderpt) = 0
	AND ((prod.descrip_1 not like '%FIP%' OR prod.descrip_2 not like '%FIP%') AND (prod.descrip_1 not like '%DO%' OR prod.descrip_2 not like '%DO%'))
	AND pwh.arpvendno <> 999
	AND pwh.arpvendno <> 560
	AND upper(pwh.ordcalcty) <> 'H' -- exclude human order control
	AND usge.prod NOT IN ({placeholders}) -- exclude specific products per Mark
    ORDER BY "ARP Vendor", "Part Number"
"""

FIVE_TRANE_THRESHOLD_QUERY = """WITH usge as (SELECT prod,
    normusage_2 AS usage_1_mo_back,
    normusage_3 AS usage_2_mo_back,
    normusage_4 AS usage_3_mo_back,
    normusage_5 AS usage_4_mo_back,
    normusage_6 AS usage_5_mo_back,
    normusage_7 AS usage_6_mo_back,
    normusage_8 AS usage_7_mo_back,
    normusage_9 AS usage_8_mo_back,
    normusage_10 AS usage_9_mo_back,
    normusage_11 AS usage_10_mo_back,
    normusage_12 AS usage_11_mo_back,
    normusage_13 AS usage_12_mo_back,
    CASE 
       	WHEN month_max_transdt = 1 THEN CAST((normusage_11 + normusage_10 + normusage_9 + normusage_8 + normusage_7 + normusage_6) AS INT)
		WHEN month_max_transdt = 2 THEN CAST((normusage_12 + normusage_11 + normusage_10 + normusage_9 + normusage_8 + normusage_7) AS INT)
		WHEN month_max_transdt = 3 THEN CAST((normusage_13 + normusage_12 + normusage_11 + normusage_10 + normusage_9 + normusage_8) AS INT)
		WHEN month_max_transdt = 4 THEN CAST((normusage_2 + normusage_13 + normusage_12 + normusage_11 + normusage_10 + normusage_9) AS INT)
		WHEN month_max_transdt = 5 THEN CAST((normusage_3 + normusage_2 + normusage_13 + normusage_12 + normusage_11 + normusage_10) AS INT)
		WHEN month_max_transdt = 6 THEN CAST((normusage_4 + normusage_3 + normusage_2 + normusage_13 + normusage_12 + normusage_11) AS INT)
		WHEN month_max_transdt = 7 THEN CAST((normusage_5 + normusage_4 + normusage_3 + normusage_2 + normusage_13 + normusage_12) AS INT)
		WHEN month_max_transdt = 8 THEN CAST((normusage_6 + normusage_5 + normusage_4 + normusage_3 + normusage_2 + normusage_13) AS INT)
		WHEN month_max_transdt = 9 THEN CAST((normusage_7 + normusage_6 + normusage_5 + normusage_4 + normusage_3 + normusage_2) AS INT)
		WHEN month_max_transdt = 10 THEN CAST((normusage_8 + normusage_7 + normusage_6 + normusage_5 + normusage_4 + normusage_3) AS INT)
		WHEN month_max_transdt = 11 THEN CAST((normusage_9 + normusage_8 + normusage_7 + normusage_6 + normusage_5 + normusage_4) AS INT)
		WHEN month_max_transdt = 12 THEN CAST((normusage_10 + normusage_9 + normusage_8 + normusage_7 + normusage_6 + normusage_5) AS INT)
	END AS Total_Summer_Usage,
    CASE 
        WHEN month_max_transdt = 1 THEN CAST((normusage_13 + normusage_12 + normusage_5 + normusage_4 + normusage_3 + normusage_2) AS INT)
		WHEN month_max_transdt = 2 THEN CAST((normusage_2 + normusage_13 + normusage_6 + normusage_5 + normusage_4 + normusage_3) AS INT)
		WHEN month_max_transdt = 3 THEN CAST((normusage_3 + normusage_2 + normusage_7 + normusage_6 + normusage_5 + normusage_4) AS INT)
		WHEN month_max_transdt = 4 THEN CAST((normusage_4 + normusage_3 + normusage_8 + normusage_7 + normusage_6 + normusage_5) AS INT)
		WHEN month_max_transdt = 5 THEN CAST((normusage_5 + normusage_4 + normusage_9 + normusage_8 + normusage_7 + normusage_6) AS INT)
		WHEN month_max_transdt = 6 THEN CAST((normusage_6 + normusage_5 + normusage_10 + normusage_9 + normusage_8 + normusage_7) AS INT)
		WHEN month_max_transdt = 7 THEN CAST((normusage_7 + normusage_6 + normusage_11 + normusage_10 + normusage_9 + normusage_8) AS INT)
		WHEN month_max_transdt = 8 THEN CAST((normusage_8 + normusage_7 + normusage_12 + normusage_11 + normusage_10 + normusage_9) AS INT)
		WHEN month_max_transdt = 9 THEN CAST((normusage_9 + normusage_8 + normusage_13 + normusage_12 + normusage_11 + normusage_10) AS INT)
		WHEN month_max_transdt = 10 THEN CAST((normusage_10 + normusage_9 + normusage_2 + normusage_13 + normusage_12 + normusage_11) AS INT)
		WHEN month_max_transdt = 11 THEN CAST((normusage_11 + normusage_10 + normusage_3 + normusage_2 + normusage_13 + normusage_12) AS INT)
		WHEN month_max_transdt = 12 THEN CAST((normusage_12 + normusage_11 + normusage_4 + normusage_3 + normusage_2 + normusage_13) AS INT)
	END AS Total_Winter_Usage,
    CAST((normusage_2 + normusage_3 + normusage_4 + normusage_5 + normusage_6 + normusage_7 + normusage_8 + normusage_9 + normusage_10 + normusage_11 + normusage_12 + normusage_13) AS INT) AS Total_Usage_12_Mo
FROM 
    default.icswu pwhu
CROSS JOIN
    (SELECT month(max(transdt)) AS month_max_transdt FROM default.icswu)
WHERE
    pwhu.whse = '05'
    AND pwhu.cono = 1)
SELECT 
    CASE WHEN LEFT(usge.prod,1) = '0' THEN '''' + usge.prod ELSE usge.prod END AS  "Part Number",
    pwh.arpvendno AS "ARP Vendor",
    prod.descrip_1 AS "Description 1",
    prod.descrip_2 AS "Description 2",
    UPPER(pwh.statustype) AS "Status Type",
    pwh.linept AS "Line Point",
    pwh.orderpt AS "Order Point",
    pwh.minthreshold AS "Min Threshold",
    pwh.qtyonhand AS "Qty On Hand",
    pwh.usagerate AS "Usage Rate",
	usge.usage_1_mo_back,
	usge.usage_2_mo_back,
	usge.usage_3_mo_back,
	usge.usage_4_mo_back,
	usge.usage_5_mo_back,
	usge.usage_6_mo_back,
	usge.usage_7_mo_back,
	usge.usage_8_mo_back,
	usge.usage_9_mo_back,
	usge.usage_10_mo_back,
	usge.usage_11_mo_back,
	usge.usage_12_mo_back,
	usge.Total_Summer_Usage,
    usge.Total_Winter_Usage,
    usge.Total_Usage_12_Mo,
    pwh.prodline AS "Product Line"
FROM 
    default.icsw pwh
INNER JOIN 
    usge ON pwh.prod = usge.prod
INNER JOIN 
    default.icsp prod ON prod.prod = pwh.prod AND prod.cono = 1
WHERE
    pwh.whse = '05'
    AND pwh.arpvendno = 775
    AND upper(pwh.ordcalcty) <> 'H' -- exclude human order control
    AND upper(pwh.statustype) = 'O'
    AND pwh.cono = 1
    AND ((prod.descrip_1 not like '%FIP%' OR prod.descrip_2 not like '%FIP%') AND (prod.descrip_1 not like '%DO%' OR prod.descrip_2 not like '%DO%'))
    AND usge.Total_Usage_12_Mo >= 4
    AND (
        (CASE WHEN usge.usage_1_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_2_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_3_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_4_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_5_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_6_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_7_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_8_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_9_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_10_mo_back > 0 THEN 1 ELSE 0 END) +
        (CASE WHEN usge.usage_11_mo_back > 0 THEN 1 ELSE 0 END) ) >= 4
--AND (pwh.usagectrl = 'B' OR NOT(usge.Total_Summer_Usage = 0 OR usge.Total_Winter_Usage = 0)) --old seasonality filter removed 12/4/25
AND (((CASE WHEN usge.usage_11_mo_back > 0 THEN 1 ELSE 0 END) + (CASE WHEN usge.usage_12_mo_back > 0 THEN 1 ELSE 0 END)) > 0) --new usage filter for seasonal added 12/4/25
	AND RIGHT(pwh.prodline, 2) <> 'LK'
	AND (pwh.linept + pwh.orderpt) = 0
	AND usge.prod NOT IN ('placeholder')
    ORDER BY "ARP Vendor", "Part Number"
"""

BRANCH_THRESHOLD_QUERY = """WITH usge as 
(SELECT prod, whse,
    linehits_2 AS hits_1_mo_back,
    linehits_3 AS hits_2_mo_back,
    linehits_4 AS hits_3_mo_back,
    linehits_5 AS hits_4_mo_back,
    linehits_6 AS hits_5_mo_back,
    linehits_7 AS hits_6_mo_back,
    linehits_8 AS hits_7_mo_back,
    linehits_9 AS hits_8_mo_back,
    linehits_10 AS hits_9_mo_back,
    linehits_11 AS hits_10_mo_back,
    linehits_12 AS hits_11_mo_back,
    linehits_13 AS hits_12_mo_back,
    CAST((linehits_2 + linehits_3 + linehits_4 + linehits_5 + linehits_6 + linehits_7 + linehits_8 + linehits_9 + linehits_10 + linehits_11 + linehits_12 + linehits_13) AS INT) AS total_hits_12_mo
FROM 
    default.icswu pwhu
WHERE
    pwhu.cono = 1
    AND CAST((linehits_2 + linehits_3 + linehits_4 + linehits_5 + linehits_6 + linehits_7 + linehits_8 + linehits_9 + linehits_10 + linehits_11 + linehits_12 + linehits_13) AS INT)>=3
    AND whse NOT IN ('03', '25', '03C', '04C', '05C','13','50')
    --purpose of union is to combine 13's hits with 50's
	UNION ALL
	SELECT prod, '50' as whse,
    sum(linehits_2) AS hits_1_mo_back,
    sum(linehits_3) AS hits_2_mo_back,
    sum(linehits_4) AS hits_3_mo_back,
    sum(linehits_5) AS hits_4_mo_back,
    sum(linehits_6) AS hits_5_mo_back,
    sum(linehits_7) AS hits_6_mo_back,
    sum(linehits_8) AS hits_7_mo_back,
    sum(linehits_9) AS hits_8_mo_back,
    sum(linehits_10) AS hits_9_mo_back,
    sum(linehits_11) AS hits_10_mo_back,
    sum(linehits_12) AS hits_11_mo_back,
    sum(linehits_13) AS hits_12_mo_back,
    CAST(SUM(linehits_2 + linehits_3 + linehits_4 + linehits_5 + linehits_6 + linehits_7 + linehits_8 + linehits_9 + linehits_10 + linehits_11 + linehits_12 + linehits_13) AS INT) AS total_hits_12_mo
FROM 
    default.icswu pwhu
WHERE
    pwhu.cono = 1
    AND whse in ('13','50')
GROUP BY prod
HAVING CAST(SUM(linehits_2 + linehits_3 + linehits_4 + linehits_5 + linehits_6 + linehits_7 + linehits_8 + linehits_9 + linehits_10 + linehits_11 + linehits_12 + linehits_13) AS INT)>=3
	),
MAIN AS 
(SELECT 
    usge.prod,
    pwh.whse,
    CAST(pwh.arpvendno AS INT) AS arpvendno,
    CASE WHEN
    	pwh.arpvendno IN --LIST SEASONAL VENDORS; SOURCE "icspdefaults.xlsx"; SYSTEMIZE IN PYTHON? AND LIST SUPCO CAPACTIOR PART #S
    		(17,25,29,40,45,47,48,51,68,85,99,120,210,215,240,242,245,256,259,317,318,325,340,351,370,371,373,375,400,435,445,470,482,488,501,505,533,535,537,540,543,
    		545,548,550,553,559,560,561,580,620,631,634,635,648,649,660,684,685,688,690,710,751,765,767,772,774,775,776,780,787,788,790,792,794,804,805,815,898)
    		THEN 'Y'
    	WHEN pwh.prod IN ('CD15-5X440R','CD20-5X440R','CD25-5X440','CD25-5X440R','CD25-7.5X440R','CD30-3X440R','CD30-5X440','CD30-5X440R','CD30-7.5X440R','CD35-3X440R',
    					'CD35-4X440R','CD35-5X440','CD35-5X440R','CD35-7.5X440','CD35-7.5X440R','CD40-10X440R','CD40-3X440R','CD40-5X440','CD40-5X440R','CD40-7.5X440R',
    					'CD45-10X440R','CD45-3X440R','CD45-5X440','CD45-5X440R','CD45-7.5X440R','CD50-10X440R','CD50-5X440R','CD50-7.5X440R','CD55-10X440R','CD55-5X440R',
    					'CD55-7.5X440R','CD60-10X440R','CD60-5X440R','CD60-7.5X440R','CD65-5X440R','CD70-10X440R','CD70-5X440R','CD70-7.5X440R','CD80-10X440R','CD80-5X440R',
    					'CD80-7.5X440R','CR10X440','CR10X440R','CR12.5X440','CR15X440','CR15X440R','CR20X440','CR20X440R','CR25X440','CR25X440R','CR2X440','CR30X440','CR30X440R',
    					'CR35X370','CR35X440','CR35X440R','CR3X440','CR40X440','CR45X440','CR45X440R','CR4X440','CR50X440','CR50X440R','CR55X440','CR55X440R','CR5X440','CR5X440R',
    					'CR60X440R','CR6X440','CR6X440R','CR7.5X440','CR7.5X440R') THEN 'Y' ELSE 'N'
    	END AS seasonal,
    prod.descrip_1,
    prod.descrip_2,
    pwh.linept,
    CAST(pwh.orderpt AS INT) AS orderpt,
    pwh.arptype,
    pwh.frozenmmyy,
    CAST(pwh.frozenmos AS INT) AS frozenmos,
    UPPER(pwh.frozentype) AS frozentype,
    pwh.prodline,
	usge.hits_1_mo_back,
	usge.hits_2_mo_back,
	usge.hits_3_mo_back,
	usge.hits_4_mo_back,
	usge.hits_5_mo_back,
	usge.hits_6_mo_back,
	usge.hits_7_mo_back,
	usge.hits_8_mo_back,
	usge.hits_9_mo_back,
	usge.hits_10_mo_back,
	usge.hits_11_mo_back,
	usge.hits_12_mo_back,
    usge.total_hits_12_mo,
    UPPER(pwh.statustype) AS statustype
FROM 
    default.icsw pwh
INNER JOIN 
    usge pwhu ON pwh.prod = pwhu.prod AND pwh.whse = pwhu.whse
INNER JOIN 
    default.icsp prod ON prod.prod = pwh.prod AND prod.cono = 1 AND prod.prodtype NOT IN ('C', 'I')
WHERE
    pwh.cono = 1
    AND pwh.whse <> '06' -- exclude 06, permanently closed
    AND ((prod.descrip_1 not like '%FIP%' OR prod.descrip_2 not like '%FIP%') AND (prod.descrip_1 not like '%DO%' OR prod.descrip_2 not like '%DO%'))
	AND upper(pwh.arptype) IN ('W','V') 
	AND upper(pwh.ordcalcty) <> 'H' -- exclude human order control
	AND pwh.frozenmos <> 99
	AND pwh.arpvendno NOT IN (47,295,560,561,774,788,789,999) -- specific exclusion vendors per Caleb
	AND UPPER(pwh.statustype) <> 'X' -- no DNR items
	AND RIGHT(pwh.prodline, 2) <> 'LK' 
	AND pwh.linept = 0 -- focuses to items which may need line point
	AND NOT (
        pwh.whse NOT IN ('20','31','32','33','34','37','40','41','42','50')  -- exception warehouses where hit threshold is 3 instead of 4
        AND usge.total_hits_12_mo < 4 
    )
	AND NOT (
	    pwh.arpvendno = 775
	    AND pwh.whse IN ('02','06','07','08','10','11','14','16','17','18','20','21','22','23')
	    AND (
	        pwh.prod LIKE 'COL%'
	        OR pwh.prod LIKE 'COM%'
	        OR pwh.prod LIKE 'ZP%'
	        OR pwh.prod LIKE 'EXC%'
	    )) -- excludes some common large trane items for specific branches with low storage
	AND NOT (
	    pwh.arpvendno IN (110,362,807,831)
	    AND pwh.whse IN ('02','06','07','08','10','11','16','17','18','20','21','22','23')
	    ) -- excludes appliances for specific branches with low storage
	ORDER BY whse, arpvendno, pwh.prod
	)
	SELECT * FROM MAIN 
	WHERE seasonal = 'N' OR (hits_11_mo_back + hits_12_mo_back)>0
"""

ICSPR_UPLOAD_READY_QUERY = """
/*ICSPR NEEDED REPORT 
 * 
 * Purpose: Identify  products in need of icspr record(s) for CFC restriction;  
 * Author: Julian Sivers
 * Last updated: 02/17/26
 * Date Automated: 3/11/26
 */


-- =====================================================
-- Keyword-based expected ICSPE flagging
-- =====================================================

WITH desc_fields AS (
    SELECT
        p.prod,
        UPPER(COALESCE(p.descrip_1, '')) AS d1,
        UPPER(COALESCE(p.descrip_2, '')) AS d2,
        UPPER(COALESCE(p.descrip3, '')) AS d3,
		    ' ' ||
		    UPPER(COALESCE(p.descrip_1, '')) || ' ' ||
		    UPPER(COALESCE(p.descrip_2, '')) || ' ' ||
		    UPPER(COALESCE(p.descrip3, '')) || ' '
		AS full_desc,
        r.restrictcd
    FROM default.icsp p
    left join icspr r on r.srcrowpointer = p.rowpointer and r.cono = p.cono
    WHERE p.cono = 1 and r.restrictcd is NULL
),
keywords AS (
    SELECT * 
    FROM (VALUES
        '% AC %','% A.C. %','% GAS PACK %','% DUAL FUEL %','% PACKAGE UNIT %',
        '% MINI SPLIT %','% MINISIPLIT %','% BTU %','% SPL% ','% SPL COOLING %',
        '% HP %','% HEAT PUMP %','% PHP %','% PACKAGE HP %','% PACKAGE HEAT PUMP %',
        '% PACK GAS %'
    ) AS t(keyword)
)
SELECT DISTINCT
	'' AS extractseqno,
	'PR' AS restricttype,
    d.prod,
    '' AS whse,
    CONVERT(VARCHAR(8), GETDATE(), 1) as startdt,
    '' AS 'source-desc-name',
    'yes' AS activefl,
    'yes' AS certrequiredfl,
    '' AS expiredt,
    'Freon' AS restrictcd,
    'no' AS restrictovrfl,
    'A' AS statuscd,
    '' AS rowpointer,
    --'ICSPR Needed' AS ICSPR_Update,
    v.arpvendno AS vendor,
    v.prodline AS productline,
    d.d1 AS descrip_1,
    d.d2 AS descrip_2,
    d.d3 AS descrip_3
FROM desc_fields d
JOIN keywords k
  ON d.full_desc LIKE k.keyword
LEFT JOIN default.icsw v
  ON d.prod = v.prod AND v.cono = 1 AND UPPER(v.statustype) <> 'X'
 WHERE 
 v.arpvendno in (85,120,121,259,318,362,370,371,400,491,495,690,774,776,807) -- finish list?
 AND 
 NOT (
    d.full_desc LIKE '%HP%MOTOR%'
 OR d.full_desc LIKE '%MOTOR%HP%'
)
AND d.prod NOT IN ('placeholder')
ORDER BY prod;
"""

PRICE_CHANGE_QUERY = """-- =====================================================
-- Vendor × Period Price Analytics
-- With Category Breakdown + Denominators
-- =====================================================

WITH
-- =====================================================
-- 1️⃣ Item universe (all items that existed)
-- =====================================================
item_universe AS (
    SELECT
        arpvendno,
        period,
        prod,
        UPPER(statustype_ending) AS statustype,
        UPPER(companyrank_ending) AS companyrank
    FROM price_change_history
),
-- =====================================================
-- 2️⃣ Expand universe into categories
-- =====================================================
universe_by_category AS (
    -- ALL items
    SELECT arpvendno, period, prod, 'ALL' AS category
    FROM item_universe
    UNION ALL
    -- STOCK items
    SELECT arpvendno, period, prod, 'STOCK'
    FROM item_universe
    WHERE statustype = 'S'
    UNION ALL
    -- Rank A–D items
    SELECT
        arpvendno,
        period,
        prod,
        'RANK_' || companyrank
    FROM item_universe
    WHERE companyrank IN ('A','B','C','D')
),
-- =====================================================
-- 3️⃣ Total items per vendor / period / category
-- =====================================================
total_items AS (
    SELECT
        arpvendno,
        period,
        category,
        COUNT(DISTINCT prod) AS total_items
    FROM universe_by_category
    GROUP BY arpvendno, period, category
),
-- =====================================================
-- 4️⃣ Price changes expanded into same categories
-- =====================================================
base_changes AS (
    -- ALL
    SELECT
        arpvendno,
        period,
        replcost_ending - replcost_starting AS price_change,
        (replcost_ending / replcost_starting - 1) * 100 AS pct_change,
        'ALL' AS category
    FROM price_change_history
    WHERE replcost_starting != replcost_ending
    UNION ALL
    -- STOCK
    SELECT
        arpvendno,
        period,
        replcost_ending - replcost_starting,
        (replcost_ending / replcost_starting - 1) * 100,
        'STOCK'
    FROM price_change_history
    WHERE replcost_starting != replcost_ending
      AND UPPER(statustype_ending) = 'S'
    UNION ALL
    -- Rank A–D
    SELECT
        arpvendno,
        period,
        replcost_ending - replcost_starting,
        (replcost_ending / replcost_starting - 1) * 100,
        'RANK_' || UPPER(companyrank_ending)
    FROM price_change_history
    WHERE replcost_starting != replcost_ending
      AND UPPER(companyrank_ending) IN ('A','B','C','D')
),
-- =====================================================
-- 5️⃣ Monthly metrics
-- =====================================================
monthly_metrics AS (
    SELECT
        arpvendno,
        period,
        category,
        COUNT(*) AS num_price_changes,
        AVG(price_change) AS avg_price_change,
        AVG(pct_change) AS avg_pct_change,
        -- increases
        SUM(CASE WHEN price_change > 0 THEN 1 ELSE 0 END) AS num_increases,
        AVG(CASE WHEN price_change > 0 THEN price_change END) AS avg_increase,
        AVG(CASE WHEN price_change > 0 THEN pct_change END) AS avg_pct_increase,
        -- decreases
        SUM(CASE WHEN price_change < 0 THEN 1 ELSE 0 END) AS num_decreases,
        AVG(CASE WHEN price_change < 0 THEN price_change END) AS avg_decrease,
        AVG(CASE WHEN price_change < 0 THEN pct_change END) AS avg_pct_decrease
    FROM base_changes
    GROUP BY arpvendno, period, category
),
-- =====================================================
-- 6️⃣ Pricing announced flag
-- =====================================================
pricing_announced AS (
    SELECT
        vendor_no AS arpvendno,
        effective_mo AS period,
        'Y' AS pricing_announced
    FROM announced_price_changes
    GROUP BY vendor_no, effective_mo
)
-- =====================================================
-- 7️⃣ Final Output
-- =====================================================
SELECT
    mm.arpvendno AS vendor,
    mm.period,
    mm.category,
    COALESCE(pa.pricing_announced, 'N') AS pricing_announced,
    -- overall
    mm.num_price_changes,
    ROUND(mm.avg_price_change, 2) AS avg_price_change,
    ROUND(mm.avg_pct_change, 2) AS avg_pct_change,
    -- increases
    mm.num_increases,
    ROUND(mm.avg_increase, 2) AS avg_increase,
    ROUND(mm.avg_pct_increase, 2) AS avg_pct_increase,
    -- decreases
    mm.num_decreases,
    ROUND(mm.avg_decrease, 2) AS avg_decrease,
    ROUND(mm.avg_pct_decrease, 2) AS avg_pct_decrease
FROM monthly_metrics mm
LEFT JOIN total_items ti
       ON ti.arpvendno = mm.arpvendno
      AND ti.period = mm.period
      AND ti.category = mm.category
LEFT JOIN pricing_announced pa
       ON pa.arpvendno = mm.arpvendno
      AND pa.period = mm.period
ORDER BY vendor, mm.period, mm.category;
"""