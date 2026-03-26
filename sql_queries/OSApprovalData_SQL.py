# OSApproval_Data_SQL.py

OSAPPROVAL_QUERY = """WITH POS AS ((SELECT shipprod, sum(qtyrcv) AS qtyfrompo from default.poel where cono = 1 and whse = '25'and shipprod not like 'ICW%' and shipprod in (select prod from default.icsw where whse = '25' and usagerate = 0 and cono = 1)
	group by shipprod having sum(qtyrcv) > 0))
SELECT 
	w.prod,
	p.unitstock AS UnitStock,
	p.unitsell AS UnitSell,
	MAX(
    CASE 
        WHEN o.qtyfrompo IS NOT NULL THEN 1 
        ELSE 0 
    END
	) AS SpecSourcChk,
	MAX(CASE
    WHEN w.whse = '25' AND w.prodline LIKE '%LK' AND w.qtyonhand <= 0 THEN 0
    WHEN w.whse = '25' AND qtybo > 0 THEN qtybo + (usagerate * 6) - qtyonorder
    WHEN w.whse = '25' AND ((usagerate * 6) - qtyonorder) < 1 THEN 0
    WHEN w.whse = '25' THEN ((usagerate * 6) - qtyonorder)
    ELSE 0
  		END) AS "25_RETURNABLE_QTY",
	MAX(CASE 
    WHEN w.whse = '50' AND w.prodline LIKE '%LK' AND w.qtyonhand <= 0 THEN 0
    WHEN w.whse = '50' AND qtybo > 0 THEN qtybo + (usagerate * 6) - qtyonorder
    WHEN w.whse = '50' AND ((usagerate * 6) - qtyonorder) < 1 THEN 0
    WHEN w.whse = '50' THEN ((usagerate * 6) - qtyonorder)
    ELSE 0
  		END) AS "50_RETURNABLE_QTY",
   	MAX(CASE WHEN w.whse = '25' and prodline = 'WPMSBR' THEN '1' ELSE '' END) AS WP_MAND,
	SUM(CASE WHEN w.whse = '02' THEN w.qtyonhand ELSE 0 END) AS "02_QOH", 
	SUM(CASE WHEN w.whse = '03' THEN w.qtyonhand ELSE 0 END) AS "03_QOH", 
	SUM(CASE WHEN w.whse = '04' THEN w.qtyonhand ELSE 0 END) AS "04_QOH", 
	SUM(CASE WHEN w.whse = '05' THEN w.qtyonhand ELSE 0 END) AS "05_QOH", 
	SUM(CASE WHEN w.whse = '06' THEN w.qtyonhand ELSE 0 END) AS "06_QOH", 
	SUM(CASE WHEN w.whse = '07' THEN w.qtyonhand ELSE 0 END) AS "07_QOH", 
	SUM(CASE WHEN w.whse = '08' THEN w.qtyonhand ELSE 0 END) AS "08_QOH", 
	SUM(CASE WHEN w.whse = '09' THEN w.qtyonhand ELSE 0 END) AS "09_QOH", 
	SUM(CASE WHEN w.whse = '10' THEN w.qtyonhand ELSE 0 END) AS "10_QOH", 
	SUM(CASE WHEN w.whse = '11' THEN w.qtyonhand ELSE 0 END) AS "11_QOH", 
	SUM(CASE WHEN w.whse = '12' THEN w.qtyonhand ELSE 0 END) AS "12_QOH", 
	SUM(CASE WHEN w.whse = '14' THEN w.qtyonhand ELSE 0 END) AS "14_QOH", 
	SUM(CASE WHEN w.whse = '16' THEN w.qtyonhand ELSE 0 END) AS "16_QOH", 
	SUM(CASE WHEN w.whse = '17' THEN w.qtyonhand ELSE 0 END) AS "17_QOH", 
	SUM(CASE WHEN w.whse = '18' THEN w.qtyonhand ELSE 0 END) AS "18_QOH", 
	SUM(CASE WHEN w.whse = '20' THEN w.qtyonhand ELSE 0 END) AS "20_QOH", 
	SUM(CASE WHEN w.whse = '21' THEN w.qtyonhand ELSE 0 END) AS "21_QOH", 
	SUM(CASE WHEN w.whse = '22' THEN w.qtyonhand ELSE 0 END) AS "22_QOH", 
	SUM(CASE WHEN w.whse = '23' THEN w.qtyonhand ELSE 0 END) AS "23_QOH", 
	SUM(CASE WHEN w.whse = '25' THEN w.qtyonhand ELSE 0 END) AS "25_QOH", 
	SUM(CASE WHEN w.whse = '31' THEN w.qtyonhand ELSE 0 END) AS "31_QOH", 
	SUM(CASE WHEN w.whse = '32' THEN w.qtyonhand ELSE 0 END) AS "32_QOH", 
	SUM(CASE WHEN w.whse = '33' THEN w.qtyonhand ELSE 0 END) AS "33_QOH", 
	SUM(CASE WHEN w.whse = '34' THEN w.qtyonhand ELSE 0 END) AS "34_QOH", 
	SUM(CASE WHEN w.whse = '37' THEN w.qtyonhand ELSE 0 END) AS "37_QOH", 
	SUM(CASE WHEN w.whse = '40' THEN w.qtyonhand ELSE 0 END) AS "40_QOH", 
	SUM(CASE WHEN w.whse = '41' THEN w.qtyonhand ELSE 0 END) AS "41_QOH", 
	SUM(CASE WHEN w.whse = '42' THEN w.qtyonhand ELSE 0 END) AS "42_QOH", 
	SUM(CASE WHEN w.whse = '50' THEN w.qtyonhand ELSE 0 END) AS "50_QOH",
	MAX(CASE WHEN w.whse = '02' THEN w.arpwhse ELSE '' END) AS "02_ARPWHSE",
	MAX(CASE WHEN w.whse = '03' THEN w.arpwhse ELSE '' END) AS "03_ARPWHSE",
	MAX(CASE WHEN w.whse = '04' THEN w.arpwhse ELSE '' END) AS "04_ARPWHSE",
	MAX(CASE WHEN w.whse = '05' THEN w.arpwhse ELSE '' END) AS "05_ARPWHSE",
	MAX(CASE WHEN w.whse = '06' THEN w.arpwhse ELSE '' END) AS "06_ARPWHSE",
	MAX(CASE WHEN w.whse = '07' THEN w.arpwhse ELSE '' END) AS "07_ARPWHSE",
	MAX(CASE WHEN w.whse = '08' THEN w.arpwhse ELSE '' END) AS "08_ARPWHSE",
	MAX(CASE WHEN w.whse = '09' THEN w.arpwhse ELSE '' END) AS "09_ARPWHSE",
	MAX(CASE WHEN w.whse = '10' THEN w.arpwhse ELSE '' END) AS "10_ARPWHSE",
	MAX(CASE WHEN w.whse = '11' THEN w.arpwhse ELSE '' END) AS "11_ARPWHSE",
	MAX(CASE WHEN w.whse = '12' THEN w.arpwhse ELSE '' END) AS "12_ARPWHSE",
	MAX(CASE WHEN w.whse = '14' THEN w.arpwhse ELSE '' END) AS "14_ARPWHSE",
	MAX(CASE WHEN w.whse = '16' THEN w.arpwhse ELSE '' END) AS "16_ARPWHSE",
	MAX(CASE WHEN w.whse = '17' THEN w.arpwhse ELSE '' END) AS "17_ARPWHSE",
	MAX(CASE WHEN w.whse = '18' THEN w.arpwhse ELSE '' END) AS "18_ARPWHSE",
	MAX(CASE WHEN w.whse = '20' THEN w.arpwhse ELSE '' END) AS "20_ARPWHSE",
	MAX(CASE WHEN w.whse = '21' THEN w.arpwhse ELSE '' END) AS "21_ARPWHSE",
	MAX(CASE WHEN w.whse = '22' THEN w.arpwhse ELSE '' END) AS "22_ARPWHSE",
	MAX(CASE WHEN w.whse = '23' THEN w.arpwhse ELSE '' END) AS "23_ARPWHSE",
	MAX(CASE WHEN w.whse = '25' THEN w.arpwhse ELSE '' END) AS "25_ARPWHSE",
	MAX(CASE WHEN w.whse = '31' THEN w.arpwhse ELSE '' END) AS "31_ARPWHSE",
	MAX(CASE WHEN w.whse = '32' THEN w.arpwhse ELSE '' END) AS "32_ARPWHSE",
	MAX(CASE WHEN w.whse = '33' THEN w.arpwhse ELSE '' END) AS "33_ARPWHSE",
	MAX(CASE WHEN w.whse = '34' THEN w.arpwhse ELSE '' END) AS "34_ARPWHSE",
	MAX(CASE WHEN w.whse = '37' THEN w.arpwhse ELSE '' END) AS "37_ARPWHSE",
	MAX(CASE WHEN w.whse = '40' THEN w.arpwhse ELSE '' END) AS "40_ARPWHSE",
	MAX(CASE WHEN w.whse = '41' THEN w.arpwhse ELSE '' END) AS "41_ARPWHSE",
	MAX(CASE WHEN w.whse = '42' THEN w.arpwhse ELSE '' END) AS "42_ARPWHSE",
	MAX(CASE WHEN w.whse = '50' THEN w.arpwhse ELSE '' END) AS "50_ARPWHSE",
	MAX(CASE WHEN w.whse = '02' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "02_statustype",
	MAX(CASE WHEN w.whse = '03' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "03_statustype",
	MAX(CASE WHEN w.whse = '04' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "04_statustype",
	MAX(CASE WHEN w.whse = '05' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "05_statustype",
	MAX(CASE WHEN w.whse = '06' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "06_statustype",
	MAX(CASE WHEN w.whse = '07' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "07_statustype",
	MAX(CASE WHEN w.whse = '08' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "08_statustype",
	MAX(CASE WHEN w.whse = '09' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "09_statustype",
	MAX(CASE WHEN w.whse = '10' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "10_statustype",
	MAX(CASE WHEN w.whse = '11' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "11_statustype",
	MAX(CASE WHEN w.whse = '12' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "12_statustype",
	MAX(CASE WHEN w.whse = '14' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "14_statustype",
	MAX(CASE WHEN w.whse = '16' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "16_statustype",
	MAX(CASE WHEN w.whse = '17' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "17_statustype",
	MAX(CASE WHEN w.whse = '18' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "18_statustype",
	MAX(CASE WHEN w.whse = '20' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "20_statustype",
	MAX(CASE WHEN w.whse = '21' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "21_statustype",
	MAX(CASE WHEN w.whse = '22' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "22_statustype",
	MAX(CASE WHEN w.whse = '23' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "23_statustype",
	MAX(CASE WHEN w.whse = '25' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "25_statustype",
	MAX(CASE WHEN w.whse = '31' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "31_statustype",
	MAX(CASE WHEN w.whse = '32' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "32_statustype",
	MAX(CASE WHEN w.whse = '33' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "33_statustype",
	MAX(CASE WHEN w.whse = '34' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "34_statustype",
	MAX(CASE WHEN w.whse = '37' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "37_statustype",
	MAX(CASE WHEN w.whse = '40' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "40_statustype",
	MAX(CASE WHEN w.whse = '41' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "41_statustype",
	MAX(CASE WHEN w.whse = '42' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "42_statustype",
	MAX(CASE WHEN w.whse = '50' THEN CAST(w.statustype AS VARCHAR(20)) ELSE '' END) AS "50_statustype"
FROM 
    default.icsw w
INNER JOIN default.icsp p 
    ON p.prod = w.prod 
   AND p.cono = w.cono
LEFT JOIN POS o on p.prod = o.shipprod
WHERE
    w.cono = 1 
GROUP BY w.prod, p.unitstock, p.unitsell"""