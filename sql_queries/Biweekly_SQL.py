#Biweekly_SQL.py

SUPERSEDING_OAN_SQL = """
SELECT 
	pwh.whse AS "Warehouse",
   	CASE WHEN LEFT(pwhu.prod,1) = '0' THEN '''' + pwhu.prod ELSE pwhu.prod END AS "Part Number",
    pwh.arpvendno AS "ARP Vendor",
    pwh.prodline AS "Product line",
    --prod.descrip_1 AS "Description 1",
    --prod.descrip_2 AS "Description 2",
    UPPER(pwh.statustype) AS "Status Type",
    pwh.minthreshold AS "Min Threshold",
    CAST((pwhu.linehits_2 + pwhu.linehits_3 + pwhu.linehits_4 + pwhu.linehits_5 + pwhu.linehits_6 + pwhu.linehits_7 + pwhu.linehits_8 + pwhu.linehits_9 + pwhu.linehits_10 + pwhu.linehits_11 + pwhu.linehits_12 + pwhu.linehits_13) AS INT) AS "Total hits 12 Months",
    CAST((pwhu.linehits_2) AS INT) AS "hits -1 months",
    CAST((pwhu.linehits_3) AS INT) AS "hits -2 months",
    CAST((pwhu.linehits_4) AS INT) AS "hits -3 months",
    CAST((pwhu.linehits_5) AS INT) AS "hits -4 months", 
    CAST((pwhu.linehits_6) AS INT) AS "hits -5 months",
    CAST((pwhu.linehits_7) AS INT) AS "hits -6 months",
    CAST((pwhu.linehits_8) AS INT) AS "hits -7 months",
    CAST((pwhu.linehits_9) AS INT) AS "hits -8 months",
    CAST((pwhu.linehits_10) AS INT) AS "hits -9 months",
    CAST((pwhu.linehits_11) AS INT) AS "hits -10 months", 
    CAST((pwhu.linehits_12) AS INT) AS "hits -11 months",
    CAST((pwhu.linehits_13) AS INT) AS "hits -12 months",
    CAST((pwhu.normusage_2 + pwhu.normusage_3 + pwhu.normusage_4 + pwhu.normusage_5 + pwhu.normusage_6 + pwhu.normusage_7 + pwhu.normusage_8 + pwhu.normusage_9 + pwhu.normusage_10 + pwhu.normusage_11 + pwhu.normusage_12 + pwhu.normusage_13) AS INT) AS "Total Usage 12 Months",
    CAST((pwhu.normusage_2) AS INT) AS "usage -1 months",
    CAST((pwhu.normusage_3) AS INT) AS "usage -2 months",
    CAST((pwhu.normusage_4) AS INT) AS "usage -3 months",
    CAST((pwhu.normusage_5) AS INT) AS "usage -4 months", 
    CAST((pwhu.normusage_6) AS INT) AS "usage -5 months",
    CAST((pwhu.normusage_7) AS INT) AS "usage -6 months",
    CAST((pwhu.normusage_8) AS INT) AS "usage -7 months",
    CAST((pwhu.normusage_9) AS INT) AS "usage -8 months",
    CAST((pwhu.normusage_10) AS INT) AS "usage -9 months",
    CAST((pwhu.normusage_11) AS INT) AS "usage -10 months", 
    CAST((pwhu.normusage_12) AS INT) AS "usage -11 months",
    CAST((pwhu.normusage_13) AS INT) AS "usage -12 months"
FROM 
    default.icswu pwhu
INNER JOIN 
    default.icsw pwh ON pwhu.prod = pwh.prod AND pwhu.whse = pwh.whse AND pwhu.cono = pwh.cono
INNER JOIN 
    default.icsp prod ON prod.prod = pwh.prod AND prod.cono = pwh.cono
CROSS JOIN
    (SELECT month(max(transdt)) AS month_max_transdt FROM default.icswu) subquery
WHERE pwhu.cono = 1
    AND (pwhu.normusage_2 + pwhu.normusage_3 + pwhu.normusage_4 + pwhu.normusage_5 + pwhu.normusage_6 + pwhu.normusage_7 + pwhu.normusage_8 + pwhu.normusage_9 + pwhu.normusage_10 + pwhu.normusage_11 + pwhu.normusage_12 + pwhu.normusage_13) >= 4
	--AND RIGHT(pwh.prodline, 2) <> 'LK'
	--AND pwh.arpvendno <> 999
	AND UPPER(pwh.statustype) = 'O'
    AND pwh.whse NOT LIKE '%C'
    AND pwh.whse NOT IN ('06')
	AND pwhu.prod in (select distinct altprod from default.icsec where UPPER(rectype) = 'P' and cono = 1)
ORDER BY pwhu.prod, pwh.whse"""

TRANE_ORDER_XREF_SQL = """WITH RankedRows AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY pono ORDER BY posuf DESC) AS RowNum
    FROM
        default.poel
    WHERE
        bono = 0 AND whse = '25'
)
SELECT distinct
 	peh.vendordno AS "Trane Order #",
 	rr.pono as "Purchase Order #",
  	CONCAT('*P', rr.pono, '*') AS "PO Barcode"
FROM
    RankedRows rr
LEFT JOIN
    default.poeh peh ON rr.pono = peh.pono AND rr.posuf = peh.posuf
LEFT JOIN
    default.poeh prev_poeh ON rr.pono = prev_poeh.pono AND rr.posuf - 1 = prev_poeh.posuf
WHERE
    rr.RowNum = 1
    AND peh.vendno = 775
    AND (
        (peh.stagecd < 5 AND rr.qtyord <> 0)
        OR (peh.stagecd >= 5 AND (rr.qtyord - rr.qtyrcv) <> 0)
    )
    AND peh.stagecd < 7 AND peh.transtype = 'po'
ORDER BY rr.pono;"""

COST_ADJ_SQL= """WITH inv AS (
    SELECT prod, vendprod, avgcost, replcost, unitbuy, replcostdt, whse
    FROM default.icsw
    WHERE cono = 1 AND whse IN ('25','50')
),
ranked AS (
    SELECT
        CASE WHEN i.replcost > t.cost THEN 'Decrease' ELSE 'Increase' END AS changetype,
        ROUND(((t.cost - i.replcost) / i.replcost) * 100.0, 2) AS "change%",
        t.prod, i.vendprod, i.unitbuy, t.origcost AS pocost, t.cost AS actcost, i.replcost, i.avgcost,
        t.orderno AS PO, t.ordersuf, t.custno, t.postdt, i.replcostdt,
        ROW_NUMBER() OVER (PARTITION BY t.prod ORDER BY t.postdt DESC) AS rn
    FROM default.icet t
    LEFT JOIN inv i ON i.prod = t.prod AND i.whse = t.whse
    WHERE t.cono = 1
   		AND t.orderno in (select distinct pono from default.poel where transtype = 'po')
      AND t.origcost != 0
      AND (((ABS
      	(i.replcost - t.cost) / i.replcost) > .02 AND (ABS
      	(i.replcost - t.cost)) > 1) OR ((ABS
      	(t.origcost - t.cost) / t.origcost) > .02 AND (ABS
      	(t.origcost - t.cost)) > 1))-- filters so price changes must be over $1 and over 2%
      AND t.whse IN ('25', '50')
      AND t.transtype = 'ca'
      AND t.cost > i.replcost -- CHANGE TO '<>' to view decreases too, default is '>' to see increases from replcost only
      AND i.replcostdt <= t.postdt
      AND vendprod <> '!999!'
      AND t.orderno not in (2511387) -- PO(s) with known discrepancy, addressed seperately
      AND CAST(t.postdt AS date) > '2025-06-30' -- roughly when Duane left/ AP accuracy increased dramatically
)
SELECT * 
FROM ranked
WHERE rn = 1
ORDER BY postdt DESC;
"""

WG_ICSPE_NEEDED_SQL = """WITH desc_fields AS (
  SELECT
    p.prod,
    UPPER(COALESCE(p.descrip_1, '')) AS d1,
    UPPER(COALESCE(p.descrip_2, '')) AS d2,
    UPPER(COALESCE(p.descrip3, '')) AS d3,
   	p.prodcat,
    COALESCE(f.state, '') AS actual_state
  FROM default.icsp p
  LEFT JOIN default.icspe f
    ON f.srcrowpointer = p.rowpointer AND f.cono = 1
  WHERE p.cono = 1 and p.prodcat in ('APEQ','COEQ','HCEQ')
),
keywords AS (
    SELECT *
    FROM (VALUES
        ('NC', '%Refrigerator%'),
        ('NC', '%Fridge%'),
        ('NC', '%Range%'),
        ('NC', '%rnge%'),
        ('NC', '%Freezer%'),
        ('NC', '%Freeze%'),
        ('NC', '%Freez%'),
        ('NC', '%Stove%'),
        ('NC', '%Built in oven%'),
        ('NC', '%Built-in oven%'),
        ('NC', '%Water Heater%'),
        ('NC', '%hot water heater%'),
        ('NC', '%Dryer%'),
        ('NC', '%Washer%'),
        ('NC', '%washing machine%'),
        ('NC', '%Dishwasher%'),
        ('NC', '%dish%'),
        ('NC', '%Unit air conditioner%'),
        ('NC', '%PTAC%'),
        ('NC', '%window air conditioner%'),
        ('SC', '%Refrigerator%'),
        ('SC', '%Fridge%'),
        ('SC', '%Freezer%'),
        ('SC', '%Freeze%'),
        ('SC', '%Freez%'),
        ('SC', '%Range%'),
        ('SC', '%rnge%'),
        ('SC', '%Stove%'),
        ('SC', '%Built in oven%'),
        ('SC', '%Built-in oven%'),
        ('SC', '%Water Heater%'),
        ('SC', '%hot water heater%'),
        ('SC', '%Dishwasher%'),
        ('SC', '%dish%'),
        ('SC', '%Washer%'),
        ('SC', '%washing machine%'),
        ('SC', '%Dryer%'),
        ('SC', '%AC %'),
        ('SC', '%A.C.%'),
        ('SC', '%GAS PACK%'),
        ('SC', '%DUAL FUEL%'),
        ('SC', '%PACKAGE UNIT%'),
        ('SC', '%MINI SPLIT%'),
        ('SC', '%MINISPLIT%'),
        ('SC', '%BTU%'),
        ('SC', '%SPL%'),
        ('SC', '%SPL COOLING%'),
        ('SC', '%HP%'),
        ('SC', '%HEAT PUMP%'),
        ('SC', '%PHP%'),
        ('SC', '%PACKAGE HP%'),
        ('SC', '%PACKAGE HEAT PUMP%'),
        ('SC', '%PACK GAS%')
    ) AS t(group_name, keyword)
),
matches AS (
  SELECT DISTINCT d.prod, k.group_name
  FROM desc_fields d
  JOIN keywords k
    ON d.d1 LIKE UPPER(k.keyword)
    OR d.d2 LIKE UPPER(k.keyword)
    OR d.d3 LIKE UPPER(k.keyword)
),
agg_expected AS (
  SELECT
    prod,
    MAX(CASE WHEN group_name = 'NC' THEN 1 ELSE 0 END) AS expected_NC,
    MAX(CASE WHEN group_name = 'SC' THEN 1 ELSE 0 END) AS expected_SC
  FROM matches
  GROUP BY prod
),
descrips AS (
	SELECT prod, descrip_1, descrip_2 from default.icsp where cono = 1),
actuals AS (
	SELECT
	    prod,
	    MAX(CASE WHEN actual_state = 'NC' THEN 1 ELSE 0 END) AS actual_NC,
	    MAX(CASE WHEN actual_state = 'SC' THEN 1 ELSE 0 END) AS actual_SC
	  FROM desc_fields
	  WHERE prod not in
	  	('0231K00038A','1146','1186445','1186455','12029207','14-04-CPK-50','1645TB','1871','2299571','24RAQ-3','24SPXC5-HP',
		'30510142','317497-701','32LT660004','45583','4581DD3003C','48663','4TXAB048BC3HU','4TXAC060BC3HU','9-321-295',
		'996-36THP','A801X060BM4SDA','A801X080BM4SDB','ABF24-400G-5KW','ABFT24-400GDVP48KW','AV1423','AV1426','B319',
		'BAYECON088A','BAYHTRC106A','BAYHTRR430A','BAYHTRV108F','BAYHTRV115F','BAYHTRV410E','BAYUV102A','BAYVALV025A',
		'CB1025','CDBX9001','CHPTA1822A4','CK-PJX02A','CP7023','CR53KQEPFV970','CRR8-9A','DA82-02711L','DE31-00058A',
		'DG94-04041D','DK900D','E020-70556374','E4AH5E36A1J30A','EB-STATE7P-01','EBR30368921','EBR89296403','F402411',
		'F403611','FGMO205KW','FHWC3025MS','FMOS1746BS','GCHPF2430B6','GCHPF3636B6','GCHPF3642C6','GCHPF4860D6','GCHPT4860D4',
		'GCHPTA3630C4','GD9S800804BN','GGC9S800603AN','GGC9S800804BN','GGCES800603AN','GGCES800804BN','GGD9S800603AN','GGD9S801005CX',
		'GGM9S800603AN','GGM9S800604BN','GGM9S800804BN','GGM9S801205DN','GGM9S920603BN','GGM9S920804CN','GGMEF800804BN','GGMES800603BN',
		'GGMES800803BN','GGMES800804CN','GGMES920403AN','GGMES920803BN','GGMES921004CN','GGMSS920804CN','GM9S920403AN','GM9S960603BN',
		'GM9S960803BN','GMSS920603BN','GMU2AEB37101SA','GMU2APB24081SA','GMU2APB30081SA','GMV2AEB39101SA','GMV2APB26081SA','GMV2APB32081SA',
		'GR9S800803BN','GSWX9X35','HB32GQ230','HB32GR229','HB33GQ231','HB41TQ113','HC33GE208','HC33GE233','HC35AE230','HC35GE240','HC36AR231',
		'HC37AE198','HC39GE237','HC43TQ115','HC91PD001','HD100AS0121','HD42AE236','HD42AQ252','HD44AR120','HD46AR344','HK42ER224','HK42ER227',
		'HK42ER228','HN61PC003','HT01CN236','J801V040AM3SEA','J801V060AM3SEA','J801V060BM4SEA','J801V080BM4SEA','J801V080CM4SEA','J801V100CM5SEA',
		'J801V120DM5SEA','J801X072BD4SAB','J952X120DU5SAA','J962V060BDVSAB','J962V080CDVSAB','J962V100CDVSAB','J962V115DDVSAB','J962X060BU3SEA',
		'J962X080BU3SEA','J962X080CU4SEA','J962X100CU5SEA','J962X100DU5SEA','J962X120DU5SEA','JN327HWW','JNM7196SKSS','JVM3160RFSS','JVM3162DJBB',
		'JVM3162DJWW','JVM3162RJSS','JVX3240SJSS','JVX3300DJBB','JXCK89','KUID308HPS','LDK-110000-070','LH680005','M10','M170','M18','M201B','M209',
		'M4CXC025BB1CA','M4CXC028BA1CA','M8','MAYTXVACHP1830A','MCK66585116','ME30A1D010AA','ME30A1D015AAA','ORM1016V1','P421-4006','PDP150AE0130',
		'PDP150AE0185SBAN','PDP200AE0130','PDP250AE0130','PP5','PTG','PTP250AS0111SBAN','RAB26A','RAB42MG','RVM5160DHWW','S1-32440880007-ECM',
		'S8B1A040M3PSC','S8B1B080M4PSCA','S8V2B080M4PCAA','S8X1B040M2PSC','S9B1B040U3PSAA','S9B1B080U4PSAA','S9B1B080U4PSAB','S9B1C100D5PSA',
		'S9B1C100U5PSA','S9B1C100U5PSAA','S9V2B040U3PSBD','S9V2B060D3PSBB','S9V2B060U3PSBB','S9V2B080D3PSBB','S9V2B080U3PS','S9V2B080U4','S9V2B080U4VSA',
		'S9V2C100U5PSA','S9X1B060U4PSBB','S9X1C100U5PSB','S9X1C100U5PSBA','S9X2C100U5PSAB','SC05DISPC1','TAYPLUS103A','TAYREFLN050','TAYREFLN060','TAYREFLN565',
		'TDC1C100A9481','TDD1C100A9541','TDE1B060A936','TDE1C100A9601','TDH1B065A9H31','TDH1D110A9H51','TDH2C100A948V','TREMOTE2AHANDA','TUC1B040A924A',
		'TUC1C100A9481','TUC1D120A9601','TUC1D120A96DA','TUD060R9V3K','TUD1A060A9241','TUD1A060A936','TUD1B060A9H31','TUD1B100A9361','TUD1B100A9451',
		'TUD1C080A9H41','TUD1C100A9481','TUD1C120A9541','TUD1D120A9601','TUD1D120A9H51','TUD2B060A9362','TUD2B080A936','TUD2C080A9V4V','TUD2C100A9V5V',
		'TUD2C100B9V5V','TUE1A040A9241','TUE1A060A9361','TUE1B100A9361','TUE1C100A9481A','TUE1C100A9601','TUE1D120A9601','TUH1B060A936','TUH1B080A9421',
		'TUH1C080A9601','TUH1C100A9481','TUH1D100A9601C','TUX1B080A9421','TWE240K4BAAP0','TXC036S3HPD','V2D120U5PSBB','W10869845','W11745759','WJA1002',
		'WMH31017HW','YR-E16B','YR-HG','ZP20K5EPFV800','3Q24FF2BEA','A801X060AM3S','CTR03460','CTR03460','FFHP302CQ2','FFPA1022R1','FFPH1222U1','FFPH1422U1',
		'FHPC082AC1','FHPC102AB1','FHPC132AB1','FHPH132AB1','FHPW122AC1','FHPW142AC1','JVM3160EFES','JVM3160EFES','PAC-SK88RJ-E','1-4020-3041','COL33500',
		'RAK3203A','RAK3203A','S8XC100M5PSC','S9V2B040U3VSB','S9V2C080U5VS')
	  GROUP BY prod
),
data AS (
	SELECT
	  COALESCE(e.prod, a.prod) AS prod,
	  COALESCE(e.expected_NC, 0) AS expected_NC,
	  COALESCE(e.expected_SC, 0) AS expected_SC,
	  COALESCE(a.actual_NC, 0) AS actual_NC,
	  COALESCE(a.actual_SC, 0) AS actual_SC,
	 w.vendor, w.productline, d.descrip_1, d.descrip_2
	FROM agg_expected e
FULL OUTER JOIN actuals a ON e.prod = a.prod
Join (SELECT 
		prod, MAX(arpvendno) AS vendor, MAX(prodline) AS productline
	from default.icsw where cono = 1 and upper(statustype) <> 'X' GROUP BY prod) 
	w ON e.prod = w.prod
LEFT JOIN descrips d on d.prod = e.prod
WHERE 
	(expected_NC <> actual_NC or expected_SC <> actual_SC)
AND e.prod not in 
	('0231K00038A','1146','1186445','1186455','12029207','14-04-CPK-50','1645TB','1871','2299571','24RAQ-3','24SPXC5-HP',
	'30510142','317497-701','32LT660004','45583','4581DD3003C','48663','4TXAB048BC3HU','4TXAC060BC3HU','9-321-295',
	'996-36THP','A801X060BM4SDA','A801X080BM4SDB','ABF24-400G-5KW','ABFT24-400GDVP48KW','AV1423','AV1426','B319',
	'BAYECON088A','BAYHTRC106A','BAYHTRR430A','BAYHTRV108F','BAYHTRV115F','BAYHTRV410E','BAYUV102A','BAYVALV025A',
	'CB1025','CDBX9001','CHPTA1822A4','CK-PJX02A','CP7023','CR53KQEPFV970','CRR8-9A','DA82-02711L','DE31-00058A',
	'DG94-04041D','DK900D','E020-70556374','E4AH5E36A1J30A','EB-STATE7P-01','EBR30368921','EBR89296403','F402411',
	'F403611','FGMO205KW','FHWC3025MS','FMOS1746BS','GCHPF2430B6','GCHPF3636B6','GCHPF3642C6','GCHPF4860D6','GCHPT4860D4',
	'GCHPTA3630C4','GD9S800804BN','GGC9S800603AN','GGC9S800804BN','GGCES800603AN','GGCES800804BN','GGD9S800603AN','GGD9S801005CX',
	'GGM9S800603AN','GGM9S800604BN','GGM9S800804BN','GGM9S801205DN','GGM9S920603BN','GGM9S920804CN','GGMEF800804BN','GGMES800603BN',
	'GGMES800803BN','GGMES800804CN','GGMES920403AN','GGMES920803BN','GGMES921004CN','GGMSS920804CN','GM9S920403AN','GM9S960603BN',
	'GM9S960803BN','GMSS920603BN','GMU2AEB37101SA','GMU2APB24081SA','GMU2APB30081SA','GMV2AEB39101SA','GMV2APB26081SA','GMV2APB32081SA',
	'GR9S800803BN','GSWX9X35','HB32GQ230','HB32GR229','HB33GQ231','HB41TQ113','HC33GE208','HC33GE233','HC35AE230','HC35GE240','HC36AR231',
	'HC37AE198','HC39GE237','HC43TQ115','HC91PD001','HD100AS0121','HD42AE236','HD42AQ252','HD44AR120','HD46AR344','HK42ER224','HK42ER227',
	'HK42ER228','HN61PC003','HT01CN236','J801V040AM3SEA','J801V060AM3SEA','J801V060BM4SEA','J801V080BM4SEA','J801V080CM4SEA','J801V100CM5SEA',
	'J801V120DM5SEA','J801X072BD4SAB','J952X120DU5SAA','J962V060BDVSAB','J962V080CDVSAB','J962V100CDVSAB','J962V115DDVSAB','J962X060BU3SEA',
	'J962X080BU3SEA','J962X080CU4SEA','J962X100CU5SEA','J962X100DU5SEA','J962X120DU5SEA','JN327HWW','JNM7196SKSS','JVM3160RFSS','JVM3162DJBB',
	'JVM3162DJWW','JVM3162RJSS','JVX3240SJSS','JVX3300DJBB','JXCK89','KUID308HPS','LDK-110000-070','LH680005','M10','M170','M18','M201B','M209',
	'M4CXC025BB1CA','M4CXC028BA1CA','M8','MAYTXVACHP1830A','MCK66585116','ME30A1D010AA','ME30A1D015AAA','ORM1016V1','P421-4006','PDP150AE0130',
	'PDP150AE0185SBAN','PDP200AE0130','PDP250AE0130','PP5','PTG','PTP250AS0111SBAN','RAB26A','RAB42MG','RVM5160DHWW','S1-32440880007-ECM',
	'S8B1A040M3PSC','S8B1B080M4PSCA','S8V2B080M4PCAA','S8X1B040M2PSC','S9B1B040U3PSAA','S9B1B080U4PSAA','S9B1B080U4PSAB','S9B1C100D5PSA',
	'S9B1C100U5PSA','S9B1C100U5PSAA','S9V2B040U3PSBD','S9V2B060D3PSBB','S9V2B060U3PSBB','S9V2B080D3PSBB','S9V2B080U3PS','S9V2B080U4','S9V2B080U4VSA',
	'S9V2C100U5PSA','S9X1B060U4PSBB','S9X1C100U5PSB','S9X1C100U5PSBA','S9X2C100U5PSAB','SC05DISPC1','TAYPLUS103A','TAYREFLN050','TAYREFLN060','TAYREFLN565',
	'TDC1C100A9481','TDD1C100A9541','TDE1B060A936','TDE1C100A9601','TDH1B065A9H31','TDH1D110A9H51','TDH2C100A948V','TREMOTE2AHANDA','TUC1B040A924A',
	'TUC1C100A9481','TUC1D120A9601','TUC1D120A96DA','TUD060R9V3K','TUD1A060A9241','TUD1A060A936','TUD1B060A9H31','TUD1B100A9361','TUD1B100A9451',
	'TUD1C080A9H41','TUD1C100A9481','TUD1C120A9541','TUD1D120A9601','TUD1D120A9H51','TUD2B060A9362','TUD2B080A936','TUD2C080A9V4V','TUD2C100A9V5V',
	'TUD2C100B9V5V','TUE1A040A9241','TUE1A060A9361','TUE1B100A9361','TUE1C100A9481A','TUE1C100A9601','TUE1D120A9601','TUH1B060A936','TUH1B080A9421',
	'TUH1C080A9601','TUH1C100A9481','TUH1D100A9601C','TUX1B080A9421','TWE240K4BAAP0','TXC036S3HPD','V2D120U5PSBB','W10869845','W11745759','WJA1002',
	'WMH31017HW','YR-E16B','YR-HG','ZP20K5EPFV800','1-4020-3041','COL33500','RAK3203A','RAK3203A','S8XC100M5PSC','S9V2B040U3VSB','S9V2C080U5VS')
ORDER BY e.prod, a.prod)
SELECT 
	prod AS Product,
	'NC needed' AS ICSPE_Update,
	vendor, productline, descrip_1, descrip_2
	FROM data WHERE expected_NC = 1
UNION ALL
SELECT 
	prod AS Product,
	'SC needed' AS ICSPE_Update,
	vendor, productline, descrip_1, descrip_2
	FROM data WHERE expected_SC = 1
Order By product
	"""