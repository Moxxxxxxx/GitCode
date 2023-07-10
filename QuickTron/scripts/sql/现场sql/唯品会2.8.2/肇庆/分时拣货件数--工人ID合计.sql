SELECT
tmp.`工作站ID/工人姓名` AS '工作站ID/工人姓名',
SUM(tmp.H00) AS 'H00',SUM(tmp.H01) AS 'H01',SUM(tmp.H02) AS 'H02',SUM(tmp.H03) AS 'H03',SUM(tmp.H04) AS 'H04',SUM(tmp.H05) AS 'H05',
SUM(tmp.H06) AS 'H06',SUM(tmp.H07) AS 'H07',SUM(tmp.H08) AS 'H08',SUM(tmp.H09) AS 'H09',SUM(tmp.H10) AS 'H10',SUM(tmp.H11) AS 'H11',
SUM(tmp.H12) AS 'H12',SUM(tmp.H13) AS 'H13',SUM(tmp.H14) AS 'H14',SUM(tmp.H15) AS 'H15',SUM(tmp.H16) AS 'H16',SUM(tmp.H17) AS 'H17',
SUM(tmp.H18) AS 'H18',SUM(tmp.H19) AS 'H19',SUM(tmp.H20) AS 'H20',SUM(tmp.H21) AS 'H21',SUM(tmp.H22) AS 'H22',SUM(tmp.H23) AS 'H23'
FROM
(
SELECT
CONCAT(d.station_code,'/',IF(u.display_name is NULL OR u.display_name ='','--',u.display_name)) AS '工作站ID/工人姓名',
CASE WHEN d.last_updated_date >= '{begin_time} 00:00:00' and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 1 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H00',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 1 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 2 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H01',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 2 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 3 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H02',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 3 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 4 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H03',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 4 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 5 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H04',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 5 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 6 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H05',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 6 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 7 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H06',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 7 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 8 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H07',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 8 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 9 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H08',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 9 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 10 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H09',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 10 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 11 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H10',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 11 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 12 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H11',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 12 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 13 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H12',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 13 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 14 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H13',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 14 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 15 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H14',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 15 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 16 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H15',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 16 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 17 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H16',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 17 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 18 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H17',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 18 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 19 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H18',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 19 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 20 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H19',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 20 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 21 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H20',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 21 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 22 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H21',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 22 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 23 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H22',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 23 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 24 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H23'
FROM evo_wes_picking.picking_order_fulfill_detail d
LEFT JOIN auth.`user` u
ON d.operator = u.username
WHERE d.last_updated_date >= '{begin_time} 00:00:00' and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 24 HOUR) AND d.state = 'DONE' AND d.quantity IS NOT NULL
GROUP BY DATE_FORMAT(d.last_updated_date,'%Y-%m-%d %H:00:00'),CONCAT(d.station_code,'/',IF(u.display_name is NULL OR u.display_name ='','--',u.display_name))
)tmp
GROUP BY tmp.`工作站ID/工人姓名`

UNION ALL

SELECT
'合计' AS '工作站ID/工人姓名',
SUM(tmp.H00) AS 'H00',SUM(tmp.H01) AS 'H01',SUM(tmp.H02) AS 'H02',SUM(tmp.H03) AS 'H03',SUM(tmp.H04) AS 'H04',SUM(tmp.H05) AS 'H05',
SUM(tmp.H06) AS 'H06',SUM(tmp.H07) AS 'H07',SUM(tmp.H08) AS 'H08',SUM(tmp.H09) AS 'H09',SUM(tmp.H10) AS 'H10',SUM(tmp.H11) AS 'H11',
SUM(tmp.H12) AS 'H12',SUM(tmp.H13) AS 'H13',SUM(tmp.H14) AS 'H14',SUM(tmp.H15) AS 'H15',SUM(tmp.H16) AS 'H16',SUM(tmp.H17) AS 'H17',
SUM(tmp.H18) AS 'H18',SUM(tmp.H19) AS 'H19',SUM(tmp.H20) AS 'H20',SUM(tmp.H21) AS 'H21',SUM(tmp.H22) AS 'H22',SUM(tmp.H23) AS 'H23'
FROM
(
SELECT
CONCAT(d.station_code,'/',IF(u.display_name is NULL OR u.display_name ='','--',u.display_name)) AS 'title',
CASE WHEN d.last_updated_date >= '{begin_time} 00:00:00' and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 1 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H00',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 1 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 2 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H01',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 2 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 3 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H02',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 3 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 4 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H03',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 4 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 5 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H04',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 5 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 6 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H05',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 6 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 7 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H06',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 7 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 8 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H07',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 8 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 9 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H08',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 9 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 10 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H09',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 10 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 11 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H10',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 11 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 12 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H11',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 12 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 13 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H12',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 13 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 14 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H13',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 14 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 15 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H14',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 15 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 16 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H15',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 16 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 17 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H16',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 17 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 18 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H17',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 18 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 19 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H18',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 19 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 20 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H19',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 20 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 21 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H20',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 21 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 22 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H21',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 22 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 23 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H22',
CASE WHEN d.last_updated_date >= DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 23 HOUR) and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 24 HOUR) THEN SUM(d.quantity) ELSE 0 END AS 'H23'
FROM evo_wes_picking.picking_order_fulfill_detail d
LEFT JOIN auth.`user` u
ON d.operator = u.username
WHERE d.last_updated_date >= '{begin_time} 00:00:00' and d.last_updated_date < DATE_ADD('{begin_time} 00:00:00' ,INTERVAL 24 HOUR) AND d.state = 'DONE' AND d.quantity IS NOT NULL
GROUP BY DATE_FORMAT(d.last_updated_date,'%Y-%m-%d %H:00:00'),CONCAT(d.station_code,'/',IF(u.display_name is NULL OR u.display_name ='','--',u.display_name))
)tmp