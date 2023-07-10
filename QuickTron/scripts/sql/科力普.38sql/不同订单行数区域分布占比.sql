SET @begin_time = '2021-11-16 07:00:00';
SET @end_time = '2021-11-17 07:00:00';


SELECT tt1.cur_date as '日期',tt1.type as '类型',tt1.order_num as '订单数',CONCAT(CAST((tt1.order_num/SUM(tt2.order_total))*100 AS DECIMAL(10,2)),'%') as '占比'
FROM
(
SELECT 
DATE(tt.cur_date) as cur_date,tt.type,COUNT(DISTINCT tt.order_id) as order_num
FROM
(
SELECT DATE_FORMAT(pw.updated_date,'%Y-%m-%d %H:00:00') as cur_date,
pw.order_id,
count(DISTINCT pwd.id) as order_linenum,
CASE WHEN count(DISTINCT pwd.id) = 1 THEN '1行订单占比'
     WHEN count(DISTINCT pwd.id) >= 2 AND count(DISTINCT pwd.id) < 10 THEN '2-10行订单占比'
     WHEN count(DISTINCT pwd.id) >= 10 AND count(DISTINCT pwd.id) < 20 THEN '10-20行订单占比'
     WHEN count(DISTINCT pwd.id) >= 20 AND count(DISTINCT pwd.id) < 30 THEN '20-30行订单占比'
     WHEN count(DISTINCT pwd.id) >= 30 AND count(DISTINCT pwd.id) < 40 THEN '30-40行订单占比'
     WHEN count(DISTINCT pwd.id) >= 40 AND count(DISTINCT pwd.id) < 50 THEN '40-50行订单占比'
     WHEN count(DISTINCT pwd.id) >= 50 AND count(DISTINCT pwd.id) < 60 THEN '50-60行订单占比'
     WHEN count(DISTINCT pwd.id) >= 60 THEN '60行以上订单占比' END AS type
FROM evo_wcs_g2p.picking_work pw
LEFT JOIN evo_wcs_g2p.picking_work_detail pwd
ON pwd.picking_work_id = pw.picking_work_id
WHERE pwd.quantity = pwd.fulfill_quantity AND pw.state = 'DONE' AND pw.updated_date >= @begin_time and pw.updated_date < @end_time AND pwd.project_code = 'A51118' AND pw.project_code = 'A51118' 
GROUP BY DATE_FORMAT(pw.updated_date,'%Y-%m-%d %H:00:00'),pw.order_id
)tt
GROUP BY DATE(tt.cur_date),tt.type
)tt1
LEFT JOIN
(
SELECT DATE(pwd.updated_date) as cur_date,
COUNT(DISTINCT pw.order_id) as order_total
FROM evo_wcs_g2p.picking_work_detail pwd
LEFT JOIN evo_wcs_g2p.picking_work pw
ON pwd.picking_work_id = pw.picking_work_id
WHERE pwd.quantity = pwd.fulfill_quantity AND pwd.updated_date >= @begin_time and pwd.updated_date < @end_time AND pwd.project_code = 'A51118' AND pw.project_code = 'A51118' 
GROUP BY DATE(pwd.updated_date)
)tt2
ON tt1.cur_date = tt2.cur_date
GROUP BY tt1.cur_date,tt1.type
ORDER BY tt1.cur_date,tt1.type+'0'