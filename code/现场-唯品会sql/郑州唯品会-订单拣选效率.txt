-- 订单拣货效率
SET @begin_time = '2021-11-16 00:00:00'; -- 设置开始时间
SET @end_time = '2021-11-17 00:00:00'; -- 设置结束时间

SELECT 
po.picking_order_number as '订单号',                                                                                 -- 取值于picking_order表的订单号
DATE(po.created_date) as '订单日期',                                                                                 -- 取值于picking_order表的订单创建时间
po.created_date as '订单创建时间',                                                                                   -- 取值于picking_order表的订单创建时间
tt3.picking_begin_time as '拣选开始时间',                                                                            -- 取值于工作站推实操的时间
po.last_updated_date as '订单完成时间',                                                                              -- 取值于picking_order表的订单完成时间
tt1.picking_time as '拣选订单耗时(s)',                                                                               -- 取值于人的拣选所耗总时间
SUM(pod.fulfill_quantity) as '拣选商品总个数',                                                                       -- 取值于picking_order_detail表的实捡数量
TIMESTAMPDIFF(SECOND,po.created_date,po.last_updated_date)/SUM(pod.fulfill_quantity) as '订单拣选总效率(pcs/件)',        -- 取值于订单总时间(s)/拣选商品总个数
CAST(tt1.picking_time/SUM(pod.fulfill_quantity) AS DECIMAL(10,2)) as '拣选商品平均速度(s/pcs)',                          -- 取值于人的拣选所耗总时间/拣选商品总个数
IFNULL(CAST(SUM(pod.fulfill_quantity)/tt2.station_into_times AS DECIMAL(10,2)),0) as '单次进站完成拣选件数'          -- 取值于拣选商品总个数/货架进站次数
FROM evo_wes_picking.picking_order po
LEFT JOIN evo_wes_picking.picking_order_detail pod
ON po.id = pod.picking_order_id
LEFT JOIN
(
SELECT tmp.order_id,SUM(TIMESTAMPDIFF(SECOND,tmp.start_time,tmp.done_time)) as picking_time -- 人的拣选所耗总时间
FROM 
(
SELECT DISTINCT tt.order_id,tt.start_time,tt.done_time
      FROM
      (
      SELECT 
      b.order_id,
      case when a.start_time >= b.start_time AND a.start_time <= b.done_time THEN b.start_time 
          when a.start_time > b.done_time AND a.done_time is not NULL THEN a.start_time 
          when a.start_time < b.start_time THEN a.start_time
          when a.start_time > b.done_time AND a.done_time is null THEN b.start_time
          when a.start_time is null THEN b.start_time
          when b.start_time is null THEN a.start_time END as start_time,
      case when a.done_time >= b.start_time AND a.done_time <= b.done_time THEN b.done_time
          when a.done_time > b.done_time THEN a.done_time
          when a.done_time < b.start_time THEN a.done_time 
          when a.done_time is null THEN b.done_time
          when a.done_time is null THEN DATE_FORMAT(DATE_ADD(a.start_time,INTERVAL 1 HOUR),'%Y-%m-%d %H:00:00.000')
          when b.done_time is null THEN a.done_time END as done_time
      FROM
      (
SELECT
se.order_id,se.start_time,se.done_time
FROM 
(
SELECT
tmp.order_id,IF(DATE_FORMAT(tmp.start_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00.000'),tmp.start_time) start_time,tmp.done_time
FROM
(
SELECT tt1.order_id,tt1.updated_date as start_time,jsc.updated_date as done_time
FROM 
(
SELECT pj.job_id,pj.order_id,pj.order_detail_id,jsc.updated_date
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON pj.job_id = jsc.job_id
WHERE pj.updated_date >= @begin_time AND pj.updated_date < @end_time AND jsc.state = 'START_EXECUTOR' 
ORDER BY pj.order_id
)tt1
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON jsc.job_id = tt1.job_id
WHERE jsc.state = 'DONE' AND jsc.job_id is not NULL
ORDER BY tt1.order_id,tt1.updated_date
)tmp
union all
SELECT
tmp.order_id,tmp.start_time,IF(DATE_FORMAT(tmp.start_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00.000'),tmp.start_time) done_time
FROM
(
SELECT tt1.order_id,tt1.updated_date as start_time,jsc.updated_date as done_time
FROM 
(
SELECT pj.job_id,pj.order_id,pj.order_detail_id,jsc.updated_date
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON pj.job_id = jsc.job_id
WHERE pj.updated_date >= @begin_time AND pj.updated_date < @end_time AND jsc.state = 'START_EXECUTOR' 
ORDER BY pj.order_id
)tt1
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON jsc.job_id = tt1.job_id
WHERE jsc.state = 'DONE' AND jsc.job_id is not NULL
ORDER BY tt1.order_id,tt1.updated_date
)tmp
)se
LEFT JOIN
(
SELECT
tmp.order_id,IF(DATE_FORMAT(tmp.start_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00.000'),tmp.start_time) start_time,tmp.done_time
FROM
(
SELECT tt1.order_id,tt1.updated_date as start_time,jsc.updated_date as done_time
FROM 
(
SELECT pj.job_id,pj.order_id,pj.order_detail_id,jsc.updated_date
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON pj.job_id = jsc.job_id
WHERE pj.updated_date >= @begin_time AND pj.updated_date < @end_time AND jsc.state = 'START_EXECUTOR' 
ORDER BY pj.order_id
)tt1
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON jsc.job_id = tt1.job_id
WHERE jsc.state = 'DONE' AND jsc.job_id is not NULL
ORDER BY tt1.order_id,tt1.updated_date
)tmp
union all
SELECT
tmp.order_id,tmp.start_time,IF(DATE_FORMAT(tmp.start_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00.000'),tmp.start_time) done_time
FROM
(
SELECT tt1.order_id,tt1.updated_date as start_time,jsc.updated_date as done_time
FROM 
(
SELECT pj.job_id,pj.order_id,pj.order_detail_id,jsc.updated_date
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON pj.job_id = jsc.job_id
WHERE pj.updated_date >= @begin_time AND pj.updated_date < @end_time AND jsc.state = 'START_EXECUTOR' 
ORDER BY pj.order_id
)tt1
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON jsc.job_id = tt1.job_id
WHERE jsc.state = 'DONE' AND jsc.job_id is not NULL
ORDER BY tt1.order_id,tt1.updated_date
)tmp
)tt
ON se.order_id = tt.order_id AND ((se.start_time >= tt.start_time AND se.done_time < tt.done_time) OR (se.done_time >= tt.start_time AND se.done_time <= tt.done_time))
WHERE tt.start_time is NULL
)a
RIGHT JOIN
(
SELECT
tmp.order_id,IF(DATE_FORMAT(tmp.start_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00.000'),MIN(tmp.start_time)) as start_time,MAX(tmp.done_time1) as done_time
FROM 
(
SELECT
se.order_id,se.start_time,se.done_time,tt.start_time as start_time1,tt.done_time as done_time1
FROM
(
SELECT
tmp.order_id,IF(DATE_FORMAT(tmp.start_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00.000'),tmp.start_time) start_time,tmp.done_time
FROM
(
SELECT tt1.order_id,tt1.updated_date as start_time,jsc.updated_date as done_time
FROM 
(
SELECT pj.job_id,pj.order_id,pj.order_detail_id,jsc.updated_date
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON pj.job_id = jsc.job_id
WHERE pj.updated_date >= @begin_time AND pj.updated_date < @end_time AND jsc.state = 'START_EXECUTOR' 
ORDER BY pj.order_id
)tt1
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON jsc.job_id = tt1.job_id
WHERE jsc.state = 'DONE' AND jsc.job_id is not NULL
ORDER BY tt1.order_id,tt1.updated_date
)tmp
union all
SELECT
tmp.order_id,tmp.start_time,IF(DATE_FORMAT(tmp.start_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00.000'),tmp.start_time) done_time
FROM
(
SELECT tt1.order_id,tt1.updated_date as start_time,jsc.updated_date as done_time
FROM 
(
SELECT pj.job_id,pj.order_id,pj.order_detail_id,jsc.updated_date
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON pj.job_id = jsc.job_id
WHERE pj.updated_date >= @begin_time AND pj.updated_date < @end_time AND jsc.state = 'START_EXECUTOR' 
ORDER BY pj.order_id
)tt1
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON jsc.job_id = tt1.job_id
WHERE jsc.state = 'DONE' AND jsc.job_id is not NULL AND DATE_FORMAT(tt1.updated_date,'%Y-%m-%d %H:00:00') != DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00')
ORDER BY tt1.order_id,tt1.updated_date
)tmp
)se
LEFT JOIN
(
SELECT
se.order_id,se.start_time,se.done_time
FROM 
(
SELECT
tmp.order_id,IF(DATE_FORMAT(tmp.start_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00.000'),tmp.start_time) start_time,tmp.done_time
FROM
(
SELECT tt1.order_id,tt1.updated_date as start_time,jsc.updated_date as done_time
FROM 
(
SELECT pj.job_id,pj.order_id,pj.order_detail_id,jsc.updated_date
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON pj.job_id = jsc.job_id
WHERE pj.updated_date >= @begin_time AND pj.updated_date < @end_time AND jsc.state = 'START_EXECUTOR' 
ORDER BY pj.order_id
)tt1
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON jsc.job_id = tt1.job_id
WHERE jsc.state = 'DONE' AND jsc.job_id is not NULL
ORDER BY tt1.order_id,tt1.updated_date
)tmp
union all
SELECT
tmp.order_id,tmp.start_time,IF(DATE_FORMAT(tmp.start_time,'%Y-%m-%d %H:00:00')<DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00'),DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:00:00.000'),tmp.start_time) done_time
FROM
(
SELECT tt1.order_id,tt1.updated_date as start_time,jsc.updated_date as done_time
FROM 
(
SELECT pj.job_id,pj.order_id,pj.order_detail_id,jsc.updated_date
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON pj.job_id = jsc.job_id
WHERE pj.updated_date >= @begin_time AND pj.updated_date < @end_time AND jsc.state = 'START_EXECUTOR'
ORDER BY pj.order_id
)tt1
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON jsc.job_id = tt1.job_id
WHERE jsc.state = 'DONE' AND jsc.job_id is not NULL AND DATE_FORMAT(tt1.updated_date,'%Y-%m-%d %H:00:00') != DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00')
ORDER BY tt1.order_id,tt1.updated_date
)tmp 
)se
)tt
ON se.order_id = tt.order_id AND ((se.start_time >= tt.done_time AND se.done_time < tt.done_time) OR (se.done_time >= tt.start_time AND se.done_time <= tt.done_time))
WHERE tt.start_time is not NULL 
ORDER BY se.order_id,se.start_time
)tmp
GROUP BY tmp.order_id,DATE_FORMAT(tmp.done_time,'%Y-%m-%d %H:%i:00')
)b
ON a.order_id = b.order_id 
ORDER BY a.order_id,a.start_time
)tt
)tmp
GROUP BY tmp.order_id
)tt1
ON tt1.order_id = po.id
LEFT JOIN 
(
SELECT pj.order_id,COUNT(se.id) as station_into_times -- 货架进站次数
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_station.station_entry se
ON se.idempotent_id = pj.job_id
WHERE pj.updated_date >= @begin_time AND pj.updated_date < @end_time AND pj.state = 'DONE'
GROUP BY pj.order_id
)tt2
ON tt2.order_id = po.id
LEFT JOIN
(
SELECT pj.order_id,MIN(jsc.updated_date) as picking_begin_time -- 拣选单开始时间
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON pj.job_id = jsc.job_id
WHERE pj.updated_date >= @begin_time AND pj.updated_date < @end_time AND jsc.state = 'START_EXECUTOR'
GROUP BY pj.order_id
)tt3
ON tt3.order_id = po.id
WHERE po.state = 'DONE' AND po.done_date >= @begin_time AND po.done_date < @end_time
GROUP BY po.picking_order_number,DATE(po.done_date)