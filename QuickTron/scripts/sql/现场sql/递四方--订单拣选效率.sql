-- 订单拣货效率
SET @begin_time = '2021-11-16 00:00:00'; -- 设置开始时间
SET @end_time = '2021-11-17 00:00:00'; -- 设置结束时间

SELECT 
po.picking_order_number as '拣选订单号',                                                                      -- 取值于picking_order表的订单号
DATE(po.done_date) as '拣货日期',                                                                             -- 取值于picking_order表的订单完成时间
po.udf4 as '拣选单开始时间',                                                                                  -- 取值于picking_order表项目定制化的字段
po.done_date as '拣选单完成时间',                                                                             -- 取值于picking_order表的订单完成时间
TIMESTAMPDIFF(SECOND,po.udf4,po.done_date) as '拣选单总时间(s)',                                              -- 取值于picking_order表的更新时间和创建时间的差值
SUM(pod.fulfill_quantity) as '拣选商品总个数',                                                                -- 取值于picking_order_detail表的实捡数量
TIMESTAMPDIFF(SECOND,po.udf4,po.done_date)/SUM(pod.fulfill_quantity) as '拣选总效率(pcs/件)',                 -- 取值于拣选单总时间(s)/拣选商品总个数
tt1.picking_time as '人的拣选所耗总时间',                                                                     -- 取值于任务完成时间和进站弹窗时间的差值
IFNULL(tt4.bucket_wait_time,0) as '货架等待时长',                                                             -- 取值于station_entry表的上一个出站和下一个进站的时间差值
tt7.first_into_station_time as '第一个货架到站时间',                                                          -- 取值于订单的第一个任务状态为开始实操的更新时间
IFNULL(tt2.station_into_times,0) as '货架进站次数',                                                           -- 取值于station_entry表的进站次数
tt5.bucket_convey_time as '货架调度总时间(s)',                                                                -- 取值于job_state_change表的小车开始执行搬运货架任务至工作站的时间差值
CAST(tt5.bucket_convey_time/tt6.bucket_num AS DECIMAL(10,2)) as '货架调度平均时间',                           -- 取值于货架调度总时间(s)/货架数
CAST(tt1.picking_time/SUM(pod.fulfill_quantity) AS DECIMAL(10,2)) as '拣选平均速度(s/pcs)',                   -- 取值于人的拣选所耗总时间/拣选商品总个数
IFNULL(CAST(SUM(pod.fulfill_quantity)/tt2.station_into_times AS DECIMAL(10,2)),0) as '单次进站完成平均件数',  -- 取值于拣选商品总个数/货架进站次数
IFNULL(CAST(SUM(tt3.order_row)/tt2.station_into_times AS DECIMAL(10,2)),0) as '单次进站拣选单行数'            -- 取值于picking_work_detail表的行数/货架进站次数
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
SELECT pwd.picking_order_detail_id,COUNT(DISTINCT pwd.id) as order_row -- 拣选单行数
FROM evo_wcs_g2p.picking_work_detail pwd
WHERE pwd.updated_date >= @begin_time AND pwd.updated_date < @end_time AND pwd.quantity = pwd.fulfill_quantity
GROUP BY pwd.picking_order_detail_id
)tt3
ON tt3.picking_order_detail_id = pod.id
LEFT JOIN
(
SELECT b.order_id,SUM(b.bucket_wait_time) as bucket_wait_time -- 货架等待时长
FROM
(
SELECT a.order_id,MIN(a.bucket_wait_time) as bucket_wait_time
FROM
(
SELECT tmp1.order_id,tmp1.entry_time,TIMESTAMPDIFF(SECOND,tmp1.exit_time,tmp2.entry_time) as bucket_wait_time
FROM
(
SELECT pj.job_id,pj.order_id,se.entry_time,se.exit_time
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_station.station_entry se
ON se.idempotent_id = pj.job_id
WHERE pj.updated_date >= @begin_time AND pj.updated_date < @end_time AND pj.state = 'DONE'
)tmp1 
LEFT JOIN
(
SELECT pj.job_id,pj.order_id,se.entry_time,se.exit_time
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_station.station_entry se
ON se.idempotent_id = pj.job_id
WHERE pj.updated_date >= @begin_time AND pj.updated_date < @end_time AND pj.state = 'DONE'
)tmp2
ON tmp1.order_id = tmp2.order_id AND tmp1.entry_time < tmp2.entry_time
WHERE tmp2.job_id is not NULL
ORDER BY tmp1.order_id,tmp1.entry_time
)a
GROUP BY a.entry_time
)b
GROUP BY b.order_id
)tt4
ON tt4.order_id = po.id
LEFT JOIN
(
SELECT a.order_id,SUM(a.bucket_convey_time) as bucket_convey_time -- 货架调度总时间(s)
FROM
(
SELECT tmp1.order_id,TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) as bucket_convey_time
FROM
(
SELECT pj.job_id,pj.order_id,jsc.state,jsc.updated_date
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON jsc.job_id = pj.job_id
WHERE pj.state = 'DONE' AND jsc.state = 'INIT_JOB' AND pj.updated_date >= @begin_time AND pj.updated_date < @end_time
)tmp1
LEFT JOIN
(
SELECT pj.job_id,pj.order_id,jsc.state,jsc.updated_date
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_wcs_g2p.job_state_change jsc
ON jsc.job_id = pj.job_id
WHERE pj.state = 'DONE' AND jsc.state = 'START_EXECUTOR' AND pj.updated_date >= @begin_time AND pj.updated_date < @end_time
)tmp2
ON tmp1.job_id = tmp2.job_id
)a
GROUP BY a.order_id
)tt5
ON tt5.order_id = po.id
LEFT JOIN
(
SELECT pj.order_id,COUNT(DISTINCT bucket_code) as bucket_num -- 货架数
FROM evo_wcs_g2p.picking_job pj
WHERE pj.state = 'DONE'  AND pj.updated_date >= @begin_time AND pj.updated_date < @end_time
GROUP BY pj.order_id
)tt6
ON tt6.order_id = po.id
LEFT JOIN
(
SELECT pj.order_id,MIN(c.updated_date) as first_into_station_time -- 第一个货架到站时间
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_wcs_g2p.job_state_change c
ON pj.job_id = c.job_id
WHERE pj.state = 'DONE' AND c.state = 'START_EXECUTOR' AND pj.updated_date >= @begin_time AND pj.updated_date < @end_time
GROUP BY pj.order_id
)tt7
ON tt7.order_id = po.id
WHERE po.state = 'DONE' AND po.done_date >= @begin_time AND po.done_date < @end_time
GROUP BY po.picking_order_number,DATE(po.done_date)