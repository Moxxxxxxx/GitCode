# 小车工作时长
SET @begin_time = '2022-01-14 00:00:00';
SET @end_time = '2022-01-15 00:00:00';

SELECT tt.`统计时间`,
       '--' as '小车编码',
       SEC_TO_TIME(SUM(tt.`工作时长`)) as '工作时长',
       SEC_TO_TIME(SUM(tt.`拣货时长`)) as '拣货时长',
       SEC_TO_TIME(SUM(tt.`入库时长`)) as '入库时长',
       SUM(tt.`拣货行数`) as '拣货行数'
			 -- SUM(tt.`入库行数`) as '入库行数'

FROM
(
SELECT t1.cur_date as '统计时间',
       t1.agv_code as '小车编码',
       t1.work_time as '工作时长',
       t2.picking_time as '拣货时长',
       t3.replenish_time as '入库时长',
       t4.picking_lines as '拣货行数'
			 -- t5.replenish_lines as '入库行数'
FROM
(
SELECT tmp.cur_date,tmp.agv_code,SUM(tmp.work_time) as work_time
FROM
(
-- 入库+拣选时长
SELECT DATE(c1.updated_date) as cur_date,c1.agv_code,SUM(TIMESTAMPDIFF(SECOND,c1.updated_date,c2.updated_date)) as work_time
FROM evo_wcs_g2p.job_state_change c1
LEFT JOIN evo_wcs_g2p.job_state_change c2
ON c1.job_id = c2.job_id AND c1.job_type = c2.job_type
WHERE c1.state = 'INIT_JOB' AND c2.state = 'DONE' AND c1.job_type in ('G2P_ONLINE_PUTAWAY','G2P_OFFLINE_PUTAWAY', 'G2P_ONLINE_PICK') AND c1.updated_date >= @begin_time AND c1.updated_date < @end_time
GROUP BY DATE(c1.updated_date),c1.agv_code
)tmp
GROUP BY tmp.cur_date,tmp.agv_code
)t1
LEFT JOIN
(
-- 拣选时长
SELECT DATE(c1.updated_date) as cur_date,c1.agv_code,SUM(TIMESTAMPDIFF(SECOND,c1.updated_date,c2.updated_date)) as picking_time
FROM evo_wcs_g2p.job_state_change c1
LEFT JOIN evo_wcs_g2p.job_state_change c2
ON c1.job_id = c2.job_id AND c1.job_type = c2.job_type
WHERE c1.state = 'INIT_JOB' AND c2.state = 'DONE' AND c1.job_type = 'G2P_ONLINE_PICK' AND c1.updated_date >= @begin_time AND c1.updated_date < @end_time
GROUP BY DATE(c1.updated_date),c1.agv_code
)t2
ON t1.agv_code = t2.agv_code AND t1.cur_date = t2.cur_date
LEFT JOIN
(
-- 入库时长
SELECT DATE(c1.updated_date) as cur_date,c1.agv_code,SUM(TIMESTAMPDIFF(SECOND,c1.updated_date,c2.updated_date)) as replenish_time
FROM evo_wcs_g2p.job_state_change c1
LEFT JOIN evo_wcs_g2p.job_state_change c2
ON c1.job_id = c2.job_id AND c1.job_type = c2.job_type
WHERE c1.state = 'INIT_JOB' AND c2.state = 'DONE' AND c1.job_type in ('G2P_ONLINE_PUTAWAY','G2P_OFFLINE_PUTAWAY') AND c1.updated_date >= @begin_time AND c1.updated_date < @end_time
GROUP BY DATE(c1.updated_date),c1.agv_code
)t3
ON t1.agv_code = t3.agv_code AND t1.cur_date = t3.cur_date
LEFT JOIN
(
-- 拣货行数
SELECT DATE(pj.updated_date) as cur_date,pj.agv_code,COUNT(DISTINCT pod.id) as picking_lines
FROM evo_wes_picking.picking_order po
LEFT JOIN evo_wes_picking.picking_order_detail pod
ON po.id = pod.picking_order_id
LEFT JOIN evo_wcs_g2p.picking_job pj
ON pod.id = pj.order_detail_id
WHERE pj.state = 'DONE' AND pj.updated_date >= @begin_time AND pj.updated_date < @end_time
GROUP BY DATE(pj.updated_date),pj.agv_code
)t4
ON t1.agv_code = t4.agv_code AND t1.cur_date = t4.cur_date
/*
LEFT JOIN
(
SELECT tmp.cur_date,tmp.agv_code,SUM(tmp.replenish_lines) as replenish_lines
FROM
(
-- 直接上架入库行数,待核实
SELECT  DATE(pj.updated_date) as cur_date,pj.agv_code,COUNT(DISTINCT pj.detail_id) as replenish_lines
FROM evo_wcs_g2p.putaway_job pj
LEFT JOIN evo_wcs_g2p.putaway_work pw
ON pj.put_away_work_id = pw.work_id
LEFT JOIN evo_wes_replenish.direct_put_away_apply_bill b
ON pw.order_id = b.id
WHERE pj.state = 'DONE' AND pj.updated_date >= @begin_time AND pj.updated_date < @end_time
GROUP BY DATE(pj.updated_date),pj.agv_code

UNION ALL

-- 指导上架入库行数,无法关联任务和订单行
SELECT DATE(pj.updated_date) as cur_date,pj.agv_code,COUNT(DISTINCT rod.id) as replenish_lines
FROM evo_wcs_g2p.guided_put_away_job pj
LEFT JOIN evo_wes_replenish.replenish_order ro
ON pj.order_id = ro.id
LEFT JOIN evo_wes_replenish.replenish_order_detail rod
ON ro.id = rod.replenish_order_id
WHERE pj.state = 'DONE' AND pj.updated_date >= @begin_time AND pj.updated_date < @end_time
GROUP BY DATE(pj.updated_date),pj.agv_code
)tmp
GROUP BY tmp.cur_date,tmp.agv_code
)t5
ON t1.agv_code = t5.agv_code AND t1.cur_date = t5.cur_date
*/
)tt
GROUP BY tt.`统计时间`

UNION ALL

SELECT t1.cur_date as '统计时间',
       t1.agv_code as '小车编码',
       t1.work_time as '工作时长',
       t2.picking_time as '拣货时长',
       t3.replenish_time as '入库时长',
       t4.picking_lines as '拣货行数'
			 -- t5.replenish_lines as '入库行数'
FROM
(
SELECT tmp.cur_date,tmp.agv_code,SEC_TO_TIME(SUM(tmp.work_time)) as work_time
FROM
(
-- 入库+拣选时长
SELECT DATE(c1.updated_date) as cur_date,c1.agv_code,SUM(TIMESTAMPDIFF(SECOND,c1.updated_date,c2.updated_date)) as work_time
FROM evo_wcs_g2p.job_state_change c1
LEFT JOIN evo_wcs_g2p.job_state_change c2
ON c1.job_id = c2.job_id AND c1.job_type = c2.job_type
WHERE c1.state = 'INIT_JOB' AND c2.state = 'DONE' AND c1.job_type in ('G2P_ONLINE_PUTAWAY','G2P_OFFLINE_PUTAWAY', 'G2P_ONLINE_PICK') AND c1.updated_date >= @begin_time AND c1.updated_date < @end_time
GROUP BY DATE(c1.updated_date),c1.agv_code
)tmp
GROUP BY tmp.cur_date,tmp.agv_code
)t1
LEFT JOIN
(
-- 拣选时长
SELECT DATE(c1.updated_date) as cur_date,c1.agv_code,SEC_TO_TIME(SUM(TIMESTAMPDIFF(SECOND,c1.updated_date,c2.updated_date))) as picking_time
FROM evo_wcs_g2p.job_state_change c1
LEFT JOIN evo_wcs_g2p.job_state_change c2
ON c1.job_id = c2.job_id AND c1.job_type = c2.job_type
WHERE c1.state = 'INIT_JOB' AND c2.state = 'DONE' AND c1.job_type = 'G2P_ONLINE_PICK' AND c1.updated_date >= @begin_time AND c1.updated_date < @end_time
GROUP BY DATE(c1.updated_date),c1.agv_code
)t2
ON t1.agv_code = t2.agv_code AND t1.cur_date = t2.cur_date
LEFT JOIN
(
-- 入库时长
SELECT DATE(c1.updated_date) as cur_date,c1.agv_code,SEC_TO_TIME(SUM(TIMESTAMPDIFF(SECOND,c1.updated_date,c2.updated_date))) as replenish_time
FROM evo_wcs_g2p.job_state_change c1
LEFT JOIN evo_wcs_g2p.job_state_change c2
ON c1.job_id = c2.job_id AND c1.job_type = c2.job_type
WHERE c1.state = 'INIT_JOB' AND c2.state = 'DONE' AND c1.job_type in ('G2P_ONLINE_PUTAWAY','G2P_OFFLINE_PUTAWAY') AND c1.updated_date >= @begin_time AND c1.updated_date < @end_time
GROUP BY DATE(c1.updated_date),c1.agv_code
)t3
ON t1.agv_code = t3.agv_code AND t1.cur_date = t3.cur_date
LEFT JOIN
(
-- 拣货行数
SELECT DATE(pj.updated_date) as cur_date,pj.agv_code,COUNT(DISTINCT pod.id) as picking_lines
FROM evo_wes_picking.picking_order po
LEFT JOIN evo_wes_picking.picking_order_detail pod
ON po.id = pod.picking_order_id
LEFT JOIN evo_wcs_g2p.picking_job pj
ON pod.id = pj.order_detail_id
WHERE pj.state = 'DONE' AND pj.updated_date >= @begin_time AND pj.updated_date < @end_time
GROUP BY DATE(pj.updated_date),pj.agv_code
)t4
ON t1.agv_code = t4.agv_code AND t1.cur_date = t4.cur_date
/*
LEFT JOIN
(
SELECT tmp.cur_date,tmp.agv_code,SUM(tmp.replenish_lines) as replenish_lines
FROM
(
-- 直接上架入库行数,待核实
SELECT  DATE(pj.updated_date) as cur_date,pj.agv_code,COUNT(DISTINCT pj.detail_id) as replenish_lines
FROM evo_wcs_g2p.putaway_job pj
LEFT JOIN evo_wcs_g2p.putaway_work pw
ON pj.put_away_work_id = pw.work_id
LEFT JOIN evo_wes_replenish.direct_put_away_apply_bill b
ON pw.order_id = b.id
WHERE pj.state = 'DONE' AND pj.updated_date >= @begin_time AND pj.updated_date < @end_time
GROUP BY DATE(pj.updated_date),pj.agv_code

UNION ALL

-- 指导上架入库行数,无法关联任务和订单行
SELECT DATE(pj.updated_date) as cur_date,pj.agv_code,COUNT(DISTINCT rod.id) as replenish_lines
FROM evo_wcs_g2p.guided_put_away_job pj
LEFT JOIN evo_wes_replenish.replenish_order ro
ON pj.order_id = ro.id
LEFT JOIN evo_wes_replenish.replenish_order_detail rod
ON ro.id = rod.replenish_order_id
WHERE pj.state = 'DONE' AND pj.updated_date >= @begin_time AND pj.updated_date < @end_time
GROUP BY DATE(pj.updated_date),pj.agv_code
)tmp
GROUP BY tmp.cur_date,tmp.agv_code
)t5
ON t1.agv_code = t5.agv_code AND t1.cur_date = t5.cur_date
*/