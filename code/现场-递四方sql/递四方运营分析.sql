-- -------------------出库数据：每日出库单量,每日出库行数,每日出库件数--------------------- --
-- -------------------拣选单结构：每日出库件单比,每日出库件行比--------------------- --
-- -------------------组波策略：每日拣选任务数--------------------- --
INSERT INTO qt_dsf.picking_date_detail(date,picking_order_num,picking_order_linenum,picking_quanlity,quanlity_num_once_rate,quanlity_linenum_once_rate,picking_job_num,picking_bucket_num,picking_bucketface_num)

SELECT DATE(pw.updated_date) as '日期',COUNT(DISTINCT pw.order_id) as '每日出库单量',COUNT(DISTINCT pwd.id) as '每日出库行数',SUM(pwd.fulfill_quantity) as '每日出库件数',
       CAST(SUM(pwd.fulfill_quantity)/COUNT(DISTINCT pw.order_id) AS decimal(10,2)) as '每日出库件单比',CAST(SUM(pwd.fulfill_quantity)/COUNT(DISTINCT pwd.id) AS decimal(10,2)) as '每日出库件行比',
       COUNT(DISTINCT pj.job_id) as '每日拣选任务数',COUNT(pj.bucket_code) as '每日拣选货架',COUNT(pj.bucket_face_num) as '每日拣选货架面'
FROM evo_wcs_g2p.picking_work pw
LEFT JOIN evo_wcs_g2p.picking_work_detail pwd
ON pw.picking_work_id = pwd.picking_work_id
LEFT JOIN evo_wcs_g2p.picking_job pj
ON pj.picking_work_id = pw.picking_work_id
WHERE pwd.quantity = pwd.fulfill_quantity
GROUP BY DATE(pw.updated_date)


-- -------------------出库数据：月出库增长率,月平均每日拣选单数,月平均每日出库行数,月平均每日出库件数,月出库总单数,月出库总行数,月出库总件数--------------------- --
-- -------------------拣选单结构：月平均件单比,月平均行单比,月平均件行比,月单品单件拣选单占比,月单品多件拣选单占比,月多品多件拣选单占比--------------------- --
-- -------------------SKU数据：平均每天出库SKU数,月整体动销SKU数--------------------- --
INSERT INTO qt_dsf.picking_month_detail(date,picking_rise_rate,picking_order_avg,picking_order_linenum_avg,picking_quanlity_avg,picking_order_month,picking_order_linenum_month,picking_quanlity_month,quanlity_order_avg,order_linenum_avg,quanlity_linenum_avg ,single_sku_quanlity_rate,single_sku_quanlities_rate,multiple_sku_quanlities_rate,picking_sku_quanlity_avg,picking_sku_num)

SELECT tmp1.`月份`,
       CAST((tmp1.`每月出库件数`- tmp2.`每月出库件数`)/tmp2.`每月出库件数` AS decimal(10,2)) as '月出库增长率',
       tmp3.`月平均每日拣选单数`,tmp3.`月平均每日出库行数`,tmp3.`月平均每日出库件数`,tmp3.`月出库总单数`,tmp3.`月出库总行数`,tmp3.`月出库总件数`,
       tmp3.`月平均件单比`,tmp3.`月平均行单比`,tmp3.`月平均件行比`,
       tmp4.`月单品单件拣选单占比`,tmp4.`月单品多件拣选单占比`,tmp4.`月多品多件拣选单占比`,
       tmp3.`平均每天出库SKU数`,tmp3.`月整体动销SKU数`
FROM
(
SELECT DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day) as '月份',SUM(pwd.fulfill_quantity) as '每月出库件数'
FROM evo_wcs_g2p.picking_work pw
LEFT JOIN evo_wcs_g2p.picking_work_detail pwd
ON pw.picking_work_id = pwd.picking_work_id
WHERE pwd.quantity = pwd.fulfill_quantity
GROUP BY DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day)
)tmp1
LEFT JOIN
(
SELECT DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day) as '月份',SUM(pwd.fulfill_quantity) as '每月出库件数'
FROM evo_wcs_g2p.picking_work pw
LEFT JOIN evo_wcs_g2p.picking_work_detail pwd
ON pw.picking_work_id = pwd.picking_work_id
WHERE pwd.quantity = pwd.fulfill_quantity
GROUP BY DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day)
)tmp2
ON TIMESTAMPDIFF(MONTH,tmp2.`月份`,tmp1.`月份`) = 1
LEFT JOIN
(
SELECT DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day) as '月份',
       CAST(COUNT(DISTINCT pw.order_id)/COUNT(DISTINCT DATE(pw.updated_date)) AS decimal(10,2)) as '月平均每日拣选单数',
       CAST(COUNT(DISTINCT pwd.id)/COUNT(DISTINCT DATE(pw.updated_date)) AS decimal(10,2)) as '月平均每日出库行数',
       CAST(SUM(pwd.fulfill_quantity)/COUNT(DISTINCT DATE(pw.updated_date)) AS decimal(10,2)) as '月平均每日出库件数',
       COUNT(DISTINCT pw.order_id) as '月出库总单数',
       COUNT(DISTINCT pwd.id) as '月出库总行数',
       SUM(pwd.fulfill_quantity) as '月出库总件数',
       CAST(SUM(pwd.fulfill_quantity)/COUNT(DISTINCT pw.order_id) AS decimal(10,2)) as '月平均件单比',
       CAST(COUNT(DISTINCT pwd.id)/COUNT(DISTINCT pw.order_id) AS decimal(10,2)) as '月平均行单比',
       CAST(SUM(pwd.fulfill_quantity)/COUNT(DISTINCT pwd.id) AS decimal(10,2)) as '月平均件行比',
       CAST(COUNT(DISTINCT pwd.sku_id)/COUNT(DISTINCT DATE(pwd.updated_date)) AS decimal(10,2)) as '平均每天出库SKU数',
       COUNT(DISTINCT pwd.sku_id)as '月整体动销SKU数'
FROM evo_wcs_g2p.picking_work pw
LEFT JOIN evo_wcs_g2p.picking_work_detail pwd
ON pw.picking_work_id = pwd.picking_work_id 
WHERE pwd.quantity = pwd.fulfill_quantity
GROUP BY DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day)
)tmp3
ON tmp1.`月份` = tmp3.`月份`
LEFT JOIN
(
SELECT t2.`月份`,
       CAST(SUM(t2.`月单品单件拣选单`)/COUNT(DISTINCT t2.order_id) AS decimal(10,2)) as '月单品单件拣选单占比',
       CAST(SUM(t2.`月单品多件拣选单`)/COUNT(DISTINCT t2.order_id) AS decimal(10,2)) as '月单品多件拣选单占比',
       CAST(SUM(t2.`月多品多件拣选单`)/COUNT(DISTINCT t2.order_id) AS decimal(10,2)) as '月多品多件拣选单占比'
FROM
(
SELECT t1.`月份`,t1.order_id,
       IF(SUM(t1.`SKU数量`)=1 AND SUM(t1.`SKU出库件数`)=1,COUNT(DISTINCT t1.order_id),0) as '月单品单件拣选单',
       IF(SUM(t1.`SKU数量`)=1 AND SUM(t1.`SKU出库件数`)>1,COUNT(DISTINCT t1.order_id),0) as '月单品多件拣选单',
       IF(SUM(t1.`SKU数量`)>1 AND SUM(t1.`SKU出库件数`)>1,COUNT(DISTINCT t1.order_id),0) as '月多品多件拣选单'
FROM
(
SELECT pw.order_id,pwd.sku_id,COUNT(pwd.sku_id) as 'SKU数量',SUM(pwd.fulfill_quantity) as 'SKU出库件数',DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day) as '月份'
FROM evo_wcs_g2p.picking_work pw
LEFT JOIN evo_wcs_g2p.picking_work_detail pwd
ON pw.picking_work_id = pwd.picking_work_id
WHERE pwd.quantity = pwd.fulfill_quantity
GROUP BY pw.order_id,pwd.sku_id
)t1
GROUP BY t1.`月份`,t1.order_id
)t2
GROUP BY t2.`月份`
)tmp4
ON tmp1.`月份` = tmp4.`月份`


-- -------------------SKU数据：各SKU月出库量--------------------- --
SELECT DATE_ADD(DATE(pwd.updated_date),INTERVAL -day(DATE(pwd.updated_date))+1 day) as '月份',
       pwd.sku_id as 'SKU',SUM(pwd.fulfill_quantity) as '各SKU月出库量'
FROM evo_wcs_g2p.picking_work_detail pwd
WHERE pwd.quantity = pwd.fulfill_quantity AND pwd.updated_date >= '2021-09-22 00:00:00' and pwd.updated_date < @end_time
GROUP BY pwd.sku_id,DATE_ADD(DATE(pwd.updated_date),INTERVAL -day(DATE(pwd.updated_date))+1 day)


-- -------------------机器人使用率\拣选效率：每日机器人使用率,每日机器人峰值使用率--------------------- --
SELECT DATE_FORMAT(t1.updated_date,'%Y-%m-%d') as '日期',
       CAST(SUM(TIMESTAMPDIFF(SECOND,t1.updated_date,t2.updated_date))/COUNT(DISTINCT t1.agv_code)/86400 AS DECIMAL(10,2)) as '每日机器人使用率', -- 按一天24小时计算
       CAST(SUM(TIMESTAMPDIFF(SECOND,t1.updated_date,t2.updated_date))/COUNT(DISTINCT t1.agv_code)/36000 AS DECIMAL(10,2)) as '每日机器人峰值使用率' -- 按一天10小时计算
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date
FROM evo_wcs_g2p.job_state_change c
WHERE c.state = 'INIT_JOB' AND c.job_type = 'G2P_BUCKET_MOVE'  -- 小车取货架
)t1
LEFT JOIN
(
SELECT c.agv_code,c.job_id,c.updated_date
FROM evo_wcs_g2p.job_state_change c
WHERE c.state = 'DONE' AND c.job_type = 'G2P_BUCKET_MOVE' -- 拣选任务结束
)t2
ON t1.job_id =t2.job_id
WHERE t2.updated_date is not NULL AND DATE(t1.updated_date) = DATE(t2.updated_date)
GROUP BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d')
ORDER BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d')


-- -------------------机器人使用率：单日机器人峰值使用率--------------------- --
SELECT DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00') as '日期',
       SUM(TIMESTAMPDIFF(SECOND,t1.updated_date,t2.updated_date))/COUNT(DISTINCT t1.agv_code)/3600 as '单日机器人使用率'
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date
FROM evo_wcs_g2p.job_state_change c
WHERE c.state = 'INIT_JOB' AND c.job_type = 'G2P_BUCKET_MOVE'  -- 小车取货架
)t1
LEFT JOIN
(
SELECT c.agv_code,c.job_id,c.updated_date
FROM evo_wcs_g2p.job_state_change c
WHERE c.state = 'DONE' AND c.job_type = 'G2P_BUCKET_MOVE' -- 拣选任务结束
)t2
ON t1.job_id =t2.job_id
WHERE t2.updated_date is not NULL AND DATE(t1.updated_date) = DATE(t2.updated_date) AND DATE(t1.updated_date) = '2021-09-22'
GROUP BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00')
ORDER BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00');


-- -------------------机器人使用率：机器人状态详情--------------------- --
SELECT tmp.`日期`,COUNT(DISTINCT tmp.agv_code) as '机器人开启台数',SUM(tmp.`上一任务占用中`)as '上一任务占用中',SUM(tmp.`等待占用结束取货架`)as '等待占用结束取货架',SUM(tmp.`接到任务取货架`)as '接到任务取货架',SUM(tmp.`送货架到工作站`) as '送货架到工作站',SUM(tmp.`工作站等待推实操`) as '工作站等待推实操',SUM(tmp.`人工拣选停留`) as '人工拣选停留',SUM(tmp.`状态回滚`) as '状态回滚',SUM(tmp.`还货架`) as '还货架'
FROM
(
SELECT t3.agv_code,DATE_FORMAT(t3.updated_date,'%Y-%m-%d %H:00:00') as '日期',
       IF((t3.job_type = 'G2P_BUCKET_MOVE' AND t3.state = 'INIT' AND t3.state1 = 'INIT_JOB'),1,0) as '上一任务占用中',
       IF((t3.job_type = 'G2P_ONLINE_PICK' AND t3.state = 'WAITING_AGV' AND t3.state1 = 'GO_TARGET'),1,0) as '等待占用结束取货架',
       IF((t3.job_type = 'G2P_BUCKET_MOVE' AND t3.state = 'INIT_JOB' AND t3.state1 = 'GO_TARGET') OR (t3.job_type = 'G2P_ONLINE_PICK' AND t3.state = 'INIT_JOB' AND t3.state1 = 'GO_TARGET'),1,0) as '接到任务取货架',
       IF( (t3.job_type = 'G2P_ONLINE_PICK' AND t3.state = 'GO_TARGET' AND t3.state1 = 'WAITING_EXECUTOR'),1,0) as '送货架到工作站',
       IF(t3.job_type = 'G2P_ONLINE_PICK' AND t3.state = 'WAITING_EXECUTOR' AND t3.state1 = 'START_EXECUTOR',1,0) as '工作站等待推实操',
       IF(t3.job_type = 'G2P_BUCKET_MOVE' AND t3.state = 'GO_TARGET' AND t3.state1 = 'DONE',1,0) as '还货架',
       IF(t3.job_type = 'G2P_ONLINE_PICK' AND t3.state = 'START_EXECUTOR' AND t3.state1 = 'DONE',1,0) as '人工拣选停留',
       IF(t3.state1 = 'ROLLBACK',1,0) as '状态回滚'
FROM
(
SELECT t1.*,t2.*
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE (c.job_type = 'G2P_ONLINE_PICK' OR c.job_type = 'G2P_BUCKET_MOVE')
)t1
LEFT JOIN
(
SELECT c.agv_code as agv_code1,c.job_id as job_id1,c.updated_date as updated_date1,c.state as state1,c.job_type job_type1
FROM evo_wcs_g2p.job_state_change c
WHERE  (c.job_type = 'G2P_ONLINE_PICK' OR c.job_type = 'G2P_BUCKET_MOVE')
)t2
ON t1.job_id = t2.job_id1 AND t1.updated_date < t2.updated_date1
WHERE t2.updated_date1 is not NULL AND t1.agv_code is not NULL AND t1.agv_code != '' 
ORDER BY t1.agv_code,t1.updated_date DESC
LIMIT 10000000
)t3
GROUP BY t3.agv_code,DATE_FORMAT(t3.updated_date,'%Y-%m-%d %H:00:00')
)tmp
GROUP BY tmp.`日期`



-- -------------------机器人任务时间：每日每小时的平均取货时间\每日每小时的送货时间\每日每小时的拣选停留时间\每日每小时的还货架时间--------------------- --
SELECT t1.`日期`,IFNULL(t1.`每日每小时的平均取货时间/s`,0) as '每日每小时的平均取货时间/s',IFNULL(t2.`每日每小时的送货时间/s`,0) as '每日每小时的送货时间/s',IFNULL(t3.`每日每小时的拣选停留时间/s`,0) as '每日每小时的拣选停留时间/s',IFNULL(t4.`每日每小时的还货架时间/s`,0) as '每日每小时的还货架时间/s'
FROM
(
SELECT DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00') as '日期',
      CAST(SUM(TIMESTAMPDIFF(SECOND,t1.updated_date,IF(DATE_FORMAT(t2.updated_date,'%Y-%m-%d %H:00:00')>DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00'),DATE_FORMAT(DATE_ADD(t1.updated_date,INTERVAL 1 HOUR),'%Y-%m-%d %H:00:00'),t2.updated_date)))/COUNT(DISTINCT t1.agv_code) AS DECIMAL(10,2)) as '每日每小时的平均取货时间/s'
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'INIT_JOB' 
)t1
LEFT JOIN
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'GO_TARGET' 
)t2
ON t1.job_id = t2.job_id
WHERE t2.job_id is not NULL
GROUP BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00')
)t1
LEFT JOIN
(
SELECT DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00') as '日期',
      CAST(SUM(TIMESTAMPDIFF(SECOND,t1.updated_date,IF(DATE_FORMAT(t2.updated_date,'%Y-%m-%d %H:00:00')>DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00'),DATE_FORMAT(DATE_ADD(t1.updated_date,INTERVAL 1 HOUR),'%Y-%m-%d %H:00:00'),t2.updated_date)))/COUNT(DISTINCT t1.agv_code) AS DECIMAL(10,2)) as '每日每小时的送货时间/s'
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'GO_TARGET' 
)t1
LEFT JOIN
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'DONE' 
)t2
ON t1.job_id = t2.job_id
WHERE t2.job_id is not NULL
GROUP BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00')
)t2
ON t1.`日期` = t2.`日期`
LEFT JOIN
(
SELECT DATE_FORMAT(tmp.updated_date,'%Y-%m-%d %H:00:00') as '日期',
      CAST(SUM(TIMESTAMPDIFF(SECOND,tmp.updated_date,IF(DATE_FORMAT(tmp.updated_date1,'%Y-%m-%d %H:00:00')>DATE_FORMAT(tmp.updated_date,'%Y-%m-%d %H:00:00'),DATE_FORMAT(DATE_ADD(tmp.updated_date,INTERVAL 1 HOUR),'%Y-%m-%d %H:00:00'),tmp.updated_date1)))/COUNT(DISTINCT tmp.agv_code) AS DECIMAL(10,2)) as '每日每小时的拣选停留时间/s'
FROM
(
SELECT t1.agv_code,t1.job_id,t1.updated_date,t1.state,t1.job_type,t2.agv_code1,t2.job_id1,t2.updated_date1,t2.state1,t2.job_type1
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_ONLINE_PICK' AND c.state = 'START_EXECUTOR' 
)t1
LEFT JOIN
(
SELECT c.agv_code as agv_code1,c.job_id as job_id1,c.updated_date as updated_date1,c.state as state1,c.job_type as job_type1
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_ONLINE_PICK' AND c.state = 'DONE' 
)t2
ON t1.job_id = t2.job_id1
WHERE t2.job_id1 is not NULL
GROUP BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:%i:00')
)tmp
GROUP BY DATE_FORMAT(tmp.updated_date,'%Y-%m-%d %H:00:00')
)t3
ON t1.`日期` = t3.`日期`
LEFT JOIN
(
SELECT DATE_FORMAT(bmj1.updated_date,'%Y-%m-%d %H:00:00') as '日期',
       TIMESTAMPDIFF(SECOND,bmj1.created_date,bmj1.updated_date) as '每日每小时的还货架时间/s'
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_wcs_g2p.bucket_move_job bmj
ON pj.bucket_move_job_id = bmj.id
LEFT JOIN evo_wcs_g2p.bucket_move_job bmj1
ON bmj.busi_group_id = bmj1.busi_group_id
WHERE pj.state = 'DONE' AND bmj1.bucket_move_type = 'RESET_JOB'
GROUP BY DATE_FORMAT(bmj.updated_date,'%Y-%m-%d %H:00:00')
)t4
ON t1.`日期` = t4.`日期`


-- -------------------拣选效率：每日平均货架间隔时间--------------------- --
SELECT DATE_FORMAT(b.entry_time,'%Y-%m-%d') as '日期',CAST(SUM(b.bucket_wait_time)/COUNT(b.order_id) AS DECIMAL(10,2)) as '每日平均货架间隔时间'
FROM
(
SELECT a.order_id,a.entry_time,a.exit_time,a.entry_time1,a.exit_time1,MIN(a.bucket_wait_time) as bucket_wait_time
FROM
(
SELECT tmp1.order_id,tmp1.entry_time,tmp1.exit_time,tmp2.entry_time as entry_time1,tmp2.exit_time as exit_time1,TIMESTAMPDIFF(SECOND,tmp1.exit_time,tmp2.entry_time) as bucket_wait_time
FROM
(
SELECT pj.job_id,pj.order_id,se.entry_time,se.exit_time
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_station.station_entry se
ON se.idempotent_id = pj.job_id
WHERE pj.state = 'DONE' AND se.entry_time is not NULL
)tmp1 
LEFT JOIN
(
SELECT pj.job_id,pj.order_id,se.entry_time,se.exit_time
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_station.station_entry se
ON se.idempotent_id = pj.job_id
WHERE pj.state = 'DONE' AND se.entry_time is not NULL
)tmp2
ON tmp1.order_id = tmp2.order_id AND tmp1.entry_time < tmp2.entry_time
WHERE tmp2.job_id is not NULL
ORDER BY tmp1.order_id,tmp1.entry_time
)a
GROUP BY a.entry_time
)b
GROUP BY DATE_FORMAT(b.entry_time,'%Y-%m-%d')


-- -------------------拣选效率：工人工作时间/等待货架到站/货架间隔时长--------------------- --
SELECT t1.`日期`,IFNULL(t1.`工人作业时间`,0) as '工人作业时间',IFNULL(t2.`等待货架到站`,0) as '等待货架到站',IFNULL(t3.`货架间隔时长`,0) as '货架间隔时长'
FROM
(
SELECT DATE_FORMAT(se.entry_time,'%Y-%m-%d') as '日期',CAST(SUM(TIMESTAMPDIFF(SECOND,se.entry_time,se.exit_time))/COUNT(DISTINCT se.station_code) AS DECIMAL(10,2)) as '工人作业时间'
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_station.station_entry se
ON pj.job_id = se.idempotent_id
WHERE pj.state = 'DONE' AND se.entry_time is not NULL
GROUP BY DATE_FORMAT(se.entry_time,'%Y-%m-%d')
)t1
LEFT JOIN
(
SELECT DATE_FORMAT(t1.updated_date,'%Y-%m-%d') as '日期',
      CAST(SUM(TIMESTAMPDIFF(SECOND,t1.updated_date,t2.updated_date))/COUNT(DISTINCT t1.agv_code) AS DECIMAL(10,2)) as '等待货架到站'
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'GO_TARGET'
)t1
LEFT JOIN
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type
FROM evo_wcs_g2p.job_state_change c
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'DONE'
)t2
ON t1.job_id = t2.job_id
WHERE t2.job_id is not NULL
GROUP BY DATE_FORMAT(t1.updated_date,'%Y-%m-%d')
)t2
ON t1.`日期` = t2.`日期`
LEFT JOIN
(
SELECT DATE_FORMAT(b.entry_time,'%Y-%m-%d') as '日期',CAST(SUM(b.bucket_wait_time)/COUNT(b.order_id) AS DECIMAL(10,2)) as '货架间隔时长'
FROM
(
SELECT a.order_id,a.entry_time,a.exit_time,a.entry_time1,a.exit_time1,MIN(a.bucket_wait_time) as bucket_wait_time
FROM
(
SELECT tmp1.order_id,tmp1.entry_time,tmp1.exit_time,tmp2.entry_time as entry_time1,tmp2.exit_time as exit_time1,TIMESTAMPDIFF(SECOND,tmp1.exit_time,tmp2.entry_time) as bucket_wait_time
FROM
(
SELECT pj.job_id,pj.order_id,se.entry_time,se.exit_time
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_station.station_entry se
ON se.idempotent_id = pj.job_id
WHERE pj.state = 'DONE' AND se.entry_time is not NULL
)tmp1 
LEFT JOIN
(
SELECT pj.job_id,pj.order_id,se.entry_time,se.exit_time
FROM evo_wcs_g2p.picking_job pj
LEFT JOIN evo_station.station_entry se
ON se.idempotent_id = pj.job_id
WHERE pj.state = 'DONE' AND se.entry_time is not NULL
)tmp2
ON tmp1.order_id = tmp2.order_id AND tmp1.entry_time < tmp2.entry_time
WHERE tmp2.job_id is not NULL
ORDER BY tmp1.order_id,tmp1.entry_time
)a
GROUP BY a.entry_time
)b
GROUP BY DATE_FORMAT(b.entry_time,'%Y-%m-%d')
)t3
ON t1.`日期` = t3.`日期`


-- -------------------拣选效率：月均拣选效率(单/工位/h)(件/工位/h)--------------------- --
SELECT DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day) as '月份',CAST(COUNT(DISTINCT pw.order_id)/COUNT(DISTINCT pj.station_code)/10 AS DECIMAL(10,2)) as '月均拣选效率(单/工位/h)',CAST(SUM(pwd.fulfill_quantity)/COUNT(DISTINCT pj.station_code)/10 AS DECIMAL(10,2)) as '月均拣选效率(件/工位/h)'
FROM evo_wcs_g2p.picking_work pw
LEFT JOIN evo_wcs_g2p.picking_work_detail pwd
ON pw.picking_work_id = pwd.picking_work_id
LEFT JOIN evo_wcs_g2p.picking_job pj
ON pj.picking_work_id = pw.picking_work_id
WHERE pwd.quantity = pwd.fulfill_quantity
GROUP BY DATE_ADD(DATE(pw.updated_date),INTERVAL -day(DATE(pw.updated_date))+1 day)


-- -------------------拣选效率：每日拣选效率(单/人/h)(行/人/h)(件/人/h)--------------------- --
SELECT DATE(pw.updated_date) as '日期',CAST(COUNT(DISTINCT pw.order_id)/COUNT(DISTINCT pofd.last_updated_user)/10 AS DECIMAL(10,2)) as '每日拣选效率(单/人/h)',CAST(COUNT(DISTINCT pwd.id)/COUNT(DISTINCT pofd.last_updated_user)/10 AS DECIMAL(10,2)) as '每日拣选效率(行/人/h)',CAST(SUM(pwd.fulfill_quantity)/COUNT(DISTINCT pofd.last_updated_user)/10 AS DECIMAL(10,2)) as '每日拣选效率(件/人/h)'
FROM evo_wcs_g2p.picking_work pw
LEFT JOIN evo_wcs_g2p.picking_work_detail pwd
ON pw.picking_work_id = pwd.picking_work_id
LEFT JOIN evo_wcs_g2p.picking_job pj
ON pj.picking_work_id = pw.picking_work_id
LEFT JOIN evo_wes_picking.picking_order_fulfill_detail pofd
ON pj.id = pofd.job_id
WHERE pwd.quantity = pwd.fulfill_quantity
GROUP BY DATE(pw.updated_date)


-- -------------------拣选效率：工作站状态时序图或数据--------------------- --
SELECT t1.`时间段`,t1.`工作站编码`,t1.`空闲率`,(1 - t1.`空闲率`) as '利用率',t2.`在线率`,IFNULL(t3.`等待货架到站`,0) as '等待货架到站'
FROM
(
SELECT 
    seq.station_code AS '工作站编码',
		tmp.theDayStartofhour as '时间段',
    cast((60*60-
	  sum(CASE WHEN seq.entry_time >= tmp.theDayStartofhour AND seq.entry_time < tmp.theDayEndofhour AND seq.exit_time < tmp.theDayEndofhour THEN timestampdiff(second,seq.entry_time,seq.exit_time)
             WHEN seq.entry_time >= tmp.theDayStartofhour AND seq.entry_time < tmp.theDayEndofhour AND seq.exit_time > tmp.theDayEndofhour THEN timestampdiff(second,seq.entry_time,tmp.theDayEndofhour)
             WHEN seq.entry_time < tmp.theDayStartofhour AND seq.entry_time < tmp.theDayEndofhour AND seq.exit_time >= tmp.theDayStartofhour AND seq.exit_time < tmp.theDayEndofhour THEN timestampdiff(second,tmp.theDayStartofhour,seq.exit_time)
             WHEN seq.entry_time < tmp.theDayStartofhour AND seq.entry_time < tmp.theDayEndofhour AND seq.exit_time > tmp.theDayEndofhour THEN timestampdiff(second,tmp.theDayStartofhour,tmp.theDayEndofhour)
             ELSE 0 END))/(60*60)as decimal(10,2)) as '空闲率'
FROM (
SELECT @i:=DATE_ADD(@i,INTERVAL 1 HOUR) as theDayStartofhour,DATE_ADD(@i,INTERVAL 3599 SECOND) as theDayEndofhour
FROM information_schema.COLUMNS,(select @i:= DATE_ADD('2021-09-22 00:00:00',INTERVAL -1 HOUR)) tmp 
WHERE @i < DATE_ADD(DATE_ADD('2021-09-22 00:00:00',INTERVAL 1 DAY),INTERVAL -1 HOUR)  
)tmp
join evo_station.station_entry seq
WHERE seq.entry_time >= '2021-09-22 00:00:00'  AND seq.exit_time < DATE_ADD('2021-09-22 00:00:00' ,INTERVAL 60*24 MINUTE) AND idempotent_id LIKE '%G2PPicking%' 
GROUP BY seq.station_code,tmp.theDayStartofhour
)t1
LEFT JOIN
(
SELECT 
   tt1.ida as '时间段',
	 tt1.station_code AS '工作站编码',
     cast(SUM(		
			CASE WHEN tt1.begin_to_exit_time <= tt1.begin_to_lineBegin_time and tt1.begin_to_lineBegin_time <= tt1.begin_to_lineEnd_time then tt1.begin_to_lineBegin_time
					 WHEN tt1.begin_to_exit_time <= tt1.begin_to_lineEnd_time and tt1.begin_to_lineEnd_time <= tt1.begin_to_lineBegin_time then tt1.begin_to_lineEnd_time
					 WHEN tt1.begin_to_lineBegin_time <= tt1.begin_to_exit_time and tt1.begin_to_exit_time <= tt1.begin_to_lineEnd_time then tt1.begin_to_exit_time
					 WHEN tt1.begin_to_lineBegin_time <= tt1.begin_to_lineEnd_time and tt1.begin_to_lineEnd_time <= tt1.begin_to_exit_time then tt1.begin_to_lineEnd_time
					 WHEN tt1.begin_to_lineEnd_time <= tt1.begin_to_exit_time and tt1.begin_to_exit_time <= tt1.begin_to_lineBegin_time then tt1.begin_to_exit_time
					 WHEN tt1.begin_to_lineEnd_time <= tt1.begin_to_lineBegin_time and tt1.begin_to_lineBegin_time <= tt1.begin_to_exit_time then tt1.begin_to_lineBegin_time
				 	 ELSE 0 END
					-
			CASE WHEN tt1.begin_to_entry_time <= tt1.begin_to_lineBegin_time and tt1.begin_to_lineBegin_time <= tt1.begin_to_lineEnd_time then tt1.begin_to_lineBegin_time
					 WHEN tt1.begin_to_entry_time <= tt1.begin_to_lineEnd_time and tt1.begin_to_lineEnd_time <= tt1.begin_to_lineBegin_time then tt1.begin_to_lineEnd_time
					 WHEN tt1.begin_to_lineBegin_time <= tt1.begin_to_entry_time and tt1.begin_to_entry_time <= tt1.begin_to_lineEnd_time then tt1.begin_to_entry_time
					 WHEN tt1.begin_to_lineBegin_time <= tt1.begin_to_lineEnd_time and tt1.begin_to_lineEnd_time <= tt1.begin_to_entry_time then tt1.begin_to_lineEnd_time
					 WHEN tt1.begin_to_lineEnd_time <= tt1.begin_to_entry_time and tt1.begin_to_entry_time <= tt1.begin_to_lineBegin_time then tt1.begin_to_entry_time
					 WHEN tt1.begin_to_lineEnd_time <= tt1.begin_to_lineBegin_time and tt1.begin_to_lineBegin_time <= tt1.begin_to_entry_time then tt1.begin_to_lineBegin_time
				  ELSE 0 END
			) /3600 as decimal(10,2)) as '在线率'
    FROM (
	  SELECT 
		tmp_line.ida,
		sl.station_code,
    TIMESTAMPDIFF(SECOND, '2021-09-22 00:00:00',if(DATE_ADD(sl.login_time,INTERVAL -3 HOUR) <= '2021-09-22 00:00:00','2021-09-22 00:00:00',DATE_ADD(sl.login_time,INTERVAL -3 HOUR))) 'begin_to_entry_time',
		TIMESTAMPDIFF(SECOND, '2021-09-22 00:00:00',if(sl.logout_time is null,'2021-09-23 00:00:00',if(sl.logout_time <= '2021-09-23 00:00:00',sl.logout_time,'2021-09-23 00:00:00'))) 'begin_to_exit_time',
		TIMESTAMPDIFF(SECOND, '2021-09-22 00:00:00',tmp_line.ida) 'begin_to_lineBegin_time',
		TIMESTAMPDIFF(SECOND, '2021-09-22 00:00:00',DATE_ADD(tmp_line.ida,INTERVAL 1 HOUR)) 'begin_to_lineEnd_time'
    FROM evo_station.station_login sl,
	     (SELECT @r:=DATE_ADD(@r,INTERVAL 1 HOUR) as ida
       FROM information_schema.COLUMNS,(select @r:= DATE_ADD('2021-09-22 00:00:00',INTERVAL -1 HOUR)) tmp 
       WHERE @r < DATE_ADD(DATE_ADD('2021-09-22 00:00:00',INTERVAL 1 DAY),INTERVAL -1 HOUR)) tmp_line
    WHERE 
       ((DATE_ADD(sl.login_time,INTERVAL -3 HOUR) >= '2021-09-22 00:00:00' and DATE_ADD(sl.login_time,INTERVAL -3 HOUR) <= '2021-09-23 00:00:00')
       OR (sl.logout_time >= '2021-09-22 00:00:00' and sl.logout_time <= '2021-09-23 00:00:00')
		   OR (DATE_ADD(sl.login_time,INTERVAL -3 HOUR) <= '2021-09-22 00:00:00' and sl.logout_time >= '2021-09-23 00:00:00'))
       AND sl.biz_type = 'PICKING_ONLINE_G2P_B2P'
        ) tt1
    GROUP BY tt1.ida,tt1.station_code
)t2
ON t1.`时间段` = t2.`时间段` AND t1.`工作站编码` = t2.`工作站编码` 
LEFT JOIN
(
SELECT DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00') as '时间段',
       t1.station_code as '工作站编码',
       CAST(SUM(TIMESTAMPDIFF(SECOND,t1.updated_date,t2.updated_date))/COUNT(DISTINCT t1.agv_code)/3600 AS DECIMAL(10,2)) as '等待货架到站'
FROM
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type,bmj.station_code
FROM evo_wcs_g2p.job_state_change c
LEFT JOIN evo_wcs_g2p.bucket_move_job bmj
ON c.job_id = bmj.job_id
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'GO_TARGET' AND DATE_FORMAT(c.updated_date,'%Y-%m-%d') = '2021-09-22' AND bmj.bucket_move_type = 'G2P_ONLINE_PICK'
)t1
LEFT JOIN
(
SELECT c.agv_code,c.job_id,c.updated_date,c.state,c.job_type,bmj.station_code
FROM evo_wcs_g2p.job_state_change c
LEFT JOIN evo_wcs_g2p.bucket_move_job bmj
ON c.job_id = bmj.job_id
WHERE c.job_type = 'G2P_BUCKET_MOVE' AND c.state = 'DONE' AND DATE_FORMAT(c.updated_date,'%Y-%m-%d') = '2021-09-22' AND bmj.bucket_move_type = 'G2P_ONLINE_PICK'
)t2
ON t1.job_id = t2.job_id
WHERE t2.job_id is not NULL
GROUP BY t1.station_code,DATE_FORMAT(t1.updated_date,'%Y-%m-%d %H:00:00')
)t3
ON t1.`时间段` = t3.`时间段` AND t1.`工作站编码` = t3.`工作站编码` 


-- -------------------SKU数据：月库存SKU峰值数量--------------------- --
SELECT DATE_ADD(DATE(pj.updated_date),INTERVAL -day(DATE(pj.updated_date))+1 day) as '月份',COUNT(DISTINCT pj.sku_id) as '月库存SKU峰值数量'
FROM evo_wcs_g2p.picking_job pj
WHERE pj.state = 'DONE'
GROUP BY DATE_ADD(DATE(pj.updated_date),INTERVAL -day(DATE(pj.updated_date))+1 day)


-- -------------------SKU数据：各SKU库存件数--------------------- --
SELECT DATE(MAX(last_updated_date)) as '日期',li.sku_id as 'sku',SUM(li.quantity) as '库存数量'
FROM evo_wes_inventory.level3_inventory li
GROUP BY li.sku_id

-- -------------------库存周转：各SKU周转天数--------------------- --
SELECT t1.sku,t1.`期初时间`,li.last_updated_date as '期末时间',t1.`期初库存数量`,li.quantity as '期末库存数量',t1.`时间段天数`,IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) as '时间段销售量',
       IF(IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity))=0,0,CAST(t1.`时间段天数`/2*(t1.`期初库存数量`+li.quantity)/IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) AS DECIMAL(10,0))) as '时间段库存周转天数'
FROM
(
SELECT it.sku_id as 'sku',MIN(it.transaction_time) as '期初时间',DATEDIFF(MAX(it.transaction_time),MIN(it.transaction_time)) as '时间段天数',
       IF(it.transaction_time = MIN(it.transaction_time),it.post_quantity,0) as '期初库存数量'
FROM evo_wes_inventory.inventory_transaction it
WHERE it.sku_id is not NULL AND it.state = 'DONE'
GROUP BY it.sku_id
)t1
LEFT JOIN evo_wes_inventory.level3_inventory li
ON t1.sku = li.sku_id
LEFT JOIN
(
SELECT pj.sku_id,SUM(pj.quantity) as quantity,pj.updated_date
FROM evo_wcs_g2p.picking_job pj
GROUP BY pj.sku_id,pj.updated_date
)t2
ON (t2.updated_date BETWEEN t1.`期初时间` AND li.last_updated_date ) AND t1.sku = t2.sku_id
GROUP BY t1.sku
ORDER BY `时间段库存周转天数`


-- -------------------库存周转：不同周转天数SKU库存件数--------------------- --
SELECT tt.`周转天数阶梯`,
       COUNT(DISTINCT tt.sku) as 'sku种类数',
       COUNT(DISTINCT tt.sku)/
(
SELECT COUNT(b.sku)
FROM 
(
SELECT CASE WHEN tmp.`时间段库存周转天数` >= 0 AND tmp.`时间段库存周转天数` < 30 THEN '0-30'
            WHEN tmp.`时间段库存周转天数` >= 30 AND tmp.`时间段库存周转天数` < 60 THEN '30-60'
            WHEN tmp.`时间段库存周转天数` >= 60 AND tmp.`时间段库存周转天数` < 90 THEN '60-90'
            WHEN tmp.`时间段库存周转天数` >= 90 AND tmp.`时间段库存周转天数` < 150 THEN '90-150'
            WHEN tmp.`时间段库存周转天数` >= 150 AND tmp.`时间段库存周转天数` < 300 THEN '150-300'
            WHEN tmp.`时间段库存周转天数` >= 300 AND tmp.`时间段库存周转天数` < 600 THEN '300-600'
            WHEN tmp.`时间段库存周转天数` >= 600  THEN '>600' END AS '周转天数阶梯',tmp.*
FROM
(
SELECT t1.sku,t1.`期初时间`,li.last_updated_date as '期末时间',t1.`期初库存数量`,li.quantity as '期末库存数量',t1.`时间段天数`,IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) as '时间段销售量',
       IF(IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity))=0,0,CAST(t1.`时间段天数`/2*(t1.`期初库存数量`+li.quantity)/IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) AS DECIMAL(10,0))) as '时间段库存周转天数'
FROM
(
SELECT it.sku_id as 'sku',MIN(it.transaction_time) as '期初时间',DATEDIFF(MAX(it.transaction_time),MIN(it.transaction_time)) as '时间段天数',
       IF(it.transaction_time = MIN(it.transaction_time),it.post_quantity,0) as '期初库存数量'
FROM evo_wes_inventory.inventory_transaction it
WHERE it.sku_id is not NULL AND it.state = 'DONE'
GROUP BY it.sku_id
)t1
LEFT JOIN evo_wes_inventory.level3_inventory li
ON t1.sku = li.sku_id
LEFT JOIN
(
SELECT pj.sku_id,SUM(pj.quantity) as quantity,pj.updated_date
FROM evo_wcs_g2p.picking_job pj
GROUP BY pj.sku_id,pj.updated_date
)t2
ON (t2.updated_date BETWEEN t1.`期初时间` AND li.last_updated_date ) AND t1.sku = t2.sku_id
GROUP BY t1.sku
ORDER BY `时间段库存周转天数`
)tmp
)b
) as 'sku种类数占比',
SUM(tt.`期末库存数量`) as '库存件数',
SUM(tt.`期末库存数量`)/
(
SELECT SUM(b.`期末库存数量`)
FROM 
(
SELECT CASE WHEN tmp.`时间段库存周转天数` >= 0 AND tmp.`时间段库存周转天数` < 30 THEN '0-30'
            WHEN tmp.`时间段库存周转天数` >= 30 AND tmp.`时间段库存周转天数` < 60 THEN '30-60'
            WHEN tmp.`时间段库存周转天数` >= 60 AND tmp.`时间段库存周转天数` < 90 THEN '60-90'
            WHEN tmp.`时间段库存周转天数` >= 90 AND tmp.`时间段库存周转天数` < 150 THEN '90-150'
            WHEN tmp.`时间段库存周转天数` >= 150 AND tmp.`时间段库存周转天数` < 300 THEN '150-300'
            WHEN tmp.`时间段库存周转天数` >= 300 AND tmp.`时间段库存周转天数` < 600 THEN '300-600'
            WHEN tmp.`时间段库存周转天数` >= 600  THEN '>600' END AS '周转天数阶梯',tmp.*
FROM
(
SELECT t1.sku,t1.`期初时间`,li.last_updated_date as '期末时间',t1.`期初库存数量`,li.quantity as '期末库存数量',t1.`时间段天数`,IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) as '时间段销售量',
       IF(IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity))=0,0,CAST(t1.`时间段天数`/2*(t1.`期初库存数量`+li.quantity)/IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) AS DECIMAL(10,0))) as '时间段库存周转天数'
FROM
(
SELECT it.sku_id as 'sku',MIN(it.transaction_time) as '期初时间',DATEDIFF(MAX(it.transaction_time),MIN(it.transaction_time)) as '时间段天数',
       IF(it.transaction_time = MIN(it.transaction_time),it.post_quantity,0) as '期初库存数量'
FROM evo_wes_inventory.inventory_transaction it
WHERE it.sku_id is not NULL AND it.state = 'DONE'
GROUP BY it.sku_id
)t1
LEFT JOIN evo_wes_inventory.level3_inventory li
ON t1.sku = li.sku_id
LEFT JOIN
(
SELECT pj.sku_id,SUM(pj.quantity) as quantity,pj.updated_date
FROM evo_wcs_g2p.picking_job pj
GROUP BY pj.sku_id,pj.updated_date
)t2
ON (t2.updated_date BETWEEN t1.`期初时间` AND li.last_updated_date ) AND t1.sku = t2.sku_id
GROUP BY t1.sku
ORDER BY `时间段库存周转天数`
)tmp
)b
) as '库存件数占比'
FROM
(
SELECT CASE WHEN tmp.`时间段库存周转天数` >= 0 AND tmp.`时间段库存周转天数` < 30 THEN '0-30'
            WHEN tmp.`时间段库存周转天数` >= 30 AND tmp.`时间段库存周转天数` < 60 THEN '30-60'
            WHEN tmp.`时间段库存周转天数` >= 60 AND tmp.`时间段库存周转天数` < 90 THEN '60-90'
            WHEN tmp.`时间段库存周转天数` >= 90 AND tmp.`时间段库存周转天数` < 150 THEN '90-150'
            WHEN tmp.`时间段库存周转天数` >= 150 AND tmp.`时间段库存周转天数` < 300 THEN '150-300'
            WHEN tmp.`时间段库存周转天数` >= 300 AND tmp.`时间段库存周转天数` < 600 THEN '300-600'
            WHEN tmp.`时间段库存周转天数` >= 600  THEN '>600' END AS '周转天数阶梯',tmp.*
FROM
(
SELECT t1.sku,t1.`期初时间`,li.last_updated_date as '期末时间',t1.`期初库存数量`,li.quantity as '期末库存数量',t1.`时间段天数`,IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) as '时间段销售量',
       IF(IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity))=0,0,CAST(t1.`时间段天数`/2*(t1.`期初库存数量`+li.quantity)/IF(SUM(t2.quantity)is NULL,0,SUM(t2.quantity)) AS DECIMAL(10,0))) as '时间段库存周转天数'
FROM
(
SELECT it.sku_id as 'sku',MIN(it.transaction_time) as '期初时间',DATEDIFF(MAX(it.transaction_time),MIN(it.transaction_time)) as '时间段天数',
       IF(it.transaction_time = MIN(it.transaction_time),it.post_quantity,0) as '期初库存数量'
FROM evo_wes_inventory.inventory_transaction it
WHERE it.sku_id is not NULL AND it.state = 'DONE'
GROUP BY it.sku_id
)t1
LEFT JOIN evo_wes_inventory.level3_inventory li
ON t1.sku = li.sku_id
LEFT JOIN
(
SELECT pj.sku_id,SUM(pj.quantity) as quantity,pj.updated_date
FROM evo_wcs_g2p.picking_job pj
GROUP BY pj.sku_id,pj.updated_date
)t2
ON (t2.updated_date BETWEEN t1.`期初时间` AND li.last_updated_date ) AND t1.sku = t2.sku_id
GROUP BY t1.sku
ORDER BY `时间段库存周转天数`
)tmp
)tt
GROUP BY tt.`周转天数阶梯`
