-- ########################################################################
/*
* 统计-拣选订单效率指标
*/

SELECT 
     tmp2.times as '时间段', -- 时间段
     SUM(tmp2.order_num) as '订单完成数', -- 订单完成数
     SUM(tmp2.order_group_num) as '集合单完成数', -- 集合单完成数
     SUM(tmp2.order_linenum) as '完成订单行数', -- 完成订单行数
     SUM(tmp2.sku_num) as '完成货品件数', -- 完成货品件数
	 SUM(tmp2.station_slot_times) as '命中槽位次数', -- 命中槽位次数
     SUM(tmp2.into_station_times) as '进站次数', -- 进站次数
     SUM(tmp2.win_open_times) as '弹窗次数', -- 弹窗次数
     cast(ifnull(SUM(tmp2.win_open_times)/SUM(tmp2.into_station_times),0)as decimal(10,2)) as '单次进站弹窗次数', -- 单次进站弹窗次数
	 cast(ifnull(SUM(tmp2.sku_num)/SUM(tmp2.into_station_times),0)as decimal(10,2)) as '单次进站完成货品件数', -- 单次进站完成货品件数
     cast(ifnull(SUM(tmp2.station_slot_times)/SUM(tmp2.win_open_times),0)as decimal(10,2)) as '单次弹窗命中槽位次数', -- 单次弹窗命中槽位次数
	 cast(ifnull(SUM(tmp2.order_linenum)/SUM(tmp2.into_station_times),0)as decimal(10,2)) as '单次进站完成订单行数' -- 单次进站完成订单行数
FROM (
SELECT DATE_FORMAT(stg.updated_date,'%Y-%m-%d %H:00:00') times,
     0 order_num,
     0 order_group_num,
     0 order_linenum,
      sum(pj.actual_quantity) sku_num, -- picking_job实捡数量
      count(pj.station_slot_code) station_slot_times, -- 工作站槽位code
	 0 into_station_times, -- 进出站时间
	 count(distinct stg.group_job_id) win_open_times -- 任务组任务id
	FROM evo_wcs_g2p.station_task_group stg 
	JOIN evo_wcs_g2p.w2p_picking_job_v2 pj 
      ON stg.job_id = pj.job_id 
	WHERE pj.state='DONE' AND stg.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and stg.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND pj.project_code = 'FH-B2021-C100' AND stg.project_code = 'FH-B2021-C100'
	group BY DATE_FORMAT(stg.updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT  DATE_FORMAT(pw.updated_date,'%Y-%m-%d %H:00:00') times,
	   COUNT(DISTINCT pw.order_id) order_num,
     COUNT(DISTINCT pw.picking_order_group_id) order_group_num,
     0 'order_linenum',
     0 'sku_num', 
     0 'station_slot_times',
     0 'into_station_times',
     0 'win_open_times'
    FROM evo_wcs_g2p.w2p_picking_work_v2 pw
	WHERE pw.state = 'DONE' AND pw.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and pw.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND pw.project_code = 'FH-B2021-C100' 
group BY DATE_FORMAT(pw.updated_date,'%Y-%m-%d %H:00:00') 

UNION ALL

SELECT DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     0 'order_group_num',
     count(distinct pwd.id) order_linenum,-- picking_work_detail行数
     0 'sku_num',
     0 'station_slot_times',
     0 'into_station_times',
     0 'win_open_times'
    FROM evo_wcs_g2p.w2p_picking_work_detail_v2 pwd
	JOIN evo_wcs_g2p.w2p_picking_job_v2 pj
	  ON pwd.picking_work_detail_id = pj.picking_work_detail_id
	WHERE pwd.quantity = pwd.fulfill_quantity AND pj.state= 'DONE'  AND pwd.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and pwd.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND pwd.project_code = 'FH-B2021-C100' AND pj.project_code = 'FH-B2021-C100' 
	GROUP BY DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00') 

UNION ALL

SELECT DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     0 'order_group_num',
     0 'order_linenum',
     0 'sku_num',
     0 'station_slot_times',
       count(se.id) into_station_times,
     0 'win_open_times'
    FROM evo_station.station_entry se
	WHERE biz_type = 'PICKING_ONLINE_G2P_W2P' and entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND entry_time < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND se.project_code = 'FH-B2021-C100' 
	GROUP BY DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00')
	  
UNION ALL

SELECT 
     tmp_line.ida times,
     0 'order_num',
     0 'order_group_num',
     0 'order_linenum',
	 0 'sku_num',
	 0 'station_slot_times',
     0 'into_station_times', 
     0 'win_open_times'
	FROM (SELECT @r:=DATE_ADD(@r,INTERVAL 1 HOUR) as ida
       FROM information_schema.COLUMNS,(select @r:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @r < DATE_ADD(DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 1 DAY),INTERVAL -1 HOUR)) tmp_line
    GROUP BY tmp_line.ida
) tmp2 
GROUP BY tmp2.times;

-- ########################################################################
/*
* 统计-拣选任务搬运指标
*/

SELECT tt.times as '工作时间/小时',
       tt.station_code as '工作站编码',
       IF(tt.single_num is NULL,0,tt.single_num) as '单工作站双作业点位完成箱数',
       IF(tt.double_num is NULL,0,tt.double_num) as '工作站作业点完成箱数（作业点-箱数）',
       IF(tt.task_time is NULL,0,tt.task_time) as '单次任务耗时/秒',
       IF(tt.entry_time is NULL,0,tt.entry_time) as '平均入站耗时/秒',
       IF(tt.picking_time is NULL,0,tt.picking_time) as '平均人工拣货耗时/秒',
       IF(tt.be_time is NULL,0,tt.be_time) as '平均两次任务间隔时长/秒'
FROM
(
SELECT tmp1.times,
       tmp1.station_code,
       tmp2.num as single_num,
       tmp3.num as double_num,
       tmp4.time as task_time,
       tmp5.time as entry_time,
       tmp6.time as picking_time,
       tmp7.time as be_time
FROM
(
select DATE_FORMAT(se.last_updated_date,'%Y-%m-%d %H:00:00') times, -- '工作时间/小时'
        se.station_code -- '工作站编码'
FROM evo_station.station_entry se
WHERE se.biz_type = 'PICKING_ONLINE_G2P_W2P' AND se.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND se.project_code = 'FH-B2021-C100'
GROUP BY se.station_code,DATE_FORMAT(se.last_updated_date,'%Y-%m-%d %H:00:00')
)tmp1
LEFT JOIN
(
select DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00') times,
       cmj.station_code, -- '工作站编码'
       count(cmj.target_way_point_code) as 'num' -- '单工作站双作业点位完成箱数'
FROM evo_wcs_g2p.container_move_job_v2 cmj
WHERE cmj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND cmj.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cmj.biz_type = 'W2P_ONLINE_PICK' AND cmj.state='DONE' AND cmj.project_code = 'FH-B2021-C100'
GROUP BY cmj.station_code,DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00')
)tmp2
on tmp1.station_code = tmp2.station_code and tmp1.times = tmp2.times
LEFT JOIN
(
select DATE_FORMAT(tmp.updated_date,'%Y-%m-%d %H:00:00') times,tmp.station_code,GROUP_CONCAT(tmp.num) as 'num'
FROM 
(
select cmj.station_code,
       concat(cmj.target_way_point_code,'-',count(cmj.target_way_point_code)) as 'num',-- '工作站作业点完成箱数'
       cmj.updated_date,
       cmj.state,
       cmj.source_way_point_code,
       cmj.target_way_point_code
FROM evo_wcs_g2p.container_move_job_v2 cmj
WHERE cmj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND cmj.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cmj.biz_type = 'W2P_ONLINE_PICK' AND cmj.state='DONE' AND cmj.project_code = 'FH-B2021-C100'
GROUP BY cmj.station_code,cmj.target_way_point_code,DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00')
)tmp
GROUP BY tmp.station_code,DATE_FORMAT(tmp.updated_date,'%Y-%m-%d %H:00:00')
)tmp3
on tmp1.station_code = tmp3.station_code and tmp1.times = tmp3.times
LEFT JOIN
(
SELECT DATE_FORMAT(wpj.updated_date,'%Y-%m-%d %H:00:00') times,wpj.station_code,CAST((SUM(TIMESTAMPDIFF(SECOND,wpj.created_date,wpj.updated_date))/COUNT(wpj.job_id)) AS DECIMAL(10,2)) as 'time' -- '单次任务耗时/秒'
FROM evo_wcs_g2p.w2p_picking_job_v2 wpj
WHERE wpj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND wpj.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND wpj.state='DONE' AND wpj.project_code = 'FH-B2021-C100'
GROUP BY wpj.station_code,DATE_FORMAT(wpj.updated_date,'%Y-%m-%d %H:00:00')
)tmp4
on tmp1.station_code = tmp4.station_code and tmp1.times = tmp4.times
LEFT JOIN
(
SELECT DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00') times,cmj.station_code,CAST(SUM(TIMESTAMPDIFF(SECOND,jsc.updated_date,cmj.updated_date))/COUNT(cmj.job_id) AS DECIMAL(10,2)) as 'time' -- '平均入站耗时/秒'
FROM evo_wcs_g2p.container_move_job_v2 cmj
JOIN evo_wcs_g2p.job_state_change jsc
ON cmj.job_id = jsc.job_id
WHERE cmj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND cmj.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cmj.biz_type = 'W2P_ONLINE_PICK' AND cmj.state='DONE' AND jsc.state = 'INIT_JOB' AND cmj.project_code = 'FH-B2021-C100' AND jsc.project_code = 'FH-B2021-C100'
GROUP BY cmj.station_code,DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00')
)tmp5
on tmp1.station_code = tmp5.station_code and tmp1.times = tmp5.times
LEFT JOIN
(
SELECT DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00') times,wpj.station_code,CAST((SUM(TIMESTAMPDIFF(SECOND,tmp.last_updated_date,jsc.updated_date))/COUNT(jsc.job_id)) AS DECIMAL(10,2))  as 'time' -- '平均人工拣货耗时/秒'
FROM evo_wcs_g2p.job_state_change jsc
JOIN evo_wcs_g2p.w2p_picking_job_v2 wpj
ON jsc.job_id = wpj.job_id
JOIN
(
select pj.job_id,t.station_code,sc.last_updated_date
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
JOIN evo_wcs_g2p.station_task_group g
ON t.task_no = g.group_job_id
JOIN evo_wcs_g2p.w2p_picking_job_v2 pj
ON g.job_id = pj.job_id
WHERE sc.state = 'PULLED' AND sc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND sc.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' AND g.project_code = 'FH-B2021-C100' AND pj.project_code = 'FH-B2021-C100'
)tmp
ON jsc.job_id = tmp.job_id
WHERE jsc.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND jsc.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND jsc.state = 'DONE' AND jsc.project_code = 'FH-B2021-C100' AND wpj.project_code = 'FH-B2021-C100'
GROUP BY wpj.station_code,DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00')
)tmp6
on tmp1.station_code = tmp6.station_code and tmp1.times = tmp6.times
LEFT JOIN
(
select DATE_FORMAT(a.NEW_updated_date,'%Y-%m-%d %H:00:00') times,a.station_code,CAST(SUM(IF(TIMESTAMPDIFF(SECOND,a.DONE_updated_date,b.NEW_updated_date)<0,0, TIMESTAMPDIFF(SECOND,a.DONE_updated_date,b.NEW_updated_date)))/COUNT(a.job_id) AS DECIMAL(10,2)) as 'time' -- '平均两次任务间隔时长' 
FROM
(
SELECT @i:=@i + 1 AS num,a.*
FROM
(
SELECT tmp.station_code,tmp.job_id,sc.last_updated_date as 'NEW_updated_date',tmp.last_updated_date as 'DONE_updated_date'
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
join 
(
SELECT pj.station_code,sc.station_task_id,sc.last_updated_date,sc.id,pj.job_id
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
JOIN evo_wcs_g2p.station_task_group g
ON t.task_no = g.group_job_id
JOIN evo_wcs_g2p.w2p_picking_job_v2 pj
ON g.job_id = pj.job_id
WHERE sc.state = 'DONE' AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' AND g.project_code = 'FH-B2021-C100' AND pj.project_code = 'FH-B2021-C100'
ORDER BY sc.last_updated_date
)tmp
ON sc.station_task_id = tmp.station_task_id 
WHERE sc.state = 'NEW'  AND sc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND sc.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' 
ORDER BY tmp.station_code,sc.last_updated_date
)a,(SELECT @i:= 0) b
)a
LEFT JOIN
(
select @r:=@r+ 1 AS num,a.* from
(
SELECT tmp.station_code,sc.last_updated_date as 'NEW_updated_date',tmp.last_updated_date as 'DONE_updated_date'
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
join 
(
SELECT pj.station_code,sc.station_task_id,sc.last_updated_date,sc.id
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
JOIN evo_wcs_g2p.station_task_group g
ON t.task_no = g.group_job_id
JOIN evo_wcs_g2p.w2p_picking_job_v2 pj
ON g.job_id = pj.job_id
WHERE sc.state = 'DONE' AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' AND g.project_code = 'FH-B2021-C100' AND pj.project_code = 'FH-B2021-C100'
ORDER BY sc.last_updated_date
)tmp
ON sc.station_task_id = tmp.station_task_id 
WHERE sc.state = 'NEW' AND sc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND sc.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100'
ORDER BY tmp.station_code,sc.last_updated_date
) a,(SELECT @r:= 0) b
)b
on a.station_code = b.station_code and a.num = b.num-1
GROUP BY  a.station_code,DATE_FORMAT(a.NEW_updated_date,'%Y-%m-%d %H:00:00')
)tmp7
on tmp1.station_code = tmp7.station_code and tmp1.times = tmp7.times
GROUP BY tmp1.times,tmp1.station_code

UNION ALL

SELECT 
 tmp_line.ids as times,
 seq.station_code,
 0 '单工作站双作业点位完成箱数',
 0 '工作站作业点完成箱数（作业点-箱数）',
 0 '单次任务耗时/秒',
 0 '平均入站耗时/秒',
 0 '平均人工拣货耗时/秒',
 0 '平均两次任务间隔时长/秒'
  FROM evo_station.station_entry seq,
       (SELECT @t:=DATE_ADD(@t,INTERVAL 1 HOUR) as ids
       FROM information_schema.COLUMNS,(select @t:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @t < DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp_line
  WHERE ((seq.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))OR (seq.exit_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')) OR (seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time >= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))
  AND seq.biz_type = 'PICKING_ONLINE_G2P_W2P' AND seq.project_code = 'FH-B2021-C100' 
GROUP BY tmp_line.ids,seq.station_code
)tt
GROUP BY tt.times,tt.station_code;

-- ########################################################################
/*
* 统计-盘点订单效率指标
*/

SELECT 
     tmp2.times as '时间段', -- 时间段
     SUM(tmp2.order_num) as '订单完成数', -- 订单完成数
     SUM(tmp2.order_linenum) as '完成订单行数', -- 完成订单行数
     SUM(tmp2.sku_num) as '完成货品件数', -- 完成货品件数
     SUM(tmp2.into_station_times) as '进站次数', -- 进站次数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.order_linenum)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成订单行数', -- 单次进站完成订单行数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.sku_num)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成货品件数' -- 单次进站完成货品件数
FROM (
SELECT  DATE_FORMAT(cc.last_updated_date,'%Y-%m-%d %H:00:00') times,
	   COUNT(DISTINCT cc.cycle_count_number)order_num,-- picking_order行数
     0 'order_linenum',
     0 'sku_num',
     0 'into_station_times'
    FROM evo_wes_cyclecount.cycle_count cc
	WHERE cc.state = 'DONE' AND cc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and cc.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cc.project_code = 'FH-B2021-C100' 
group BY DATE_FORMAT(cc.last_updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT DATE_FORMAT(cwd.updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     count(distinct cwd.id) order_linenum, -- picking_work_detail行数
     0 'sku_num',
     0 'into_station_times'
    FROM evo_wcs_g2p.w2p_countcheck_work_detail_v2 cwd
	JOIN evo_wcs_g2p.w2p_countcheck_job_v2 cj
	  ON cwd.id = cj.detail_id
	WHERE cwd.state = 'DONE' AND cj.state= 'DONE' AND cwd.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and cwd.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cwd.project_code = 'FH-B2021-C100' AND cj.project_code = 'FH-B2021-C100' 
	GROUP BY DATE_FORMAT(cwd.updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT DATE_FORMAT(ccd.last_updated_date,'%Y-%m-%d %H:00:00') times,
	   0 'order_num',
     0 'order_linenum',
     IF(sum(ccd.actual_quantity) is not null,sum(ccd.actual_quantity),0) sku_num, -- 实捡数量
     0 'into_station_times'
FROM evo_wes_cyclecount.cycle_count_detail ccd
WHERE ccd.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and ccd.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND ccd.project_code = 'FH-B2021-C100'
group BY DATE_FORMAT(ccd.last_updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     0 'order_linenum',
     0 'sku_num',
       count(se.id) into_station_times
    FROM evo_station.station_entry se
	WHERE se.biz_type = 'CYCLECOUNT_ONLINE_G2P_W2P' and se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND se.project_code = 'FH-B2021-C100' 
	GROUP BY DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT 
     tmp_line.ida times,
     0 'order_num',
     0 'order_linenum',
     0 'sku_num',
     0 'into_station_times'
	FROM (SELECT @r:=DATE_ADD(@r,INTERVAL 1 HOUR) as ida
       FROM information_schema.COLUMNS,(select @r:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @r < DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp_line
    GROUP BY tmp_line.ida
) tmp2 
GROUP BY tmp2.times;

-- ########################################################################
/*
* 统计-盘点任务搬运指标
*/

SELECT tt.times as '工作时间/小时',
       tt.station_code as '工作站编码',
       IF(tt.single_num is NULL,0,tt.single_num) as '单工作站双作业点位完成箱数',
       IF(tt.double_num is NULL,0,tt.double_num) as '工作站作业点完成箱数（作业点-箱数）',
       IF(tt.task_time is NULL,0,tt.task_time) as '单次任务耗时/秒',
       IF(tt.entry_time is NULL,0,tt.entry_time) as '平均入站耗时/秒',
       IF(tt.picking_time is NULL,0,tt.picking_time) as '平均人工拣货耗时/秒',
       IF(tt.be_time is NULL,0,tt.be_time) as '平均两次任务间隔时长/秒'
FROM
(
SELECT tmp1.times,
       tmp1.station_code,
       tmp2.num as single_num,
       tmp3.num as double_num,
       tmp4.time as task_time,
       tmp5.time as entry_time,
       tmp6.time as picking_time,
       tmp7.time as be_time
FROM
(
select DATE_FORMAT(se.last_updated_date,'%Y-%m-%d %H:00:00') times, -- '工作时间/小时'
        se.station_code -- '工作站编码'
FROM evo_station.station_entry se
WHERE se.biz_type = 'CYCLECOUNT_ONLINE_G2P_W2P' AND se.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.last_updated_date < DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 1 DAY) AND se.project_code = 'FH-B2021-C100'
GROUP BY se.station_code,DATE_FORMAT(se.last_updated_date,'%Y-%m-%d %H:00:00')
)tmp1
LEFT JOIN
(
select DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00') times,
       cmj.station_code, -- '工作站编码'
       count(cmj.target_way_point_code) as 'num' -- '单工作站双作业点位完成箱数'
FROM evo_wcs_g2p.container_move_job_v2 cmj
WHERE cmj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND cmj.updated_date < DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 1 DAY) AND cmj.biz_type = 'W2P_ONLINE_COUNTCHECK' AND cmj.state='DONE' AND cmj.project_code = 'FH-B2021-C100'
GROUP BY cmj.station_code,DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00')
)tmp2
on tmp1.station_code = tmp2.station_code and tmp1.times = tmp2.times
LEFT JOIN
(
select DATE_FORMAT(tmp.updated_date,'%Y-%m-%d %H:00:00') times,tmp.station_code,GROUP_CONCAT(tmp.num) as 'num'
FROM 
(
select cmj.station_code,
       concat(cmj.target_way_point_code,'-',count(cmj.target_way_point_code)) as 'num',-- '工作站作业点完成箱数'
       cmj.updated_date,
       cmj.state,
       cmj.source_way_point_code,
       cmj.target_way_point_code
FROM evo_wcs_g2p.container_move_job_v2 cmj
WHERE cmj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND cmj.updated_date < DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 1 DAY) AND cmj.biz_type = 'W2P_ONLINE_COUNTCHECK' AND cmj.state='DONE' AND cmj.project_code = 'FH-B2021-C100'
GROUP BY cmj.station_code,cmj.target_way_point_code,DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00')
)tmp
GROUP BY tmp.station_code,DATE_FORMAT(tmp.updated_date,'%Y-%m-%d %H:00:00')
)tmp3
on tmp1.station_code = tmp3.station_code and tmp1.times = tmp3.times
LEFT JOIN
(
SELECT DATE_FORMAT(wcj.updated_date,'%Y-%m-%d %H:00:00') times,wcj.station_code,CAST((SUM(TIMESTAMPDIFF(SECOND,wcj.created_date,wcj.updated_date))/COUNT(wcj.job_id)) AS DECIMAL(10,2)) as 'time' -- '单次任务耗时/秒'
FROM evo_wcs_g2p.w2p_countcheck_job_v2 wcj
WHERE wcj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND  wcj.updated_date < DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 1 DAY) AND wcj.state='DONE' AND wcj.project_code = 'FH-B2021-C100'
GROUP BY wcj.station_code,DATE_FORMAT(wcj.updated_date,'%Y-%m-%d %H:00:00')
)tmp4
on tmp1.station_code = tmp4.station_code and tmp1.times = tmp4.times
LEFT JOIN
(
SELECT DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00') times,cmj.station_code,CAST(SUM(TIMESTAMPDIFF(SECOND,jsc.updated_date,cmj.updated_date))/COUNT(cmj.job_id) AS DECIMAL(10,2)) as 'time' -- '平均入站耗时/秒'
FROM evo_wcs_g2p.container_move_job_v2 cmj
JOIN evo_wcs_g2p.job_state_change jsc
ON cmj.job_id = jsc.job_id
WHERE cmj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND cmj.updated_date < DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 1 DAY) AND cmj.biz_type = 'W2P_ONLINE_COUNTCHECK' AND cmj.state='DONE' AND jsc.state = 'INIT_JOB' AND cmj.project_code = 'FH-B2021-C100' AND jsc.project_code = 'FH-B2021-C100'
GROUP BY cmj.station_code,DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00')
)tmp5
on tmp1.station_code = tmp5.station_code and tmp1.times = tmp5.times
LEFT JOIN
(
SELECT DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00') times,wcj.station_code,CAST((SUM(TIMESTAMPDIFF(SECOND,tmp.last_updated_date,jsc.updated_date))/COUNT(jsc.job_id)) AS DECIMAL(10,2))  as 'time' -- '平均人工拣货耗时/秒'
FROM evo_wcs_g2p.job_state_change jsc
JOIN evo_wcs_g2p.w2p_countcheck_job_v2 wcj
ON jsc.job_id = wcj.job_id
JOIN
(
select wcj.job_id,t.station_code,sc.last_updated_date
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
JOIN evo_wcs_g2p.w2p_countcheck_job_v2 wcj
ON t.task_no = wcj.job_id
WHERE sc.state = 'PULLED' AND sc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND sc.last_updated_date < DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 1 DAY) AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' AND wcj.project_code = 'FH-B2021-C100'
)tmp
ON jsc.job_id = tmp.job_id
WHERE jsc.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND jsc.updated_date < DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 1 DAY) AND jsc.state = 'DONE' AND jsc.project_code = 'FH-B2021-C100' AND wcj.project_code = 'FH-B2021-C100'
GROUP BY wcj.station_code,DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00')
)tmp6
on tmp1.station_code = tmp6.station_code and tmp1.times = tmp6.times
LEFT JOIN
(
select DATE_FORMAT(a.NEW_updated_date,'%Y-%m-%d %H:00:00') times,a.station_code,CAST(SUM(IF(TIMESTAMPDIFF(SECOND,a.DONE_updated_date,b.NEW_updated_date)<0,0, TIMESTAMPDIFF(SECOND,a.DONE_updated_date,b.NEW_updated_date)))/COUNT(a.job_id) AS DECIMAL(10,2)) as 'time' -- '平均两次任务间隔时长' 
FROM
(
SELECT @i:=@i + 1 AS num,a.*
FROM
(
SELECT tmp.station_code,tmp.job_id,sc.last_updated_date as 'NEW_updated_date',tmp.last_updated_date as 'DONE_updated_date'
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
join 
(
SELECT wcj.station_code,sc.station_task_id,sc.last_updated_date,sc.id,wcj.job_id
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
JOIN evo_wcs_g2p.w2p_countcheck_job_v2 wcj
ON t.task_no = wcj.job_id
WHERE sc.state = 'DONE' AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' AND wcj.project_code = 'FH-B2021-C100'
ORDER BY sc.last_updated_date
)tmp
ON sc.station_task_id = tmp.station_task_id 
WHERE sc.state = 'NEW' AND sc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND sc.last_updated_date < DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 1 DAY) AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' 
ORDER BY tmp.station_code,sc.last_updated_date
)a,(SELECT @i:= 0) b
)a
LEFT JOIN
(
select @r:=@r+ 1 AS num,a.* from
(
SELECT tmp.station_code,sc.last_updated_date as 'NEW_updated_date',tmp.last_updated_date as 'DONE_updated_date'
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
join 
(
SELECT wcj.station_code,sc.station_task_id,sc.last_updated_date,sc.id
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
JOIN evo_wcs_g2p.w2p_countcheck_job_v2 wcj
ON t.task_no = wcj.job_id
WHERE sc.state = 'DONE' AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' AND wcj.project_code = 'FH-B2021-C100'
ORDER BY sc.last_updated_date
)tmp
ON sc.station_task_id = tmp.station_task_id 
WHERE sc.state = 'NEW' AND sc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND sc.last_updated_date < DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 1 DAY) AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100'
ORDER BY tmp.station_code,sc.last_updated_date
) a,(SELECT @r:= 0) b
)b
on a.station_code = b.station_code and a.num = b.num-1
GROUP BY  a.station_code,DATE_FORMAT(a.NEW_updated_date,'%Y-%m-%d %H:00:00')
)tmp7
on tmp1.station_code = tmp7.station_code and tmp1.times = tmp7.times

UNION ALL

SELECT 
 tmp_line.ids as times,
 seq.station_code,
 0 '单工作站双作业点位完成箱数',
 0 '工作站作业点完成箱数（作业点-箱数）',
 0 '单次任务耗时/秒',
 0 '平均入站耗时/秒',
 0 '平均人工拣货耗时/秒',
 0 '平均两次任务间隔时长/秒'
  FROM evo_station.station_entry seq,
       (SELECT @t:=DATE_ADD(@t,INTERVAL 1 HOUR) as ids
       FROM information_schema.COLUMNS,(select @t:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @t < DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp_line
  WHERE ((seq.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))OR (seq.exit_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')) OR (seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time >= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))
  AND seq.biz_type = 'CYCLECOUNT_ONLINE_G2P_W2P' AND seq.project_code = 'FH-B2021-C100' 
GROUP BY tmp_line.ids,seq.station_code
)tt
GROUP BY tt.times,tt.station_code;

-- ########################################################################
/*
* 统计-推荐上架订单效率指标
*/

SELECT 
     tmp2.times as '时间段', -- 时间段
     SUM(tmp2.order_num) as '订单完成数', -- 订单完成数
     SUM(tmp2.order_linenum) as '完成订单行数', -- 完成订单行数
     SUM(tmp2.sku_num) as '完成货品件数', -- 完成货品件数
     SUM(tmp2.into_station_times) as '进站次数', -- 进站次数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.order_linenum)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成订单行数', -- 单次进站完成订单行数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.sku_num)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成货品件数' -- 单次进站完成货品件数
FROM (
SELECT  DATE_FORMAT(ro.last_updated_date,'%Y-%m-%d %H:00:00') times,
	   COUNT(DISTINCT ro.replenish_order_number)order_num,-- picking_order行数
     0 'order_linenum',
     0 'sku_num',
     0 'into_station_times'
    FROM evo_wes_replenish.replenish_order ro
    JOIN evo_wcs_g2p.w2p_guided_put_away_job wpj
      ON ro.id = wpj.order_id
	WHERE ro.state = 'DONE' AND ro.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and ro.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')AND ro.project_code = 'FH-B2021-C100' AND wpj.project_code = 'FH-B2021-C100'
group BY DATE_FORMAT(ro.last_updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT DATE_FORMAT(cwd.updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     count(distinct cwd.id) order_linenum, -- picking_work_detail行数
     0 'sku_num',
     0 'into_station_times'
    FROM evo_wcs_g2p.w2p_guided_putaway_work_detail cwd
	JOIN evo_wcs_g2p.w2p_guided_put_away_job wpj
	  ON cwd.detail_id = wpj.detail_id
	WHERE cwd.quantity = cwd.fulfill_quantity AND wpj.state= 'DONE' AND cwd.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and cwd.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cwd.project_code = 'FH-B2021-C100' AND wpj.project_code = 'FH-B2021-C100'
	GROUP BY DATE_FORMAT(cwd.updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT DATE_FORMAT(j.updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     0 'order_linenum',
       sum(j.fullfill_quantity) sku_num, 
	   0 'into_station_times'
	FROM evo_wcs_g2p.w2p_guided_put_away_job j
	WHERE j.state='DONE' AND j.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and j.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND j.project_code = 'FH-B2021-C100' 
	group BY DATE_FORMAT(j.updated_date,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00') times,
     0 'order_num',
     0 'order_linenum',
     0 'sku_num',
       count(se.id) into_station_times
    FROM evo_station.station_entry se
	WHERE se.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' and se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND se.project_code = 'FH-B2021-C100' 
	GROUP BY DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00')

UNION ALL

SELECT 
     tmp_line.ida times,
     0 'order_num',
     0 'order_linenum',
     0 'sku_num',
     0 'into_station_times'
	FROM (SELECT @r:=DATE_ADD(@r,INTERVAL 1 HOUR) as ida
       FROM information_schema.COLUMNS,(select @r:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @r < DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp_line
    GROUP BY tmp_line.ida
) tmp2 
GROUP BY tmp2.times;

-- ########################################################################
/*
* 统计-推荐上架任务搬运指标
*/

SELECT tt.times as '工作时间/小时',
       tt.station_code as '工作站编码',
       IF(tt.single_num is NULL,0,tt.single_num) as '单工作站双作业点位完成箱数',
       IF(tt.double_num is NULL,0,tt.double_num) as '工作站作业点完成箱数（作业点-箱数）',
       IF(tt.task_time is NULL,0,tt.task_time) as '单次任务耗时/秒',
       IF(tt.entry_time is NULL,0,tt.entry_time) as '平均入站耗时/秒',
       IF(tt.picking_time is NULL,0,tt.picking_time) as '平均人工拣货耗时/秒',
       IF(tt.be_time is NULL,0,tt.be_time) as '平均两次任务间隔时长/秒'
FROM
(
SELECT tmp1.times,
       tmp1.station_code,
       tmp2.num as single_num,
       tmp3.num as double_num,
       tmp4.time as task_time,
       tmp5.time as entry_time,
       tmp6.time as picking_time,
       tmp7.time as be_time
FROM
(
select DATE_FORMAT(se.last_updated_date,'%Y-%m-%d %H:00:00') times, -- '工作时间/小时'
        se.station_code -- '工作站编码'
FROM evo_station.station_entry se
WHERE se.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' AND se.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND se.project_code = 'FH-B2021-C100'
GROUP BY se.station_code,DATE_FORMAT(se.last_updated_date,'%Y-%m-%d %H:00:00')
)tmp1
LEFT JOIN
(
select DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00') times,
       cmj.station_code, -- '工作站编码'
       count(cmj.target_way_point_code) as 'num' -- '单工作站双作业点位完成箱数'
FROM evo_wcs_g2p.container_move_job_v2 cmj
WHERE cmj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND cmj.updated_date <DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cmj.biz_type = 'W2P_ONLINE_PUTAWAY_GUIDED' AND cmj.state='DONE' AND cmj.project_code = 'FH-B2021-C100'
GROUP BY cmj.station_code,DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00')
)tmp2
on tmp1.station_code = tmp2.station_code and tmp1.times = tmp2.times
LEFT JOIN
(
select DATE_FORMAT(tmp.updated_date,'%Y-%m-%d %H:00:00') times,tmp.station_code,GROUP_CONCAT(tmp.num) as 'num'
FROM 
(
select cmj.station_code,
       concat(cmj.target_way_point_code,'-',count(cmj.target_way_point_code)) as 'num',-- '工作站作业点完成箱数'
       cmj.updated_date,
       cmj.state,
       cmj.source_way_point_code,
       cmj.target_way_point_code
FROM evo_wcs_g2p.container_move_job_v2 cmj
WHERE cmj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND cmj.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cmj.biz_type = 'W2P_ONLINE_PUTAWAY_GUIDED' AND cmj.state='DONE' AND cmj.project_code = 'FH-B2021-C100'
GROUP BY cmj.station_code,cmj.target_way_point_code,DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00')
)tmp
GROUP BY tmp.station_code,DATE_FORMAT(tmp.updated_date,'%Y-%m-%d %H:00:00')
)tmp3
on tmp1.station_code = tmp3.station_code and tmp1.times = tmp3.times
LEFT JOIN
(
SELECT DATE_FORMAT(wpj.updated_date,'%Y-%m-%d %H:00:00') times,wpj.station_code,CAST((SUM(TIMESTAMPDIFF(SECOND,wpj.created_date,wpj.updated_date))/COUNT(wpj.job_id)) AS DECIMAL(10,2)) as 'time' -- '单次任务耗时/秒'
FROM evo_wcs_g2p.w2p_guided_put_away_job wpj
WHERE wpj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND wpj.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND wpj.state='DONE' AND wpj.project_code = 'FH-B2021-C100'
GROUP BY wpj.station_code,DATE_FORMAT(wpj.updated_date,'%Y-%m-%d %H:00:00')
)tmp4
on tmp1.station_code = tmp4.station_code and tmp1.times = tmp4.times
LEFT JOIN
(
SELECT DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00') times,cmj.station_code,CAST(SUM(TIMESTAMPDIFF(SECOND,jsc.updated_date,cmj.updated_date))/COUNT(cmj.job_id) AS DECIMAL(10,2)) as 'time' -- '平均入站耗时/秒'
FROM evo_wcs_g2p.container_move_job_v2 cmj
JOIN evo_wcs_g2p.job_state_change jsc
ON cmj.job_id = jsc.job_id
WHERE cmj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND cmj.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cmj.biz_type = 'W2P_ONLINE_PUTAWAY_GUIDED' AND cmj.state='DONE' AND jsc.state = 'INIT_JOB' AND cmj.project_code = 'FH-B2021-C100' AND jsc.project_code = 'FH-B2021-C100'
GROUP BY cmj.station_code,DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00')
)tmp5
on tmp1.station_code = tmp5.station_code and tmp1.times = tmp5.times
LEFT JOIN
(
SELECT DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00') times,wpj.station_code,CAST((SUM(TIMESTAMPDIFF(SECOND,tmp.last_updated_date,jsc.updated_date))/COUNT(jsc.job_id)) AS DECIMAL(10,2))  as 'time' -- '平均人工拣货耗时/秒'
FROM evo_wcs_g2p.job_state_change jsc
JOIN evo_wcs_g2p.w2p_guided_put_away_job wpj
ON jsc.job_id = wpj.job_id
JOIN
(
select wpj.job_id,t.station_code,sc.last_updated_date
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
JOIN evo_wcs_g2p.w2p_guided_put_away_job wpj
ON t.task_no = wpj.job_id
WHERE sc.state = 'PULLED' AND sc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND sc.last_updated_date < DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 1 DAY) AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' AND wpj.project_code = 'FH-B2021-C100'
)tmp
ON jsc.job_id = tmp.job_id
WHERE jsc.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND jsc.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND jsc.state = 'DONE' AND jsc.project_code = 'FH-B2021-C100' AND wpj.project_code = 'FH-B2021-C100'
GROUP BY wpj.station_code,DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00')
)tmp6
on tmp1.station_code = tmp6.station_code and tmp1.times = tmp6.times
LEFT JOIN
(
select DATE_FORMAT(a.NEW_updated_date,'%Y-%m-%d %H:00:00') times,a.station_code,CAST(SUM(IF(TIMESTAMPDIFF(SECOND,a.DONE_updated_date,b.NEW_updated_date)<0,0, TIMESTAMPDIFF(SECOND,a.DONE_updated_date,b.NEW_updated_date)))/COUNT(a.job_id) AS DECIMAL(10,2)) as 'time' -- '平均两次任务间隔时长' 
FROM
(
SELECT @i:=@i + 1 AS num,a.*
FROM
(
SELECT tmp.station_code,tmp.job_id,sc.last_updated_date as 'NEW_updated_date',tmp.last_updated_date as 'DONE_updated_date'
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
join 
(
SELECT wpj.station_code,sc.station_task_id,sc.last_updated_date,sc.id,wpj.job_id
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
JOIN evo_wcs_g2p.w2p_guided_put_away_job wpj
ON t.task_no = wpj.job_id
WHERE sc.state = 'DONE' AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' AND wpj.project_code = 'FH-B2021-C100'
ORDER BY sc.last_updated_date
)tmp
ON sc.station_task_id = tmp.station_task_id 
WHERE sc.state = 'NEW' AND sc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND sc.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' 
ORDER BY tmp.station_code,sc.last_updated_date
)a,(SELECT @i:= 0) b
)a
LEFT JOIN
(
select @r:=@r+ 1 AS num,a.* from
(
SELECT tmp.station_code,sc.last_updated_date as 'NEW_updated_date',tmp.last_updated_date as 'DONE_updated_date'
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
join 
(
SELECT wpj.station_code,sc.station_task_id,sc.last_updated_date,sc.id
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
JOIN evo_wcs_g2p.w2p_guided_put_away_job wpj
ON t.task_no = wpj.job_id
WHERE sc.state = 'DONE' AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' AND wpj.project_code = 'FH-B2021-C100'
ORDER BY sc.last_updated_date
)tmp
ON sc.station_task_id = tmp.station_task_id 
WHERE sc.state = 'NEW' AND sc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND sc.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' 
ORDER BY tmp.station_code,sc.last_updated_date
) a,(SELECT @r:= 0) b
)b
on a.station_code = b.station_code and a.num = b.num-1
GROUP BY  a.station_code,DATE_FORMAT(a.NEW_updated_date,'%Y-%m-%d %H:00:00')
)tmp7
on tmp1.station_code = tmp7.station_code and tmp1.times = tmp7.times
UNION ALL

SELECT 
 tmp_line.ids as times,
 seq.station_code,
 0 '单工作站双作业点位完成箱数',
 0 '工作站作业点完成箱数（作业点-箱数）',
 0 '单次任务耗时/秒',
 0 '平均入站耗时/秒',
 0 '平均人工拣货耗时/秒',
 0 '平均两次任务间隔时长/秒'
  FROM evo_station.station_entry seq,
       (SELECT @t:=DATE_ADD(@t,INTERVAL 1 HOUR) as ids
       FROM information_schema.COLUMNS,(select @t:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @t < DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp_line
  WHERE ((seq.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))OR (seq.exit_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')) OR (seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time >= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))
  AND seq.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' AND seq.project_code = 'FH-B2021-C100' 
GROUP BY tmp_line.ids,seq.station_code
)tt
GROUP BY tt.times,tt.station_code;

-- ########################################################################
/*
* 统计-直接上架架任务搬运指标
*/

SELECT tt.times as '工作时间/小时',
       tt.station_code as '工作站编码',
       IF(tt.single_num is NULL,0,tt.single_num) as '单工作站双作业点位完成箱数',
       IF(tt.double_num is NULL,0,tt.double_num) as '工作站作业点完成箱数（作业点-箱数）',
       IF(tt.task_time is NULL,0,tt.task_time) as '单次任务耗时/秒',
       IF(tt.entry_time is NULL,0,tt.entry_time) as '平均入站耗时/秒',
       IF(tt.picking_time is NULL,0,tt.picking_time) as '平均人工拣货耗时/秒',
       IF(tt.be_time is NULL,0,tt.be_time) as '平均两次任务间隔时长/秒'
FROM
(
SELECT tmp1.times,
       tmp1.station_code,
       tmp2.num as single_num,
       tmp3.num as double_num,
       tmp4.time as task_time,
       tmp5.time as entry_time,
       tmp6.time as picking_time,
       tmp7.time as be_time
FROM
(
select DATE_FORMAT(se.last_updated_date,'%Y-%m-%d %H:00:00') times, -- '工作时间/小时'
        se.station_code -- '工作站编码'
FROM evo_station.station_entry se
WHERE se.biz_type = 'PUTAWAY_ONLINE_G2P_DIRECT_W2P' AND se.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND se.project_code = 'FH-B2021-C100'
GROUP BY se.station_code,DATE_FORMAT(se.last_updated_date,'%Y-%m-%d %H:00:00')
)tmp1
LEFT JOIN
(
select DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00') times,
       cmj.station_code, -- '工作站编码'
       count(cmj.target_way_point_code) as 'num' -- '单工作站双作业点位完成箱数'
FROM evo_wcs_g2p.container_move_job_v2 cmj
WHERE cmj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND cmj.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cmj.biz_type = 'W2P_ONLINE_PUTAWAY' AND cmj.state='DONE' AND cmj.project_code = 'FH-B2021-C100'
GROUP BY cmj.station_code,DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00')
)tmp2
on tmp1.station_code = tmp2.station_code and tmp1.times = tmp2.times
LEFT JOIN
(
select DATE_FORMAT(tmp.updated_date,'%Y-%m-%d %H:00:00') times,tmp.station_code,GROUP_CONCAT(tmp.num) as 'num'
FROM 
(
select cmj.station_code,
       concat(cmj.target_way_point_code,'-',count(cmj.target_way_point_code)) as 'num',-- '工作站作业点完成箱数'
       cmj.updated_date,
       cmj.state,
       cmj.source_way_point_code,
       cmj.target_way_point_code
FROM evo_wcs_g2p.container_move_job_v2 cmj
WHERE cmj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND cmj.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cmj.biz_type = 'W2P_ONLINE_PUTAWAY' AND cmj.state='DONE' AND cmj.project_code = 'FH-B2021-C100'
GROUP BY cmj.station_code,cmj.target_way_point_code,DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00')
)tmp
GROUP BY tmp.station_code,DATE_FORMAT(tmp.updated_date,'%Y-%m-%d %H:00:00')
)tmp3
on tmp1.station_code = tmp3.station_code and tmp1.times = tmp3.times
LEFT JOIN
(
SELECT DATE_FORMAT(wpj.updated_date,'%Y-%m-%d %H:00:00') times,wpj.station_code,CAST((SUM(TIMESTAMPDIFF(SECOND,wpj.created_date,wpj.updated_date))/COUNT(wpj.job_id)) AS DECIMAL(10,2)) as 'time' -- '单次任务耗时/秒'
FROM evo_wcs_g2p.w2p_putaway_job_v2 wpj
WHERE wpj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND wpj.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND wpj.state='DONE' AND wpj.project_code = 'FH-B2021-C100'
GROUP BY wpj.station_code,DATE_FORMAT(wpj.updated_date,'%Y-%m-%d %H:00:00')
)tmp4
on tmp1.station_code = tmp4.station_code and tmp1.times = tmp4.times
LEFT JOIN
(
SELECT DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00') times,cmj.station_code,CAST(SUM(TIMESTAMPDIFF(SECOND,jsc.updated_date,cmj.updated_date))/COUNT(cmj.job_id) AS DECIMAL(10,2)) as 'time' -- '平均入站耗时/秒'
FROM evo_wcs_g2p.container_move_job_v2 cmj
JOIN evo_wcs_g2p.job_state_change jsc
ON cmj.job_id = jsc.job_id
WHERE cmj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND cmj.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cmj.biz_type = 'W2P_ONLINE_PUTAWAY' AND cmj.state='DONE' AND jsc.state = 'INIT_JOB' AND cmj.project_code = 'FH-B2021-C100' AND jsc.project_code = 'FH-B2021-C100'
GROUP BY cmj.station_code,DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00')
)tmp5
on tmp1.station_code = tmp5.station_code and tmp1.times = tmp5.times
LEFT JOIN
(
SELECT DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00') times,wpj.station_code,CAST((SUM(TIMESTAMPDIFF(SECOND,tmp.last_updated_date,jsc.updated_date))/COUNT(jsc.job_id)) AS DECIMAL(10,2))  as 'time' -- '平均人工拣货耗时/秒'
FROM evo_wcs_g2p.job_state_change jsc
JOIN evo_wcs_g2p.w2p_putaway_job_v2 wpj
ON jsc.job_id = wpj.job_id
JOIN
(
select wpj.job_id,t.station_code,sc.last_updated_date
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
JOIN evo_wcs_g2p.w2p_putaway_job_v2 wpj
ON t.task_no = wpj.job_id
WHERE sc.state = 'PULLED' AND sc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND sc.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' AND wpj.project_code = 'FH-B2021-C100'
)tmp
ON jsc.job_id = tmp.job_id
WHERE jsc.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND jsc.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND jsc.state = 'DONE' AND jsc.project_code = 'FH-B2021-C100' AND wpj.project_code = 'FH-B2021-C100'
GROUP BY wpj.station_code,DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00')
)tmp6
on tmp1.station_code = tmp6.station_code and tmp1.times = tmp6.times
LEFT JOIN
(
select DATE_FORMAT(a.NEW_updated_date,'%Y-%m-%d %H:00:00') times,a.station_code,CAST(SUM(IF(TIMESTAMPDIFF(SECOND,a.DONE_updated_date,b.NEW_updated_date)<0,0, TIMESTAMPDIFF(SECOND,a.DONE_updated_date,b.NEW_updated_date)))/COUNT(a.job_id) AS DECIMAL(10,2)) as 'time' -- '平均两次任务间隔时长' 
FROM
(
SELECT @i:=@i + 1 AS num,a.*
FROM
(
SELECT tmp.station_code,tmp.job_id,sc.last_updated_date as 'NEW_updated_date',tmp.last_updated_date as 'DONE_updated_date'
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
join 
(
SELECT wpj.station_code,sc.station_task_id,sc.last_updated_date,sc.id,wpj.job_id
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
JOIN evo_wcs_g2p.w2p_putaway_job_v2 wpj
ON t.task_no = wpj.job_id
WHERE sc.state = 'DONE' AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' AND wpj.project_code = 'FH-B2021-C100'
ORDER BY sc.last_updated_date
)tmp
ON sc.station_task_id = tmp.station_task_id 
WHERE sc.state = 'NEW' AND sc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND sc.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' 
ORDER BY tmp.station_code,sc.last_updated_date
)a,(SELECT @i:= 0) b
)a
LEFT JOIN
(
select @r:=@r+ 1 AS num,a.* from
(
SELECT tmp.station_code,sc.last_updated_date as 'NEW_updated_date',tmp.last_updated_date as 'DONE_updated_date'
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
join 
(
SELECT wpj.station_code,sc.station_task_id,sc.last_updated_date,sc.id
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
JOIN evo_wcs_g2p.w2p_putaway_job_v2 wpj
ON t.task_no = wpj.job_id
WHERE sc.state = 'DONE' AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' AND wpj.project_code = 'FH-B2021-C100'
ORDER BY sc.last_updated_date
)tmp
ON sc.station_task_id = tmp.station_task_id 
WHERE sc.state = 'NEW' AND sc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND sc.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' 
ORDER BY tmp.station_code,sc.last_updated_date
) a,(SELECT @r:= 0) b
)b
on a.station_code = b.station_code and a.num = b.num-1
GROUP BY  a.station_code,DATE_FORMAT(a.NEW_updated_date,'%Y-%m-%d %H:00:00')
)tmp7
on tmp1.station_code = tmp7.station_code and tmp1.times = tmp7.times

UNION ALL

SELECT 
 tmp_line.ids as times,
 seq.station_code,
 0 '单工作站双作业点位完成箱数',
 0 '工作站作业点完成箱数（作业点-箱数）',
 0 '单次任务耗时/秒',
 0 '平均入站耗时/秒',
 0 '平均人工拣货耗时/秒',
 0 '平均两次任务间隔时长/秒'
  FROM evo_station.station_entry seq,
       (SELECT @t:=DATE_ADD(@t,INTERVAL 1 HOUR) as ids
       FROM information_schema.COLUMNS,(select @t:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @t < DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp_line
  WHERE ((seq.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))OR (seq.exit_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')) OR (seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and seq.exit_time >= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))
  AND seq.biz_type = 'PUTAWAY_ONLINE_G2P_DIRECT_W2P' AND seq.project_code = 'FH-B2021-C100' 
GROUP BY tmp_line.ids,seq.station_code
)tt
GROUP BY tt.times,tt.station_code;

-- ########################################################################
/*
* 统计-拣选订单工作站分时效率指标
*/

SELECT  
     tmp2.times as '时间段', -- 时间段
     tmp2.station_code as '工作站', -- 工作站
     SUM(tmp2.into_station_times) as '进站次数', -- 进站次数
     SUM(tmp2.order_linenum) as '完成订单行数', -- 完成订单行数
     SUM(tmp2.sku_num) as '完成货品件数', -- 完成货品件数
	   SUM(tmp2.station_slot_times) as '命中槽位次数', -- 命中槽位次数
     SUM(tmp2.win_open_times) as '弹窗次数', -- 弹窗次数
     cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.win_open_times)/SUM(tmp2.into_station_times),0),0)) as decimal(10,2)) as '单次进站弹窗次数', -- 单次进站弹窗次数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.sku_num)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成货品件数', -- 单次进站完成货品件数
     cast((if(SUM(tmp2.win_open_times)!=0,ifnull(SUM(tmp2.station_slot_times)/SUM(tmp2.win_open_times),0),0))as decimal(10,2)) as '单次弹窗命中槽位次数', -- 单次弹窗命中槽位次数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.order_linenum)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成订单行数', -- 单次进站完成订单行数
     cast(SUM(tmp2.station_used)/3600 as decimal(10,2)) as '工作站利用率', -- 工作站利用率
     cast((if(SUM(tmp2.station_busy)!=0,ifnull(SUM(tmp2.station_used)/SUM(tmp2.station_busy),0),0)) as decimal(10,2)) as '工作站繁忙率', -- 工作站繁忙率
     cast(SUM(tmp2.station_busy)/3600 as decimal(10,2)) as '工作站在线率', -- 工作站在线率
     cast(SUM(tmp2.time) as decimal(10,2)) as '平均人工拣货耗时/秒' -- '平均人工拣货耗时/秒'
FROM (
SELECT DATE_FORMAT(stg.updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_linenum',
     pj.station_code, 
     sum(pj.actual_quantity) sku_num,
     count(pj.station_slot_code) station_slot_times,
	   0 'into_station_times', 
	   count(distinct stg.group_job_id) win_open_times,
     0 'station_used',
     0 'station_busy',
     0 'time'
	FROM evo_wcs_g2p.station_task_group stg 
  JOIN evo_wcs_g2p.w2p_picking_job_v2 pj 
  ON stg.job_id = pj.job_id 
	WHERE pj.state='DONE' AND stg.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and stg.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND pj.project_code = 'FH-B2021-C100' AND stg.project_code = 'FH-B2021-C100'
	group BY DATE_FORMAT(stg.updated_date,'%Y-%m-%d %H:00:00'),pj.station_code

UNION ALL

SELECT DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00') times,
       count(distinct pwd.id) order_linenum,
       pj.station_code,
       0 'sku_num', 
       0 'station_slot_times',
       0 'into_station_times',
       0 'win_open_times',
       0 'station_used',
       0 'station_busy',
       0 'time'
    FROM evo_wcs_g2p.w2p_picking_work_detail_v2  pwd
	 JOIN evo_wcs_g2p.w2p_picking_job_v2 pj
	 ON pwd.picking_work_detail_id = pj.picking_work_detail_id
	WHERE pwd.quantity = pwd.fulfill_quantity AND pj.state= 'DONE' and pwd.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and pwd.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND pwd.project_code = 'FH-B2021-C100' AND pj.project_code = 'FH-B2021-C100'
	GROUP BY DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00'),pj.station_code

UNION ALL

SELECT DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00') times,
     0 'order_linenum',
     se.station_code,
     0 'sku_num',
     0 'station_slot_times',
     count(se.id) into_station_times,
     0 'win_open_times',
     0 'station_used',
     0 'station_busy',
     0 'time'
    FROM evo_station.station_entry se
	WHERE biz_type = 'PICKING_ONLINE_G2P_W2P' and entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND entry_time < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND se.project_code = 'FH-B2021-C100'
	GROUP BY DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00'),se.station_code

UNION ALL

SELECT 
   tmp1.ids times,
   0 'order_linenum',
	 tmp1.station_code,
   0 'sku_num',
	 0 'station_slot_times', 
	 0 'into_station_times',
	 0 'win_open_times',
   SUM(		
			CASE WHEN tmp1.begin_to_exit_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_lineBegin_time
					 WHEN tmp1.begin_to_exit_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_exit_time and tmp1.begin_to_exit_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_exit_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_exit_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_exit_time and tmp1.begin_to_exit_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_exit_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_exit_time then tmp1.begin_to_lineBegin_time
				 	 ELSE 0 END
					-
			CASE WHEN tmp1.begin_to_entry_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_lineBegin_time
					 WHEN tmp1.begin_to_entry_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_entry_time and tmp1.begin_to_entry_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_entry_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_entry_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_entry_time and tmp1.begin_to_entry_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_entry_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_entry_time then tmp1.begin_to_lineBegin_time
				  ELSE 0 END
			) station_used,
    0 'station_busy',
    0 'time'
	FROM (
	  SELECT 
		tmp_line.ids,
		seq.station_code,
    TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),seq.entry_time)) 'begin_to_entry_time',
		TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(seq.exit_time is null,DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),if(seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),seq.exit_time,DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))) 'begin_to_exit_time',
		TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),tmp_line.ids) 'begin_to_lineBegin_time',
		TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(tmp_line.ids,INTERVAL 1 HOUR)) 'begin_to_lineEnd_time'
    FROM (
SELECT a.station_code,
       IF(a.hjk = 1,a.entry_time1,MIN(a.entry_time2)) as entry_time,
       IF(a.hjk = 1,a.exit_time1,MAX(a.exit_time2)) as exit_time
       
FROM
(
SELECT tt1.station_code1 as station_code,tt1.rn1,tt1.entry_time1,tt1.exit_time1,tt1.rn2,tt1.entry_time2,tt1.exit_time2,tt1.cf1,tt2.cf2,
       case when tt1.rn1 = 1 AND tt1.cf1 = 1 then 1
            when tt1.cf1 <= tt2.cf2 AND tt1.cf1 = 0 then @i:=@i 
            when tt1.cf1 != tt2.cf2 then @i := @i + 1 + tt2.cf2 
            end as hjk
FROM
(
SELECT *,IF(t2.entry_time2 >= t1.entry_time1 AND t2.entry_time2 < t1.exit_time1,0,@t1:=@t1+1) as cf1
FROM
(
SELECT (@rn1:=@rn1+1) as rn1,se.station_code as station_code1,se.entry_time as entry_time1,se.exit_time as exit_time1
FROM evo_station.station_entry se,(SELECT @rn1:=0)rn
WHERE se.biz_type = 'PICKING_ONLINE_G2P_W2P' AND se.project_code = 'FH-B2021-C100' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t1
JOIN
(
SELECT (@rn2:=@rn2+1) as rn2,se.station_code as station_code2,se.entry_time as entry_time2,se.exit_time as exit_time2
FROM evo_station.station_entry se,(SELECT @rn2:=0)rn
WHERE se.biz_type = 'PICKING_ONLINE_G2P_W2P' AND se.project_code = 'FH-B2021-C100' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t2
ON t1.station_code1 = t2.station_code2 AND t1.rn1 = t2.rn2 - 1
JOIN
(SELECT @t1:=0)tmp1
)tt1
JOIN
(
SELECT *,IF(t2.entry_time2 >= t1.entry_time1 AND t2.entry_time2 < t1.exit_time1,0,@t2:=@t2+1) as cf2
FROM
(
SELECT (@rn3:=@rn3+1) as rn1,se.station_code as station_code1,se.entry_time as entry_time1,se.exit_time as exit_time1
FROM evo_station.station_entry se,(SELECT @rn3:=0)rn
WHERE se.biz_type = 'PICKING_ONLINE_G2P_W2P' AND se.project_code = 'FH-B2021-C100' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t1
JOIN
(
SELECT (@rn4:=@rn4+1) as rn2,se.station_code as station_code2,se.entry_time as entry_time2,se.exit_time as exit_time2
FROM evo_station.station_entry se,(SELECT @rn4:=0)rn
WHERE se.biz_type = 'PICKING_ONLINE_G2P_W2P' AND se.project_code = 'FH-B2021-C100' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t2
ON t1.station_code1 = t2.station_code2 AND t1.rn1 = t2.rn2 - 1
JOIN
(SELECT @t2:=0)tmp1
)tt2
ON tt1.station_code1 = tt2.station_code1 AND tt1.rn1 = tt2.rn1 - 1
JOIN
(SELECT @i:=0)tmp
)a
GROUP BY a.station_code,a.hjk
ORDER BY a.station_code,`entry_time`
)seq,
	    (SELECT @t:=DATE_ADD(@t,INTERVAL 1 HOUR) as ids
       FROM information_schema.COLUMNS,(select @t:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @t < DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp_line
) tmp1
GROUP BY tmp1.ids,tmp1.station_code

UNION ALL

SELECT 
   tt1.ida times,
   0 'order_linenum',
	 tt1.station_code,
   0 'sku_num',
	 0 'station_slot_times', 
	 0 'into_station_times',
	 0 'win_open_times',
   0 'station_used',
     SUM(		
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
			) station_busy,
      0 'time' 
    FROM (
	  SELECT 
		tmp_line.ida,
		sl.station_code,
    TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(sl.login_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),sl.login_time)) 'begin_to_entry_time',
		TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(sl.logout_time is null,DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 60*24 MINUTE),if(sl.logout_time <= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 60*24 MINUTE),sl.logout_time,DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 60*24 MINUTE)))) 'begin_to_exit_time',
		TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),tmp_line.ida) 'begin_to_lineBegin_time',
		TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(tmp_line.ida,INTERVAL 1 HOUR)) 'begin_to_lineEnd_time'
    FROM evo_station.station_login sl,
	     (SELECT @r:=DATE_ADD(@r,INTERVAL 1 HOUR) as ida
       FROM information_schema.COLUMNS,(select @r:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @r < DATE_ADD(DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 1 DAY),INTERVAL -1 HOUR)) tmp_line
    WHERE 
       ((sl.login_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and sl.login_time <= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 60*24 MINUTE))
       OR (sl.logout_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and sl.logout_time <= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 60*24 MINUTE))
		   OR (sl.login_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and sl.logout_time >= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 60*24 MINUTE)))
       AND sl.biz_type = 'PICKING_ONLINE_G2P_W2P' AND sl.project_code = 'FH-B2021-C100' 
        ) tt1
    GROUP BY tt1.ida,tt1.station_code

UNION ALL

SELECT 
   DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00') times,
   0 'order_linenum',
	 pj.station_code,
   0 'sku_num',
	 0 'station_slot_times', 
	 0 'into_station_times',
	 0 'win_open_times',
   0 'station_used',
   0 'station_busy',
   SUM(TIMESTAMPDIFF(SECOND,tmp.last_updated_date,jsc.updated_date))/COUNT(pj.job_id) time
FROM evo_wcs_g2p.job_state_change jsc
JOIN evo_wcs_g2p.w2p_picking_job_v2 pj
ON jsc.job_id = pj.job_id
JOIN
(
SELECT pj.job_id,t.station_code,sc.last_updated_date
FROM evo_station.station_task_state_change sc
JOIN evo_station.station_task t
ON sc.station_task_id = t.id
JOIN evo_wcs_g2p.station_task_group g
ON t.task_no = g.group_job_id
JOIN evo_wcs_g2p.w2p_picking_job_v2 pj
ON g.job_id = pj.job_id
WHERE sc.state = 'PULLED' AND sc.last_updated_date BETWEEN DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' AND g.project_code = 'FH-B2021-C100' AND pj.project_code = 'FH-B2021-C100' 
)tmp
ON jsc.job_id = tmp.job_id
WHERE jsc.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND jsc.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND jsc.state = 'DONE' AND jsc.project_code = 'FH-B2021-C100'  AND pj.project_code = 'FH-B2021-C100' 
GROUP BY DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00') ,pj.station_code
) tmp2 
GROUP BY tmp2.times,tmp2.station_code;

-- ########################################################################
/*
* 统计-盘点订单工作站分时效率指标
*/

SELECT  
     tmp2.times as '时间段', -- 时间段
     tmp2.station_code as '工作站', -- 工作站
     SUM(tmp2.into_station_times) as '进站次数', -- 进站次数
     SUM(tmp2.order_linenum) as '完成订单行数', -- 完成订单行数
     SUM(tmp2.sku_num) as '完成货品件数', -- 完成货品件数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.sku_num)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成货品件数', -- 单次进站完成货品件数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.order_linenum)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成订单行数', -- 单次进站完成订单行数
     cast(SUM(tmp2.station_used)/3600 as decimal(10,2)) as '工作站利用率', -- 工作站利用率
     cast((if(SUM(tmp2.station_busy)!=0,ifnull(SUM(tmp2.station_used)/SUM(tmp2.station_busy),0),0)) as decimal(10,2)) as '工作站繁忙率', -- 工作站繁忙率
     cast(SUM(tmp2.station_busy)/3600 as decimal(10,2)) as '工作站在线率', -- 工作站在线率
     cast(SUM(tmp2.time) as decimal(10,2)) as '平均人工盘点耗时/秒' -- '平均人工盘点耗时/秒'
FROM (
SELECT DATE_FORMAT(ccd.last_updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_linenum',
     ccd.station_code, 
     IF(sum(ccd.actual_quantity) is not null,sum(ccd.actual_quantity),0) sku_num,
	   0 'into_station_times', 
     0 'station_used',
     0 'station_busy',
     0 'time'
FROM evo_wes_cyclecount.cycle_count_detail ccd
WHERE ccd.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and ccd.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')  AND ccd.station_code != ' ' AND ccd.station_code is not null  AND ccd.project_code = 'FH-B2021-C100' 
group BY DATE_FORMAT(ccd.last_updated_date,'%Y-%m-%d %H:00:00'),ccd.station_code

UNION ALL

SELECT DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00') times,
       count(distinct pwd.id) order_linenum,
       cj.station_code,
       0 'sku_num', 
       0 'into_station_times',
       0 'station_used',
       0 'station_busy',
       0 'time'
    FROM evo_wcs_g2p.w2p_countcheck_work_detail_v2 pwd
    LEFT JOIN evo_wcs_g2p.w2p_countcheck_job_v2 cj
    ON pwd.id = cj.detail_id
	WHERE pwd.state= 'DONE'  AND pwd.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and pwd.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND pwd.project_code = 'FH-B2021-C100' AND cj.project_code = 'FH-B2021-C100'
	GROUP BY DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00'),cj.station_code

UNION ALL

SELECT DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00') times,
     0 'order_linenum',
     se.station_code,
     0 'sku_num',
     count(se.id) into_station_times,
     0 'station_used',
     0 'station_busy',
     0 'time'
    FROM evo_station.station_entry se
	WHERE se.biz_type = 'CYCLECOUNT_ONLINE_G2P_W2P' and entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND entry_time < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND se.project_code = 'FH-B2021-C100'
	GROUP BY DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00'),se.station_code

UNION ALL

SELECT 
   tmp1.ids times,
   0 'order_linenum',
	 tmp1.station_code,
   0 'sku_num',
	 0 'into_station_times',
   SUM(		
			CASE WHEN tmp1.begin_to_exit_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_lineBegin_time
					 WHEN tmp1.begin_to_exit_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_exit_time and tmp1.begin_to_exit_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_exit_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_exit_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_exit_time and tmp1.begin_to_exit_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_exit_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_exit_time then tmp1.begin_to_lineBegin_time
				 	 ELSE 0 END
					-
			CASE WHEN tmp1.begin_to_entry_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_lineBegin_time
					 WHEN tmp1.begin_to_entry_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_entry_time and tmp1.begin_to_entry_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_entry_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_entry_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_entry_time and tmp1.begin_to_entry_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_entry_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_entry_time then tmp1.begin_to_lineBegin_time
				  ELSE 0 END
			) station_used,
    0 'station_busy',
    0 'time'
	FROM (
	  SELECT 
		tmp_line.ids,
		seq.station_code,
    TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),seq.entry_time)) 'begin_to_entry_time',
		TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(seq.exit_time is null,DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),if(seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),seq.exit_time,DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))) 'begin_to_exit_time',
		TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),tmp_line.ids) 'begin_to_lineBegin_time',
		TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(tmp_line.ids,INTERVAL 1 HOUR)) 'begin_to_lineEnd_time'
    FROM (
SELECT a.station_code,
       IF(a.hjk = 1,a.entry_time1,MIN(a.entry_time2)) as entry_time,
       IF(a.hjk = 1,a.exit_time1,MAX(a.exit_time2)) as exit_time
       
FROM
(
SELECT tt1.station_code1 as station_code,tt1.rn1,tt1.entry_time1,tt1.exit_time1,tt1.rn2,tt1.entry_time2,tt1.exit_time2,tt1.cf1,tt2.cf2,
       case when tt1.rn1 = 1 AND tt1.cf1 = 1 then 1
            when tt1.cf1 <= tt2.cf2 AND tt1.cf1 = 0 then @i:=@i 
            when tt1.cf1 != tt2.cf2 then @i := @i + 1 + tt2.cf2 
            end as hjk
FROM
(
SELECT *,IF(t2.entry_time2 >= t1.entry_time1 AND t2.entry_time2 < t1.exit_time1,0,@t1:=@t1+1) as cf1
FROM
(
SELECT (@rn1:=@rn1+1) as rn1,se.station_code as station_code1,se.entry_time as entry_time1,se.exit_time as exit_time1
FROM evo_station.station_entry se,(SELECT @rn1:=0)rn
WHERE se.biz_type = 'CYCLECOUNT_ONLINE_G2P_W2P' AND se.project_code = 'FH-B2021-C100' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t1
JOIN
(
SELECT (@rn2:=@rn2+1) as rn2,se.station_code as station_code2,se.entry_time as entry_time2,se.exit_time as exit_time2
FROM evo_station.station_entry se,(SELECT @rn2:=0)rn
WHERE se.biz_type = 'CYCLECOUNT_ONLINE_G2P_W2P' AND se.project_code = 'FH-B2021-C100' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t2
ON t1.station_code1 = t2.station_code2 AND t1.rn1 = t2.rn2 - 1
JOIN
(SELECT @t1:=0)tmp1
)tt1
JOIN
(
SELECT *,IF(t2.entry_time2 >= t1.entry_time1 AND t2.entry_time2 < t1.exit_time1,0,@t2:=@t2+1) as cf2
FROM
(
SELECT (@rn3:=@rn3+1) as rn1,se.station_code as station_code1,se.entry_time as entry_time1,se.exit_time as exit_time1
FROM evo_station.station_entry se,(SELECT @rn3:=0)rn
WHERE se.biz_type = 'CYCLECOUNT_ONLINE_G2P_W2P' AND se.project_code = 'FH-B2021-C100' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t1
JOIN
(
SELECT (@rn4:=@rn4+1) as rn2,se.station_code as station_code2,se.entry_time as entry_time2,se.exit_time as exit_time2
FROM evo_station.station_entry se,(SELECT @rn4:=0)rn
WHERE se.biz_type = 'CYCLECOUNT_ONLINE_G2P_W2P' AND se.project_code = 'FH-B2021-C100' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t2
ON t1.station_code1 = t2.station_code2 AND t1.rn1 = t2.rn2 - 1
JOIN
(SELECT @t2:=0)tmp1
)tt2
ON tt1.station_code1 = tt2.station_code1 AND tt1.rn1 = tt2.rn1 - 1
JOIN
(SELECT @i:=0)tmp
)a
GROUP BY a.station_code,a.hjk
ORDER BY a.station_code,`entry_time`
)seq,
	    (SELECT @t:=DATE_ADD(@t,INTERVAL 1 HOUR) as ids
       FROM information_schema.COLUMNS,(select @t:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @t < DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp_line
) tmp1
GROUP BY tmp1.ids,tmp1.station_code

UNION ALL

SELECT 
   tt1.ida times,
   0 'order_linenum',
	 tt1.station_code,
   0 'sku_num',
	 0 'into_station_times',
   0 'station_used',
     SUM(		
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
			) station_busy,
      0 'time' 
    FROM (
	  SELECT 
		tmp_line.ida,
		sl.station_code,
    TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(sl.login_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),sl.login_time)) 'begin_to_entry_time',
		TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(sl.logout_time is null,DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),if(sl.logout_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),sl.logout_time,DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))) 'begin_to_exit_time',
		TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),tmp_line.ida) 'begin_to_lineBegin_time',
		TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(tmp_line.ida,INTERVAL 1 HOUR)) 'begin_to_lineEnd_time'
    FROM evo_station.station_login sl,
	     (SELECT @r:=DATE_ADD(@r,INTERVAL 1 HOUR) as ida
       FROM information_schema.COLUMNS,(select @r:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @r < DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp_line
    WHERE 
       ((sl.login_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and sl.login_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
       OR (sl.logout_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and sl.logout_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
		   OR (sl.login_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and sl.logout_time >= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))
       AND sl.biz_type = 'CYCLECOUNT_ONLINE_G2P_W2P' AND sl.project_code = 'FH-B2021-C100' 
        ) tt1
    GROUP BY tt1.ida,tt1.station_code

UNION ALL

SELECT 
   DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00') times,
   0 'order_linenum',
	 cj.station_code,
   0 'sku_num',
	 0 'into_station_times',
   0 'station_used',
   0 'station_busy',
   SUM(TIMESTAMPDIFF(SECOND,tmp.last_updated_date,jsc.updated_date))/COUNT(cj.job_id) time
FROM evo_wcs_g2p.job_state_change jsc
LEFT JOIN evo_wcs_g2p.countcheck_job cj
ON jsc.job_id = cj.job_id
LEFT JOIN
(
SELECT cj.job_id,t.station_code,sc.last_updated_date
FROM evo_station.station_task_state_change sc
LEFT JOIN evo_station.station_task t
ON sc.station_task_id = t.id
LEFT JOIN evo_wcs_g2p.countcheck_job cj
ON t.task_no = cj.job_id
WHERE sc.state = 'PULLED' AND sc.last_updated_date BETWEEN DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' AND cj.project_code = 'FH-B2021-C100' 
)tmp
ON jsc.job_id = tmp.job_id
WHERE jsc.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND jsc.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND jsc.state = 'DONE' AND jsc.project_code = 'FH-B2021-C100'  AND cj.project_code = 'FH-B2021-C100' 
GROUP BY DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00'),cj.station_code
) tmp2 
GROUP BY tmp2.times,tmp2.station_code;

-- ########################################################################
/*
* 统计-推荐上架订单工作站分时效率指标
*/

SELECT  
     tmp2.times as '时间段', -- 时间段
     tmp2.station_code as '工作站', -- 工作站
     SUM(tmp2.into_station_times) as '进站次数', -- 进站次数
     SUM(tmp2.order_linenum) as '完成订单行数', -- 完成订单行数
     SUM(tmp2.sku_num) as '完成货品件数', -- 完成货品件数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.sku_num)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成货品件数', -- 单次进站完成货品件数
	   cast((if(SUM(tmp2.into_station_times)!=0,ifnull(SUM(tmp2.order_linenum)/SUM(tmp2.into_station_times),0),0))as decimal(10,2)) as '单次进站完成订单行数', -- 单次进站完成订单行数
     cast(SUM(tmp2.station_used)/3600 as decimal(10,2)) as '工作站利用率', -- 工作站利用率
     cast((if(SUM(tmp2.station_busy)!=0,ifnull(SUM(tmp2.station_used)/SUM(tmp2.station_busy),0),0)) as decimal(10,2)) as '工作站繁忙率', -- 工作站繁忙率
     cast(SUM(tmp2.station_busy)/3600 as decimal(10,2)) as '工作站在线率', -- 工作站在线率
     cast(SUM(tmp2.time) as decimal(10,2)) as '平均人工上架耗时/秒' -- '平均人工上架耗时/秒'
FROM (
SELECT DATE_FORMAT(rod.last_updated_date,'%Y-%m-%d %H:00:00') times,
     0 'order_linenum',
     w.station_code, 
     sum(rod.fulfill_quantity) sku_num,
	   0 'into_station_times', 
     0 'station_used',
     0 'station_busy',
     0 'time'
FROM evo_wes_replenish.replenish_order_detail rod
LEFT JOIN evo_wes_replenish.replenish_work w
ON rod.replenish_order_id = w.source_order_id 
WHERE rod.quantity = rod.fulfill_quantity AND rod.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and rod.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND w.replenish_mode LIKE '%GUIDED%' AND rod.project_code = 'FH-B2021-C100' AND w.project_code = 'FH-B2021-C100' 
group BY DATE_FORMAT(rod.last_updated_date,'%Y-%m-%d %H:00:00'),w.station_code

UNION ALL

SELECT DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00') times,
       count(distinct pwd.id) order_linenum,
       j.station_code,
       0 'sku_num', 
       0 'into_station_times',
       0 'station_used',
       0 'station_busy',
       0 'time'
    FROM evo_wcs_g2p.w2p_guided_putaway_work_detail pwd
    LEFT JOIN evo_wcs_g2p.w2p_guided_put_away_job  j
    ON pwd.detail_id = j.detail_id
	WHERE j.state= 'DONE' AND pwd.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and pwd.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND pwd.project_code = 'FH-B2021-C100' AND j.project_code = 'FH-B2021-C100'
	GROUP BY DATE_FORMAT(pwd.updated_date,'%Y-%m-%d %H:00:00'),j.station_code

UNION ALL

SELECT DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00') times,
     0 'order_linenum',
     se.station_code,
     0 'sku_num',
     count(se.id) into_station_times,
     0 'station_used',
     0 'station_busy',
     0 'time'
    FROM evo_station.station_entry se
	WHERE se.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' and se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND se.project_code = 'FH-B2021-C100'
	GROUP BY DATE_FORMAT(se.entry_time,'%Y-%m-%d %H:00:00'),se.station_code

UNION ALL

SELECT 
   tmp1.ids times,
   0 'order_linenum',
	 tmp1.station_code,
   0 'sku_num',
	 0 'into_station_times',
   SUM(		
			CASE WHEN tmp1.begin_to_exit_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_lineBegin_time
					 WHEN tmp1.begin_to_exit_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_exit_time and tmp1.begin_to_exit_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_exit_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_exit_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_exit_time and tmp1.begin_to_exit_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_exit_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_exit_time then tmp1.begin_to_lineBegin_time
				 	 ELSE 0 END
					-
			CASE WHEN tmp1.begin_to_entry_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_lineBegin_time
					 WHEN tmp1.begin_to_entry_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_entry_time and tmp1.begin_to_entry_time <= tmp1.begin_to_lineEnd_time then tmp1.begin_to_entry_time
					 WHEN tmp1.begin_to_lineBegin_time <= tmp1.begin_to_lineEnd_time and tmp1.begin_to_lineEnd_time <= tmp1.begin_to_entry_time then tmp1.begin_to_lineEnd_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_entry_time and tmp1.begin_to_entry_time <= tmp1.begin_to_lineBegin_time then tmp1.begin_to_entry_time
					 WHEN tmp1.begin_to_lineEnd_time <= tmp1.begin_to_lineBegin_time and tmp1.begin_to_lineBegin_time <= tmp1.begin_to_entry_time then tmp1.begin_to_lineBegin_time
				  ELSE 0 END
			) station_used,
    0 'station_busy',
    0 'time'
	FROM (
	  SELECT 
		tmp_line.ids,
		seq.station_code,
    TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(seq.entry_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),seq.entry_time)) 'begin_to_entry_time',
		TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(seq.exit_time is null,DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),if(seq.exit_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),seq.exit_time,DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))) 'begin_to_exit_time',
		TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),tmp_line.ids) 'begin_to_lineBegin_time',
		TIMESTAMPDIFF(SECOND,DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(tmp_line.ids,INTERVAL 1 HOUR)) 'begin_to_lineEnd_time'
    FROM (
SELECT a.station_code,
       IF(a.hjk = 1,a.entry_time1,MIN(a.entry_time2)) as entry_time,
       IF(a.hjk = 1,a.exit_time1,MAX(a.exit_time2)) as exit_time
       
FROM
(
SELECT tt1.station_code1 as station_code,tt1.rn1,tt1.entry_time1,tt1.exit_time1,tt1.rn2,tt1.entry_time2,tt1.exit_time2,tt1.cf1,tt2.cf2,
       case when tt1.rn1 = 1 AND tt1.cf1 = 1 then 1
            when tt1.cf1 <= tt2.cf2 AND tt1.cf1 = 0 then @i:=@i 
            when tt1.cf1 != tt2.cf2 then @i := @i + 1 + tt2.cf2 
            end as hjk
FROM
(
SELECT *,IF(t2.entry_time2 >= t1.entry_time1 AND t2.entry_time2 < t1.exit_time1,0,@t1:=@t1+1) as cf1
FROM
(
SELECT (@rn1:=@rn1+1) as rn1,se.station_code as station_code1,se.entry_time as entry_time1,se.exit_time as exit_time1
FROM evo_station.station_entry se,(SELECT @rn1:=0)rn
WHERE se.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' AND se.project_code = 'FH-B2021-C100' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t1
JOIN
(
SELECT (@rn2:=@rn2+1) as rn2,se.station_code as station_code2,se.entry_time as entry_time2,se.exit_time as exit_time2
FROM evo_station.station_entry se,(SELECT @rn2:=0)rn
WHERE se.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' AND se.project_code = 'FH-B2021-C100' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t2
ON t1.station_code1 = t2.station_code2 AND t1.rn1 = t2.rn2 - 1
JOIN
(SELECT @t1:=0)tmp1
)tt1
JOIN
(
SELECT *,IF(t2.entry_time2 >= t1.entry_time1 AND t2.entry_time2 < t1.exit_time1,0,@t2:=@t2+1) as cf2
FROM
(
SELECT (@rn3:=@rn3+1) as rn1,se.station_code as station_code1,se.entry_time as entry_time1,se.exit_time as exit_time1
FROM evo_station.station_entry se,(SELECT @rn3:=0)rn
WHERE se.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' AND se.project_code = 'FH-B2021-C100' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t1
JOIN
(
SELECT (@rn4:=@rn4+1) as rn2,se.station_code as station_code2,se.entry_time as entry_time2,se.exit_time as exit_time2
FROM evo_station.station_entry se,(SELECT @rn4:=0)rn
WHERE se.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' AND se.project_code = 'FH-B2021-C100' AND se.entry_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.entry_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')
ORDER BY se.station_code,se.entry_time
)t2
ON t1.station_code1 = t2.station_code2 AND t1.rn1 = t2.rn2 - 1
JOIN
(SELECT @t2:=0)tmp1
)tt2
ON tt1.station_code1 = tt2.station_code1 AND tt1.rn1 = tt2.rn1 - 1
JOIN
(SELECT @i:=0)tmp
)a
GROUP BY a.station_code,a.hjk
ORDER BY a.station_code,`entry_time`
)seq,
	    (SELECT @t:=DATE_ADD(@t,INTERVAL 1 HOUR) as ids
       FROM information_schema.COLUMNS,(select @t:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @t < DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp_line
) tmp1
GROUP BY tmp1.ids,tmp1.station_code

UNION ALL

SELECT 
   tt1.ida times,
   0 'order_linenum',
	 tt1.station_code,
   0 'sku_num',
	 0 'into_station_times',
   0 'station_used',
     SUM(		
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
			) station_busy,
      0 'time' 
    FROM (
	  SELECT 
		tmp_line.ida,
		sl.station_code,
    TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(sl.login_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),sl.login_time)) 'begin_to_entry_time',
		TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),if(sl.logout_time is null,DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),if(sl.logout_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),sl.logout_time,DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))) 'begin_to_exit_time',
		TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),tmp_line.ida) 'begin_to_lineBegin_time',
		TIMESTAMPDIFF(SECOND, DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),DATE_ADD(tmp_line.ida,INTERVAL 1 HOUR)) 'begin_to_lineEnd_time'
    FROM evo_station.station_login sl,
	     (SELECT @r:=DATE_ADD(@r,INTERVAL 1 HOUR) as ida
       FROM information_schema.COLUMNS,(select @r:= DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL -1 HOUR)) tmp 
       WHERE @r < DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 HOUR)) tmp_line
    WHERE 
       ((sl.login_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and sl.login_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
       OR (sl.logout_time >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and sl.logout_time <= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'))
		   OR (sl.login_time <= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) and sl.logout_time >= DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00')))
       AND sl.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' AND sl.project_code = 'FH-B2021-C100' 
        ) tt1
    GROUP BY tt1.ida,tt1.station_code

UNION ALL

SELECT 
   DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00') times,
   0 'order_linenum',
	 pj.station_code,
   0 'sku_num',
	 0 'into_station_times',
   0 'station_used',
   0 'station_busy',
   SUM(TIMESTAMPDIFF(SECOND,tmp.last_updated_date,jsc.updated_date))/COUNT(pj.job_id) time
FROM evo_wcs_g2p.job_state_change jsc
LEFT JOIN evo_wcs_g2p.w2p_guided_put_away_job pj
ON jsc.job_id = pj.job_id
LEFT JOIN
(
SELECT pj.job_id,t.station_code,sc.last_updated_date
FROM evo_station.station_task_state_change sc
LEFT JOIN evo_station.station_task t
ON sc.station_task_id = t.id
LEFT JOIN evo_wcs_g2p.w2p_guided_put_away_job pj
ON t.task_no = pj.job_id
WHERE sc.state = 'PULLED' AND sc.last_updated_date BETWEEN DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND sc.project_code = 'FH-B2021-C100' AND t.project_code = 'FH-B2021-C100' AND pj.project_code = 'FH-B2021-C100' 
)tmp
ON jsc.job_id = tmp.job_id
WHERE jsc.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND jsc.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND jsc.state = 'DONE' AND jsc.project_code = 'FH-B2021-C100'  AND pj.project_code = 'FH-B2021-C100' 
GROUP BY DATE_FORMAT(jsc.updated_date,'%Y-%m-%d %H:00:00'),pj.station_code
) tmp2 
GROUP BY tmp2.times,tmp2.station_code;