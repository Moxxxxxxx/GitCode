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
WHERE se.biz_type = 'PUTAWAY_ONLINE_G2P_GUIDED_W2P' AND se.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND se.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND se.project_code = 'C35052'
GROUP BY se.station_code,DATE_FORMAT(se.last_updated_date,'%Y-%m-%d %H:00:00')
)tmp1
LEFT JOIN
(
select DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00') times,
       cmj.station_code, -- '工作站编码'
       count(cmj.target_way_point_code) as 'num' -- '单工作站双作业点位完成箱数'
FROM evo_wcs_g2p.container_move_job_v2 cmj
WHERE cmj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND cmj.updated_date <DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cmj.biz_type = 'W2P_ONLINE_PUTAWAY_GUIDED' AND cmj.state='DONE' AND cmj.project_code = 'C35052'
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
WHERE cmj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND cmj.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cmj.biz_type = 'W2P_ONLINE_PUTAWAY_GUIDED' AND cmj.state='DONE' AND cmj.project_code = 'C35052'
GROUP BY cmj.station_code,cmj.target_way_point_code,DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00')
)tmp
GROUP BY tmp.station_code,DATE_FORMAT(tmp.updated_date,'%Y-%m-%d %H:00:00')
)tmp3
on tmp1.station_code = tmp3.station_code and tmp1.times = tmp3.times
LEFT JOIN
(
SELECT DATE_FORMAT(wpj.updated_date,'%Y-%m-%d %H:00:00') times,wpj.station_code,CAST((SUM(TIMESTAMPDIFF(SECOND,wpj.created_date,wpj.updated_date))/COUNT(wpj.job_id)) AS DECIMAL(10,2)) as 'time' -- '单次任务耗时/秒'
FROM evo_wcs_g2p.w2p_guided_put_away_job wpj
WHERE wpj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND wpj.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND wpj.state='DONE' AND wpj.project_code = 'C35052'
GROUP BY wpj.station_code,DATE_FORMAT(wpj.updated_date,'%Y-%m-%d %H:00:00')
)tmp4
on tmp1.station_code = tmp4.station_code and tmp1.times = tmp4.times
LEFT JOIN
(
SELECT DATE_FORMAT(cmj.updated_date,'%Y-%m-%d %H:00:00') times,cmj.station_code,CAST(SUM(TIMESTAMPDIFF(SECOND,jsc.updated_date,cmj.updated_date))/COUNT(cmj.job_id) AS DECIMAL(10,2)) as 'time' -- '平均入站耗时/秒'
FROM evo_wcs_g2p.container_move_job_v2 cmj
JOIN evo_wcs_g2p.job_state_change jsc
ON cmj.job_id = jsc.job_id
WHERE cmj.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND cmj.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND cmj.biz_type = 'W2P_ONLINE_PUTAWAY_GUIDED' AND cmj.state='DONE' AND jsc.state = 'INIT_JOB' AND cmj.project_code = 'C35052' AND jsc.project_code = 'C35052'
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
WHERE sc.state = 'PULLED' AND sc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND sc.last_updated_date < DATE_ADD(DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY),INTERVAL 1 DAY) AND sc.project_code = 'C35052' AND t.project_code = 'C35052' AND wpj.project_code = 'C35052'
)tmp
ON jsc.job_id = tmp.job_id
WHERE jsc.updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND jsc.updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND jsc.state = 'DONE' AND jsc.project_code = 'C35052' AND wpj.project_code = 'C35052'
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
WHERE sc.state = 'DONE' AND sc.project_code = 'C35052' AND t.project_code = 'C35052' AND wpj.project_code = 'C35052'
ORDER BY sc.last_updated_date
)tmp
ON sc.station_task_id = tmp.station_task_id 
WHERE sc.state = 'NEW' AND sc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND sc.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND sc.project_code = 'C35052' AND t.project_code = 'C35052' 
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
WHERE sc.state = 'DONE' AND sc.project_code = 'C35052' AND t.project_code = 'C35052' AND wpj.project_code = 'C35052'
ORDER BY sc.last_updated_date
)tmp
ON sc.station_task_id = tmp.station_task_id 
WHERE sc.state = 'NEW' AND sc.last_updated_date >= DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) AND sc.last_updated_date < DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00') AND sc.project_code = 'C35052' AND t.project_code = 'C35052' 
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
  AND seq.biz_type LIKE '%GUIDED%' AND seq.project_code = 'C35052' 
GROUP BY tmp_line.ids,seq.station_code
)tt
GROUP BY tt.times,tt.station_code