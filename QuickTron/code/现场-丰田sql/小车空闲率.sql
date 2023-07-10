SET @begin_time = '2021-11-25 00:00:00'; -- 开始时间
SET @line_num = 24; -- 默认24小时的时间段
SET @interval_time = 60; -- 间隔时间 单位：分钟

SELECT a.theDayStartofhour,a.agv_code,a.free_rate
FROM
(
SELECT tt.theDayStartofhour,
       tt.agv_code,
       CONCAT(CAST((3600-(IFNULL(tt.using_time,0)+IFNULL(tt1.using_time,0)))/36 AS DECIMAL(10,2)),'%') as free_rate -- 空闲率
FROM
(
SELECT tmp.theDayStartofhour,
       tmp1.agv_code,
       SUM(CASE WHEN tmp1.updated_date >= tmp.theDayStartofhour AND tmp2.updated_date <= tmp.theDayEndofhour THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp2.updated_date) 
                WHEN tmp1.updated_date >= tmp.theDayStartofhour AND tmp1.updated_date <= tmp.theDayEndofhour AND tmp2.updated_date > tmp.theDayEndofhour THEN TIMESTAMPDIFF(SECOND,tmp1.updated_date,tmp.theDayEndofhour) 
                WHEN tmp1.updated_date < tmp.theDayStartofhour AND tmp2.updated_date >= tmp.theDayStartofhour AND tmp2.updated_date <= tmp.theDayEndofhour THEN TIMESTAMPDIFF(SECOND,tmp.theDayStartofhour,tmp2.updated_date) 
                WHEN tmp1.updated_date < tmp.theDayStartofhour AND tmp2.updated_date > tmp.theDayEndofhour THEN TIMESTAMPDIFF(SECOND,tmp.theDayStartofhour,tmp.theDayEndofhour)
                ELSE 0 END) as using_time -- 小车搬运货架+到站拣货+还货架
FROM 
(
SELECT @i:=DATE_ADD(@i,INTERVAL 1 HOUR) as theDayStartofhour,DATE_ADD(@i,INTERVAL 3599 SECOND) as theDayEndofhour
FROM information_schema.COLUMNS,(select @i:= DATE_ADD(@begin_time,INTERVAL -1 HOUR)) tmp 
WHERE @i < DATE_ADD(DATE_ADD(@begin_time,INTERVAL 1 DAY),INTERVAL -1 HOUR)  
)tmp
JOIN
(
SELECT c.agv_code,IF(g.group_job_id is null,c.job_id,g.group_job_id) as job_id,MIN(c.updated_date) as updated_date
FROM evo_wcs_g2p.job_state_change c
LEFT JOIN evo_wcs_g2p.station_task_group g
ON c.job_id = g.job_id
LEFT JOIN evo_wcs_g2p.picking_job pj
ON c.job_id = pj.job_id
LEFT JOIN evo_wcs_g2p.guided_put_away_job aj
ON c.job_id = aj.job_id
LEFT JOIN evo_wcs_g2p.countcheck_job cj
ON c.job_id = cj.job_id
WHERE c.state = 'INIT_JOB' AND c.job_id LIKE '%G2P%' AND c.updated_date >= @begin_time AND c.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE) 
GROUP BY pj.bucket_move_job_id,aj.bucket_move_job_id,cj.bucket_move_job_id
)tmp1
LEFT JOIN
(
SELECT c.agv_code,IF(g.group_job_id is null,c.job_id,g.group_job_id) as job_id,MAX(c.updated_date) as updated_date
FROM evo_wcs_g2p.job_state_change c
LEFT JOIN evo_wcs_g2p.station_task_group g
ON c.job_id = g.job_id
LEFT JOIN evo_wcs_g2p.picking_job pj
ON c.job_id = pj.job_id
LEFT JOIN evo_wcs_g2p.guided_put_away_job aj
ON c.job_id = aj.job_id
LEFT JOIN evo_wcs_g2p.countcheck_job cj
ON c.job_id = cj.job_id
WHERE c.state = 'DONE' AND c.job_id LIKE '%G2P%' AND c.updated_date >= @begin_time AND c.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)
GROUP BY pj.bucket_move_job_id,aj.bucket_move_job_id,cj.bucket_move_job_id
)tmp2
ON tmp1.job_id =tmp2.job_id
WHERE ((tmp1.updated_date >= @begin_time AND tmp1.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE))
     OR(tmp1.updated_date < @begin_time AND tmp2.updated_date > @begin_time)
     OR(tmp1.updated_date < @begin_time AND tmp2.updated_date >= @begin_time AND tmp2.updated_date < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)))
     AND tmp2.job_id is not NULL 
GROUP BY tmp.theDayStartofhour,tmp1.agv_code
)tt
LEFT JOIN
(
SELECT tmp.theDayStartofhour,
       tmp1.agv_code,
       SUM(CASE WHEN tmp1.job_execute_time >= tmp.theDayStartofhour AND tmp2.job_finish_time <= tmp.theDayEndofhour THEN TIMESTAMPDIFF(SECOND,tmp1.job_execute_time,tmp2.job_finish_time) 
                WHEN tmp1.job_execute_time >= tmp.theDayStartofhour AND tmp1.job_execute_time <= tmp.theDayEndofhour AND tmp2.job_finish_time > tmp.theDayEndofhour THEN TIMESTAMPDIFF(SECOND,tmp1.job_execute_time,tmp.theDayEndofhour) 
                WHEN tmp1.job_execute_time < tmp.theDayStartofhour AND tmp2.job_finish_time >= tmp.theDayStartofhour AND tmp2.job_finish_time <= tmp.theDayEndofhour THEN TIMESTAMPDIFF(SECOND,tmp.theDayStartofhour,tmp2.job_finish_time) 
                WHEN tmp1.job_execute_time < tmp.theDayStartofhour AND tmp2.job_finish_time > tmp.theDayEndofhour THEN TIMESTAMPDIFF(SECOND,tmp.theDayStartofhour,tmp.theDayEndofhour)
                ELSE 0 END) as using_time -- 小车充电
FROM 
(
SELECT @r:=DATE_ADD(@r,INTERVAL 1 HOUR) as theDayStartofhour,DATE_ADD(@r,INTERVAL 3599 SECOND) as theDayEndofhour
FROM information_schema.COLUMNS,(select @r:= DATE_ADD(@begin_time,INTERVAL -1 HOUR)) tmp 
WHERE @r < DATE_ADD(DATE_ADD(@begin_time,INTERVAL 1 DAY),INTERVAL -1 HOUR)  
)tmp
JOIN
(
SELECT j.agv_code,j.job_id,j.job_execute_time
FROM evo_rcs.agv_job_history j
WHERE j.job_state = 'JOB_COMPLETED' AND j.job_type = 'CHARGE_JOB' AND j.job_execute_time >= @begin_time AND j.job_execute_time < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)

)tmp1
LEFT JOIN
(
SELECT j.agv_code,j.job_id,j.job_finish_time
FROM evo_rcs.agv_job_history j
WHERE j.job_state = 'JOB_COMPLETED' AND j.job_type = 'CHARGE_JOB' AND j.job_execute_time >= @begin_time AND j.job_execute_time < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)
)tmp2
ON tmp1.job_id =tmp2.job_id
WHERE ((tmp1.job_execute_time >= @begin_time AND tmp1.job_execute_time < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE))
     OR(tmp1.job_execute_time < @begin_time AND tmp2.job_finish_time > @begin_time)
     OR(tmp1.job_execute_time < @begin_time AND tmp2.job_finish_time >= @begin_time AND tmp2.job_finish_time < DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)))
GROUP BY tmp.theDayStartofhour,tmp1.agv_code
)tt1
ON tt.theDayStartofhour = tt1.theDayStartofhour AND tt.agv_code = tt1.agv_code
GROUP BY tt.theDayStartofhour,tt.agv_code
)a
ORDER BY a.theDayStartofhour ASC,a.agv_code DESC