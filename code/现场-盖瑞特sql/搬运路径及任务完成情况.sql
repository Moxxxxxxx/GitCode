SET @begin_time = '2021-08-23 00:00:00';
SET @end_time = '2021-08-24 00:00:00';

SELECT CONCAT( DATE_FORMAT(tmp1.time,'%Y-%m-%d %H:00:00') ,'-',DATE_ADD( DATE_FORMAT(tmp1.time,'%Y-%m-%d %H:00:00'),INTERVAL 1 HOUR)) AS '时间段',
       SUM(tmp1.total_num)AS '任务总数量',
       SUM(tmp2.done_num)AS '任务完成数量',
       CAST(SUM(tmp2.done_num)/SUM(tmp1.total_num) as decimal(10,2))AS '任务完成率',
       CONCAT(tmp3.start_point,' - ',tmp3.target_point) AS '搬运路线',
       COUNT(CONCAT(tmp3.start_point,' - ',tmp3.target_point)) AS '搬运次数',
       SUM(TIMESTAMPDIFF(SECOND, tmp3.created_date,tmp3.updated_date))/COUNT(CONCAT(tmp3.start_point,' - ',tmp3.target_point)) AS '平均搬运时间/s',
       MAX(TIMESTAMPDIFF(SECOND, tmp3.created_date,tmp3.updated_date)) AS '最长搬运时间/s',
       MIN(TIMESTAMPDIFF(SECOND, tmp3.created_date,tmp3.updated_date)) AS '最短搬运时间/s'  
FROM
(
SELECT DATE_FORMAT(j.updated_date,'%Y-%m-%d %H:00:00') as time,j.job_id,count(j.id) as total_num
FROM evo_rcs.robot_job_history j
WHERE j.updated_date between @begin_time and @end_time and j.job_type = 'TRAY_MOVE'
GROUP BY j.job_id, DATE_FORMAT(j.updated_date,'%Y-%m-%d %H:00:00') 
)tmp1
left join
(
SELECT j.job_id,count(j.id) as done_num
FROM evo_rcs.robot_job_history j
WHERE j.state = 'DONE' and j.job_type = 'TRAY_MOVE' and updated_date between @begin_time and @end_time
GROUP BY j.job_id
)tmp2
ON tmp1.job_id = tmp2.job_id
left join
(
SELECT j.job_id,
       j.start_point,
	   j.target_point,
	   j.created_date,
	   j.updated_date
FROM evo_rcs.robot_job_history j
WHERE updated_date between @begin_time and @end_time
)tmp3
ON tmp1.job_id = tmp3.job_id
GROUP BY CONCAT(tmp3.start_point,' - ',tmp3.target_point),CONCAT(DATE_FORMAT(tmp1.time,'%Y-%m-%d %H:00:00') ,'-',DATE_ADD( DATE_FORMAT(tmp1.time,'%Y-%m-%d %H:00:00'),INTERVAL 1 HOUR)) 