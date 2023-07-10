/****** Script for SelectTopNRows command from SSMS  ******/
declare  @begin_time datetime 
declare @end_time datetime 
SET @begin_time = '2021-08-20 00:00:00';
SET @end_time = '2021-08-21 00:00:00';
SELECT CONCAT(dateadd(hour,datepart(hour, tmp1.time),convert(varchar(10), tmp1.time,112)),'-',dateadd(HOUR,1,dateadd(hour,datepart(hour, tmp1.time),convert(varchar(10), tmp1.time,112)))) AS '时间段',
       SUM(tmp1.total_num)AS '任务总数量',
       SUM(tmp2.done_num)AS '任务完成数量',
       CAST(SUM(tmp2.done_num)/SUM(tmp1.total_num) as decimal(10,2))AS '任务完成率',
       CONCAT(tmp3.source_waypoint_code,' - ',tmp3.target_waypoint_code) AS '搬运路线',
       COUNT(CONCAT(tmp3.source_waypoint_code,' - ',tmp3.target_waypoint_code)) AS '搬运次数',
       SUM(DATEDIFF(SECOND, tmp3.created_date,tmp3.updated_date))/COUNT(CONCAT(tmp3.source_waypoint_code,' - ',tmp3.target_waypoint_code)) AS '平均搬运时间/s',
       MAX(DATEDIFF(SECOND, tmp3.created_date,tmp3.updated_date)) AS '最长搬运时间/s',
       MIN(DATEDIFF(SECOND, tmp3.created_date,tmp3.updated_date)) AS '最短搬运时间/s'  
FROM
(
SELECT dateadd(hour,datepart(hour, j.updated_date),convert(varchar(10), j.updated_date,112)) as time,j.job_id,count(j.id) as total_num
FROM [evo_wcs_g2p].[dbo].[bucket_move_job] j
WHERE j.updated_date between @begin_time and @end_time
GROUP BY j.job_id, dateadd(hour,datepart(hour, j.updated_date),convert(varchar(10), j.updated_date,112))
)tmp1
left join
(
SELECT j.job_id,count(j.id) as done_num
FROM [evo_wcs_g2p].[dbo].[bucket_move_job] j
WHERE state = 'DONE' and updated_date between @begin_time and @end_time
GROUP BY j.job_id
)tmp2
ON tmp1.job_id = tmp2.job_id
left join
(
SELECT j.job_id,
       j.source_waypoint_code,
	   j.target_waypoint_code,
	   j.created_date,
	   j.updated_date
FROM [evo_wcs_g2p].[dbo].[bucket_move_job] j
WHERE updated_date between @begin_time and @end_time
)tmp3
ON tmp1.job_id = tmp3.job_id
GROUP BY CONCAT(tmp3.source_waypoint_code,' - ',tmp3.target_waypoint_code),CONCAT(dateadd(hour,datepart(hour, tmp1.time),convert(varchar(10), tmp1.time,112)),'-',dateadd(HOUR,1,dateadd(hour,datepart(hour, tmp1.time),convert(varchar(10), tmp1.time,112)))) 