SET @interval_time = 60; -- 间隔时间 单位：分钟
SET @line_num = 3; -- 时间段
SET @begin_time = '2021-07-30 18:00:00'; -- 开始时间
SELECT
CONCAT(@begin_time,' - ',DATE_ADD(@begin_time,INTERVAL @interval_time*@line_num MINUTE)) AS '起止时间',
TIMESTAMPDIFF(SECOND, min(created_date),max(updated_date)) AS '仿真运行时间/s'
FROM bucket_robot_job
WHERE created_date >= @begin_time 
AND created_date < DATE_ADD(@begin_time ,INTERVAL @interval_time*@line_num MINUTE) 
AND state ='DONE'