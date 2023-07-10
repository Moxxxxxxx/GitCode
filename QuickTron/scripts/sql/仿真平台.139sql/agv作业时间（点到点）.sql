SET @begin_time = '2021-08-06 12:00:00'; -- 开始时间
SET @line_num = 6; -- 默认6小时的时间段
SET @interval_time = 60; -- 间隔时间 单位：分钟
SELECT CONCAT(j.start_point,' - ',j.target_point) AS '搬运路线',
       COUNT(CONCAT(j.start_point,' - ',j.target_point)) AS '搬运次数',
       SUM(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date))/COUNT(CONCAT(j.start_point,' - ',j.target_point)) AS '平均搬运时间/s',
       MAX(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最长搬运时间/s',
       MIN(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最短搬运时间/s'    
FROM evo_wcs_g2p.bucket_robot_job j
GROUP BY CONCAT(j.start_point,' - ',j.target_point)