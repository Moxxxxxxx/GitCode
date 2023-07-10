SET @begin_time = '2021-07-30 18:00:00'; -- 开始时间
SET @line_num = 6; -- 默认6小时的时间段
SET @interval_time = 60; -- 间隔时间 单位：分钟
SELECT CONCAT(tmp.区域编码,' - ',tmp1.区域编码) AS '搬运路线',
       COUNT(CONCAT(tmp.区域编码,' - ',tmp1.区域编码)) AS '搬运次数',
       SUM(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date))/COUNT(CONCAT(tmp.区域编码,' - ',tmp1.区域编码)) AS '平均搬运时间/s',
       MAX(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最长搬运时间/s',
       MIN(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最短搬运时间/s'    
FROM
evo_wcs_g2p.bucket_robot_job j 
JOIN
(
SELECT DISTINCT j.start_point AS 'job码值',
                a.area_code AS '区域编码'
 FROM evo_wcs_g2p.bucket_robot_job j 
 JOIN evo_rcs.basic_area a 
 WHERE INSTR(a.point_code,j.start_point)>0
)tmp
ON j.start_point = tmp.job码值
JOIN
(
SELECT DISTINCT j.target_point AS 'job码值',
                a.area_code AS '区域编码'
 FROM evo_wcs_g2p.bucket_robot_job j 
 JOIN evo_rcs.basic_area a 
 WHERE INSTR(a.point_code,j.target_point)>0
)tmp1
ON j.target_point = tmp1.job码值
WHERE j.state = 'DONE' AND j.created_date >= @begin_time AND j.created_date < DATE_ADD(@begin_time ,INTERVAL @interval_time*@line_num MINUTE) 
GROUP BY CONCAT(tmp.区域编码,' - ',tmp1.区域编码)

UNION ALL

SELECT CONCAT(j.start_point,' - ',j.end_area) AS '搬运路线',
       COUNT(CONCAT(j.start_point,' - ',j.end_area)) AS '搬运次数',
       SUM(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date))/COUNT(CONCAT(j.start_point,' - ',j.end_area)) AS '平均搬运时间/s',
       MAX(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最长搬运时间/s',
       MIN(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最短搬运时间/s'   
FROM evo_wcs_g2p.bucket_robot_job j 
WHERE j.start_point not in
(
SELECT DISTINCT j.start_point AS 'job码值'
 FROM evo_wcs_g2p.bucket_robot_job j 
 JOIN evo_rcs.basic_area a 
 WHERE INSTR(a.point_code,j.start_point)>0
)
AND j.target_point in
(
SELECT DISTINCT j.target_point AS 'job码值'
 FROM evo_wcs_g2p.bucket_robot_job j 
 JOIN evo_rcs.basic_area a 
 WHERE INSTR(a.point_code,j.target_point)>0
)
AND j.state = 'DONE' AND j.created_date >= @begin_time AND j.created_date < DATE_ADD(@begin_time ,INTERVAL @interval_time*@line_num MINUTE) 
GROUP BY CONCAT(j.start_point,' - ',j.end_area)

UNION ALL

SELECT CONCAT(j.start_point,' - ',j.end_area) AS '搬运路线',
       COUNT(CONCAT(j.start_point,' - ',j.end_area)) AS '搬运次数',
       SUM(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date))/COUNT(CONCAT(j.start_point,' - ',j.end_area)) AS '平均搬运时间/s',
       MAX(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最长搬运时间/s',
       MIN(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最短搬运时间/s'   
FROM evo_wcs_g2p.bucket_robot_job j 
WHERE j.start_point not in
(
SELECT DISTINCT j.start_point AS 'job码值'
 FROM evo_wcs_g2p.bucket_robot_job j 
 JOIN evo_rcs.basic_area a 
 WHERE INSTR(a.point_code,j.start_point)>0
)
AND j.target_point not in
(
SELECT DISTINCT j.target_point AS 'job码值'
 FROM evo_wcs_g2p.bucket_robot_job j 
 JOIN evo_rcs.basic_area a 
 WHERE INSTR(a.point_code,j.target_point)>0
)
AND j.state = 'DONE' AND j.created_date >= @begin_time AND j.created_date < DATE_ADD(@begin_time ,INTERVAL @interval_time*@line_num MINUTE) 
GROUP BY CONCAT(j.start_point,' - ',j.end_area)

UNION ALL

SELECT CONCAT(tmp.区域编码,' - ',j.target_point) AS '搬运路线',
       COUNT(CONCAT(tmp.区域编码,' - ',j.target_point)) AS '搬运次数',
       SUM(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date))/COUNT(CONCAT(tmp.区域编码,' - ',j.target_point)) AS '平均搬运时间/s',
       MAX(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最长搬运时间/s',
       MIN(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最短搬运时间/s'   
FROM
evo_wcs_g2p.bucket_robot_job j 
JOIN
(
SELECT DISTINCT j.start_point AS 'job码值',
                a.area_code AS '区域编码'
 FROM evo_wcs_g2p.bucket_robot_job j 
 JOIN evo_rcs.basic_area a 
 WHERE INSTR(a.point_code,j.start_point)>0
)tmp
ON j.start_point = tmp.job码值
WHERE 
j.target_point not in
(
SELECT DISTINCT j.target_point AS 'job码值'
 FROM evo_wcs_g2p.bucket_robot_job j 
 JOIN evo_rcs.basic_area a 
 WHERE INSTR(a.point_code,j.target_point)>0
)
AND j.state = 'DONE' AND j.created_date >= @begin_time AND j.created_date < DATE_ADD(@begin_time ,INTERVAL @interval_time*@line_num MINUTE) 
GROUP BY CONCAT(tmp.区域编码,' - ',j.target_point)