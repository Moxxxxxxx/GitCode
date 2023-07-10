SET @begin_time = '2021-08-31 00:00:00';
SET @end_time = '2021-09-01 00:00:00';
-- DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY) T-1时间

-- ------------------------------------------------------------------------------------------搬运任务次数及耗时------------------------------------------------------------------------------------------------------
SELECT DATE_FORMAT(j.created_date,'%Y-%m-%d') AS '日期',
       j.job_id AS '任务编码',
       COUNT(j.job_id) AS '任务次数',
       TIMESTAMPDIFF(MINUTE,j.created_date,j.updated_date) AS '任务耗时/分'
FROM evo_wcs_g2p.bucket_move_job j
WHERE j.created_date >= @begin_time AND j.updated_date < @end_time AND j.state = 'DONE'
GROUP BY j.job_id

   
-- ------------------------------------------------------------------------------------------------搬运时间-----------------------------------------------------------------------------------------------------------
SELECT DATE_FORMAT(j.created_date,'%Y-%m-%d') AS '日期',
       j.job_id AS '任务编码',
       j.source_waypoint_code AS '起点代码',
       j.target_waypoint_code AS '终点代码',
       COUNT(j.job_id) AS '任务次数',
       TIMESTAMPDIFF(MINUTE,j.created_date,j.updated_date) AS '任务耗时/分'
FROM evo_wcs_g2p.bucket_move_job j
WHERE j.created_date >= @begin_time AND j.updated_date < @end_time AND j.state = 'DONE'
GROUP BY j.job_id

   
-- --------------------------------------------------------------------------------------------AGV搬运产能明细---------------------------------------------------------------------------------------------------------
SELECT j.zone_code AS '库区',
       j.job_id AS '搬运作业单号',
       j.robot_job_id AS '上游系统单号',
       j.bucket_code AS '货架编号',
       j.state AS '状态',
       j.start_point AS '起始点',
       p1.station_point AS '起点分类',
       p1.station_point_category AS '起点站名',
       j.target_point AS '终点',
       p2.station_point AS '终点分类',
       p2.station_point_category AS '终点站名',
       j.agv_code AS '小车编号',
       j.created_date AS '开始时间',
       j.updated_date AS '完成时间',
       sec_to_time(TIMESTAMPDIFF(SECOND,j.created_date,j.updated_date)) AS '任务耗时/分'
FROM evo_wcs_g2p.bucket_robot_job j
LEFT JOIN evo_wcs_g2p.station_ponit p1
ON j.start_point = p1.point
LEFT JOIN evo_wcs_g2p.station_ponit p2
ON j.target_point = p2.point
WHERE j.created_date >= @begin_time AND j.updated_date < @end_time AND j.state = 'DONE'
ORDER BY j.job_id


-- --------------------------------------------------------------------------------------------AGV搬运时间---------------------------------------------------------------------------------------------------------
SELECT 
      CASE WHEN p1.station_point_category = 'H' AND p2.station_point_category = 'RS' THEN 'A'
           WHEN p1.station_point_category = 'RS' AND p2.station_point_category = 'RX' THEN 'B-B1'
           WHEN p1.station_point_category = 'RS' AND p2.station_point_category = 'H' THEN 'B-B2'
           WHEN p1.station_point_category = 'H' AND p2.station_point_category = 'RX' THEN 'B-B3'
           WHEN p1.station_point_category = 'RX' AND p2.station_point_category = 'CS' THEN 'C-C1'
           WHEN p1.station_point_category = 'RX' AND p2.station_point_category = 'H' THEN 'C-C2'
           WHEN p1.station_point_category = 'H' AND p2.station_point_category = 'CS' THEN 'C-C3'
           WHEN p1.station_point_category = 'CS' AND p2.station_point_category = 'PJ' THEN 'D-D1'
           WHEN p1.station_point_category = 'CS' AND p2.station_point_category = 'H' THEN 'D-D2'
           WHEN p1.station_point_category = 'H' AND p2.station_point_category = 'PJ' THEN 'D-D3'
           WHEN p1.station_point_category = 'PJ' AND p2.station_point_category = 'PC' THEN 'E-E1'
           WHEN p1.station_point_category = 'PJ' AND p2.station_point_category = 'H' THEN 'E-E2'
           WHEN p1.station_point_category = 'H' AND p2.station_point_category = 'PC' THEN 'E-E3'
           WHEN p1.station_point_category = 'PC' AND p2.station_point_category = 'KJ' THEN 'F-F1'
           WHEN p1.station_point_category = 'PC' AND p2.station_point_category = 'H' THEN 'F-F2'
           WHEN p1.station_point_category = 'H' AND p2.station_point_category = 'KJ' THEN 'F-F3'
           WHEN p1.station_point_category = 'KJ' AND p2.station_point_category = 'KC' THEN 'G-G1'
           WHEN p1.station_point_category = 'KJ' AND p2.station_point_category = 'H' THEN 'G-G2'
           WHEN p1.station_point_category = 'H' AND p2.station_point_category = 'KC' THEN 'G-G3'
           WHEN p1.station_point_category = 'KC' AND p2.station_point_category = 'H' THEN 'H'
           WHEN p1.station_point_category = 'FL' AND p2.station_point_category = 'RD' THEN 'I'
           WHEN p1.station_point_category = 'RD' AND p2.station_point_category = 'FL' THEN 'J'
       ELSE NULL END AS '路线',
       p1.station_point_category AS '起点站名',
       p1.category_desc AS '起点',
       p2.station_point_category AS '终点站名',
       p2.category_desc AS '终点',
       COUNT(j.job_id) AS '总次数',
       sec_to_time(CAST(SUM(TIMESTAMPDIFF(SECOND,j.created_date,j.updated_date))/COUNT(j.job_id) AS DECIMAL(10.2))) AS '平均时间'
FROM evo_wcs_g2p.bucket_robot_job j
LEFT JOIN evo_wcs_g2p.station_ponit p1
ON j.start_point = p1.point
LEFT JOIN evo_wcs_g2p.station_ponit p2
ON j.target_point = p2.point
WHERE j.created_date >= @begin_time AND j.updated_date < @end_time AND j.state = 'DONE'
GROUP BY p1.station_point_category,p2.station_point_category
ORDER BY `路线`