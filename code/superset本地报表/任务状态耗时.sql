SET @begin_time = DATE_ADD(DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00'),INTERVAL -1 DAY);
SET @end_time = DATE_FORMAT(sysdate(),'%Y-%m-%d 00:00:00');

-- -------------------------------------------------------------任务类型状态耗时明细------------------------------------------------------------------------------------
SELECT tmp.job_id AS '任务编码',tmp.job_type AS '任务类型',tmp.state AS '任务起始状态',tmp.updated_date AS '任务起始状态更新时间',tmp.state1 AS '任务结束状态',tmp.updated_date1 AS '任务结束状态更新时间',MIN(tmp.time) AS '任务状态耗时'
FROM
(
SELECT c.job_id,c.job_type,c.state,c.updated_date,c1.job_id as job_id1,c1.job_type as job_type1,c1.state as state1,c1.updated_date as updated_date1,
        TIMESTAMPDIFF(SECOND,c.updated_date,c1.updated_date) AS time
FROM evo_wcs_g2p.job_state_change c
LEFT JOIN evo_wcs_g2p.job_state_change c1
ON c.job_id = c1.job_id AND c.job_type = c1.job_type AND c.project_code = c1.project_code
WHERE c.updated_date >= @begin_time AND c.updated_date < @end_time AND c.job_type != ' ' AND c.project_code = 'A51118'
ORDER BY c.job_id,c.job_type,c.updated_date)tmp
WHERE tmp.time >= 0 AND tmp.updated_date < tmp.updated_date1
GROUP BY tmp.job_id,tmp.job_type,tmp.state


-- -------------------------------------------------------------任务类型状态耗时平均值------------------------------------------------------------------------------------

SELECT tt.job_type AS '任务类型',tt.state AS '起始状态',tt.state1 AS '结束状态',cast(SUM(tt.min)/COUNT(DISTINCT tt.job_id) AS DECIMAL(10,2)) AS '任务状态耗时平均值'
FROM
(
SELECT tmp.job_id,tmp.job_type,tmp.state,tmp.updated_date,tmp.job_id1,tmp.job_type1,tmp.state1,tmp.updated_date1,MIN(tmp.time) as min
FROM
(
SELECT c.job_id,c.job_type,c.state,c.updated_date,c1.job_id as job_id1,c1.job_type as job_type1,c1.state as state1,c1.updated_date as updated_date1,
        TIMESTAMPDIFF(SECOND,c.updated_date,c1.updated_date) AS time
FROM evo_wcs_g2p.job_state_change c
LEFT JOIN evo_wcs_g2p.job_state_change c1
ON c.job_id = c1.job_id AND c.job_type = c1.job_type AND c.project_code = c1.project_code
WHERE c.updated_date >= @begin_time AND c.updated_date < @end_time AND c.job_type != ' ' AND c.project_code = 'A51118'
ORDER BY c.job_id,c.job_type,c.updated_date)tmp
WHERE tmp.time >= 0 AND tmp.updated_date < tmp.updated_date1
GROUP BY tmp.job_id,tmp.job_type,tmp.state
)tt
GROUP BY tt.job_type,tt.state,tt.state1