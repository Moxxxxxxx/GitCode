-- ----------------------------------------------- 判断起点在库区、终点在库区 ---------------------------------------------------------
        SELECT         
        j.agv_code,
        CONCAT(CONCAT(tmp.job码值,'(',tmp.区域编码,')'),' - ',CONCAT(tmp1.job码值,'(',tmp1.区域编码,')')) AS 'router', -- 搬运路线
        COUNT(CONCAT(CONCAT(tmp.job码值,'(',tmp.区域编码,')'),' - ',CONCAT(tmp1.job码值,'(',tmp1.区域编码,')'))) AS 'num', -- 搬运次数
        SUM(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date))/COUNT(CONCAT(CONCAT(tmp.job码值,'(',tmp.区域编码,')'),' - ',CONCAT(tmp1.job码值,'(',tmp1.区域编码,')'))) AS 'aveTime', -- 平均搬运时间/s
        MAX(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS 'longest', -- 最长搬运时间/s
        MIN(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS 'shortest' -- 最短搬运时间/s
        FROM
        evo_wcs_g2p.bucket_robot_job j
        JOIN
        (
        SELECT DISTINCT j.start_point AS 'job码值',a.area_code AS '区域编码'
        FROM evo_wcs_g2p.bucket_robot_job j
        JOIN evo_rcs.base_area a
        WHERE INSTR(a.point_code,j.start_point)>0
        )tmp
        ON j.start_point = tmp.job码值
        JOIN
        (
        SELECT DISTINCT j.target_point AS 'job码值',a.area_code AS '区域编码'
        FROM evo_wcs_g2p.bucket_robot_job j
        JOIN evo_rcs.base_area a
        WHERE INSTR(a.point_code,j.target_point)>0
        )tmp1
        ON j.target_point = tmp1.job码值
        WHERE j.state = 'DONE' 
        GROUP BY CONCAT(CONCAT(tmp.job码值,'(',tmp.区域编码,')'),' - ',CONCAT(tmp1.job码值,'(',tmp1.区域编码,')')),j.agv_code

        UNION ALL
-- ----------------------------------------------- 判断起点不在库区、终点在库区 ---------------------------------------------------------
        SELECT         
        j.agv_code,
        CONCAT(j.start_point,' - ',CONCAT(j.target_point,'(',tmp.区域编码,')')) AS '搬运路线',
        COUNT(CONCAT(j.start_point,' - ',CONCAT(j.target_point,'(',tmp.区域编码,')'))) AS '搬运次数',
        SUM(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date))/COUNT(CONCAT(j.start_point,' - ',CONCAT(j.target_point,'(',tmp.区域编码,')'))) AS '平均搬运时间/s',
        MAX(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最长搬运时间/s',
        MIN(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最短搬运时间/s'
        FROM evo_wcs_g2p.bucket_robot_job j
        JOIN
        (
        SELECT DISTINCT j.target_point AS 'job码值',
        a.area_code AS '区域编码'
        FROM evo_wcs_g2p.bucket_robot_job j
        JOIN evo_rcs.base_area a
        WHERE INSTR(a.point_code,j.target_point)>0
        )tmp
        ON j.target_point = tmp.job码值
        WHERE j.start_point not in
        (
        SELECT DISTINCT j.start_point AS 'job码值'
        FROM evo_wcs_g2p.bucket_robot_job j
        JOIN evo_rcs.base_area a
        WHERE INSTR(a.point_code,j.start_point)>0
        )
        AND j.state = 'DONE'
        GROUP BY CONCAT(j.start_point,' - ',CONCAT(j.target_point,'(',tmp.区域编码,')')),j.agv_code

        UNION ALL
-- ----------------------------------------------- 判断起点不在库区、终点不在库区 ---------------------------------------------------------
        SELECT         
        j.agv_code,
        CONCAT(j.start_point,' - ',j.target_point) AS '搬运路线',
        COUNT(CONCAT(j.start_point,' - ',j.target_point)) AS '搬运次数',
        SUM(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date))/COUNT(CONCAT(j.start_point,' - ',j.target_point)) AS '平均搬运时间/s',
        MAX(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最长搬运时间/s',
        MIN(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最短搬运时间/s'
        FROM evo_wcs_g2p.bucket_robot_job j
        WHERE j.start_point not in
        (
        SELECT DISTINCT j.start_point AS 'job码值'
        FROM evo_wcs_g2p.bucket_robot_job j
        JOIN evo_rcs.base_area a
        WHERE INSTR(a.point_code,j.start_point)>0
        )
        AND j.target_point not in
        (
        SELECT DISTINCT j.target_point AS 'job码值'
        FROM evo_wcs_g2p.bucket_robot_job j
        JOIN evo_rcs.base_area a
        WHERE INSTR(a.point_code,j.target_point)>0
        )
        AND j.state = 'DONE' 
        GROUP BY CONCAT(j.start_point,' - ',j.target_point),j.agv_code

        UNION ALL
-- ----------------------------------------------- 判断起点在库区、终点不在库区 ---------------------------------------------------------
        SELECT         
        j.agv_code,
        CONCAT(CONCAT(tmp.job码值,'(',tmp.区域编码,')'),' - ',j.target_point) AS '搬运路线',
        COUNT(CONCAT(CONCAT(tmp.job码值,'(',tmp.区域编码,')'),' - ',j.target_point)) AS '搬运次数',
        SUM(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date))/COUNT(CONCAT(CONCAT(tmp.job码值,'(',tmp.区域编码,')'),' - ',j.target_point)) AS '平均搬运时间/s',
        MAX(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最长搬运时间/s',
        MIN(TIMESTAMPDIFF(SECOND, j.created_date,j.updated_date)) AS '最短搬运时间/s'
        FROM
        evo_wcs_g2p.bucket_robot_job j
        JOIN
        (
        SELECT DISTINCT j.start_point AS 'job码值',
        a.area_code AS '区域编码'
        FROM evo_wcs_g2p.bucket_robot_job j
        JOIN evo_rcs.base_area a
        WHERE INSTR(a.point_code,j.start_point)>0
        )tmp
        ON j.start_point = tmp.job码值
        WHERE
        j.target_point not in
        (
        SELECT DISTINCT j.target_point AS 'job码值'
        FROM evo_wcs_g2p.bucket_robot_job j
        JOIN evo_rcs.base_area a
        WHERE INSTR(a.point_code,j.target_point)>0
        )
        AND j.state = 'DONE'
        GROUP BY CONCAT(CONCAT(tmp.job码值,'(',tmp.区域编码,')'),' - ',j.target_point),j.agv_code
