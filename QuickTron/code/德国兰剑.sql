SELECT tud.start_time as cur_hour, -- 统计时间
       IFNULL(out_task.out_num,0) as out_num, -- 出库任务数
       IFNULL(in_task.in_num,0) as in_num, -- 回库任务数
       IFNULL(out_task.out_num,0) + IFNULL(in_task.in_num,0) as total_num, -- 总任务数
       IFNULL(eff.eff_duration,0) as total_eff_duration, -- 总有效作业时长
       IFNULL(eff.agv_num,0) as agv_num, -- 小车数量
       IFNULL(eff.agv_num,0) * 3600 - IFNULL(eff.eff_duration,0) as total_free_duration, -- 总空闲时间
       IFNULL(free.free_duration,0) as task_free_duration, -- 有任务空闲时间
       IFNULL(free.job_num,0) * 100 as free_duration, -- 空闲时间
       (IFNULL(eff.agv_num,0) * 3600) - IFNULL(eff.eff_duration,0) - IFNULL(free.free_duration,0) - (IFNULL(free.job_num,0) * 100) as notask_free_duration, -- 无任务空闲时间
       IFNULL(IFNULL(eff.eff_duration,0)/(IFNULL(eff.agv_num,0) * 3600),0) as using_rate, -- 设备利用率
       IFNULL(error.error_num,0) as error_num -- 设备报警次数
FROM
(
  SELECT t1.times as start_time,
         IF(t2.times is null,CONCAT(CURRENT_DATE(),' ','00:00:00'),t2.times) as end_time
  FROM 
  (
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','00:00:00') as times UNION 
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','01:00:00') as times UNION 
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','02:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','03:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','04:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','05:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','06:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','07:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','08:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','09:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','10:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','11:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','12:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','13:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','14:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','15:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','16:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','17:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','18:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','19:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','20:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','21:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','22:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','23:00:00') 
  )t1
  LEFT JOIN
  (
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','00:00:00') as times UNION 
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','01:00:00') as times UNION 
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','02:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','03:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','04:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','05:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','06:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','07:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','08:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','09:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','10:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','11:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','12:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','13:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','14:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','15:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','16:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','17:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','18:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','19:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','20:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','21:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','22:00:00') as times UNION
    SELECT CONCAT(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY),' ','23:00:00') 
  )t2
  ON t1.times = DATE_SUB(t2.times,INTERVAL 1 HOUR)
)tud
LEFT JOIN 
(
  SELECT DATE_FORMAT(t.create_time,'%Y-%m-%d %H:00:00') as cur_hour,
         COUNT(DISTINCT t.id) as out_num,
         SUM(TIMESTAMPDIFF(SECOND,c.start_time,c.end_time)) as eff_duration
  FROM db_winit_wms_deguo.tb_task_d t
  LEFT JOIN evo_rcs.agv_job_history j
  ON t.id = j.robot_job_id
  LEFT JOIN 
  (
    SELECT c1.job_id,
           c1.updated_date as start_time,
           c2.updated_date as end_time
    FROM evo_wcs_g2p.job_state_change c1
    LEFT JOIN evo_wcs_g2p.job_state_change c2
    ON c1.job_id = c2.job_id AND c2.state = 'DONE'
    WHERE c1.state = 'MOVE_BEGIN'
  )c
  ON j.job_id = c.job_id
  WHERE t.type = 2 AND (t.to_unit RLIKE '^A' OR t.to_unit RLIKE '^B' OR t.to_unit RLIKE '^C' OR t.to_unit RLIKE '^D' OR t.to_unit RLIKE '^E') AND LENGTH(t.to_unit) = 2 -- 出库任务
  GROUP BY DATE_FORMAT(t.create_time,'%Y-%m-%d %H:00:00')
)out_task
ON out_task.cur_hour = tud.start_time
LEFT JOIN 
(
  SELECT DATE_FORMAT(t.create_time,'%Y-%m-%d %H:00:00') as cur_hour,
         COUNT(DISTINCT t.id) as in_num,
         SUM(TIMESTAMPDIFF(SECOND,c.start_time,c.end_time)) as eff_duration
  FROM db_winit_wms_deguo.tb_task_d t
  LEFT JOIN evo_rcs.agv_job_history j
  ON t.id = j.robot_job_id
  LEFT JOIN 
  (
    SELECT c1.job_id,
           c1.updated_date as start_time,
           c2.updated_date as end_time
    FROM evo_wcs_g2p.job_state_change c1
    LEFT JOIN evo_wcs_g2p.job_state_change c2
    ON c1.job_id = c2.job_id AND c2.state = 'DONE'
    WHERE c1.state = 'MOVE_BEGIN'
  )c
  ON j.job_id = c.job_id
  WHERE t.type = 1 AND (t.from_unit RLIKE '^A' OR t.from_unit RLIKE '^B' OR t.from_unit RLIKE '^C' OR t.from_unit RLIKE '^D' OR t.from_unit RLIKE '^E') AND LENGTH(t.from_unit) = 2 -- 回库任务
  GROUP BY DATE_FORMAT(t.create_time,'%Y-%m-%d %H:00:00')
)in_task
ON in_task.cur_hour = tud.start_time
LEFT JOIN
(
  SELECT DATE_FORMAT(c.start_time,'%Y-%m-%d %H:00:00') as cur_hour,
         SUM(TIMESTAMPDIFF(SECOND,c.start_time,c.end_time)) as eff_duration,
         COUNT(DISTINCT j.agv_code) as agv_num
  FROM db_winit_wms_deguo.tb_task_d t
  LEFT JOIN evo_rcs.agv_job_history j
  ON t.id = j.robot_job_id
  LEFT JOIN 
  (
    SELECT c1.job_id,
           c1.updated_date as start_time,
           c2.updated_date as end_time
    FROM evo_wcs_g2p.job_state_change c1
    LEFT JOIN evo_wcs_g2p.job_state_change c2
    ON c1.job_id = c2.job_id AND c2.state = 'DONE'
    WHERE c1.state = 'MOVE_BEGIN'
  )c
  ON j.job_id = c.job_id
  WHERE (t.type = 2 AND (t.to_unit RLIKE '^A' OR t.to_unit RLIKE '^B' OR t.to_unit RLIKE '^C' OR t.to_unit RLIKE '^D' OR t.to_unit RLIKE '^E') AND LENGTH(t.to_unit) = 2)
     OR (t.type = 1 AND (t.from_unit RLIKE '^A' OR t.from_unit RLIKE '^B' OR t.from_unit RLIKE '^C' OR t.from_unit RLIKE '^D' OR t.from_unit RLIKE '^E') AND LENGTH(t.from_unit) = 2)
  GROUP BY DATE_FORMAT(c.start_time,'%Y-%m-%d %H:00:00')
)eff
ON eff.cur_hour = tud.start_time
LEFT JOIN
(
  SELECT DATE_FORMAT(m.happen_at,'%Y-%m-%d %H:00:00') as cur_hour,
         COUNT(DISTINCT m.message_id) as error_num
  FROM evo_basic.notification_message m
  WHERE m.title IN ('RCS_RbtErr_UNKONW','RCS_RbtErr_NotOnCode')
  GROUP BY DATE_FORMAT(m.happen_at,'%Y-%m-%d %H:00:00')
)error
ON error.cur_hour = tud.start_time
LEFT JOIN
(
  SELECT cur_hour,
         SUM(free_duration) as free_duration,
         SUM(job_num) as job_num
  FROM
  (
    SELECT DATE_FORMAT(t1.end_time,'%Y-%m-%d %H:00:00') as cur_hour,
           COUNT(DISTINCT t1.job_id) * 100 as free_duration,
           COUNT(DISTINCT t1.job_id) as job_num
    FROM
    (
      SELECT t.id,t.barcode,t.from_unit,t.to_unit,t.agv_code,t.bucket_code,t.create_time,j.job_id,c.start_time,c.end_time
      FROM db_winit_wms_deguo.tb_task_d t
      LEFT JOIN evo_rcs.agv_job_history j
      ON t.id = j.robot_job_id
      LEFT JOIN 
      (
        SELECT c1.job_id,
               c1.updated_date as start_time,
               c2.updated_date as end_time
        FROM evo_wcs_g2p.job_state_change c1
        LEFT JOIN evo_wcs_g2p.job_state_change c2
        ON c1.job_id = c2.job_id AND c2.state = 'DONE'
        WHERE c1.state = 'MOVE_BEGIN'
      )c
      ON j.job_id = c.job_id
      WHERE (t.type = 2 AND (t.to_unit RLIKE '^A' OR t.to_unit RLIKE '^B' OR t.to_unit RLIKE '^C' OR t.to_unit RLIKE '^D' OR t.to_unit RLIKE '^E') AND LENGTH(t.to_unit) = 2) -- 出库任务
    )t1    
    LEFT JOIN
    (
      SELECT t.id,t.barcode,t.from_unit,t.to_unit,t.agv_code,t.bucket_code,t.create_time,j.job_id,c.start_time,c.end_time
      FROM db_winit_wms_deguo.tb_task_d t
      LEFT JOIN evo_rcs.agv_job_history j
      ON t.id = j.robot_job_id
      LEFT JOIN 
      (
        SELECT c1.job_id,
               c1.updated_date as start_time,
               c2.updated_date as end_time
        FROM evo_wcs_g2p.job_state_change c1
        LEFT JOIN evo_wcs_g2p.job_state_change c2
        ON c1.job_id = c2.job_id AND c2.state = 'DONE'
        WHERE c1.state = 'MOVE_BEGIN'
      )c
      ON j.job_id = c.job_id
      WHERE (t.type = 1 AND (t.from_unit RLIKE '^A' OR t.from_unit RLIKE '^B' OR t.from_unit RLIKE '^C' OR t.from_unit RLIKE '^D' OR t.from_unit RLIKE '^E') AND LENGTH(t.from_unit) = 2) -- 回库任务
    )t2   
    ON t1.bucket_code = t2.bucket_code
    WHERE DATE_FORMAT(t1.end_time,'%Y-%m-%d %H:00:00') = DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00')
    GROUP BY DATE_FORMAT(t1.end_time,'%Y-%m-%d %H:00:00')

    UNION ALL

    SELECT cur_hour,
           sum(free_duration) as free_duration,
           COUNT(DISTINCT job_id) as job_num
    FROM
    (
      SELECT t1.job_id,
             DATE_FORMAT(t1.end_time,'%Y-%m-%d %H:00:00') as cur_hour,
             IF(TIMESTAMPDIFF(SECOND,t1.end_time,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'))>=50 and TIMESTAMPDIFF(SECOND,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'),t2.start_time)>=50,TIMESTAMPDIFF(SECOND,t1.end_time,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'))-50,TIMESTAMPDIFF(SECOND,t1.end_time,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'))-100) as free_duration
      FROM
      (
        SELECT t.id,t.barcode,t.from_unit,t.to_unit,t.agv_code,t.bucket_code,t.create_time,j.job_id,c.start_time,c.end_time
        FROM db_winit_wms_deguo.tb_task_d t
        LEFT JOIN evo_rcs.agv_job_history j
        ON t.id = j.robot_job_id
        LEFT JOIN 
        (
          SELECT c1.job_id,
                 c1.updated_date as start_time,
                 c2.updated_date as end_time
          FROM evo_wcs_g2p.job_state_change c1
          LEFT JOIN evo_wcs_g2p.job_state_change c2
          ON c1.job_id = c2.job_id AND c2.state = 'DONE'
          WHERE c1.state = 'MOVE_BEGIN'
        )c
        ON j.job_id = c.job_id
        WHERE (t.type = 2 AND (t.to_unit RLIKE '^A' OR t.to_unit RLIKE '^B' OR t.to_unit RLIKE '^C' OR t.to_unit RLIKE '^D' OR t.to_unit RLIKE '^E') AND LENGTH(t.to_unit) = 2) -- 出库任务
      )t1    
      LEFT JOIN
      (
        SELECT t.id,t.barcode,t.from_unit,t.to_unit,t.agv_code,t.bucket_code,t.create_time,j.job_id,c.start_time,c.end_time
        FROM db_winit_wms_deguo.tb_task_d t
        LEFT JOIN evo_rcs.agv_job_history j
        ON t.id = j.robot_job_id
        LEFT JOIN 
        (
          SELECT c1.job_id,
                 c1.updated_date as start_time,
                 c2.updated_date as end_time
          FROM evo_wcs_g2p.job_state_change c1
          LEFT JOIN evo_wcs_g2p.job_state_change c2
          ON c1.job_id = c2.job_id AND c2.state = 'DONE'
          WHERE c1.state = 'MOVE_BEGIN'
        )c
        ON j.job_id = c.job_id
        WHERE (t.type = 1 AND (t.from_unit RLIKE '^A' OR t.from_unit RLIKE '^B' OR t.from_unit RLIKE '^C' OR t.from_unit RLIKE '^D' OR t.from_unit RLIKE '^E') AND LENGTH(t.from_unit) = 2) -- 回库任务
      )t2   
      ON t1.bucket_code = t2.bucket_code
      WHERE DATE_FORMAT(t1.end_time,'%Y-%m-%d %H:00:00') != DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00') 
        AND IF(TIMESTAMPDIFF(SECOND,t1.end_time,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'))>=50 and TIMESTAMPDIFF(SECOND,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'),t2.start_time)>=50,TIMESTAMPDIFF(SECOND,t1.end_time,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'))-50,TIMESTAMPDIFF(SECOND,t1.end_time,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'))-100) >=0
  
      UNION ALL

      SELECT t1.job_id,
             DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00') as cur_hour,
             TIMESTAMPDIFF(SECOND,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'),t2.start_time) + IF(TIMESTAMPDIFF(SECOND,t1.end_time,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'))>=50 and TIMESTAMPDIFF(SECOND,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'),t2.start_time)>=50,TIMESTAMPDIFF(SECOND,t1.end_time,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'))-50,TIMESTAMPDIFF(SECOND,t1.end_time,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'))-100) as free_duration
      FROM
      (
        SELECT t.id,t.barcode,t.from_unit,t.to_unit,t.agv_code,t.bucket_code,t.create_time,j.job_id,c.start_time,c.end_time
        FROM db_winit_wms_deguo.tb_task_d t
        LEFT JOIN evo_rcs.agv_job_history j
        ON t.id = j.robot_job_id
        LEFT JOIN 
        (
          SELECT c1.job_id,
                 c1.updated_date as start_time,
                 c2.updated_date as end_time
          FROM evo_wcs_g2p.job_state_change c1
          LEFT JOIN evo_wcs_g2p.job_state_change c2
          ON c1.job_id = c2.job_id AND c2.state = 'DONE'
          WHERE c1.state = 'MOVE_BEGIN'
        )c
        ON j.job_id = c.job_id
        WHERE (t.type = 2 AND (t.to_unit RLIKE '^A' OR t.to_unit RLIKE '^B' OR t.to_unit RLIKE '^C' OR t.to_unit RLIKE '^D' OR t.to_unit RLIKE '^E') AND LENGTH(t.to_unit) = 2) -- 出库任务
      )t1    
      LEFT JOIN
      (
        SELECT t.id,t.barcode,t.from_unit,t.to_unit,t.agv_code,t.bucket_code,t.create_time,j.job_id,c.start_time,c.end_time
        FROM db_winit_wms_deguo.tb_task_d t
        LEFT JOIN evo_rcs.agv_job_history j
        ON t.id = j.robot_job_id
        LEFT JOIN 
        (
          SELECT c1.job_id,
                 c1.updated_date as start_time,
                 c2.updated_date as end_time
          FROM evo_wcs_g2p.job_state_change c1
          LEFT JOIN evo_wcs_g2p.job_state_change c2
          ON c1.job_id = c2.job_id AND c2.state = 'DONE'
          WHERE c1.state = 'MOVE_BEGIN'
        )c
        ON j.job_id = c.job_id
        WHERE (t.type = 1 AND (t.from_unit RLIKE '^A' OR t.from_unit RLIKE '^B' OR t.from_unit RLIKE '^C' OR t.from_unit RLIKE '^D' OR t.from_unit RLIKE '^E') AND LENGTH(t.from_unit) = 2) -- 回库任务
      )t2   
      ON t1.bucket_code = t2.bucket_code
      WHERE DATE_FORMAT(t1.end_time,'%Y-%m-%d %H:00:00') != DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00')
        AND IF(TIMESTAMPDIFF(SECOND,t1.end_time,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'))>=50 and TIMESTAMPDIFF(SECOND,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'),t2.start_time)>=50,TIMESTAMPDIFF(SECOND,t1.end_time,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'))-50,TIMESTAMPDIFF(SECOND,t1.end_time,DATE_FORMAT(t2.start_time,'%Y-%m-%d %H:00:00'))-100) <0
    )tmp
    GROUP BY tmp.cur_hour
  )tt
  GROUP BY tt.cur_hour
)free
on free.cur_hour = tud.start_time;
