-- 15分钟分时
with times as 
(
  SELECT unix_timestamp(CONCAT(days,' ',start_time,'.000')) as start_time,
         unix_timestamp(CONCAT(days,' ',end_time,'.000')) as end_time,
         CONCAT(days,' ',start_time,'.000') as start_date,
		 CONCAT(days,' ',end_time,'.000') as end_date
  FROM dim.dim_day_date d
  LEFT JOIN 
  (
    SELECT a.second_of_day as start_time,
	       nvl(b.second_of_day,'00:00:00') as end_time
    FROM 
    (
      SELECT *
      FROM
      (
        SELECT *,
		       row_number()over(PARTITION by start_hour,start_minute order by second_of_day)rn
        FROM dim.dim_day_of_second
        WHERE start_minute IN (15,30,45,00)
      )tmp
      WHERE tmp.rn=1
    )a
    LEFT JOIN 
    (
      SELECT *
      FROM
      (
        SELECT *,
		       row_number()over(PARTITION by start_hour,start_minute order by second_of_day)rn
        FROM dim.dim_day_of_second
        WHERE start_minute IN (15,30,45,00)
      )tmp
      WHERE tmp.rn=1
    )b
    ON (b.start_hour = a.start_hour AND b.start_minute = a.start_minute + 15) or (b.start_hour = a.start_hour + 1 AND a.start_minute = 45 and b.start_minute = 00)
  )tmp
WHERE d.days >= '2022-10-10' AND d.days <= '2022-10-14'
)

-- 正常记录（未去掉最高最低）
SELECT tmp.station_code,
       tmp.biz_type,
       a.start_date, 
       a.end_date,
       COUNT(DISTINCT tmp.id) as into_station_times,
       SUM(unix_timestamp(tmp.exit_time) - unix_timestamp(tmp.entry_time)) as into_station_duration,
       SUM(tmp.picking_orderline_num) as picking_orderline_num,
       SUM(tmp.picking_quantity) as picking_quantity
FROM times a
LEFT JOIN 
(
  SELECT se.station_code,
         se.biz_type,
         se.id,
         se.entry_time,
         se.exit_time,
         COUNT(DISTINCT pj.picking_work_detail_id) as picking_orderline_num,
         SUM(nvl(pj.actual_quantity,0)) as picking_quantity
  FROM dwd.dwd_station_station_entry_info se
  LEFT JOIN dwd.dwd_g2p_station_task_group_info stg
  ON se.idempotent_id = stg.job_id AND stg.d >= '2022-10-10' AND stg.pt = 'A51149'
  LEFT JOIN dwd.dwd_g2p_station_task_group_info stg1
  ON stg.group_job_id = stg1.group_job_id AND stg1.d >= '2022-10-10' AND stg1.pt = 'A51149'
  LEFT JOIN dwd.dwd_g2p_picking_job_info pj
  ON stg1.job_id = pj.job_id AND pj.d >= '2022-10-10' AND pj.pt = 'A51149'
  WHERE se.d >= '2022-10-10' AND se.pt = 'A51149' AND se.entry_time >= '2022-10-10 00:00:00' AND se.entry_time <= '2022-10-14 00:00:00' AND se.biz_type = 'PICKING_ONLINE_G2P_B2P'
  GROUP BY se.station_code,se.biz_type,se.id,se.entry_time,se.exit_time
)tmp
ON unix_timestamp(tmp.entry_time) < a.end_time AND unix_timestamp(tmp.entry_time) >= a.start_time
WHERE tmp.station_code is not null 
GROUP BY tmp.station_code,tmp.biz_type,a.start_date,a.end_date



-- 按15分钟分时（去掉最高最低）
SELECT t1.station_code,
       t1.biz_type,
       t1.start_date, 
       t1.end_date,
       COUNT(DISTINCT t1.id) as into_station_times,
       SUM(t1.into_station_duration)/COUNT(DISTINCT t1.id) as into_station_duration,
       SUM(t1.picking_orderline_num) as picking_orderline_num,
       SUM(t1.picking_quantity) as picking_quantity
FROM 
(
  SELECT tmp.station_code,
         tmp.biz_type,
         a.start_date, 
         a.end_date,
         tmp.id,
         unix_timestamp(tmp.exit_time) - unix_timestamp(tmp.entry_time) into_station_duration,
         tmp.picking_orderline_num,
         tmp.picking_quantity
  FROM times a
  LEFT JOIN 
  (
    SELECT se.station_code,
           se.biz_type,
           se.id,
           se.entry_time,
           se.exit_time,
           COUNT(DISTINCT pj.picking_work_detail_id) as picking_orderline_num,
           SUM(nvl(pj.actual_quantity,0)) as picking_quantity
    FROM dwd.dwd_station_station_entry_info se
    LEFT JOIN dwd.dwd_g2p_station_task_group_info stg
    ON se.idempotent_id = stg.job_id AND stg.d >= '2022-10-10' AND stg.pt = 'A51149'
    LEFT JOIN dwd.dwd_g2p_station_task_group_info stg1
    ON stg.group_job_id = stg1.group_job_id AND stg1.d >= '2022-10-10' AND stg1.pt = 'A51149'
    LEFT JOIN dwd.dwd_g2p_picking_job_info pj
    ON stg1.job_id = pj.job_id AND pj.d >= '2022-10-10' AND pj.pt = 'A51149'
    WHERE se.d >= '2022-10-10' AND se.pt = 'A51149' AND se.entry_time >= '2022-10-10 00:00:00' AND se.entry_time <= '2022-10-14 00:00:00' AND se.biz_type = 'PICKING_ONLINE_G2P_B2P'
    GROUP BY se.station_code,se.biz_type,se.id,se.entry_time,se.exit_time
  )tmp
  ON unix_timestamp(tmp.entry_time) < a.end_time AND unix_timestamp(tmp.entry_time) >= a.start_time
  WHERE tmp.station_code is not null 
  --GROUP BY tmp.station_code,tmp.biz_type,a.start_date,a.end_date
)t1
LEFT JOIN
(
  SELECT tmp.station_code,
         tmp.biz_type,
         tmp.start_date, 
         tmp.end_date,
         percentile(tmp.into_station_duration,0.05) as low_s,
         percentile(tmp.into_station_duration,0.95) as high_s
  FROM
  (
    SELECT tmp.station_code,
           tmp.biz_type,
           a.start_date, 
           a.end_date,
           tmp.id,
           unix_timestamp(tmp.exit_time) - unix_timestamp(tmp.entry_time) into_station_duration,
           tmp.picking_orderline_num,
           tmp.picking_quantity
    FROM times a
    LEFT JOIN 
    (
      SELECT se.station_code,
             se.biz_type,
             se.id,
             se.entry_time,
             se.exit_time,
             COUNT(DISTINCT pj.picking_work_detail_id) as picking_orderline_num,
             SUM(nvl(pj.actual_quantity,0)) as picking_quantity
      FROM dwd.dwd_station_station_entry_info se
      LEFT JOIN dwd.dwd_g2p_station_task_group_info stg
      ON se.idempotent_id = stg.job_id AND stg.d >= '2022-10-10' AND stg.pt = 'A51149'
      LEFT JOIN dwd.dwd_g2p_station_task_group_info stg1
      ON stg.group_job_id = stg1.group_job_id AND stg1.d >= '2022-10-10' AND stg1.pt = 'A51149'
      LEFT JOIN dwd.dwd_g2p_picking_job_info pj
      ON stg1.job_id = pj.job_id AND pj.d >= '2022-10-10' AND pj.pt = 'A51149'
      WHERE se.d >= '2022-10-10' AND se.pt = 'A51149' AND se.entry_time >= '2022-10-10 00:00:00' AND se.entry_time <= '2022-10-14 00:00:00' AND se.biz_type = 'PICKING_ONLINE_G2P_B2P'
      GROUP BY se.station_code,se.biz_type,se.id,se.entry_time,se.exit_time
    )tmp
    ON unix_timestamp(tmp.entry_time) < a.end_time AND unix_timestamp(tmp.entry_time) >= a.start_time
    WHERE tmp.station_code is not null 
  )tmp
  GROUP BY tmp.station_code,tmp.biz_type,tmp.start_date,tmp.end_date
)t2
ON t1.station_code = t2.station_code AND t1.start_date = t2.start_date and t1.end_date = t2.end_date
WHERE t1.into_station_duration >= t2.low_s AND t1.into_station_duration <= t2.high_s
GROUP BY t1.station_code,t1.biz_type,t1.start_date,t1.end_date;





-- （去掉最高最低）
SELECT t1.station_code,
       t1.biz_type,
       COUNT(DISTINCT t1.id) as into_station_times,
       SUM(t1.into_station_duration)/COUNT(DISTINCT t1.id) as into_station_duration,
       SUM(t1.picking_orderline_num) as picking_orderline_num,
       SUM(t1.picking_quantity) as picking_quantity
FROM
(
  SELECT tmp.station_code,
         tmp.biz_type,
         tmp.id,
         COUNT(DISTINCT tmp.id) as into_station_times,
         SUM(unix_timestamp(tmp.exit_time) - unix_timestamp(tmp.entry_time)) as into_station_duration,
         SUM(tmp.picking_orderline_num) as picking_orderline_num,
         SUM(tmp.picking_quantity) as picking_quantity
  FROM 
  (
    SELECT se.station_code,
           se.biz_type,
           se.id,
           se.entry_time,
           se.exit_time,
           COUNT(DISTINCT pj.picking_work_detail_id) as picking_orderline_num,
           SUM(nvl(pj.actual_quantity,0)) as picking_quantity
    FROM dwd.dwd_station_station_entry_info se
    LEFT JOIN dwd.dwd_g2p_station_task_group_info stg
    ON se.idempotent_id = stg.job_id AND stg.d = '2022-11-01' AND stg.pt = 'A51149'
    LEFT JOIN dwd.dwd_g2p_station_task_group_info stg1
    ON stg.group_job_id = stg1.group_job_id AND stg1.d = '2022-11-01' AND stg1.pt = 'A51149'
    LEFT JOIN dwd.dwd_g2p_picking_job_info pj
    ON stg1.job_id = pj.job_id AND pj.d = '2022-11-01' AND pj.pt = 'A51149'
    WHERE se.d = '2022-11-01' AND se.pt = 'A51149' AND se.entry_time >= '2022-11-01 00:45:00' AND se.entry_time <= '2022-11-01 01:45:00' AND se.biz_type = 'PICKING_ONLINE_G2P_B2P'
    GROUP BY se.station_code,se.biz_type,se.id,se.entry_time,se.exit_time
  )tmp
  WHERE tmp.station_code is not null 
  GROUP BY tmp.station_code,tmp.biz_type,tmp.id
)t1
LEFT JOIN 
(
  SELECT tmp.station_code,
         tmp.biz_type,
         percentile(tmp.into_station_duration,0.05) as low_s,
         percentile(tmp.into_station_duration,0.95) as high_s
  FROM
  (
    SELECT tmp.station_code,
           tmp.biz_type,
           tmp.id,
           COUNT(DISTINCT tmp.id) as into_station_times,
           SUM(unix_timestamp(tmp.exit_time) - unix_timestamp(tmp.entry_time)) as into_station_duration,
           SUM(tmp.picking_orderline_num) as picking_orderline_num,
           SUM(tmp.picking_quantity) as picking_quantity
    FROM 
    (
      SELECT se.station_code,
             se.biz_type,
             se.id,
             se.entry_time,
             se.exit_time,
             COUNT(DISTINCT pj.picking_work_detail_id) as picking_orderline_num,
             SUM(nvl(pj.actual_quantity,0)) as picking_quantity
      FROM dwd.dwd_station_station_entry_info se
      LEFT JOIN dwd.dwd_g2p_station_task_group_info stg
      ON se.idempotent_id = stg.job_id AND stg.d >= '2022-11-01' AND stg.pt = 'A51149'
      LEFT JOIN dwd.dwd_g2p_station_task_group_info stg1
      ON stg.group_job_id = stg1.group_job_id AND stg1.d >= '2022-11-01' AND stg1.pt = 'A51149'
      LEFT JOIN dwd.dwd_g2p_picking_job_info pj
      ON stg1.job_id = pj.job_id AND pj.d >= '2022-11-01' AND pj.pt = 'A51149'
      WHERE se.d >= '2022-11-01' AND se.pt = 'A51149' AND se.entry_time >= '2022-11-01 00:45:00' AND se.entry_time <= '2022-11-01 01:45:00' AND se.biz_type = 'PICKING_ONLINE_G2P_B2P'
      GROUP BY se.station_code,se.biz_type,se.id,se.entry_time,se.exit_time
    )tmp
    WHERE tmp.station_code is not null 
    GROUP BY tmp.station_code,tmp.biz_type,tmp.id
  )tmp
  GROUP BY tmp.station_code,tmp.biz_type
)t2
ON t1.station_code = t2.station_code
WHERE t1.into_station_duration >= t2.low_s AND t1.into_station_duration <= t2.high_s
GROUP BY t1.station_code,t1.biz_type;