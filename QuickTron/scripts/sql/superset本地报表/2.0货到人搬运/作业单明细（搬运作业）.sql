--货到人搬运
INSERT overwrite table ${ads_dbname}.ads_g2p_work_detail
-- 拣选订单
SELECT '' as id,
       t1.work_id,
       t1.start_point,
       t1.target_point,
       t1.agv_mes,
       t1.work_count,
       t2.separate_car_count, -- 分车耗时（秒）
       t3.into_station_count,-- 进站前耗时（秒）
       t4.instation_work_count,-- 站内耗时（秒）
       coalesce(t5.breakdown_num, 0) as exception_num, -- 异常次数
       t1.work_created_time,
       t1.work_updated_time,
       t1.project_code,
       t1.work_type,
       t1.order_type,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time, -- 创建时间
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time -- 更新时间
FROM 
(
SELECT pj.job_id as work_id, -- 作业单ID
       bmj.source_waypoint_code as start_point, -- 起始点
       bmj.target_waypoint_code as target_point, -- 目标点
       concat(pj.agv_code, ':', case when t.first_classification = 'WORKBIN' then '料箱车'
            when t.first_classification = 'STOREFORKBIN' then '存储一体式'
            when t.first_classification = 'CARRIER' then '潜伏式机器人'
            when t.first_classification = 'ROLLER' then '辊筒机器人'
            when t.first_classification = 'FORKLIFT' then '堆高全向车'
            when t.first_classification = 'DELIVER' then '投递车'
            when t.first_classification = 'SC'
            then '四向穿梭车' end) as agv_mes,
       unix_timestamp(pj.job_updated_time) - unix_timestamp(pj.job_created_time) as work_count, -- 作业单总耗时（秒）
       pj.job_created_time as work_created_time, -- 作业单创建时间
       pj.job_updated_time as work_updated_time, -- 作业单完成时间
       pj.pt as project_code, -- 项目编码
       pj.d,
       '货架到人' as work_type, -- 作业类型
       '拣选订单' as order_type -- 订单类型
FROM ${dim_dbname}.dim_project_product_type p
LEFT JOIN ${dwd_dbname}.dwd_g2p_picking_job_info pj
ON p.project_code = pj.project_code
LEFT JOIN ${dwd_dbname}.dwd_g2p_bucket_move_job_info bmj
ON pj.bucket_move_job_id = bmj.id AND pj.d = bmj.d AND pj.pt = bmj.pt
LEFT JOIN ${dwd_dbname}.dwd_rcs_basic_agv_info a
ON pj.agv_code = a.agv_code AND pj.d = a.d AND pj.pt = a.pt
LEFT JOIN ${dwd_dbname}.dwd_rcs_basic_agv_type_info t
ON a.agv_type_id = t.id AND a.d = t.d AND a.pt = t.pt
WHERE p.product_type = 1 AND pj.d = DATE_ADD(CURRENT_DATE(), -1) AND pj.job_state = 'DONE' -- 货到人
)t1
-- 分车耗时
LEFT JOIN
(
SELECT tmp.job_id,tmp.d,tmp.pt,SUM(tmp.prev_cost_time) as separate_car_count
FROM 
(
SELECT c.job_id,c.agv_code,c.job_state,c.job_created_time,c.job_updated_time,c.d,c.pt,
unix_timestamp(c.job_created_time) - unix_timestamp(lag(c.job_created_time, 1)  over (partition by c.project_code,c.job_id order by c.job_created_time,c.id asc)) as prev_cost_time
FROM ${dwd_dbname}.dwd_g2p_job_state_change_info c
WHERE c.d = DATE_ADD(CURRENT_DATE(), -1) 
)tmp
WHERE tmp.job_state = 'INIT_JOB'
GROUP BY tmp.job_id,tmp.d,tmp.pt
)t2
ON t1.work_id = t2.job_id AND t1.project_code = t2.pt AND t1.d = t2.d
-- 进站前耗时
LEFT JOIN
(
SELECT tmp.job_id,tmp.d,tmp.pt,SUM(tmp.into_station_time) as into_station_count
FROM
(
SELECT c.job_id,c.agv_code,c.job_state,c.job_created_time,c.d,c.pt,e.entry_time,unix_timestamp(e.entry_time) - unix_timestamp(c.job_created_time) as into_station_time
FROM ${dwd_dbname}.dwd_g2p_job_state_change_info c
LEFT JOIN ${dwd_dbname}.dwd_station_station_entry_info e
ON c.job_id = e.idempotent_id AND c.pt = e.pt AND c.d = e.d
WHERE c.d = DATE_ADD(CURRENT_DATE(), -1) AND c.job_state = 'GO_TARGET'
)tmp
GROUP BY tmp.job_id,tmp.d,tmp.pt
)t3
ON t1.work_id = t3.job_id AND t1.project_code = t3.pt AND t1.d = t3.d
-- 站内耗时
LEFT JOIN
(
SELECT tmp.idempotent_id,tmp.d,tmp.pt,SUM(tmp.instation_work_time) as instation_work_count
FROM
(
SELECT e.idempotent_id,e.agv_code,e.d,e.pt,e.entry_time,e.exit_time,unix_timestamp(e.exit_time) - unix_timestamp(e.entry_time) as instation_work_time
FROM ${dwd_dbname}.dwd_station_station_entry_info e
WHERE e.d = DATE_ADD(CURRENT_DATE(), -1)
)tmp
GROUP BY tmp.idempotent_id,tmp.d,tmp.pt
)t4
ON t1.work_id = t4.idempotent_id AND t1.project_code = t4.pt AND t1.d = t4.d
-- 异常次数
LEFT JOIN
(
  SELECT t1.project_code,
         t1.job_id,
        count(distinct concat(t2.agv_code, '-', t2.breakdown_id, '-', t2.error_code)) as breakdown_num
  FROM
  (
    SELECT project_code,
           job_id,
           agv_code,
           job_created_time                     as agv_start_time,
           COALESCE(sort_time[0], sort_time[1]) as agv_end_time
    FROM 
    (
      SELECT *, 
             lead(job_state, 1) over (partition by project_code,job_id order by job_created_time asc) as next_state,
             lead(job_created_time, 1) over (partition by project_code,job_id order by job_created_time asc) as next_time,
             sort_array(ARRAY(lead(job_created_time, 1) over (partition by project_code,job_id order by job_created_time asc),last_updated_time)) as sort_time
      FROM
      (
        SELECT t2.project_code,
               t2.job_id,
               t2.agv_code,
               t2.job_state,
               t2.job_created_time,
               last_value(t2.job_created_time) over (partition by t2.project_code,t2.job_id) as last_updated_time
        FROM ${dwd_dbname}.dwd_g2p_picking_job_info t1
        LEFT JOIN ${dwd_dbname}.dwd_g2p_job_state_change_info t2
        ON t2.project_code = t1.project_code AND t2.job_id = t1.job_id AND t2.d = DATE_ADD(CURRENT_DATE(), -1)
        WHERE 1 = 1 AND t1.d = DATE_ADD(CURRENT_DATE(), -1)
        ORDER BY t2.project_code,t2.job_id,t2.job_created_time asc
      ) t
      WHERE t.job_state in ('INIT_JOB', 'WAITING_AGV')
    ) t
    WHERE job_state = 'INIT_JOB'
  ) t1
  LEFT JOIN ${dwd_dbname}.dwd_agv_breakdown_astringe_v4_di t2
  ON t2.project_code = t1.project_code AND t2.agv_code = t1.agv_code AND t2.d = DATE_ADD(CURRENT_DATE(), -1)
  WHERE t1.agv_code is not null AND t1.agv_code != '' AND t2.breakdown_log_time >= t1.agv_start_time AND t2.breakdown_log_time <= t1.agv_end_time
  GROUP BY t1.project_code, t1.job_id
) t5
ON t5.project_code = t1.project_code AND t5.job_id = t1.work_id

UNION ALL 

-- 上架订单
SELECT '' as id,
       t1.work_id,
       t1.start_point,
       t1.target_point,
       t1.agv_mes,
       t1.work_count,
       t2.separate_car_count, -- 分车耗时（秒）
       t3.into_station_count,-- 进站前耗时（秒）
       t4.instation_work_count,-- 站内耗时（秒）
       coalesce(t5.breakdown_num, 0) as exception_num, -- 异常次数
       t1.work_created_time,
       t1.work_updated_time,
       t1.project_code,
       t1.work_type,
       t1.order_type,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time, -- 创建时间
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time -- 更新时间
FROM 
(
SELECT pj.job_id as work_id, -- 作业单ID
       bmj.source_waypoint_code as start_point, -- 起始点
       bmj.target_waypoint_code as target_point, -- 目标点
       concat(pj.agv_code, ':', case when t.first_classification = 'WORKBIN' then '料箱车'
            when t.first_classification = 'STOREFORKBIN' then '存储一体式'
            when t.first_classification = 'CARRIER' then '潜伏式机器人'
            when t.first_classification = 'ROLLER' then '辊筒机器人'
            when t.first_classification = 'FORKLIFT' then '堆高全向车'
            when t.first_classification = 'DELIVER' then '投递车'
            when t.first_classification = 'SC'
            then '四向穿梭车' end) as agv_mes,
       unix_timestamp(pj.job_updated_time) - unix_timestamp(pj.job_created_time) as work_count, -- 作业单总耗时（秒）
       pj.job_created_time as work_created_time, -- 作业单创建时间
       pj.job_updated_time as work_updated_time, -- 作业单完成时间
       pj.pt as project_code, -- 项目编码
       pj.d,
       '货架到人' as work_type, -- 作业类型
       '上架订单' as order_type -- 订单类型
FROM ${dim_dbname}.dim_project_product_type p
LEFT JOIN ${dwd_dbname}.dwd_g2p_guided_putaway_job_info pj
ON p.project_code = pj.project_code
LEFT JOIN ${dwd_dbname}.dwd_g2p_bucket_move_job_info bmj
ON pj.bucket_move_job_id = bmj.id AND pj.d = bmj.d AND pj.pt = bmj.pt
LEFT JOIN ${dwd_dbname}.dwd_rcs_basic_agv_info a
ON pj.agv_code = a.agv_code AND pj.d = a.d AND pj.pt = a.pt
LEFT JOIN ${dwd_dbname}.dwd_rcs_basic_agv_type_info t
ON a.agv_type_id = t.id AND a.d = t.d AND a.pt = t.pt
WHERE p.product_type = 1 AND pj.d = DATE_ADD(CURRENT_DATE(), -1) AND pj.job_state = 'DONE' 

UNION ALL 

SELECT pj.job_id as work_id, -- 作业单ID
       bmj.source_waypoint_code as start_point, -- 起始点
       bmj.target_waypoint_code as target_point, -- 目标点
       concat(pj.agv_code, ':', case when t.first_classification = 'WORKBIN' then '料箱车'
            when t.first_classification = 'STOREFORKBIN' then '存储一体式'
            when t.first_classification = 'CARRIER' then '潜伏式机器人'
            when t.first_classification = 'ROLLER' then '辊筒机器人'
            when t.first_classification = 'FORKLIFT' then '堆高全向车'
            when t.first_classification = 'DELIVER' then '投递车'
            when t.first_classification = 'SC'
            then '四向穿梭车' end) as agv_mes,
       unix_timestamp(pj.job_updated_time) - unix_timestamp(pj.job_created_time) as work_count, -- 作业单总耗时（秒）
       pj.job_created_time as work_created_time, -- 作业单创建时间
       pj.job_updated_time as work_updated_time, -- 作业单完成时间
       pj.pt as project_code, -- 项目编码
       pj.d,
       '货架到人' as work_type, -- 作业类型
       '上架订单' as order_type -- 订单类型
FROM ${dim_dbname}.dim_project_product_type p
LEFT JOIN ${dwd_dbname}.dwd_g2p_putaway_job_info pj
ON p.project_code = pj.project_code
LEFT JOIN ${dwd_dbname}.dwd_g2p_bucket_move_job_info bmj
ON pj.bucket_move_job_id = bmj.id AND pj.d = bmj.d AND pj.pt = bmj.pt
LEFT JOIN ${dwd_dbname}.dwd_rcs_basic_agv_info a
ON pj.agv_code = a.agv_code AND pj.d = a.d AND pj.pt = a.pt
LEFT JOIN ${dwd_dbname}.dwd_rcs_basic_agv_type_info t
ON a.agv_type_id = t.id AND a.d = t.d AND a.pt = t.pt
WHERE p.product_type = 1 AND pj.d = DATE_ADD(CURRENT_DATE(), -1) AND pj.job_state = 'DONE' 
)t1
-- 分车耗时
LEFT JOIN
(
SELECT tmp.job_id,tmp.d,tmp.pt,SUM(tmp.prev_cost_time) as separate_car_count
FROM 
(
SELECT c.job_id,c.agv_code,c.job_state,c.job_created_time,c.job_updated_time,c.d,c.pt,
unix_timestamp(c.job_created_time) - unix_timestamp(lag(c.job_created_time, 1)  over (partition by c.project_code,c.job_id order by c.job_created_time,c.id asc)) as prev_cost_time
FROM ${dwd_dbname}.dwd_g2p_job_state_change_info c
WHERE c.d = DATE_ADD(CURRENT_DATE(), -1)
)tmp
WHERE tmp.job_state = 'INIT_JOB'
GROUP BY tmp.job_id,tmp.d,tmp.pt
)t2
ON t1.work_id = t2.job_id AND t1.project_code = t2.pt AND t1.d = t2.d
-- 进站前耗时
LEFT JOIN
(
SELECT tmp.job_id,tmp.d,tmp.pt,SUM(tmp.into_station_time) as into_station_count
FROM
(
SELECT c.job_id,c.agv_code,c.job_state,c.job_created_time,c.d,c.pt,e.entry_time,unix_timestamp(e.entry_time) - unix_timestamp(c.job_created_time) as into_station_time
FROM ${dwd_dbname}.dwd_g2p_job_state_change_info c
LEFT JOIN ${dwd_dbname}.dwd_station_station_entry_info e
ON c.job_id = e.idempotent_id AND c.pt = e.pt AND c.d = e.d
WHERE c.d = DATE_ADD(CURRENT_DATE(), -1) AND c.job_state = 'GO_TARGET'
)tmp
GROUP BY tmp.job_id,tmp.d,tmp.pt
)t3
ON t1.work_id = t3.job_id AND t1.project_code = t3.pt AND t1.d = t3.d
-- 站内耗时
LEFT JOIN
(
SELECT tmp.idempotent_id,tmp.d,tmp.pt,SUM(tmp.instation_work_time) as instation_work_count
FROM
(
SELECT e.idempotent_id,e.agv_code,e.d,e.pt,e.entry_time,e.exit_time,unix_timestamp(e.exit_time) - unix_timestamp(e.entry_time) as instation_work_time
FROM ${dwd_dbname}.dwd_station_station_entry_info e
WHERE e.d = DATE_ADD(CURRENT_DATE(), -1)
)tmp
GROUP BY tmp.idempotent_id,tmp.d,tmp.pt
)t4
ON t1.work_id = t4.idempotent_id AND t1.project_code = t4.pt AND t1.d = t4.d
-- 异常次数
LEFT JOIN
(
  SELECT t1.project_code,
         t1.job_id,
        count(distinct concat(t2.agv_code, '-', t2.breakdown_id, '-', t2.error_code)) as breakdown_num
  FROM
  (
    SELECT project_code,
           job_id,
           agv_code,
           job_created_time                     as agv_start_time,
           COALESCE(sort_time[0], sort_time[1]) as agv_end_time
    FROM 
    (
      SELECT *, 
             lead(job_state, 1) over (partition by project_code,job_id order by job_created_time asc) as next_state,
             lead(job_created_time, 1) over (partition by project_code,job_id order by job_created_time asc) as next_time,
             sort_array(ARRAY(lead(job_created_time, 1) over (partition by project_code,job_id order by job_created_time asc),last_updated_time)) as sort_time
      FROM
      (
        SELECT t2.project_code,
               t2.job_id,
               t2.agv_code,
               t2.job_state,
               t2.job_created_time,
               last_value(t2.job_created_time) over (partition by t2.project_code,t2.job_id) as last_updated_time
        FROM ${dwd_dbname}.dwd_g2p_job_state_change_info t2
        WHERE t2.d = DATE_ADD(CURRENT_DATE(), -1)
        ORDER BY t2.project_code,t2.job_id,t2.job_created_time asc
      ) t
      WHERE t.job_state in ('INIT_JOB', 'WAITING_AGV')
    ) t
    WHERE job_state = 'INIT_JOB'
  ) t1
  LEFT JOIN ${dwd_dbname}.dwd_agv_breakdown_astringe_v4_di t2
  ON t2.project_code = t1.project_code AND t2.agv_code = t1.agv_code AND t2.d = DATE_ADD(CURRENT_DATE(), -1)
  WHERE t1.agv_code is not null AND t1.agv_code != '' AND t2.breakdown_log_time >= t1.agv_start_time AND t2.breakdown_log_time <= t1.agv_end_time
  GROUP BY t1.project_code, t1.job_id
) t5
ON t5.project_code = t1.project_code AND t5.job_id = t1.work_id;