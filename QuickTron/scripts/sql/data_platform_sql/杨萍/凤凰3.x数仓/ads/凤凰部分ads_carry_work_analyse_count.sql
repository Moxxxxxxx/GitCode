-- 凤凰3.X CARRIER逻辑   
union all
SELECT ''                                                      as id,                        -- 主键
       td.upstream_order_no                                    as upper_work_id,             -- 上游作业单ID
       td.order_no                                             as work_id,                   -- 搬运作业单
       td.work_path,                                                                         -- 路径
       td.start_point                                          as start_point,               -- 起始点
       td.target_point                                         as target_point,              -- 目标点
       td.order_state                                          as work_state,                -- 作业单状态
       td.dispatch_first_classification_str                    as first_classification,      -- 机器人类型
       td.dispatch_first_classification_name_str               as first_classification_desc, -- 机器人类型中文描述	
       td.dispatch_robot_type_code_str                         as agv_type_code,             -- 机器人类型编码
       td.dispatch_robot_code_str                              as agv_code,                  -- 机器人编码
       nvl(td.dispatch_robot_code_num, 0)                      as robot_num,                 -- 分配机器人数量
       nvl(td.total_cost / 1000, 0)                            as wotk_duration_total,       -- 总耗时
       nvl(td.assign_cost / 1000, 0)                           as robot_assign_duration,     -- 分车耗时
       nvl(td.only_carry_total_time_consuming / 1000, 0)       as robot_move_duration,       -- 搬运耗时
       NULL                                                    as station_executor_duration, -- 进站实操耗时
       td.order_create_time,                                                                 -- 作业单创建时间
       td.order_update_time,                                                                 -- 作业单完成时间
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
       c.project_code,
       substr(td.order_update_time, 1, 10)                     as d,
       c.project_code                                          as pt
FROM (SELECT *
      FROM ${dim_dbname}.dim_collection_project_record_ful
      WHERE project_version like '3.%') c
         inner join
     (select tc.project_code,
             tc.upstream_order_no,
             tc.order_no,
             case
                 when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code
                 else 'unknown' end                                      as start_point,  -- 起始点
             case
                 when t.start_area_code <> '' and t.start_area_code is not null then t.start_area_code
                 else 'unknown' end                                      as start_area,   -- 起始区域
             case
                 when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code
                 else 'unknown' end                                      as target_point, -- 目标点
             case
                 when t.target_area_code <> '' and t.target_area_code is not null then t.target_area_code
                 else 'unknown' end                                      as target_area,  -- 目标区域
             CONCAT(case
                        when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code
                        else 'unknown' end, '-', case
                                                     when t.target_point_code <> '' and t.target_point_code is not null
                                                         then t.target_point_code
                                                     else 'unknown' end) as work_path,
             t.order_state,
             tc.total_cost,
             tc.assign_cost,
             tc.move_cost,
             tc.total_cost - tc.assign_cost - tc.move_cost               as only_carry_total_time_consuming,
             t.order_create_time,
             tc.order_update_time,
             tj.dispatch_robot_code_str,                                                  -- 分配的机器人
             tj.dispatch_robot_type_code_str,
             tj.dispatch_robot_type_name_str,
             tj.dispatch_first_classification_str,
             tj.dispatch_first_classification_name_str,
             tj.dispatch_robot_code_num

      from ${dwd_dbname}.dwd_phx_rss_transport_order_carrier_cost_info_di tc
               left join ${dwd_dbname}.dwd_phx_rss_transport_order_info_di t
                         on tc.project_code = t.project_code and t.order_no = tc.order_no and
                            t.d >= DATE_ADD('${pre1_date}', -1)
               left join
           (select tc.project_code,
                   tc.order_no,
                   concat_ws(',', collect_set(tj.robot_code))           as dispatch_robot_code_str, -- 分配的机器人
                   concat_ws(',', collect_set(tr.robot_type_code))      as dispatch_robot_type_code_str,
                   concat_ws(',', collect_set(tr.robot_type_name))      as dispatch_robot_type_name_str,
                   concat_ws(',', collect_set(tr.first_classification)) as dispatch_first_classification_str,
                   concat_ws(',', collect_set(
                           case
                               when tr.first_classification = 'WORKBIN' then '料箱车'
                               when tr.first_classification = 'STOREFORKBIN' then '存储一体式'
                               when tr.first_classification = 'CARRIER' then '潜伏式机器人'
                               when tr.first_classification = 'ROLLER' then '辊筒机器人'
                               when tr.first_classification = 'FORKLIFT' then '堆高全向车'
                               when tr.first_classification = 'DELIVER' then '投递车'
                               when tr.first_classification = 'SC' then '四向穿梭车'
                               end))                                    as dispatch_first_classification_name_str,
                   count(distinct tj.robot_code)                        as dispatch_robot_code_num
            from ${dwd_dbname}.dwd_phx_rss_transport_order_carrier_cost_info_di tc
                     left join ${dwd_dbname}.dwd_phx_rss_transport_order_carrier_job_info_di tj
                               on tj.project_code = tc.project_code and tj.order_no = tc.order_no and
                                  tj.d >= DATE_ADD('${pre1_date}', -1)
                     left join ${dwd_dbname}.dwd_phx_basic_robot_base_info_df tr
                               on tr.project_code = tc.project_code and tr.robot_code = tj.robot_code and tr.d = tj.d
            where tc.d >= '${pre1_date}'
            group by tc.project_code, tc.order_no) tj on tj.order_no = tc.order_no
      where tc.d >= '${pre1_date}') td on td.project_code = c.project_code