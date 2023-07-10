-- ads.ads_lite_amr_breakdown


-- 凤凰
with phx_robot_info as (
select tr.d,
       tr.project_code,
       tr.robot_code,
       tr.robot_type_code,
       tr.robot_type_name
from ${dwd_dbname}.dwd_phx_basic_robot_base_info_df tr
         inner join
     (SELECT project_code
      FROM ${dim_dbname}.dim_collection_project_record_ful
      where project_version like '3%') thp on thp.project_code = tr.project_code
where tr.d >= '${pre11_date}'
  and tr.d <= DATE_ADD(current_date(), -1)
  and tr.robot_usage_state = 'using'
),
-- 搬运作业单机器人任务明细
phx_create_order_job_detail as (
select to_date(t1.order_create_time) as cur_date,
       t1.project_code,
       t1.order_create_time,
       t1.order_no,
	   t1.order_state, 
       t2.job_create_time,
       t2.job_sn,
	   t2.job_state,
       t2.robot_code,
       tr.robot_type_code,
       tr.robot_type_name
from ${dwd_dbname}.dwd_phx_rss_transport_order_info_di t1
         left join ${dwd_dbname}.dwd_phx_rss_transport_order_carrier_job_info_di t2
                   on t2.pt = t1.pt and t2.order_id = t1.id and t2.d >= '${pre11_date}'
         left join ${dwd_dbname}.dwd_phx_basic_robot_base_info_df tr
                   on tr.pt = t2.pt and tr.d = t2.d and tr.robot_code = t2.robot_code and tr.d >= '${pre11_date}'
where t1.d >= '${pre11_date}'
),
-- 所有符合执行范围内的机器人故障明细
phx_robot_error_day_duration_detail as (
select t.project_code,
       t.id,
       t.robot_code,
       t.robot_type_code,
       t.robot_type_name,
       t.first_classification,
       t.error_code,
       t.error_name,
       t.error_start_time,
       t.error_end_time,
       t.error_level,
       t.error_detail,
       t.error_module,
       t.error_start_date,
       t.stat_date,
       t.stat_error_start_time,
       t.stat_error_end_time,
       unix_timestamp(from_unixtime(unix_timestamp(t.stat_error_end_time), 'yyyy-MM-dd HH:mm:ss')) -
       unix_timestamp(from_unixtime(unix_timestamp(t.stat_error_start_time),'yyyy-MM-dd HH:mm:ss')) as stat_error_duration,
       t.pos
from (select tmp.project_code,
             tmp.id,
             tmp.robot_code,
             tmp.robot_type_code,
             tmp.robot_type_name,
             tmp.first_classification,
             tmp.error_code,
             tmp.error_name,
             tmp.error_start_time,
             tmp.error_end_time,
             tmp.error_level,
             tmp.error_detail,
             tmp.error_module,
             to_date(tmp.error_start_time)                                                     as error_start_date,
             if(b.pos = 0, to_date(tmp.error_start_time), date_add(tmp.error_start_time, pos)) as stat_date,
             if(b.pos = 0, tmp.error_start_time,date_format(date_add(tmp.error_start_time, pos), 'yyyy-MM-dd 00:00:00')) as stat_error_start_time,
             case
                 when b.pos = 0 and DATEDIFF(tmp.error_end_time, tmp.error_start_time) = b.pos then tmp.error_end_time
                 when DATEDIFF(tmp.error_end_time, tmp.error_start_time) != b.pos then date_format(date_add(tmp.error_start_time, pos + 1), 'yyyy-MM-dd 00:00:00')
                 when b.pos != 0 and DATEDIFF(tmp.error_end_time, tmp.error_start_time) = b.pos then tmp.error_end_time end as stat_error_end_time,
             b.pos
      from (select te.project_code,
                   te.id,
                   te.robot_code,
                   te.robot_type_code,
                   te.robot_type_name,
                   te.first_classification,
                   te.error_code,
                   tde.error_name,
                   te.error_start_time,
                   te.error_end_time,
                   te.error_level,
                   te.error_detail,
                   te.error_module
            from ${dwd_dbname}.dwd_phx_robot_breakdown_astringe_v1_di te
                     left join ${dim_dbname}.dim_phx_basic_error_info_ful tde on tde.error_code = te.error_code
            where te.d >= DATE_ADD('${pre11_date}',-10)
              and te.d <= DATE_ADD(current_date(), -1)
              and te.error_module = 'robot'
              and te.error_level >= 3) tmp
               lateral view posexplode(split(repeat('o', (DATEDIFF(tmp.error_end_time, tmp.error_start_time))),'o')) b) t
),
-- 机器人每日理论开始结束时间
phx_robot_state_change_detail as (
select project_code,
       robot_code,
       cur_date,
       day_first_run_state_start_time,
       day_first_run_state_end_time,
       day_theory_run_start_time,
       day_theory_run_end_time,
       COALESCE(sort_array(ARRAY(day_first_run_state_start_time, day_theory_run_start_time))[0],
                sort_array(ARRAY(day_first_run_state_start_time, day_theory_run_start_time))[1]) as theory_start_time,
       sort_array(ARRAY(day_first_run_state_end_time, day_theory_run_end_time))[1]               as theory_end_time
from (select ts.project_code,
             ts.robot_code,
             to_date(ts.create_time)                                                            as cur_date,
             min(case
                     when (ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1)
                         then ts.create_time end)                                                  day_first_run_state_start_time,
             max(case
                     when (ts.pre1_online_state = 'REGISTERED' or ts.pre1_work_state = 'ERROR' or ts.pre1_is_error = 1)
                         then ts.create_time end)                                                  day_first_run_state_end_time,
             min(case
                     when ts.asc_rk = 1 and
                          (ts.pre1_online_state = 'REGISTERED' or ts.pre1_work_state = 'ERROR' or ts.pre1_is_error = 1)
                         then DATE_FORMAT(to_date(create_time), 'yyyy-MM-dd 00:00:00') end)     as day_theory_run_start_time,
             max(case
                     when ts.desc_rk = 1 and
                          (ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1)
                         then DATE_FORMAT(date_add(create_time, 1), 'yyyy-MM-dd 00:00:00') end) as day_theory_run_end_time
      from (select project_code,
                   robot_code,
                   id                                                                                                      as state_id,
                   create_time,
                   network_state,
                   online_state,
                   work_state,
                   is_error,
                   ROW_NUMBER() over (PARTITION by project_code,robot_code,to_date(create_time) order by create_time asc)  as asc_rk,
                   ROW_NUMBER() over (PARTITION by project_code,robot_code,to_date(create_time) order by create_time desc) as desc_rk,
                   lag(create_time, 1)
                       over (PARTITION by project_code,robot_code order by create_time asc)                                as pre1_create_time,
                   lag(network_state, 1)
                       over (PARTITION by project_code,robot_code order by create_time asc)                                as pre1_network_state,
                   lag(online_state, 1)
                       over (PARTITION by project_code,robot_code order by create_time asc)                                as pre1_online_state,
                   lag(work_state, 1)
                       over (PARTITION by project_code,robot_code order by create_time asc)                                as pre1_work_state,
                   lag(is_error, 1)
                       over (PARTITION by project_code,robot_code order by create_time asc)                                as pre1_is_error,
                   lead(create_time, 1)
                        over (PARTITION by project_code,robot_code order by create_time asc)                               as next1_create_time,
                   lead(network_state, 1)
                        over (PARTITION by project_code,robot_code order by create_time asc)                               as next1_network_state,
                   lead(online_state, 1)
                        over (PARTITION by project_code,robot_code order by create_time asc)                               as next1_online_state,
                   lead(work_state, 1)
                        over (PARTITION by project_code,robot_code order by create_time asc)                               as next1_work_state,
                   lead(is_error, 1)
                        over (PARTITION by project_code,robot_code order by create_time asc)                               as next1_is_error
            from ${dwd_dbname}.dwd_phx_rms_robot_state_info_di
            where d >= DATE_ADD('${pre11_date}', -10)) ts
      group by ts.project_code, ts.robot_code, to_date(ts.create_time)) t 
)
 
 
 
 
SELECT ''                                                                        AS id,                  -- 主键
       NULL                                                                      AS data_time,           -- 数据产生时间（业务无关）
       bd.error_id_list                                                          as breakdown_id,        -- 故障编码
       ba.robot_code                                                             AS amr_code,            -- 机器人编码
       ba.robot_type_code                                                        AS amr_type,            -- 机器人类型
       nvl(mw.create_order_num, 0)                                               AS carry_order_num,     -- 搬运任务数量
       nvl(mw.completed_order_num, 0)                                            AS right_order_num,     -- 正常完成的搬运作业单数量
       nvl(mw.create_job_num, 0)                                                 AS amr_task,            -- 机器人任务数量
       nvl(ct.charger_times, 0)                                                  AS total_charge,        -- 充电次数
       nvl(ct.unusual_charger_times, 0)                                          AS exc_charge,          -- 充电异常次数
       nvl(bd.error_duration, 0)                                                 AS error_duration,      -- 机器人故障时长
       nvl(mttr.end_error_num, 0)                                                AS mttr_error_num,      -- mttr故障次数
       nvl(mttr.end_error_duration, 0)                                           AS mttr_error_duration, -- mttr故障时长
       SUBSTR(ad.theory_start_time, 12, 8)                                       AS start_time,          -- 开始运行时段
       SUBSTR(ad.theory_end_time, 12, 8)                                         AS end_time,            -- 结束运行时段
       unix_timestamp(ad.theory_end_time) - unix_timestamp(ad.theory_start_time) AS actual_duration,     -- 时间运行时间
       c.project_code,                                                                                   -- 项目编码
       ba.d                                                                      AS happen_time,         -- 统计日期
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')                   AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')                   AS update_time,
       abi.create_error_id_list                                                  as add_breakdown_id,    -- 新增故障id
       ba.d,
       c.project_code                                                            AS pt
FROM (SELECT *
      FROM ${dim_dbname}.dim_collection_project_record_ful
      WHERE project_version like '3.%') c
         left join phx_robot_info ba on c.project_code = ba.project_code
         left join
     (select cur_date,
             project_code,
             robot_code,
             count(distinct order_no)                                              as create_order_num,
             count(distinct job_sn)                                                as create_job_num,
             count(distinct case when order_state = 'COMPLETED' then order_no end) as completed_order_num,
             count(distinct case when job_state = 'DONE' then job_sn end)          as done_order_num
      from phx_create_order_job_detail
      group by cur_date, project_code, robot_code) mw
     ON mw.cur_date = ba.d AND mw.project_code = ba.project_code AND mw.robot_code = ba.robot_code
         left join
     (select stat_date                                       as cur_date,
             project_code,
             robot_code,
             concat_ws(',', collect_set(cast(id as string))) as error_id_list,
             sum(stat_error_duration)                        as error_duration
      from phx_robot_error_day_duration_detail
      group by stat_date, project_code, robot_code) bd
     on bd.cur_date = ba.d AND bd.project_code = ba.project_code AND bd.robot_code = ba.robot_code
         left join
     (select TO_DATE(error_start_time)                       as cur_date,
             project_code,
             robot_code,
             concat_ws(',', collect_set(cast(id as string))) as create_error_id_list
      from phx_robot_error_day_duration_detail
      group by TO_DATE(error_start_time), project_code, robot_code) abi
     on abi.cur_date = ba.d AND abi.project_code = ba.project_code AND abi.robot_code = ba.robot_code
         left join
     (select TO_DATE(error_end_time)                                                as cur_date,
             project_code,
             robot_code,
             count(distinct id)                                                     as end_error_num,
             sum(unix_timestamp(error_end_time) - unix_timestamp(error_start_time)) as end_error_duration
      from phx_robot_error_day_duration_detail
      group by TO_DATE(error_end_time), project_code, robot_code) mttr
     on mttr.cur_date = ba.d AND mttr.project_code = ba.project_code AND mttr.robot_code = ba.robot_code
         left join phx_robot_state_change_detail ad
                   on ad.cur_date = ba.d AND ad.project_code = ba.project_code AND ad.robot_code = ba.robot_code
         left join
     (select d,
             project_code,
             robot_code,
             count(distinct job_sn)                  as charger_times,
             SUM(IF(job_state != 'COMPLETED', 1, 0)) AS unusual_charger_times
      from ${dwd_dbname}.dwd_phx_rms_job_history_info_di
      where d >= '${pre11_date}'
        and job_type = 'CHARGE'
      group by d, project_code, robot_code) ct
     on ct.d = ba.d AND ct.project_code = ba.project_code AND ct.robot_code = ba.robot_code