set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=5000;




-- 机器人小时列表
with phx_hour_robot_list_detail  as (
select tt.cur_week,        -- 统计星期
       tt.cur_date,        -- 统计日期
       tt.cur_hour,        -- 统计小时
       tt.project_code,    -- 项目编码
       tr.robot_code,      -- 机器人编码
       tr.robot_type_code, -- 机器人类型编码
       tr.robot_type_name  -- 机器人类型名称
-- 时间维度信息
from (select t1.cur_week,     -- 统计星期
             t1.cur_date,     -- 统计日期
             t1.cur_hour,     -- 统计小时
             thp.project_code -- 项目编码
      from (SELECT WEEKOFYEAR(t1.days)                                                               as cur_week,
                   t1.days                                                                           as cur_date,
                   date_format(concat(t1.days, ' ', tt1.hourofday, ':00:00'), 'yyyy-MM-dd HH:00:00') as cur_hour
            FROM ${dim_dbname}.dim_day_date t1
                     LEFT JOIN ${dim_dbname}.dim_day_of_hour tt1
            WHERE t1.days >= '${pre1_date}'
              AND t1.days <= DATE_ADD(current_date(), -1)) t1
               left join
           (SELECT project_code
            FROM ${dim_dbname}.dim_collection_project_record_ful
            where project_version like '3%') thp) tt
-- 机器人信息
         join ${dwd_dbname}.dwd_phx_basic_robot_base_info_df tr
              on tr.d >= '${pre1_date}' and tr.d <= DATE_ADD(current_date(), -1) and tr.robot_usage_state = 'using' and
                 tr.pt = tt.project_code and tr.d = tt.cur_date
),
-- 机器人每小时故障时长明细
phx_hour_robot_error_duration as (
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
       case when t.error_end_time is not null then (nvl(unix_timestamp(from_unixtime(unix_timestamp(t.error_end_time),'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(from_unixtime(unix_timestamp(t.error_start_time),'yyyy-MM-dd HH:mm:ss')), 0) * 1000 + nvl(cast(SUBSTRING(rpad(t.error_end_time, 23, '0'), 21, 3) as int),0) - nvl(cast(SUBSTRING(rpad(t.error_start_time, 23, '0'), 21, 3) as int),0)) / 1000 end as mttr_error_duration,   -- 故障时长	   
       t.error_level,
       t.error_detail,
       t.error_module,
       t.cur_date,
	   date_format(t.cur_hour, 'yyyy-MM-dd HH:00:00') as cur_hour,
       t.stat_start_time,
       t.stat_end_time,
       case when t.stat_end_time is not null then (nvl(unix_timestamp(from_unixtime(unix_timestamp(t.stat_end_time),'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(from_unixtime(unix_timestamp(t.stat_start_time),'yyyy-MM-dd HH:mm:ss')), 0) * 1000 + nvl(cast(SUBSTRING(rpad(t.stat_end_time, 23, '0'), 21, 3) as int),0) - nvl(cast(SUBSTRING(rpad(t.stat_start_time, 23, '0'), 21, 3) as int),0)) / 1000 end as stat_error_duration,   -- 机器人在小时内持续的故障时长
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
             to_date(tmp.error_start_time)                                                                 as cur_date,
             if(b.pos = 0, tmp.hour_value,from_unixtime((unix_timestamp(tmp.hour_value) + b.pos * 3600), 'yyyy-MM-dd HH:mm:ss.000')) as cur_hour,
             if(b.pos = 0, tmp.error_start_time, from_unixtime((unix_timestamp(tmp.hour_value) + b.pos * 3600),'yyyy-MM-dd HH:mm:ss.000')) as stat_start_time,
             case when b.pos = 0 and DATEDIFF(tmp.error_end_time, tmp.error_start_time) * 24 + hour(tmp.error_end_time) - hour(tmp.error_start_time) = b.pos then tmp.error_end_time 
			      when DATEDIFF(tmp.error_end_time, tmp.error_start_time) * 24 + hour(tmp.error_end_time) - hour(tmp.error_start_time) != b.pos then from_unixtime((unix_timestamp(tmp.hour_value) + (b.pos + 1) * 3600), 'yyyy-MM-dd HH:mm:ss.000') 
				  when b.pos != 0 and DATEDIFF(tmp.error_end_time, tmp.error_start_time) * 24 + hour(tmp.error_end_time) - hour(tmp.error_start_time) = b.pos then tmp.error_end_time end as stat_end_time,
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
                   te.error_module,
                   date_format(error_start_time, 'yyyy-MM-dd HH:00:00.000') as hour_value
            from ${dwd_dbname}.dwd_phx_robot_breakdown_astringe_v1_di te
                     left join ${dim_dbname}.dim_phx_basic_error_info_ful tde
                               on tde.project_code = te.project_code and tde.error_code = te.error_code
            where te.d >= DATE_ADD('${pre1_date}', -10)
              and te.d <= DATE_ADD(current_date(), -1)
              and te.error_module = 'robot'
              and te.error_level >= 3) tmp
               lateral view posexplode(split(repeat('o', (DATEDIFF(tmp.error_end_time, tmp.error_start_time) * 24 +hour(tmp.error_end_time) - hour(tmp.error_start_time))),'o')) b) t
),
-- 机器人每小时理论运行时长
phx_hour_robot_theory_run_duration as (
select t.project_code,
       t.robot_code,
       tr.robot_type_code,
       tr.robot_type_name,
	   t.cur_date,
       date_format(t.cur_hour, 'yyyy-MM-dd HH:00:00') as cur_hour,
       sum(state_duration)                            as theory_run_duration -- 理论运行时长（秒）
from (select t.state_id,
             t.project_code,
             t.robot_code,
             t.create_time,
             t.stat_next_create_time,
             t.network_state,
             t.online_state,
             t.work_state,
             t.is_error,
             t.cur_date,
             t.cur_hour,
             t.stat_start_time,
             t.stat_end_time,
             case when t.stat_end_time is not null then (nvl(unix_timestamp(from_unixtime(unix_timestamp(t.stat_end_time), 'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(from_unixtime(unix_timestamp(t.stat_start_time), 'yyyy-MM-dd HH:mm:ss')), 0) * 1000 + nvl(cast(SUBSTRING(rpad(t.stat_end_time, 23, '0'), 21, 3) as int),0) - nvl(cast(SUBSTRING(rpad(t.stat_start_time, 23, '0'), 21, 3) as int),0)) / 1000 end as state_duration, -- 机器人在小时内持续的故障时长
             t.pos
      from (select tmp.state_id,
                   tmp.project_code,
                   tmp.robot_code,
                   tmp.create_time,
                   tmp.stat_next_create_time,
                   tmp.network_state,
                   tmp.online_state,
                   tmp.work_state,
                   tmp.is_error,
                   to_date(tmp.create_time)                                                                      as cur_date,
                   if(b.pos = 0, tmp.hour_value, from_unixtime((unix_timestamp(tmp.hour_value) + b.pos * 3600),'yyyy-MM-dd HH:mm:ss.000'))                       as cur_hour,
                   if(b.pos = 0, tmp.create_time, from_unixtime((unix_timestamp(tmp.hour_value) + b.pos * 3600),'yyyy-MM-dd HH:mm:ss.000'))                      as stat_start_time,
                   case when b.pos = 0 and DATEDIFF(tmp.stat_next_create_time, tmp.create_time) * 24 + hour(tmp.stat_next_create_time) - hour(tmp.create_time) = b.pos then tmp.stat_next_create_time
                        when DATEDIFF(tmp.stat_next_create_time, tmp.create_time) * 24 + hour(tmp.stat_next_create_time) - hour(tmp.create_time) != b.pos then from_unixtime((unix_timestamp(tmp.hour_value) + (b.pos + 1) * 3600), 'yyyy-MM-dd HH:mm:ss.000')
                        when b.pos != 0 and DATEDIFF(tmp.stat_next_create_time, tmp.create_time) * 24 + hour(tmp.stat_next_create_time) - hour(tmp.create_time) = b.pos then tmp.stat_next_create_time end as stat_end_time,
                   b.pos
            from (select ts.state_id,
                         ts.project_code,
                         ts.robot_code,
                         ts.create_time,
                         date_format(ts.create_time, 'yyyy-MM-dd HH:00:00.000')                             as           hour_value,
                         ts.network_state,
                         ts.online_state,
                         ts.work_state,
                         ts.is_error,
                         lead(ts.create_time, 1) over (PARTITION by ts.project_code,ts.robot_code order by ts.create_time asc) as           next_create_time,
                         case when to_date(ts.create_time) <> to_date(lead(ts.create_time, 1) over (PARTITION by ts.project_code,ts.robot_code order by ts.create_time asc)) or lead(ts.create_time, 1) over (PARTITION by ts.project_code,ts.robot_code order by ts.create_time asc) is null then date_format(DATE_ADD(ts.create_time, 1), 'yyyy-MM-dd 00:00:00.000') else lead(ts.create_time, 1) over (PARTITION by ts.project_code,ts.robot_code order by ts.create_time asc) end stat_next_create_time
                  from (
                           -- 每辆小车每天第一刻数据
                           select td.id                                                     as state_id,
                                  td.project_code,
                                  td.robot_code,
                                  date_format(DATE_ADD(td.d, 1), 'yyyy-MM-dd 00:00:00.000') as create_time,
                                  td.network_state,
                                  td.online_state,
                                  td.work_state,
                                  td.is_error
                           from dwd.dwd_phx_rms_robot_state_daily_info_di td
                           where td.d >= DATE_ADD('${pre1_date}', -1)
                             and td.d < DATE_ADD(current_date(), -1)
                             and create_time is not null
                           -- 每辆小车每天状态变更数据
                           union all
                           select t.id as state_id,
                                  t.project_code,
                                  t.robot_code,
                                  t.create_time,
                                  t.network_state,
                                  t.online_state,
                                  t.work_state,
                                  t.is_error
                           from ${dwd_dbname}.dwd_phx_rms_robot_state_info_di t
                           where t.d >= '${pre1_date}'
                             and t.d <= DATE_ADD(current_date(), -1)) ts) tmp
                     lateral view posexplode(split(repeat('o',(DATEDIFF(tmp.stat_next_create_time, tmp.create_time) * 24 + hour(tmp.stat_next_create_time) - hour(tmp.create_time))),'o')) b) t) t
					 left join ${dwd_dbname}.dwd_phx_basic_robot_base_info_df tr on tr.project_code =t.project_code and tr.robot_code=t.robot_code and tr.d=t.cur_date	 
where t.online_state = 'REGISTERED'
   or t.work_state = 'ERROR'
   or t.is_error = 1
group by t.project_code, t.robot_code,tr.robot_type_code,tr.robot_type_name, t.cur_date,date_format(t.cur_hour, 'yyyy-MM-dd HH:00:00')
),
-- 搬运作业单机器人任务明细
phx_create_order_job_detail as (
select t1.project_code,
       t1.order_create_time,
       t1.order_no,
       t2.job_create_time,
       t2.job_sn,
       t2.robot_code,
       tr.robot_type_code,
       tr.robot_type_name
from ${dwd_dbname}.dwd_phx_rss_transport_order_info_di t1
         left join ${dwd_dbname}.dwd_phx_rss_transport_order_carrier_job_info_di t2
                   on t2.pt = t1.pt and t2.order_id = t1.id and t2.d >= '${pre1_date}'
         left join ${dwd_dbname}.dwd_phx_basic_robot_base_info_df tr
                   on tr.pt = t2.pt and tr.d = t2.d and tr.robot_code = t2.robot_code and tr.d >= '${pre1_date}'
where t1.d >= '${pre1_date}'
)




-- INSERT overwrite table ${ads_dbname}.ads_phx_amr_breakdown partition(d,pt)
-- 分时分小车
SELECT 
'' as id, -- 主键
rl.cur_hour as data_time, -- 统计小时
rl.project_code, -- 项目编码
rl.cur_hour as happen_time, -- 统计小时
'single' as type_class, -- 数据类型
rl.robot_type_code as amr_type,  -- 机器人类型编码
rl.robot_type_name as amr_type_des,    -- 机器人类型名称
rl.robot_code as amr_code,  -- 机器人编码
ted.keep_error_list as breakdown_id, -- 故障集合
COALESCE(tor.create_order_num,0) as carry_order_num,  -- 搬运作业单胆量
COALESCE(tor.create_order_job_num,0) as carry_task_num,  -- 机器人任务量
nvl(trd.theory_run_duration,0) as theory_time, -- 理论运行时长
nvl(ted.error_duration,0) as error_duration, -- 故障时长
nvl(ted.end_error_sum_duration,0) as mttr_error_duration, -- mttr故障时长
nvl(ted.end_error_num,0) as mttr_error_num, -- mttr故障次数
cast(nvl((tadd.add_theory_time-tadd.add_error_duration)/tadd.add_error_num,tadd.add_theory_time) as decimal(10,2))  as add_mtbf, -- 累计mtbf
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
ted.start_error_id_list as add_breakdown_id, -- 新增故障id集合
SUBSTR(rl.cur_hour,1,10) as d,
rl.project_code as pt

from phx_hour_robot_list_detail rl 
left join (select project_code,
                  robot_code,
                  cur_date,
                  cur_hour,
                  sum(stat_error_duration)                                                   as error_duration,
                  count(distinct case when pos = 0 then id end)                              as start_error_num,
                  concat_ws(',', collect_set(case when pos = 0 then cast(id as string) end)) as start_error_id_list,
                  concat_ws(',', collect_set(cast(id as string)))                            as keep_error_list,
				  count(distinct case when date_format(error_end_time, 'yyyy-MM-dd HH:00:00')=cur_hour then id end) as end_error_num,
				  sum(case when date_format(error_end_time, 'yyyy-MM-dd HH:00:00')=cur_hour then mttr_error_duration end) as end_error_sum_duration
           from phx_hour_robot_error_duration
           group by project_code, robot_code, cur_date, cur_hour) ted
          on ted.project_code = rl.project_code and ted.robot_code = rl.robot_code and ted.cur_date = rl.cur_date and ted.cur_hour = rl.cur_hour
left join 
(select project_code,
       robot_code,
       to_date(order_create_time)                            as cur_date,
       date_format(order_create_time, 'yyyy-MM-dd HH:00:00') as cur_hour,
       count(distinct order_no)                              as create_order_num,
       count(distinct job_sn)                                as create_order_job_num
from phx_create_order_job_detail
group by project_code, robot_code, to_date(order_create_time), date_format(order_create_time, 'yyyy-MM-dd HH:00:00')
)tor on tor.project_code=rl.project_code and tor.robot_code=rl.robot_code and tor.cur_date=rl.cur_date and tor.cur_hour=rl.cur_hour
left join phx_hour_robot_theory_run_duration trd on trd.project_code=rl.project_code and trd.robot_code=rl.robot_code and trd.cur_date=rl.cur_date and trd.cur_hour=rl.cur_hour
left join 
(select t.project_code,
       t.robot_code,
       t.robot_type_code,
       t.robot_type_name,
       t.cur_hour,
       add_theory_time,
       add_error_duration,
       add_error_num
FROM ${tmp_dbname}.tmp_phx_error_mtbf_add t
where t.d >= '${pre1_date}' and t.d <= DATE_ADD(current_date(), -1)
)tadd on tadd.project_code=rl.project_code and tadd.robot_code=rl.robot_code and tadd.cur_hour=rl.cur_hour


-- 分时分机器人类型
UNION ALL 
SELECT 
'' as id, -- 主键
rl.cur_hour as data_time, -- 统计小时
rl.project_code, -- 项目编码
rl.cur_hour as happen_time, -- 统计小时
'part' as type_class, -- 数据类型
rl.robot_type_code as amr_type,  -- 机器人类型编码
rl.robot_type_name as amr_type_des,    -- 机器人类型名称
null as amr_code,  -- 机器人编码
ted.keep_error_list as breakdown_id, -- 故障集合
COALESCE(tor.create_order_num,0) as carry_order_num,  -- 搬运作业单胆量
COALESCE(tor.create_order_job_num,0) as carry_task_num,  -- 机器人任务量
nvl(trd.theory_run_duration,0) as theory_time, -- 理论运行时长
nvl(ted.error_duration,0) as error_duration, -- 故障时长
nvl(ted.end_error_sum_duration,0) as mttr_error_duration, -- mttr故障时长
nvl(ted.end_error_num,0) as mttr_error_num, -- mttr故障次数
cast(nvl((tadd.add_theory_time-tadd.add_error_duration)/tadd.add_error_num,tadd.add_theory_time) as decimal(10,2))  as add_mtbf, -- 累计mtbf
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
ted.start_error_id_list as add_breakdown_id, -- 新增故障id集合
SUBSTR(rl.cur_hour,1,10) as d,
rl.project_code as pt

from 
(select cur_date,
       cur_hour,
       project_code,
       robot_type_code,
       robot_type_name
from phx_hour_robot_list_detail
group by cur_date, cur_hour, project_code, robot_type_code, robot_type_name)rl 
left join (select project_code,
                  robot_type_code,
				  robot_type_name,
                  cur_date,
                  cur_hour,
                  sum(stat_error_duration)                                                   as error_duration,
                  count(distinct case when pos = 0 then id end)                              as start_error_num,
                  concat_ws(',', collect_set(case when pos = 0 then cast(id as string) end)) as start_error_id_list,
                  concat_ws(',', collect_set(cast(id as string)))                            as keep_error_list,
				  count(distinct case when date_format(error_end_time, 'yyyy-MM-dd HH:00:00')=cur_hour then id end) as end_error_num,
				  sum(case when date_format(error_end_time, 'yyyy-MM-dd HH:00:00')=cur_hour then mttr_error_duration end) as end_error_sum_duration
           from phx_hour_robot_error_duration
           group by project_code, robot_type_code,robot_type_name, cur_date, cur_hour) ted
          on ted.project_code = rl.project_code and ted.robot_type_code = rl.robot_type_code and ted.robot_type_name = rl.robot_type_name and ted.cur_date = rl.cur_date and ted.cur_hour = rl.cur_hour

left join 
(select project_code,
       robot_type_code,
       robot_type_name, 
       to_date(order_create_time)                            as cur_date,
       date_format(order_create_time, 'yyyy-MM-dd HH:00:00') as cur_hour,
       count(distinct order_no)                              as create_order_num,
       count(distinct job_sn)                                as create_order_job_num
from phx_create_order_job_detail
group by project_code, robot_type_code,robot_type_name, to_date(order_create_time), date_format(order_create_time, 'yyyy-MM-dd HH:00:00')
)tor on tor.project_code=rl.project_code and tor.robot_type_code=rl.robot_type_code and tor.robot_type_name=rl.robot_type_name and tor.cur_date=rl.cur_date and tor.cur_hour=rl.cur_hour
left join 
(select project_code,
       robot_type_code,
       robot_type_name,
       cur_date,
       cur_hour,
       sum(theory_run_duration) as theory_run_duration
from phx_hour_robot_theory_run_duration
group by project_code, robot_type_code, robot_type_name, cur_date, cur_hour
)trd on trd.project_code=rl.project_code and trd.robot_type_code=rl.robot_type_code and trd.robot_type_name=rl.robot_type_name and trd.cur_date=rl.cur_date and trd.cur_hour=rl.cur_hour
left join 
(select t.project_code,
       t.cur_hour,
       t.robot_type_code,
       t.robot_type_name,
       sum(add_theory_time)    as add_theory_time,
       sum(add_error_duration) as add_error_duration,
       sum(add_error_num)      as add_error_num
FROM ${tmp_dbname}.tmp_phx_error_mtbf_add t
where t.d >= '${pre1_date}'
  and t.d <= DATE_ADD(current_date(), -1)
group by t.project_code, t.cur_hour, t.robot_type_code, t.robot_type_name
)tadd on tadd.project_code=rl.project_code and tadd.robot_type_code=rl.robot_type_code and tadd.robot_type_name=rl.robot_type_name and tadd.cur_hour=rl.cur_hour

-- 分时分项目
UNION ALL 
SELECT 
'' as id, -- 主键
rl.cur_hour as data_time, -- 统计小时
rl.project_code, -- 项目编码
rl.cur_hour as happen_time, -- 统计小时
'all' as type_class, -- 数据类型
null as amr_type,  -- 机器人类型编码
null as amr_type_des,    -- 机器人类型名称
null as amr_code,  -- 机器人编码
ted.keep_error_list as breakdown_id, -- 故障集合
COALESCE(tor.create_order_num,0) as carry_order_num,  -- 搬运作业单胆量
COALESCE(tor.create_order_job_num,0) as carry_task_num,  -- 机器人任务量
nvl(trd.theory_run_duration,0) as theory_time, -- 理论运行时长
nvl(ted.error_duration,0) as error_duration, -- 故障时长
nvl(ted.end_error_sum_duration,0) as mttr_error_duration, -- mttr故障时长
nvl(ted.end_error_num,0) as mttr_error_num, -- mttr故障次数
cast(nvl((tadd.add_theory_time-tadd.add_error_duration)/tadd.add_error_num,tadd.add_theory_time) as decimal(10,2))  as add_mtbf, -- 累计mtbf
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
ted.start_error_id_list as add_breakdown_id, -- 新增故障id集合
SUBSTR(rl.cur_hour,1,10) as d,
rl.project_code as pt

from 
(select cur_date,
       cur_hour,
       project_code
from phx_hour_robot_list_detail
group by cur_date, cur_hour, project_code)rl 
left join (select project_code,
                  cur_date,
                  cur_hour,
                  sum(stat_error_duration)                                                   as error_duration,
                  count(distinct case when pos = 0 then id end)                              as start_error_num,
                  concat_ws(',', collect_set(case when pos = 0 then cast(id as string) end)) as start_error_id_list,
                  concat_ws(',', collect_set(cast(id as string)))                            as keep_error_list,
				  count(distinct case when date_format(error_end_time, 'yyyy-MM-dd HH:00:00')=cur_hour then id end) as end_error_num,
				  sum(case when date_format(error_end_time, 'yyyy-MM-dd HH:00:00')=cur_hour then mttr_error_duration end) as end_error_sum_duration
           from phx_hour_robot_error_duration
           group by project_code,cur_date, cur_hour) ted
          on ted.project_code = rl.project_code and ted.cur_date = rl.cur_date and ted.cur_hour = rl.cur_hour

left join 
(select project_code,
       to_date(order_create_time)                            as cur_date,
       date_format(order_create_time, 'yyyy-MM-dd HH:00:00') as cur_hour,
       count(distinct order_no)                              as create_order_num,
       count(distinct job_sn)                                as create_order_job_num
from phx_create_order_job_detail
group by project_code, to_date(order_create_time), date_format(order_create_time, 'yyyy-MM-dd HH:00:00')
)tor on tor.project_code=rl.project_code and tor.cur_date=rl.cur_date and tor.cur_hour=rl.cur_hour
left join 
(select project_code,
       cur_date,
       cur_hour,
       sum(theory_run_duration) as theory_run_duration
from phx_hour_robot_theory_run_duration
group by project_code,cur_date, cur_hour
)trd on trd.project_code=rl.project_code and trd.cur_date=rl.cur_date and trd.cur_hour=rl.cur_hour
left join 
(select t.project_code,
       t.cur_hour,
       sum(add_theory_time)    as add_theory_time,
       sum(add_error_duration) as add_error_duration,
       sum(add_error_num)      as add_error_num
FROM ${tmp_dbname}.tmp_phx_error_mtbf_add t
where t.d >= '${pre1_date}'
  and t.d <= DATE_ADD(current_date(), -1)
group by t.project_code, t.cur_hour
)tadd on tadd.project_code=rl.project_code and tadd.cur_hour=rl.cur_hour