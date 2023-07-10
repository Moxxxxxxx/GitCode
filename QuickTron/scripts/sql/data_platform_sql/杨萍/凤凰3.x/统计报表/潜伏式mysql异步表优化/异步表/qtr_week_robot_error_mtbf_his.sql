set @now_time=sysdate();   --  当前时间
set @dt_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @dt_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间
set @dt_week_start_time=date_format(DATE_SUB(sysdate(),INTERVAL WEEKDAY(sysdate()) + 0 DAY), '%Y-%m-%d 00:00:00'); -- 当前一周的开始时间
set @dt_next_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) -7 DAY), '%Y-%m-%d 00:00:00'); --  下一周的开始时间
select @now_time,@dt_hour_start_time,@dt_next_hour_start_time,@dt_day_start_time,@dt_next_day_start_time,@dt_week_start_time,@dt_next_week_start_time;

-- 插入数据（mysql参数）
-- insert into qt_smartreport.qtr_week_robot_error_mtbf_his(create_time,update_time,date_value,week_start_time,next_week_start_time,robot_code,theory_run_duration,error_duration,error_num,mtbf,accum_theory_run_duration,accum_error_duration,accum_error_num,accum_mtbf)
select 
@now_time as create_time,
@now_time as update_time,
date(@dt_week_start_time) as date_value,
@dt_week_start_time as week_start_time,
@dt_next_week_start_time as next_week_start_time,
br.robot_code,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t2.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
case when COALESCE(t2.error_num,0) != 0 then (COALESCE(t1.theory_run_duration,0)-COALESCE(t2.error_duration,0))/COALESCE(t2.error_num,0) else null end as mtbf,
COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num,
case when COALESCE(t3.accum_error_num,0) != 0 then ((COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0))-(COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0)))/COALESCE(t3.accum_error_num,0) else null end as accum_mtbf
-- part1:机器人全集
from(select distinct robot_code from phoenix_basic.basic_robot)br
-- part2:机器人理论运行时间	
left join 				
(
select 
br.robot_code,
COALESCE(t1.theory_run_duration,0) as theory_run_duration -- 理论运行时长（秒） 
from 
(select distinct robot_code from phoenix_basic.basic_robot)br
left join 
(select 
ts.robot_code,
sum(stat_state_duration) as theory_run_duration
from 
(select 
t1.robot_code,
t2.id              as                           state_id,
t2.create_time     as                           state_create_time,
t2.network_state,
t2.online_state,
t2.work_state,
t2.job_sn,
t2.cause,
t2.is_error, 
t2.duration / 1000 as                           duration,
UNIX_TIMESTAMP(coalesce(t3.the_week_first_create_time, LEAST(@dt_next_week_start_time,@now_time)))-UNIX_TIMESTAMP(@dt_week_start_time) as stat_state_duration  -- 每个机器人计算周之前的最后一条状态在该天内持续时长（秒）					
from 
-- 找到每个机器人计算周之前的最后一条状态变化数据
(select 
robot_code, max(id) as before_the_hour_last_id 
from phoenix_rms.robot_state_history
where create_time < @dt_week_start_time
group by robot_code)t1 
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
-- 找到每个机器人计算周之内的第一条状态变化数据
left join 
(select 
robot_code, min(create_time) as the_week_first_create_time
from phoenix_rms.robot_state_history
where create_time >= @dt_week_start_time and create_time < @dt_next_week_start_time
group by robot_code)t3 on t3.robot_code=t1.robot_code

union all 

select 
t4.robot_code,	   
t4.id              as           state_id,
t4.create_time     as           state_create_time,
t4.network_state,
t4.online_state,
t4.work_state,
t4.job_sn,
t4.cause,
t4.is_error, 
t4.duration / 1000 as           duration,
case when t5.the_hour_last_id is not null then UNIX_TIMESTAMP(LEAST(@dt_next_week_start_time,@now_time))-UNIX_TIMESTAMP(t4.create_time) else t4.duration / 1000 end as stat_state_duration  -- 每个机器人在计算周内的每条状态持续时长（最后一条要做特殊处理）（秒）
from 
-- 每个机器人计算周之内的状态变化数据
(select 
*
from phoenix_rms.robot_state_history 
where create_time >= @dt_week_start_time and create_time < @dt_next_week_start_time )t4 
left join 
-- 找到每个机器人在计算周的最后一条状态变化数据
(select 
robot_code, 
max(id) as the_hour_last_id,
max(create_time) as the_day_last_create_time   
from phoenix_rms.robot_state_history
where create_time >= @dt_week_start_time and create_time < @dt_next_week_start_time
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts 	
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code)t1 on t1.robot_code=br.robot_code
)t1 on t1.robot_code=br.robot_code
left join 
-- part3: 计算故障次数、故障时长
(select robot_code,
sum(unix_timestamp(stat_end_time)-unix_timestamp(stat_start_time)) as error_duration,  -- 该周故障时长
count(distinct error_id) as error_num  -- 该周故障次数
from 
(select
t1.id  as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.robot_code,
case when t1.start_time < @dt_week_start_time then @dt_week_start_time else t1.start_time end as stat_start_time,
case when t1.end_time is null or t1.end_time >= @dt_next_week_start_time then LEAST(@dt_next_week_start_time,@now_time) else LEAST(t1.end_time,@now_time) end as stat_end_time  -- 注意：算当前周期的指标值时，要考虑当前周期时间并没有全部过完这一客观事实
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @dt_week_start_time and start_time < @dt_next_week_start_time and
               coalesce(end_time, @now_time) < @dt_next_week_start_time) or
              (start_time >= @dt_week_start_time and start_time < @dt_next_week_start_time and
               coalesce(end_time, @now_time) >= @dt_next_week_start_time) or
              (start_time < @dt_week_start_time and coalesce(end_time, @now_time) >= @dt_week_start_time and
               coalesce(end_time, @now_time) < @dt_next_week_start_time) or
              (start_time < @dt_week_start_time and coalesce(end_time, @now_time) >= @dt_next_week_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @dt_week_start_time and start_time < @dt_next_week_start_time and
                              coalesce(end_time, @now_time) < @dt_next_week_start_time) or
                             (start_time >= @dt_week_start_time and start_time < @dt_next_week_start_time and
                              coalesce(end_time, @now_time) >= @dt_next_week_start_time) or
                             (start_time < @dt_week_start_time and coalesce(end_time, @now_time) >= @dt_week_start_time and
                              coalesce(end_time, @now_time) < @dt_next_week_start_time) or
                             (start_time < @dt_week_start_time and coalesce(end_time, @now_time) >= @dt_next_week_start_time)
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
					group by robot_code
)t2 on t2.robot_code=br.robot_code		 			
left join 
-- part4:计算累计故障次数  	
(select robot_code,count(distinct error_id) as accum_error_num -- 累计故障次数
from 
(select robot_code,error_id
FROM qt_smartreport.qtr_week_robot_error_list_his
where date_value < @dt_week_start_time
union all 
select
t1.robot_code,
t1.id  as error_id
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @dt_week_start_time and start_time < @dt_next_week_start_time and
               coalesce(end_time, @now_time) < @dt_next_week_start_time) or
              (start_time >= @dt_week_start_time and start_time < @dt_next_week_start_time and
               coalesce(end_time, @now_time) >= @dt_next_week_start_time) or
              (start_time < @dt_week_start_time and coalesce(end_time, @now_time) >= @dt_week_start_time and
               coalesce(end_time, @now_time) < @dt_next_week_start_time) or
              (start_time < @dt_week_start_time and coalesce(end_time, @now_time) >= @dt_next_week_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @dt_week_start_time and start_time < @dt_next_week_start_time and
                              coalesce(end_time, @now_time) < @dt_next_week_start_time) or
                             (start_time >= @dt_week_start_time and start_time < @dt_next_week_start_time and
                              coalesce(end_time, @now_time) >= @dt_next_week_start_time) or
                             (start_time < @dt_week_start_time and coalesce(end_time, @now_time) >= @dt_week_start_time and
                              coalesce(end_time, @now_time) < @dt_next_week_start_time) or
                             (start_time < @dt_week_start_time and coalesce(end_time, @now_time) >= @dt_next_week_start_time)
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t 
group by robot_code)t3 on t3.robot_code=br.robot_code				
left join 
(select robot_code ,
sum(theory_run_duration) as accum_theory_run_duration,  -- 该周之前累计理论运行时长
sum(error_duration) as accum_error_duration   -- 该周之前累计故障时长
from qt_smartreport.qtr_day_robot_error_mtbf_his
where date_value < @dt_week_start_time
group by robot_code)t4 on t4.robot_code=br.robot_code




					
--------------------------------------------------------------------------------------------------------------------------
			
-- 插入数据（异步表）qt_smartreport.qtr_week_robot_error_mtbf_his	
-- {{ dt_relative_time(dt) }}
-- {{ now_time }}
-- {{ dt_hour_start_time }}
-- {{ dt_next_hour_start_time }}
-- {{ dt_day_start_time }}
-- {{ dt_next_day_start_time }}
-- {{ dt_week_start_time }}
-- {{ dt_next_week_start_time }}	


-- 定义时间参数
{% set now_time=datetime.datetime.now().strftime("'%Y-%m-%d %H:%M:%S.000000'") %}  -- 客观当前时间
{% set dt_hour_start_time=dt_relative_time(dt,default="%Y-%m-%d %H:00:00.000000") %}   -- dt所在小时的开始时间
{% set dt_next_hour_start_time=dt_relative_time(dt,hours=1,default="%Y-%m-%d %H:00:00.000000") %}  -- dt所在小时的下一个小时的开始时间
{% set dt_day_start_time=dt_relative_time(dt,default="%Y-%m-%d 00:00:00.000000") %}  -- dt所在天的开始时间
{% set dt_next_day_start_time=dt_relative_time(dt,days=1,default="%Y-%m-%d 00:00:00.000000") %}  -- dt所在天的下一天的开始时间
{% set dt_week_start_time=(dt - datetime.timedelta(days=dt.now().weekday())).strftime("'%Y-%m-%d 00:00:00.000000'") %}  -- dt所在周的开始时间
{% set dt_next_week_start_time=(dt + datetime.timedelta(days=7-dt.now().weekday())).strftime("'%Y-%m-%d 00:00:00.000000'") %}  -- dt所在周的下一周的开始时间



-- 插入逻辑  	
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
date({{ dt_week_start_time }}) as date_value,
{{ dt_week_start_time }} as week_start_time,
{{ dt_next_week_start_time }} as next_week_start_time,
br.robot_code,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t2.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
case when COALESCE(t2.error_num,0) != 0 then (COALESCE(t1.theory_run_duration,0)-COALESCE(t2.error_duration,0))/COALESCE(t2.error_num,0) else null end as mtbf,
COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num,
case when COALESCE(t3.accum_error_num,0) != 0 then ((COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0))-(COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0)))/COALESCE(t3.accum_error_num,0) else null end as accum_mtbf
-- part1:机器人全集
from(select distinct robot_code from phoenix_basic.basic_robot)br
-- part2:机器人理论运行时间
left join
(
select
br.robot_code,
COALESCE(t1.theory_run_duration,0) as theory_run_duration -- 理论运行时长（秒）
from
(select distinct robot_code from phoenix_basic.basic_robot)br
left join
(select
ts.robot_code,
sum(stat_state_duration) as theory_run_duration
from
(select
t1.robot_code,
t2.id              as                           state_id,
t2.create_time     as                           state_create_time,
t2.network_state,
t2.online_state,
t2.work_state,
t2.job_sn,
t2.cause,
t2.is_error,
t2.duration / 1000 as                           duration,
UNIX_TIMESTAMP(coalesce(t3.the_week_first_create_time, LEAST({{ dt_next_week_start_time }},{{ now_time }})))-UNIX_TIMESTAMP({{ dt_week_start_time }}) as stat_state_duration  -- 每个机器人计算周之前的最后一条状态在该天内持续时长（秒）
from
-- 找到每个机器人计算周之前的最后一条状态变化数据
(select
robot_code, max(id) as before_the_hour_last_id
from phoenix_rms.robot_state_history
where create_time < {{ dt_week_start_time }}
group by robot_code)t1
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
-- 找到每个机器人计算周之内的第一条状态变化数据
left join
(select
robot_code, min(create_time) as the_week_first_create_time
from phoenix_rms.robot_state_history
where create_time >= {{ dt_week_start_time }} and create_time < {{ dt_next_week_start_time }}
group by robot_code)t3 on t3.robot_code=t1.robot_code

union all

select
t4.robot_code,
t4.id              as           state_id,
t4.create_time     as           state_create_time,
t4.network_state,
t4.online_state,
t4.work_state,
t4.job_sn,
t4.cause,
t4.is_error,
t4.duration / 1000 as           duration,
case when t5.the_hour_last_id is not null then UNIX_TIMESTAMP(LEAST({{ dt_next_week_start_time }},{{ now_time }}))-UNIX_TIMESTAMP(t4.create_time) else t4.duration / 1000 end as stat_state_duration  -- 每个机器人在计算周内的每条状态持续时长（最后一条要做特殊处理）（秒）
from
-- 每个机器人计算周之内的状态变化数据
(select
*
from phoenix_rms.robot_state_history
where create_time >= {{ dt_week_start_time }} and create_time < {{ dt_next_week_start_time }} )t4
left join
-- 找到每个机器人在计算周的最后一条状态变化数据
(select
robot_code,
max(id) as the_hour_last_id,
max(create_time) as the_day_last_create_time
from phoenix_rms.robot_state_history
where create_time >= {{ dt_week_start_time }} and create_time < {{ dt_next_week_start_time }}
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code)t1 on t1.robot_code=br.robot_code
)t1 on t1.robot_code=br.robot_code
left join
-- part3: 计算故障次数、故障时长
(select robot_code,
sum(unix_timestamp(stat_end_time)-unix_timestamp(stat_start_time)) as error_duration,  -- 该周故障时长
count(distinct error_id) as error_num  -- 该周故障次数
from
(select
t1.id  as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.robot_code,
case when t1.start_time < {{ dt_week_start_time }} then {{ dt_week_start_time }} else t1.start_time end as stat_start_time,
case when t1.end_time is null or t1.end_time >= {{ dt_next_week_start_time }} then LEAST({{ dt_next_week_start_time }},{{ now_time }}) else LEAST(t1.end_time,{{ now_time }}) end as stat_end_time   -- 注意：算当前周期的指标值时，要考虑当前周期时间并没有全部过完这一客观事实
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= {{ dt_week_start_time }} and start_time < {{ dt_next_week_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_week_start_time }}) or
              (start_time >= {{ dt_week_start_time }} and start_time < {{ dt_next_week_start_time }} and
               coalesce(end_time, {{ now_time }}) >= {{ dt_next_week_start_time }}) or
              (start_time < {{ dt_week_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_week_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_week_start_time }}) or
              (start_time < {{ dt_week_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_week_start_time }})
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= {{ dt_week_start_time }} and start_time < {{ dt_next_week_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_week_start_time }}) or
                             (start_time >= {{ dt_week_start_time }} and start_time < {{ dt_next_week_start_time }} and
                              coalesce(end_time, {{ now_time }}) >= {{ dt_next_week_start_time }}) or
                             (start_time < {{ dt_week_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_week_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_week_start_time }}) or
                             (start_time < {{ dt_week_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_week_start_time }})
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
					group by robot_code
)t2 on t2.robot_code=br.robot_code
left join
-- part4:计算累计故障次数
(select robot_code,count(distinct error_id) as accum_error_num -- 累计故障次数
from
(select robot_code,error_id
FROM qt_smartreport.qtr_week_robot_error_list_his
where date_value < {{ dt_week_start_time }}
union all
select
t1.robot_code,
t1.id  as error_id
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= {{ dt_week_start_time }} and start_time < {{ dt_next_week_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_week_start_time }}) or
              (start_time >= {{ dt_week_start_time }} and start_time < {{ dt_next_week_start_time }} and
               coalesce(end_time, {{ now_time }}) >= {{ dt_next_week_start_time }}) or
              (start_time < {{ dt_week_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_week_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_week_start_time }}) or
              (start_time < {{ dt_week_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_week_start_time }})
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= {{ dt_week_start_time }} and start_time < {{ dt_next_week_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_week_start_time }}) or
                             (start_time >= {{ dt_week_start_time }} and start_time < {{ dt_next_week_start_time }} and
                              coalesce(end_time, {{ now_time }}) >= {{ dt_next_week_start_time }}) or
                             (start_time < {{ dt_week_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_week_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_week_start_time }}) or
                             (start_time < {{ dt_week_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_week_start_time }})
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
group by robot_code)t3 on t3.robot_code=br.robot_code
left join
(select robot_code ,
sum(theory_run_duration) as accum_theory_run_duration,  -- 该周之前累计理论运行时长
sum(error_duration) as accum_error_duration   -- 该周之前累计故障时长
from qt_smartreport.qtr_day_robot_error_mtbf_his
where date_value < {{ dt_week_start_time }}
group by robot_code)t4 on t4.robot_code=br.robot_code