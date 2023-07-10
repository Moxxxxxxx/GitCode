--  MTBF = (sum(theory_run_duration)-sum(error_duration))/sum(error_num)
-- 累计MTBF = (sum(accum_theory_run_duration)-sum(accum_error_duration))/sum(accum_error_num)

select 
'hour' as stat_time_type,
t.hour_start_time as stat_time_value,
t.robot_code,
brt.robot_type_code,
brt.robot_type_name,
t.theory_run_duration,   -- 机器人理论运行时长
t.error_duration,        -- 机器人故障时长
t.error_num,             -- 机器人故障次数
t.accum_theory_run_duration,  --  机器人累计理论运行时长
t.accum_error_duration,      -- 机器人累计故障时长
t.accum_error_num             -- 机器人累计故障次数
from qt_smartreport.qt_hour_robot_error_mtbf_his t
inner join phoenix_basic.basic_robot br on br.robot_code = t.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where t.hour_start_time BETWEEN  {start_time} AND  {end_time}
union all 
select 
'hour' as stat_time_type,
date_format( {now_hour_start_time}, '%Y-%m-%d %H:00:00') as stat_time_value,
br.robot_code,
brt.robot_type_code,
brt.robot_type_name,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t2.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num
from phoenix_basic.basic_robot br
inner join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id and br.usage_state = 'using'
-- 计算当前小时机器人的理论运行时长
left join 
(select 
br.robot_code,
COALESCE(t1.theory_run_duration,0) as theory_run_duration
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
case when  {now_time} <  {now_next_hour_start_time} then UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time,  {now_time})) - UNIX_TIMESTAMP( {now_hour_start_time}) else UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time,  {now_next_hour_start_time})) - UNIX_TIMESTAMP( {now_hour_start_time}) end stat_state_duration				
from 
(select 
robot_code, max(id) as before_the_hour_last_id 
from phoenix_rms.robot_state_history
where create_time <  {now_hour_start_time}
group by robot_code)t1 
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
left join 
(select 
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >=  {now_hour_start_time} and create_time <  {now_next_hour_start_time}
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
case when t5.the_hour_last_id is not null and  {now_time} >=  {now_next_hour_start_time} then UNIX_TIMESTAMP( {now_next_hour_start_time})-UNIX_TIMESTAMP(t4.create_time)
when t5.the_hour_last_id is not null and  {now_time} <  {now_next_hour_start_time} then UNIX_TIMESTAMP( {now_time}) - UNIX_TIMESTAMP(t4.create_time)
else t4.duration / 1000 end stat_state_duration
from 
(select 
*
from phoenix_rms.robot_state_history 
where create_time >=  {now_hour_start_time} and create_time <  {now_next_hour_start_time})t4 
left join 
(select 
robot_code, 
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time   
from phoenix_rms.robot_state_history
where create_time >=  {now_hour_start_time} and create_time <  {now_next_hour_start_time}
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts 	
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code)t1 on t1.robot_code=br.robot_code)t1 on t1.robot_code=br.robot_code
-- 计算当前小时机器人的故障次数及故障时长
left join 
(select robot_code,hour_start_time,
sum(unix_timestamp(stat_end_time)-unix_timestamp(stat_start_time)) as error_duration,
count(distinct error_id) as error_num
from 
(select  {now_hour_start_time} as hour_start_time,
       t1.id                                     as error_id,
       t1.error_code,
       t1.start_time,
       t1.end_time,
       t1.warning_spec,
       t1.alarm_module,
       t1.alarm_service,
       t1.alarm_type,
       t1.alarm_level,
       t1.alarm_detail,
       t1.param_value,
       t1.job_order,
       t1.robot_job,
       t1.robot_code,
       t1.device_code,
       t1.server_code,
       t1.transport_object,
	   case when t1.start_time <  {now_hour_start_time} then  {now_hour_start_time} else t1.start_time end as stat_start_time,
	   case when t1.end_time is null or t1.end_time >=  {now_next_hour_start_time} then  {now_next_hour_start_time} else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >=  {now_hour_start_time} and start_time <  {now_next_hour_start_time} and
               coalesce(end_time,  {now_time}) <  {now_next_hour_start_time}) or
              (start_time >=  {now_hour_start_time} and start_time <  {now_next_hour_start_time} and
               coalesce(end_time,  {now_time}) >=  {now_next_hour_start_time}) or
              (start_time <  {now_hour_start_time} and coalesce(end_time,  {now_time}) >=  {now_hour_start_time} and
               coalesce(end_time,  {now_time}) <  {now_next_hour_start_time}) or
              (start_time <  {now_hour_start_time} and coalesce(end_time,  {now_time}) >=  {now_next_hour_start_time})
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >=  {now_hour_start_time} and start_time <  {now_next_hour_start_time} and
                              coalesce(end_time,  {now_time}) <  {now_next_hour_start_time}) or
                             (start_time >=  {now_hour_start_time} and start_time <  {now_next_hour_start_time} and
                              coalesce(end_time,  {now_time}) >=  {now_next_hour_start_time}) or
                             (start_time <  {now_hour_start_time} and coalesce(end_time,  {now_time}) >=  {now_hour_start_time} and
                              coalesce(end_time,  {now_time}) <  {now_next_hour_start_time}) or
                             (start_time <  {now_hour_start_time} and coalesce(end_time,  {now_time}) >=  {now_next_hour_start_time})
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)tce
					group by robot_code,hour_start_time)t2 on t2.robot_code=br.robot_code
-- 	计算截止当前小时机器人的故障次数			
left join 
(select 
robot_code,count(distinct error_id) as accum_error_num 
from 
(select distinct robot_code,error_id  
FROM qt_smartreport.qt_hour_robot_error_list_his
where hour_start_time< {now_hour_start_time}
union all 
select t1.robot_code,
       t1.id  as error_id
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >=  {now_hour_start_time} and start_time <  {now_next_hour_start_time} and
               coalesce(end_time,  {now_time}) <  {now_next_hour_start_time}) or
              (start_time >=  {now_hour_start_time} and start_time <  {now_next_hour_start_time} and
               coalesce(end_time,  {now_time}) >=  {now_next_hour_start_time}) or
              (start_time <  {now_hour_start_time} and coalesce(end_time,  {now_time}) >=  {now_hour_start_time} and
               coalesce(end_time,  {now_time}) <  {now_next_hour_start_time}) or
              (start_time <  {now_hour_start_time} and coalesce(end_time,  {now_time}) >=  {now_next_hour_start_time})
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >=  {now_hour_start_time} and start_time <  {now_next_hour_start_time} and
                              coalesce(end_time,  {now_time}) <  {now_next_hour_start_time}) or
                             (start_time >=  {now_hour_start_time} and start_time <  {now_next_hour_start_time} and
                              coalesce(end_time,  {now_time}) >=  {now_next_hour_start_time}) or
                             (start_time <  {now_hour_start_time} and coalesce(end_time,  {now_time}) >=  {now_hour_start_time} and
                              coalesce(end_time,  {now_time}) <  {now_next_hour_start_time}) or
                             (start_time <  {now_hour_start_time} and coalesce(end_time,  {now_time}) >=  {now_next_hour_start_time})
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
					group by robot_code)t3 on t3.robot_code=br.robot_code		
-- 计算截止当前小时机器人的理论运行时长与累计故障时长
left join 
(select 
robot_code ,accum_theory_run_duration,accum_error_duration 
from qt_smartreport.qt_hour_robot_error_mtbf_his
where hour_start_time=date_format(DATE_ADD( {now_hour_start_time}, INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00'))t4  
on t4.robot_code=br.robot_code							







##################################################################################################


--  MTBF = (sum(theory_run_duration)-sum(error_duration))/sum(error_num)
-- 累计MTBF = (sum(accum_theory_run_duration)-sum(accum_error_duration))/sum(accum_error_num)


#####  包括当前小时逻辑
set @now_hour_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
set @now_next_hour_start_time = date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00');
select @now_hour_start_time, @now_next_hour_start_time;


select 
'hour' as stat_time_type,
t.hour_start_time as stat_time_value,
t.robot_code,
brt.robot_type_code,
brt.robot_type_name,
t.theory_run_duration,   -- 机器人理论运行时长
t.error_duration,        -- 机器人故障时长
t.error_num,             -- 机器人故障次数
t.accum_theory_run_duration,  --  机器人累计理论运行时长
t.accum_error_duration,      -- 机器人累计故障时长
t.accum_error_num             -- 机器人累计故障次数
from qt_smartreport.qt_hour_robot_error_mtbf_his t
inner join phoenix_basic.basic_robot br on br.robot_code = t.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
-- where t.hour_start_time BETWEEN {start_time} AND {end_time}
union all 
select 
'hour' as stat_time_type,
date_format(@now_hour_start_time, '%Y-%m-%d %H:00:00') as stat_time_value,
br.robot_code,
brt.robot_type_code,
brt.robot_type_name,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t2.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num
from phoenix_basic.basic_robot br
inner join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id and br.usage_state = 'using'
-- 计算当前小时机器人的理论运行时长
left join 
(select 
br.robot_code,
COALESCE(t1.theory_run_duration,0) as theory_run_duration
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
case when sysdate() < @now_next_hour_start_time then UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, sysdate())) - UNIX_TIMESTAMP(@now_hour_start_time) else UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, @now_next_hour_start_time)) - UNIX_TIMESTAMP(@now_hour_start_time) end stat_state_duration				
from 
(select 
robot_code, max(id) as before_the_hour_last_id 
from phoenix_rms.robot_state_history
where create_time < @now_hour_start_time
group by robot_code)t1 
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
left join 
(select 
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >= @now_hour_start_time and create_time < @now_next_hour_start_time
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
case when t5.the_hour_last_id is not null and sysdate() >= @now_next_hour_start_time then UNIX_TIMESTAMP(@now_next_hour_start_time)-UNIX_TIMESTAMP(t4.create_time)
when t5.the_hour_last_id is not null and sysdate() < @now_next_hour_start_time then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t4.create_time)
else t4.duration / 1000 end stat_state_duration
from 
(select 
*
from phoenix_rms.robot_state_history 
where create_time >= @now_hour_start_time and create_time < @now_next_hour_start_time)t4 
left join 
(select 
robot_code, 
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time   
from phoenix_rms.robot_state_history
where create_time >= @now_hour_start_time and create_time < @now_next_hour_start_time
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts 	
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code)t1 on t1.robot_code=br.robot_code)t1 on t1.robot_code=br.robot_code
-- 计算当前小时机器人的故障次数及故障时长
left join 
(select robot_code,hour_start_time,
sum(unix_timestamp(stat_end_time)-unix_timestamp(stat_start_time)) as error_duration,
count(distinct error_id) as error_num
from 
(select @now_hour_start_time as hour_start_time,
       t1.id                                     as error_id,
       t1.error_code,
       t1.start_time,
       t1.end_time,
       t1.warning_spec,
       t1.alarm_module,
       t1.alarm_service,
       t1.alarm_type,
       t1.alarm_level,
       t1.alarm_detail,
       t1.param_value,
       t1.job_order,
       t1.robot_job,
       t1.robot_code,
       t1.device_code,
       t1.server_code,
       t1.transport_object,
	   case when t1.start_time < @now_hour_start_time then @now_hour_start_time else t1.start_time end as stat_start_time,
	   case when t1.end_time is null or t1.end_time >= @now_next_hour_start_time then @now_next_hour_start_time else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
               coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
              (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
               coalesce(end_time, sysdate()) >= @now_next_hour_start_time) or
              (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_hour_start_time and
               coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
              (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_next_hour_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
                              coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
                             (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
                              coalesce(end_time, sysdate()) >= @now_next_hour_start_time) or
                             (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_hour_start_time and
                              coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
                             (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_next_hour_start_time)
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)tce
					group by robot_code,hour_start_time)t2 on t2.robot_code=br.robot_code
-- 	计算截止当前小时机器人的故障次数			
left join 
(select 
robot_code,count(distinct error_id) as accum_error_num 
from 
(select distinct robot_code,error_id  
FROM qt_smartreport.qt_hour_robot_error_list_his
where hour_start_time<@now_hour_start_time
union all 
select t1.robot_code,
       t1.id  as error_id
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
               coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
              (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
               coalesce(end_time, sysdate()) >= @now_next_hour_start_time) or
              (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_hour_start_time and
               coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
              (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_next_hour_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
                              coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
                             (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
                              coalesce(end_time, sysdate()) >= @now_next_hour_start_time) or
                             (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_hour_start_time and
                              coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
                             (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_next_hour_start_time)
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
					group by robot_code)t3 on t3.robot_code=br.robot_code		
-- 计算截止当前小时机器人的理论运行时长与累计故障时长
left join 
(select 
robot_code ,accum_theory_run_duration,accum_error_duration 
from qt_smartreport.qt_hour_robot_error_mtbf_his
where hour_start_time=date_format(DATE_ADD(@now_hour_start_time, INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00'))t4  
on t4.robot_code=br.robot_code							