set @now_time=sysdate();   --  当前时间
set @dt_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @dt_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间
set @dt_week_start_time=date_format(DATE_SUB(sysdate(),INTERVAL WEEKDAY(sysdate()) + 0 DAY), '%Y-%m-%d 00:00:00'); -- 当前一周的开始时间
set @dt_next_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) -7 DAY), '%Y-%m-%d 00:00:00'); --  下一周的开始时间
select @now_time,@dt_hour_start_time,@dt_next_hour_start_time,@dt_day_start_time,@dt_next_day_start_time,@dt_week_start_time,@dt_next_week_start_time;



-- 插入数据（mysql参数）
-- insert into qt_smartreport.qtr_hour_robot_error_time_detail_his(create_time,update_time,date_value,hour_start_time, next_hour_start_time, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object, the_hour_cost_seconds)
select 
@now_time as create_time,
@now_time as update_time,
date(@dt_day_start_time) as date_value,
t1.hour_start_time,
t1.next_hour_start_time,
t2.error_id,
t2.error_code,
t2.start_time,
t2.end_time,
t2.warning_spec,
t2.alarm_module,
t2.alarm_service,
t2.alarm_type,
t2.alarm_level,
t2.alarm_detail,
t2.param_value,
t2.job_order,
t2.robot_job,
t2.robot_code,
t2.device_code,
t2.server_code,
transport_object,
case 
when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,@now_time) and t2.stat_end_time < least(t1.next_hour_start_time,@now_time) then UNIX_TIMESTAMP(t2.stat_end_time) - UNIX_TIMESTAMP(t2.stat_start_time) 
when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,@now_time) and t2.stat_end_time >= least(t1.next_hour_start_time,@now_time) then UNIX_TIMESTAMP(least(t1.next_hour_start_time,@now_time)) - UNIX_TIMESTAMP(t2.stat_start_time)
when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and t2.stat_end_time < least(t1.next_hour_start_time,@now_time) then UNIX_TIMESTAMP(t2.stat_end_time) - UNIX_TIMESTAMP(t1.hour_start_time) 
when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= least(t1.next_hour_start_time,@now_time) then UNIX_TIMESTAMP(least(t1.next_hour_start_time,@now_time)) - UNIX_TIMESTAMP(t1.hour_start_time) 
end as the_hour_cost_seconds
from 
-- 小时维表
(select 
date_format(concat(date(@dt_day_start_time),' ',hour_start_time),'%Y-%m-%d %H:%i:%s') as hour_start_time,
date_format(concat(date(@dt_day_start_time),' ',next_hour_start_time),'%Y-%m-%d %H:%i:%s') as next_hour_start_time
from qt_smartreport.qtr_dim_hour
where date_format(concat(date(@dt_day_start_time),' ',hour_start_time),'%Y-%m-%d %H:%i:%s') <= @now_time) t1
-- 天内参与计算的机器人故障集合
inner join
(select 
t.date_value, 
t.error_id, 
t.error_code, 
t.start_time, 
t.end_time,
t.warning_spec,
t.alarm_module,
t.alarm_service,
t.alarm_type,
t.alarm_level,
t.alarm_detail,
t.param_value,
t.job_order,
t.robot_job,
t.robot_code,
t.device_code,
t.server_code,
t.transport_object,
case when t.start_time < @dt_day_start_time then @dt_day_start_time else t.start_time end stat_start_time,
coalesce(t.end_time, @now_time) as stat_end_time
from 
(select
@now_time as create_time,
@now_time as update_time,
date(@dt_day_start_time) as date_value,
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
t1.transport_object
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @dt_day_start_time and start_time <@dt_next_day_start_time and
               coalesce(end_time, @now_time) <@dt_next_day_start_time) or
              (start_time >= @dt_day_start_time and start_time <@dt_next_day_start_time and
               coalesce(end_time, @now_time) >=@dt_next_day_start_time) or
              (start_time < @dt_day_start_time and coalesce(end_time, @now_time) >= @dt_day_start_time and
               coalesce(end_time, @now_time) <@dt_next_day_start_time) or
              (start_time < @dt_day_start_time and coalesce(end_time, @now_time) >=@dt_next_day_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @dt_day_start_time and start_time <@dt_next_day_start_time and
                              coalesce(end_time, @now_time) <@dt_next_day_start_time) or
                             (start_time >= @dt_day_start_time and start_time <@dt_next_day_start_time and
                              coalesce(end_time, @now_time) >=@dt_next_day_start_time) or
                             (start_time < @dt_day_start_time and coalesce(end_time, @now_time) >= @dt_day_start_time and
                              coalesce(end_time, @now_time) <@dt_next_day_start_time) or
                             (start_time < @dt_day_start_time and coalesce(end_time, @now_time) >=@dt_next_day_start_time)
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
where t.date_value=date(@dt_day_start_time)
) t2 on(
(t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,@now_time) and t2.stat_end_time < least(t1.next_hour_start_time,@now_time)) or 
(t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,@now_time) and t2.stat_end_time >= least(t1.next_hour_start_time,@now_time)) or 
(t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and t2.stat_end_time < least(t1.next_hour_start_time,@now_time)) or 
(t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= least(t1.next_hour_start_time,@now_time))
)



--------------------------------------------------------------------------------------------------------------------------
			
-- 插入数据（异步表）qt_smartreport.qtr_hour_robot_error_time_detail_his	
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
date({{ dt_day_start_time }}) as date_value,
t1.hour_start_time,
t1.next_hour_start_time,
t2.error_id,
t2.error_code,
t2.start_time,
t2.end_time,
t2.warning_spec,
t2.alarm_module,
t2.alarm_service,
t2.alarm_type,
t2.alarm_level,
t2.alarm_detail,
t2.param_value,
t2.job_order,
t2.robot_job,
t2.robot_code,
t2.device_code,
t2.server_code,
transport_object,
case
when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,{{ now_time }}) and t2.stat_end_time < least(t1.next_hour_start_time,{{ now_time }}) then UNIX_TIMESTAMP(t2.stat_end_time) - UNIX_TIMESTAMP(t2.stat_start_time)
when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,{{ now_time }}) and t2.stat_end_time >= least(t1.next_hour_start_time,{{ now_time }}) then UNIX_TIMESTAMP(least(t1.next_hour_start_time,{{ now_time }})) - UNIX_TIMESTAMP(t2.stat_start_time)
when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and t2.stat_end_time < least(t1.next_hour_start_time,{{ now_time }}) then UNIX_TIMESTAMP(t2.stat_end_time) - UNIX_TIMESTAMP(t1.hour_start_time)
when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= least(t1.next_hour_start_time,{{ now_time }}) then UNIX_TIMESTAMP(least(t1.next_hour_start_time,{{ now_time }})) - UNIX_TIMESTAMP(t1.hour_start_time)
end as the_hour_cost_seconds
from
-- 小时维表
(select
date_format(concat(date({{ dt_day_start_time }}),' ',hour_start_time),'%Y-%m-%d %H:%i:%s') as hour_start_time,
date_format(concat(date({{ dt_day_start_time }}),' ',next_hour_start_time),'%Y-%m-%d %H:%i:%s') as next_hour_start_time
from qt_smartreport.qtr_dim_hour
where date_format(concat(date({{ dt_day_start_time }}),' ',hour_start_time),'%Y-%m-%d %H:%i:%s') <= {{ now_time }}) t1
-- 天内参与计算的机器人故障集合
inner join
(select
t.date_value,
t.error_id,
t.error_code,
t.start_time,
t.end_time,
t.warning_spec,
t.alarm_module,
t.alarm_service,
t.alarm_type,
t.alarm_level,
t.alarm_detail,
t.param_value,
t.job_order,
t.robot_job,
t.robot_code,
t.device_code,
t.server_code,
t.transport_object,
case when t.start_time < {{ dt_day_start_time }} then {{ dt_day_start_time }} else t.start_time end stat_start_time,
coalesce(t.end_time, {{ now_time }}) as stat_end_time
from
(select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
date({{ dt_day_start_time }}) as date_value,
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
t1.transport_object
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= {{ dt_day_start_time }} and start_time <{{ dt_next_day_start_time }} and
               coalesce(end_time, {{ now_time }}) <{{ dt_next_day_start_time }}) or
              (start_time >= {{ dt_day_start_time }} and start_time <{{ dt_next_day_start_time }} and
               coalesce(end_time, {{ now_time }}) >={{ dt_next_day_start_time }}) or
              (start_time < {{ dt_day_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_day_start_time }} and
               coalesce(end_time, {{ now_time }}) <{{ dt_next_day_start_time }}) or
              (start_time < {{ dt_day_start_time }} and coalesce(end_time, {{ now_time }}) >={{ dt_next_day_start_time }})
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= {{ dt_day_start_time }} and start_time <{{ dt_next_day_start_time }} and
                              coalesce(end_time, {{ now_time }}) <{{ dt_next_day_start_time }}) or
                             (start_time >= {{ dt_day_start_time }} and start_time <{{ dt_next_day_start_time }} and
                              coalesce(end_time, {{ now_time }}) >={{ dt_next_day_start_time }}) or
                             (start_time < {{ dt_day_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_day_start_time }} and
                              coalesce(end_time, {{ now_time }}) <{{ dt_next_day_start_time }}) or
                             (start_time < {{ dt_day_start_time }} and coalesce(end_time, {{ now_time }}) >={{ dt_next_day_start_time }})
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
where t.date_value=date({{ dt_day_start_time }})
) t2 on(
(t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,{{ now_time }}) and t2.stat_end_time < least(t1.next_hour_start_time,{{ now_time }})) or
(t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,{{ now_time }}) and t2.stat_end_time >= least(t1.next_hour_start_time,{{ now_time }})) or
(t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and t2.stat_end_time < least(t1.next_hour_start_time,{{ now_time }})) or
(t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= least(t1.next_hour_start_time,{{ now_time }}))
)


