-- part1：mysql逻辑


-- mysql时间参数
set @now_time=sysdate();   --  当前时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间

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
                            COALESCE(end_time, 'unfinished') as end_time,
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
                     group by robot_code, COALESCE(end_time, 'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
where t.date_value=date(@dt_day_start_time)
) t2 on(
(t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,@now_time) and t2.stat_end_time < least(t1.next_hour_start_time,@now_time)) or 
(t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,@now_time) and t2.stat_end_time >= least(t1.next_hour_start_time,@now_time)) or 
(t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and t2.stat_end_time < least(t1.next_hour_start_time,@now_time)) or 
(t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= least(t1.next_hour_start_time,@now_time))
)


-- part2：sqlserver逻辑

-- sqlserver时间参数
declare @now_time as datetime=sysdatetime() 
declare @dt_hour_start_time as datetime=FORMAT(sysdatetime(),'yyyy-MM-dd HH:00:00')
declare @dt_next_hour_start_time as datetime=FORMAT(DATEADD(hh,1,sysdatetime()),'yyyy-MM-dd HH:00:00')
declare @dt_day_start_time as datetime=FORMAT(sysdatetime(),'yyyy-MM-dd 00:00:00')
declare @dt_next_day_start_time as datetime=FORMAT(DATEADD(dd,1,sysdatetime()),'yyyy-MM-dd 00:00:00')
declare @dt_week_start_time as datetime=FORMAT(DATEADD(wk,datediff(wk,0,getdate()),0),'yyyy-MM-dd 00:00:00')
declare @dt_next_week_start_time as datetime=FORMAT(DATEADD(wk,datediff(wk,0,getdate()),7),'yyyy-MM-dd 00:00:00')


select 
@now_time as create_time,
@now_time as update_time,
FORMAT(cast(@dt_day_start_time as datetime),'yyyy-MM-dd') as date_value,
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
case when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < (case when t1.next_hour_start_time <= @now_time then t1.next_hour_start_time else @now_time end) and t2.stat_end_time < (case when t1.next_hour_start_time <= @now_time then t1.next_hour_start_time else @now_time end) then datediff(ms,t2.stat_start_time,t2.stat_end_time)/cast(1000 as decimal) when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < (case when t1.next_hour_start_time <= @now_time then t1.next_hour_start_time else @now_time end) and t2.stat_end_time >= (case when t1.next_hour_start_time <= @now_time then t1.next_hour_start_time else @now_time end) then datediff(ms,t2.stat_start_time,(case when t1.next_hour_start_time <= @now_time then t1.next_hour_start_time else @now_time end))/cast(1000 as decimal) when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and t2.stat_end_time < (case when t1.next_hour_start_time <= @now_time then t1.next_hour_start_time else @now_time end) then datediff(ms,t1.hour_start_time,t2.stat_end_time)/cast(1000 as decimal) when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= (case when t1.next_hour_start_time <= @now_time then t1.next_hour_start_time else @now_time end) then datediff(ms,t1.hour_start_time,(case when t1.next_hour_start_time <= @now_time then t1.next_hour_start_time else @now_time end))/cast(1000 as decimal) end as the_hour_cost_seconds
-- case when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,@now_time) and t2.stat_end_time < least(t1.next_hour_start_time,@now_time) then UNIX_TIMESTAMP(t2.stat_end_time) - UNIX_TIMESTAMP(t2.stat_start_time) when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,@now_time) and t2.stat_end_time >= least(t1.next_hour_start_time,@now_time) then UNIX_TIMESTAMP(least(t1.next_hour_start_time,@now_time)) - UNIX_TIMESTAMP(t2.stat_start_time)when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and t2.stat_end_time < least(t1.next_hour_start_time,@now_time) then UNIX_TIMESTAMP(t2.stat_end_time) - UNIX_TIMESTAMP(t1.hour_start_time) when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= least(t1.next_hour_start_time,@now_time) then UNIX_TIMESTAMP(least(t1.next_hour_start_time,@now_time)) - UNIX_TIMESTAMP(t1.hour_start_time) end as the_hour_cost_seconds

from 
-- 小时维表
(select 
concat(CONVERT(varchar, GETDATE(), 23),' ',hour_start_time) as hour_start_time,
format(DATEADD(hh,1,concat(CONVERT(varchar, GETDATE(), 23),' ',hour_start_time)),'yyyy-MM-dd HH:00:00') as next_hour_start_time
from qt_smartreport.dbo.qtr_dim_hour
where concat(CONVERT(varchar, GETDATE(), 23),' ',hour_start_time) <= @now_time) t1
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
FORMAT(cast(@dt_day_start_time as datetime),'yyyy-MM-dd') as date_value,
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
      from phoenix_basic.dbo.basic_notification
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
                            COALESCE(cast(end_time as char), N'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.dbo.basic_notification
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
                     group by robot_code,COALESCE(cast(end_time as char), N'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
where t.date_value=FORMAT(cast(@dt_day_start_time as datetime),'yyyy-MM-dd')
) t2 on(
(t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < (case when t1.next_hour_start_time <= @now_time then t1.next_hour_start_time else @now_time end) and t2.stat_end_time < (case when t1.next_hour_start_time <= @now_time then t1.next_hour_start_time else @now_time end)) or 
(t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < (case when t1.next_hour_start_time <= @now_time then t1.next_hour_start_time else @now_time end) and t2.stat_end_time >= (case when t1.next_hour_start_time <= @now_time then t1.next_hour_start_time else @now_time end)) or 
(t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and t2.stat_end_time < (case when t1.next_hour_start_time <= @now_time then t1.next_hour_start_time else @now_time end)) or 
(t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= (case when t1.next_hour_start_time <= @now_time then t1.next_hour_start_time else @now_time end))
)


-- part3：异步表兼容逻辑

-- 定义时间参数
{% set now_time=datetime.datetime.now().strftime("'%Y-%m-%d %H:%M:%S'") %}  -- 客观当前时间
{% set dt_day_start_time=dt_relative_time(dt,default="%Y-%m-%d 00:00:00") %}  -- dt所在天的开始时间
{% set dt_next_day_start_time=dt_relative_time(dt,days=1,default="%Y-%m-%d 00:00:00") %}  -- dt所在天的下一天的开始时间


{% if db_type=="MYSQL" %}
-- mysql逻辑
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
                            COALESCE(end_time, 'unfinished') as end_time,
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
                     group by robot_code, COALESCE(end_time, 'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
where t.date_value=date({{ dt_day_start_time }})
) t2 on(
(t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,{{ now_time }}) and t2.stat_end_time < least(t1.next_hour_start_time,{{ now_time }})) or
(t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,{{ now_time }}) and t2.stat_end_time >= least(t1.next_hour_start_time,{{ now_time }})) or
(t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and t2.stat_end_time < least(t1.next_hour_start_time,{{ now_time }})) or
(t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= least(t1.next_hour_start_time,{{ now_time }}))
)
{% elif db_type=="SQLSERVER" %}
-- sqlserver逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
FORMAT(cast({{ dt_day_start_time }} as datetime),'yyyy-MM-dd') as date_value,
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
case when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < (case when t1.next_hour_start_time <= {{ now_time }} then t1.next_hour_start_time else {{ now_time }} end) and t2.stat_end_time < (case when t1.next_hour_start_time <= {{ now_time }} then t1.next_hour_start_time else {{ now_time }} end) then datediff(ms,t2.stat_start_time,t2.stat_end_time)/cast(1000 as decimal) when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < (case when t1.next_hour_start_time <= {{ now_time }} then t1.next_hour_start_time else {{ now_time }} end) and t2.stat_end_time >= (case when t1.next_hour_start_time <= {{ now_time }} then t1.next_hour_start_time else {{ now_time }} end) then datediff(ms,t2.stat_start_time,(case when t1.next_hour_start_time <= {{ now_time }} then t1.next_hour_start_time else {{ now_time }} end))/cast(1000 as decimal) when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and t2.stat_end_time < (case when t1.next_hour_start_time <= {{ now_time }} then t1.next_hour_start_time else {{ now_time }} end) then datediff(ms,t1.hour_start_time,t2.stat_end_time)/cast(1000 as decimal) when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= (case when t1.next_hour_start_time <= {{ now_time }} then t1.next_hour_start_time else {{ now_time }} end) then datediff(ms,t1.hour_start_time,(case when t1.next_hour_start_time <= {{ now_time }} then t1.next_hour_start_time else {{ now_time }} end))/cast(1000 as decimal) end as the_hour_cost_seconds
-- case when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,{{ now_time }}) and t2.stat_end_time < least(t1.next_hour_start_time,{{ now_time }}) then UNIX_TIMESTAMP(t2.stat_end_time) - UNIX_TIMESTAMP(t2.stat_start_time) when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < least(t1.next_hour_start_time,{{ now_time }}) and t2.stat_end_time >= least(t1.next_hour_start_time,{{ now_time }}) then UNIX_TIMESTAMP(least(t1.next_hour_start_time,{{ now_time }})) - UNIX_TIMESTAMP(t2.stat_start_time)when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and t2.stat_end_time < least(t1.next_hour_start_time,{{ now_time }}) then UNIX_TIMESTAMP(t2.stat_end_time) - UNIX_TIMESTAMP(t1.hour_start_time) when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= least(t1.next_hour_start_time,{{ now_time }}) then UNIX_TIMESTAMP(least(t1.next_hour_start_time,{{ now_time }})) - UNIX_TIMESTAMP(t1.hour_start_time) end as the_hour_cost_seconds

from
-- 小时维表
(select
concat(CONVERT(varchar, GETDATE(), 23),' ',hour_start_time) as hour_start_time,
format(DATEADD(hh,1,concat(CONVERT(varchar, GETDATE(), 23),' ',hour_start_time)),'yyyy-MM-dd HH:00:00') as next_hour_start_time
from qt_smartreport.qtr_dim_hour
where concat(CONVERT(varchar, GETDATE(), 23),' ',hour_start_time) <= {{ now_time }}) t1
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
FORMAT(cast({{ dt_day_start_time }} as datetime),'yyyy-MM-dd') as date_value,
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
                            COALESCE(cast(end_time as char), N'unfinished') as end_time,
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
                     group by robot_code, COALESCE(cast(end_time as char), N'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
where t.date_value=FORMAT(cast({{ dt_day_start_time }} as datetime),'yyyy-MM-dd')
) t2 on(
(t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < (case when t1.next_hour_start_time <= {{ now_time }} then t1.next_hour_start_time else {{ now_time }} end) and t2.stat_end_time < (case when t1.next_hour_start_time <= {{ now_time }} then t1.next_hour_start_time else {{ now_time }} end)) or
(t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < (case when t1.next_hour_start_time <= {{ now_time }} then t1.next_hour_start_time else {{ now_time }} end) and t2.stat_end_time >= (case when t1.next_hour_start_time <= {{ now_time }} then t1.next_hour_start_time else {{ now_time }} end)) or
(t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and t2.stat_end_time < (case when t1.next_hour_start_time <= {{ now_time }} then t1.next_hour_start_time else {{ now_time }} end)) or
(t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= (case when t1.next_hour_start_time <= {{ now_time }} then t1.next_hour_start_time else {{ now_time }} end))
)
{% endif %}