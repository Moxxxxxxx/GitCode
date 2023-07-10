-- part1：mysql逻辑


-- mysql时间参数
set @now_time=sysdate();   --  当前时间
set @dt_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @dt_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间


select
@now_time as create_time,
@now_time as update_time,
date(@dt_hour_start_time) as date_value,
DATE_FORMAT(@dt_hour_start_time, '%Y-%m-%d %H:00:00.000000') as hour_start_time,
DATE_FORMAT(@dt_next_hour_start_time, '%Y-%m-%d %H:00:00.000000') as  next_hour_start_time,
t1.id                                     as error_id,
t1.error_code,
t1.start_time,
       t1.end_time,
       t1.warning_spec,
       t1.alarm_module,
       t1.alarm_service,
       t1.alarm_type,
       t1.alarm_level,
       t3.alarm_name as alarm_detail,
       t1.param_value,
       t1.job_order,
       t1.robot_job,
       t1.robot_code,
       t1.device_code,
       t1.server_code,
       t1.transport_object,
	   GREATEST(t1.start_time,@dt_hour_start_time)  as stat_start_time,
       case when t1.end_time is null or t1.end_time >= LEAST(@dt_next_hour_start_time,@now_time) then LEAST(@dt_next_hour_start_time,@now_time) else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
                         )
                     group by robot_code, COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
					left join phoenix_basic.basic_error_info t3 on t3.error_code =t1.error_code



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
FORMAT(cast(@dt_hour_start_time as datetime),'yyyy-MM-dd') as date_value,
FORMAT(cast(@dt_hour_start_time as datetime), 'yyyy-MM-dd HH:00:00.0000000') as hour_start_time,
FORMAT(cast(@dt_next_hour_start_time as datetime), 'yyyy-MM-dd HH:00:00.0000000') as  next_hour_start_time,
t1.id                                     as error_id,
t1.error_code,
t1.start_time,
       t1.end_time,
       t1.warning_spec,
       t1.alarm_module,
       t1.alarm_service,
       t1.alarm_type,
       t1.alarm_level,
       t3.alarm_name as alarm_detail,
       t1.param_value,
       t1.job_order,
       t1.robot_job,
       t1.robot_code,
       t1.device_code,
       t1.server_code,
       t1.transport_object,
	   case when t1.start_time < @dt_hour_start_time then @dt_hour_start_time else t1.start_time end as stat_start_time,
	   case when t1.end_time is null or t1.end_time >= (case when @dt_next_hour_start_time <= @now_time then @dt_next_hour_start_time else @now_time end) then (case when @dt_next_hour_start_time <= @now_time then @dt_next_hour_start_time else @now_time end) else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.dbo.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
               coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
               coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
              (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.dbo.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and
                              coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and
                              coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
                             (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
                         )
                     group by robot_code, COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
					left join phoenix_basic.dbo.basic_error_info t3 on t3.error_code =t1.error_code




-- part3：异步表兼容逻辑


-- 定义时间参数
{% set now_time=datetime.datetime.now().strftime("'%Y-%m-%d %H:%M:%S'") %}  -- 客观当前时间
{% set dt_hour_start_time=dt_relative_time(dt,default="%Y-%m-%d %H:00:00") %}   -- dt所在小时的开始时间
{% set dt_next_hour_start_time=dt_relative_time(dt,hours=1,default="%Y-%m-%d %H:00:00") %}  -- dt所在小时的下一个小时的开始时间


{% if db_type=="MYSQL" %}
-- mysql逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
date({{ dt_hour_start_time }}) as date_value,
DATE_FORMAT({{ dt_hour_start_time }}, '%Y-%m-%d %H:00:00.000000') as hour_start_time,
DATE_FORMAT({{ dt_next_hour_start_time }}, '%Y-%m-%d %H:00:00.000000') as  next_hour_start_time,
t1.id                                     as error_id,
t1.error_code,
t1.start_time,
       t1.end_time,
       t1.warning_spec,
       t1.alarm_module,
       t1.alarm_service,
       t1.alarm_type,
       t1.alarm_level,
       t3.alarm_name as alarm_detail,
       t1.param_value,
       t1.job_order,
       t1.robot_job,
       t1.robot_code,
       t1.device_code,
       t1.server_code,
       t1.transport_object,
	   GREATEST(t1.start_time,{{ dt_hour_start_time }})  as stat_start_time,
       case when t1.end_time is null or t1.end_time >= LEAST({{ dt_next_hour_start_time }},{{ now_time }}) then LEAST({{ dt_next_hour_start_time }},{{ now_time }}) else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
          )) t1
         inner join (select robot_code,
                            COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
                         )
                     group by robot_code, COALESCE(DATE_FORMAT(end_time, '%Y-%m-%d %H:%i:%s'), 'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
					left join phoenix_basic.basic_error_info t3 on t3.error_code =t1.error_code
{% elif db_type=="SQLSERVER" %}
-- sqlserver逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
FORMAT(cast({{ dt_hour_start_time }} as datetime),'yyyy-MM-dd') as date_value,
FORMAT(cast({{ dt_hour_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as hour_start_time,
FORMAT(cast({{ dt_next_hour_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as  next_hour_start_time,
t1.id                                     as error_id,
t1.error_code,
t1.start_time,
       t1.end_time,
       t1.warning_spec,
       t1.alarm_module,
       t1.alarm_service,
       t1.alarm_type,
       t1.alarm_level,
       t3.alarm_name as alarm_detail,
       t1.param_value,
       t1.job_order,
       t1.robot_job,
       t1.robot_code,
       t1.device_code,
       t1.server_code,
       t1.transport_object,
	   case when t1.start_time < {{ dt_hour_start_time }} then {{ dt_hour_start_time }} else t1.start_time end as stat_start_time,
	   case when t1.end_time is null or t1.end_time >= (case when {{ dt_next_hour_start_time }} <= {{ now_time }} then {{ dt_next_hour_start_time }} else {{ now_time }} end) then (case when {{ dt_next_hour_start_time }} <= {{ now_time }} then {{ dt_next_hour_start_time }} else {{ now_time }} end) else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
               coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
              (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
          )) t1
         inner join (select robot_code,
                            COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and
                              coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
                             (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
                         )
                     group by robot_code,COALESCE(cast(CONVERT(varchar(100), end_time, 20 ) as char), N'unfinished')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
					left join phoenix_basic.basic_error_info t3 on t3.error_code =t1.error_code
{% endif %}