-- part1：mysql逻辑



-- mysql时间参数
set @now_time=sysdate();   --  当前时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间

select 
@now_time as create_time,
@now_time as update_time,
date(@dt_day_start_time) as date_value,
t1.id                                        as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.warning_spec,
t1.alarm_module,
t1.alarm_service,
t1.alarm_type,
t1.alarm_level,
t2.alarm_name as alarm_detail,
t1.param_value,
t1.job_order,
t1.robot_job,
t1.robot_code,
t1.device_code,
t1.server_code,
t1.transport_object
from phoenix_basic.basic_notification t1
left join phoenix_basic.basic_error_info t2 on t2.error_code =t1.error_code
where t1.alarm_module in ('system', 'server')
  and t1.alarm_level >= 3
        and (
              (t1.start_time >= @dt_day_start_time and t1.start_time < @dt_next_day_start_time and coalesce(t1.end_time, @now_time) < @dt_next_day_start_time) or
              (t1.start_time >= @dt_day_start_time and t1.start_time < @dt_next_day_start_time and coalesce(t1.end_time, @now_time) >= @dt_next_day_start_time) or
              (t1.start_time < @dt_day_start_time and coalesce(t1.end_time, @now_time) >= @dt_day_start_time and coalesce(t1.end_time, @now_time) < @dt_next_day_start_time) or
              (t1.start_time < @dt_day_start_time and coalesce(t1.end_time, @now_time) >= @dt_next_day_start_time)
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
t1.id                                        as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.warning_spec,
t1.alarm_module,
t1.alarm_service,
t1.alarm_type,
t1.alarm_level,
t2.alarm_name as alarm_detail,
t1.param_value,
t1.job_order,
t1.robot_job,
t1.robot_code,
t1.device_code,
t1.server_code,
t1.transport_object
from phoenix_basic.dbo.basic_notification t1
left join phoenix_basic.dbo.basic_error_info t2 on t2.error_code =t1.error_code
where t1.alarm_module in ('system', 'server')
  and t1.alarm_level >= 3
        and (
              (t1.start_time >= @dt_day_start_time and t1.start_time < @dt_next_day_start_time and coalesce(t1.end_time, @now_time) < @dt_next_day_start_time) or
              (t1.start_time >= @dt_day_start_time and t1.start_time < @dt_next_day_start_time and coalesce(t1.end_time, @now_time) >= @dt_next_day_start_time) or
              (t1.start_time < @dt_day_start_time and coalesce(t1.end_time, @now_time) >= @dt_day_start_time and coalesce(t1.end_time, @now_time) < @dt_next_day_start_time) or
              (t1.start_time < @dt_day_start_time and coalesce(t1.end_time, @now_time) >= @dt_next_day_start_time)
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
t1.id                                        as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.warning_spec,
t1.alarm_module,
t1.alarm_service,
t1.alarm_type,
t1.alarm_level,
t2.alarm_name as alarm_detail,
t1.param_value,
t1.job_order,
t1.robot_job,
t1.robot_code,
t1.device_code,
t1.server_code,
t1.transport_object
from phoenix_basic.basic_notification t1
left join phoenix_basic.basic_error_info t2 on t2.error_code =t1.error_code
where t1.alarm_module in ('system', 'server')
  and t1.alarm_level >= 3
        and (
              (t1.start_time >= {{ dt_day_start_time }} and t1.start_time < {{ dt_next_day_start_time }} and coalesce(t1.end_time, {{ now_time }}) < {{ dt_next_day_start_time }}) or
              (t1.start_time >= {{ dt_day_start_time }} and t1.start_time < {{ dt_next_day_start_time }} and coalesce(t1.end_time, {{ now_time }}) >= {{ dt_next_day_start_time }}) or
              (t1.start_time < {{ dt_day_start_time }} and coalesce(t1.end_time, {{ now_time }}) >= {{ dt_day_start_time }} and coalesce(t1.end_time, {{ now_time }}) < {{ dt_next_day_start_time }}) or
              (t1.start_time < {{ dt_day_start_time }} and coalesce(t1.end_time, {{ now_time }}) >= {{ dt_next_day_start_time }})
            )
{% elif db_type=="SQLSERVER" %}
-- sqlserver逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
FORMAT(cast({{ dt_day_start_time }} as datetime),'yyyy-MM-dd') as date_value,
t1.id                                        as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.warning_spec,
t1.alarm_module,
t1.alarm_service,
t1.alarm_type,
t1.alarm_level,
t2.alarm_name as alarm_detail,
t1.param_value,
t1.job_order,
t1.robot_job,
t1.robot_code,
t1.device_code,
t1.server_code,
t1.transport_object
from phoenix_basic.basic_notification t1
left join phoenix_basic.basic_error_info t2 on t2.error_code =t1.error_code
where t1.alarm_module in ('system', 'server')
  and t1.alarm_level >= 3
        and (
              (t1.start_time >= {{ dt_day_start_time }} and t1.start_time < {{ dt_next_day_start_time }} and coalesce(t1.end_time, {{ now_time }}) < {{ dt_next_day_start_time }}) or
              (t1.start_time >= {{ dt_day_start_time }} and t1.start_time < {{ dt_next_day_start_time }} and coalesce(t1.end_time, {{ now_time }}) >= {{ dt_next_day_start_time }}) or
              (t1.start_time < {{ dt_day_start_time }} and coalesce(t1.end_time, {{ now_time }}) >= {{ dt_day_start_time }} and coalesce(t1.end_time, {{ now_time }}) < {{ dt_next_day_start_time }}) or
              (t1.start_time < {{ dt_day_start_time }} and coalesce(t1.end_time, {{ now_time }}) >= {{ dt_next_day_start_time }})
            )
{% endif %}