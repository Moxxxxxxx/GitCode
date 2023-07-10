-- part1：mysql逻辑


-- mysql时间参数
set @now_time=sysdate();   --  当前时间
set @dt_week_start_time=date_format(DATE_SUB(sysdate(),INTERVAL WEEKDAY(sysdate()) + 0 DAY), '%Y-%m-%d 00:00:00'); -- 当前一周的开始时间
set @dt_next_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) -7 DAY), '%Y-%m-%d 00:00:00'); --  下一周的开始时间

select 
@now_time as create_time,
@now_time as update_time,
date(@dt_week_start_time) as date_value,
DATE_FORMAT(@dt_week_start_time, '%Y-%m-%d %H:00:00.000000') as week_start_time,
DATE_FORMAT(@dt_next_week_start_time, '%Y-%m-%d %H:00:00.000000') as  next_week_start_time,
t.error_id,
bn.error_code,
bn.start_time,
bn.end_time,
bn.warning_spec,
bn.alarm_module,
bn.alarm_service,
bn.alarm_type,
bn.alarm_level,
bn.alarm_detail,
bn.param_value,
bn.job_order,
bn.robot_job,
bn.robot_code,
bn.device_code,
bn.server_code,
bn.transport_object 
from 
(select distinct error_id  -- 一定要记得对之前小时维度的故障集合去重
from qt_smartreport.qtr_hour_sys_error_list_his 
where hour_start_time>= @dt_week_start_time and hour_start_time <  @dt_next_week_start_time)t 
left join phoenix_basic.basic_notification bn on bn.id=t.error_id




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
FORMAT(cast(@dt_week_start_time as datetime),'yyyy-MM-dd') as date_value,
FORMAT(cast(@dt_week_start_time as datetime), 'yyyy-MM-dd HH:00:00.0000000') as week_start_time,
FORMAT(cast(@dt_next_week_start_time as datetime), 'yyyy-MM-dd HH:00:00.0000000') as  next_week_start_time,
t.error_id,
bn.error_code,
bn.start_time,
bn.end_time,
bn.warning_spec,
bn.alarm_module,
bn.alarm_service,
bn.alarm_type,
bn.alarm_level,
bn.alarm_detail,
bn.param_value,
bn.job_order,
bn.robot_job,
bn.robot_code,
bn.device_code,
bn.server_code,
bn.transport_object 
from 
(select distinct error_id  -- 一定要记得对之前小时维度的故障集合去重
from qt_smartreport.dbo.qtr_hour_sys_error_list_his 
where hour_start_time>= @dt_week_start_time and hour_start_time <  @dt_next_week_start_time)t 
left join phoenix_basic.dbo.basic_notification bn on bn.id=t.error_id




-- part3：异步表兼容逻辑

-- 定义时间参数
{% set now_time=datetime.datetime.now().strftime("'%Y-%m-%d %H:%M:%S'") %}  -- 客观当前时间
{% set dt_week_start_time=(dt - datetime.timedelta(days=dt.now().weekday())).strftime("'%Y-%m-%d 00:00:00'") %}  -- dt所在周的开始时间
{% set dt_next_week_start_time=(dt + datetime.timedelta(days=7-dt.now().weekday())).strftime("'%Y-%m-%d 00:00:00'") %}  -- dt所在周的下一周的开始时间


{% if db_type=="MYSQL" %}
-- mysql逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
date({{ dt_week_start_time }}) as date_value,
DATE_FORMAT({{ dt_week_start_time }}, '%Y-%m-%d %H:00:00.000000') as week_start_time,
DATE_FORMAT({{ dt_next_week_start_time }}, '%Y-%m-%d %H:00:00.000000') as  next_week_start_time,
t.error_id,
bn.error_code,
bn.start_time,
bn.end_time,
bn.warning_spec,
bn.alarm_module,
bn.alarm_service,
bn.alarm_type,
bn.alarm_level,
bn.alarm_detail,
bn.param_value,
bn.job_order,
bn.robot_job,
bn.robot_code,
bn.device_code,
bn.server_code,
bn.transport_object
from
(select distinct error_id  -- 一定要记得对之前小时维度的故障集合去重
from qt_smartreport.qtr_hour_sys_error_list_his
where hour_start_time>= {{ dt_week_start_time }} and hour_start_time <  {{ dt_next_week_start_time }})t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
{% elif db_type=="SQLSERVER" %}
-- sqlserver逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
FORMAT(cast({{ dt_week_start_time }} as datetime),'yyyy-MM-dd') as date_value,
FORMAT(cast({{ dt_week_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as week_start_time,
FORMAT(cast({{ dt_next_week_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as  next_week_start_time,
t.error_id,
bn.error_code,
bn.start_time,
bn.end_time,
bn.warning_spec,
bn.alarm_module,
bn.alarm_service,
bn.alarm_type,
bn.alarm_level,
bn.alarm_detail,
bn.param_value,
bn.job_order,
bn.robot_job,
bn.robot_code,
bn.device_code,
bn.server_code,
bn.transport_object
from
(select distinct error_id  -- 一定要记得对之前小时维度的故障集合去重
from qt_smartreport.qtr_hour_sys_error_list_his
where hour_start_time>= {{ dt_week_start_time }} and hour_start_time <  {{ dt_next_week_start_time }})t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
{% endif %}