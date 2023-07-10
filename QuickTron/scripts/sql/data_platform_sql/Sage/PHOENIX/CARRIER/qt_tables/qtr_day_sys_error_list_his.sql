-- part1：mysql逻辑


-- mysql时间参数
set @now_time=sysdate();   --  当前时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间

select 
@now_time as create_time,
@now_time as update_time,
date(@dt_day_start_time) as date_value,
t.error_id,
bn.error_code,
bn.start_time,
bn.end_time,
bn.warning_spec,
bn.alarm_module,
bn.alarm_service,
bn.alarm_type,
bn.alarm_level,
t2.alarm_name as alarm_detail,
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
where hour_start_time>= @dt_day_start_time and hour_start_time <  @dt_next_day_start_time)t 
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
left join phoenix_basic.basic_error_info t2 on t2.error_code =bn.error_code



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
t.error_id,
bn.error_code,
bn.start_time,
bn.end_time,
bn.warning_spec,
bn.alarm_module,
bn.alarm_service,
bn.alarm_type,
bn.alarm_level,
t2.alarm_name as alarm_detail,
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
where hour_start_time>= @dt_day_start_time and hour_start_time <  @dt_next_day_start_time)t 
left join phoenix_basic.dbo.basic_notification bn on bn.id=t.error_id
left join phoenix_basic.dbo.basic_error_info t2 on t2.error_code =bn.error_code




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
t.error_id,
bn.error_code,
bn.start_time,
bn.end_time,
bn.warning_spec,
bn.alarm_module,
bn.alarm_service,
bn.alarm_type,
bn.alarm_level,
t2.alarm_name as alarm_detail,
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
where hour_start_time>= {{ dt_day_start_time }} and hour_start_time <  {{ dt_next_day_start_time }})t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
left join phoenix_basic.basic_error_info t2 on t2.error_code =bn.error_code
{% elif db_type=="SQLSERVER" %}
-- sqlserver逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
FORMAT(cast({{ dt_day_start_time }} as datetime),'yyyy-MM-dd') as date_value,
t.error_id,
bn.error_code,
bn.start_time,
bn.end_time,
bn.warning_spec,
bn.alarm_module,
bn.alarm_service,
bn.alarm_type,
bn.alarm_level,
t2.alarm_name as alarm_detail,
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
where hour_start_time>= {{ dt_day_start_time }} and hour_start_time <  {{ dt_next_day_start_time }})t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
left join phoenix_basic.basic_error_info t2 on t2.error_code =bn.error_code
{% endif %}