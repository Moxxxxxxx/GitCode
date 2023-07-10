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
action_uid,  -- action的ID
action_begin_time,  -- action开始时间
action_end_time,  -- action结束时间
robot_code,   -- 机器人编码
job_sn,       -- 机器人任务编码
action_code,  -- action编码
is_loading,   -- action是否带载（action结束时）
terminal_guide_start_time, -- 末端引导开始时间
terminal_guide_end_time, -- 末端引导结束时间
UNIX_TIMESTAMP(terminal_guide_end_time)-UNIX_TIMESTAMP(terminal_guide_start_time) as terminal_guide_cost_time  -- 末端引导耗时
from 
(select 
t1.action_uid,
t1.action_begin_time,
t1.action_end_time,
t1.job_sn ,
case when t1.is_loading=1 then 1 else 0 end as is_loading,
t1.action_code, 
t1.robot_code, 
max(case when t2.operation_name='terminalGuide' then t2.start_time end) as terminal_guide_start_time,
max(case when t2.operation_name='terminalGuide' then t2.end_time end) as terminal_guide_end_time
from phoenix_rms.job_action_statistics_data t1
inner join phoenix_rms.job_action_operation_record t2 
on t2.action_uid =t1.action_uid and t2.operation_name in ('terminalGuide')
where t1.action_end_time >= @dt_hour_start_time and t1.action_end_time < @dt_next_hour_start_time
group by t1.action_uid,t1.action_begin_time,t1.action_end_time,t1.job_sn ,t1.is_loading,t1.action_code, t1.robot_code)t



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
action_uid,  -- action的ID
action_begin_time,  -- action开始时间
action_end_time,  -- action结束时间
robot_code,   -- 机器人编码
job_sn,       -- 机器人任务编码
action_code,  -- action编码
is_loading,   -- action是否带载（action结束时）
terminal_guide_start_time, -- 末端引导开始时间
terminal_guide_end_time, -- 末端引导结束时间
datediff(ms,terminal_guide_start_time,terminal_guide_end_time)/cast(1000 as decimal) as terminal_guide_cost_time  -- 末端引导耗时
-- UNIX_TIMESTAMP(terminal_guide_end_time)-UNIX_TIMESTAMP(terminal_guide_start_time) as terminal_guide_cost_time  -- 末端引导耗时
from 
(select 
t1.action_uid,
t1.action_begin_time,
t1.action_end_time,
t1.job_sn ,
case when t1.is_loading=1 then 1 else 0 end as is_loading,
t1.action_code, 
t1.robot_code, 
max(case when t2.operation_name='terminalGuide' then t2.start_time end) as terminal_guide_start_time,
max(case when t2.operation_name='terminalGuide' then t2.end_time end) as terminal_guide_end_time
from phoenix_rms.dbo.job_action_statistics_data t1
inner join phoenix_rms.dbo.job_action_operation_record t2 
on t2.action_uid =t1.action_uid and t2.operation_name in ('terminalGuide')
where t1.action_end_time >= @dt_hour_start_time and t1.action_end_time < @dt_next_hour_start_time
group by t1.action_uid,t1.action_begin_time,t1.action_end_time,t1.job_sn ,t1.is_loading,t1.action_code, t1.robot_code)t



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
action_uid,  -- action的ID
action_begin_time,  -- action开始时间
action_end_time,  -- action结束时间
robot_code,   -- 机器人编码
job_sn,       -- 机器人任务编码
action_code,  -- action编码
is_loading,   -- action是否带载（action结束时）
terminal_guide_start_time, -- 末端引导开始时间
terminal_guide_end_time, -- 末端引导结束时间
UNIX_TIMESTAMP(terminal_guide_end_time)-UNIX_TIMESTAMP(terminal_guide_start_time) as terminal_guide_cost_time  -- 末端引导耗时
from
(select
t1.action_uid,
t1.action_begin_time,
t1.action_end_time,
t1.job_sn ,
case when t1.is_loading=1 then 1 else 0 end as is_loading,
t1.action_code,
t1.robot_code,
max(case when t2.operation_name='terminalGuide' then t2.start_time end) as terminal_guide_start_time,
max(case when t2.operation_name='terminalGuide' then t2.end_time end) as terminal_guide_end_time
from phoenix_rms.job_action_statistics_data t1
inner join phoenix_rms.job_action_operation_record t2
on t2.action_uid =t1.action_uid and t2.operation_name in ('terminalGuide')
where t1.action_end_time >= {{ dt_hour_start_time }} and t1.action_end_time < {{ dt_next_hour_start_time }}
group by t1.action_uid,t1.action_begin_time,t1.action_end_time,t1.job_sn ,t1.is_loading,t1.action_code, t1.robot_code)t
{% elif db_type=="SQLSERVER" %}
-- sqlserver逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
FORMAT(cast({{ dt_hour_start_time }} as datetime),'yyyy-MM-dd') as date_value,
FORMAT(cast({{ dt_hour_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as hour_start_time,
FORMAT(cast({{ dt_next_hour_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as  next_hour_start_time,
action_uid,  -- action的ID
action_begin_time,  -- action开始时间
action_end_time,  -- action结束时间
robot_code,   -- 机器人编码
job_sn,       -- 机器人任务编码
action_code,  -- action编码
is_loading,   -- action是否带载（action结束时）
terminal_guide_start_time, -- 末端引导开始时间
terminal_guide_end_time, -- 末端引导结束时间
datediff(ms,terminal_guide_start_time,terminal_guide_end_time)/cast(1000 as decimal)  as terminal_guide_cost_time  -- 末端引导耗时
-- UNIX_TIMESTAMP(terminal_guide_end_time)-UNIX_TIMESTAMP(terminal_guide_start_time) as terminal_guide_cost_time  -- 末端引导耗时
from
(select
t1.action_uid,
t1.action_begin_time,
t1.action_end_time,
t1.job_sn ,
case when t1.is_loading=1 then 1 else 0 end as is_loading,
t1.action_code,
t1.robot_code,
max(case when t2.operation_name='terminalGuide' then t2.start_time end) as terminal_guide_start_time,
max(case when t2.operation_name='terminalGuide' then t2.end_time end) as terminal_guide_end_time
from phoenix_rms.job_action_statistics_data t1
inner join phoenix_rms.job_action_operation_record t2
on t2.action_uid =t1.action_uid and t2.operation_name in ('terminalGuide')
where t1.action_end_time >= {{ dt_hour_start_time }} and t1.action_end_time < {{ dt_next_hour_start_time }}
group by t1.action_uid,t1.action_begin_time,t1.action_end_time,t1.job_sn ,t1.is_loading,t1.action_code, t1.robot_code)t
{% endif %}