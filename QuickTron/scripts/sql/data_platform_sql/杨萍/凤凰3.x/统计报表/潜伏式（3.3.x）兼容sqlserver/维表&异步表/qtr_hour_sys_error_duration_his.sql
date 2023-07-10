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
ts.alarm_service,
case when @now_time < @dt_next_hour_start_time then unix_timestamp(date_format(DATE_ADD(@now_time, INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s'))-unix_timestamp (@dt_hour_start_time) else 3600 end as sys_run_duration,  -- 小时内理论运行时长
COALESCE(te.sys_error_duration,0) as sys_error_duration,
COALESCE(te.sys_error_num,0) as sys_error_num
from 
-- 系统服务集合
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
-- 系统小时内故障时长（去重）、故障次数
left join 
(select 
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as sys_error_duration,
count(distinct t.error_id) as sys_error_num
from 
(select 
alarm_service as sys_name,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when end_time=date_format(@now_time, '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag 
-- 小时内故障集合
from 
(select alarm_service,
       id as error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   GREATEST(start_time,@dt_hour_start_time) AS start_time,
	   LEAST(COALESCE(end_time,@now_time),@dt_next_hour_start_time) AS end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
        (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
        (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
        (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
    )
order by alarm_service,original_start_time asc)t)t
-- 生成3600序列
left join (select seq_list  from qt_smartreport.qtr_dim_hour_seconds_sequence where seq_list >=1 and seq_list <= 3600) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)te on te.alarm_service=ts.alarm_service


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
ts.alarm_service,
case when @now_time < @dt_next_hour_start_time then DATEDIFF(ss,@dt_hour_start_time,DATEADD(ss,1,@now_time)) else 3600 end as sys_run_duration,  -- 小时内理论运行时长
-- case when @now_time < @dt_next_hour_start_time then unix_timestamp(date_format(DATE_ADD(@now_time, INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s'))-unix_timestamp (@dt_hour_start_time) else 3600 end as sys_run_duration,  -- 小时内理论运行时长
COALESCE(te.sys_error_duration,0) as sys_error_duration,
COALESCE(te.sys_error_num,0) as sys_error_num
from 
-- 系统服务集合
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.dbo.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
-- 系统小时内故障时长（去重）、故障次数
left join 
(select 
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as sys_error_duration,
count(distinct t.error_id) as sys_error_num
from 
(select 
alarm_service as sys_name,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(SUBSTRING(FORMAT(start_time,'yyyy-MM-dd HH:mm:ss'),15,2) as int)*60 + cast(SUBSTRING(FORMAT(start_time,'yyyy-MM-dd HH:mm:ss'),18,2) as int)+1 as start_seq_lag,
-- cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when FORMAT(end_time,'yyyy-MM-dd HH:mm:ss')=FORMAT(cast(@now_time as datetime),'yyyy-MM-dd HH:00:00') then 3600 else cast(SUBSTRING(FORMAT(end_time,'yyyy-MM-dd HH:mm:ss'),15,2) as int)*60+cast(SUBSTRING(FORMAT(end_time,'yyyy-MM-dd HH:mm:ss'),18,2) as int)+1 end as end_seq_lag
-- case when end_time=date_format(@now_time, '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag 
-- 小时内故障集合
from 
(select alarm_service,
       id as error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time >= @dt_hour_start_time then start_time else @dt_hour_start_time end as start_time,
	   -- GREATEST(start_time,@dt_hour_start_time) AS start_time,
	   case when COALESCE(end_time,@now_time) <= @dt_next_hour_start_time then COALESCE(end_time,@now_time) else @dt_next_hour_start_time end as end_time
	   -- LEAST(COALESCE(end_time,@now_time),@dt_next_hour_start_time) AS end_time
from phoenix_basic.dbo.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
        (start_time >= @dt_hour_start_time and start_time < @dt_next_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time) or
        (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_hour_start_time and coalesce(end_time, @now_time) < @dt_next_hour_start_time) or
        (start_time < @dt_hour_start_time and coalesce(end_time, @now_time) >= @dt_next_hour_start_time)
    )
-- order by alarm_service,original_start_time asc
)t)t
-- 生成3600序列
left join (select seq_list from qt_smartreport.dbo.qtr_dim_hour_seconds_sequence where seq_list >=1 and seq_list <= 3600) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)te on te.alarm_service=ts.alarm_service



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
ts.alarm_service,
case when {{ now_time }} < {{ dt_next_hour_start_time }} then unix_timestamp(date_format(DATE_ADD({{ now_time }}, INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s'))-unix_timestamp ({{ dt_hour_start_time }}) else 3600 end as sys_run_duration,  -- 小时内理论运行时长
COALESCE(te.sys_error_duration,0) as sys_error_duration,
COALESCE(te.sys_error_num,0) as sys_error_num
from
-- 系统服务集合
(select
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
-- 系统小时内故障时长（去重）、故障次数
left join
(select
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as sys_error_duration,
count(distinct t.error_id) as sys_error_num
from
(select
alarm_service as sys_name,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when end_time=date_format({{ now_time }}, '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag
-- 小时内故障集合
from
(select alarm_service,
       id as error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   GREATEST(start_time,{{ dt_hour_start_time }}) AS start_time,
	   LEAST(COALESCE(end_time,{{ now_time }}),{{ dt_next_hour_start_time }}) AS end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
        (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
        (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
        (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
    )
order by alarm_service,original_start_time asc)t)t
-- 生成3600序列
left join (select seq_list  from qt_smartreport.qtr_dim_hour_seconds_sequence where seq_list >=1 and seq_list <= 3600) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)te on te.alarm_service=ts.alarm_service
{% elif db_type=="SQLSERVER" %}
-- sqlserver逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
FORMAT(cast({{ dt_hour_start_time }} as datetime),'yyyy-MM-dd') as date_value,
FORMAT(cast({{ dt_hour_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as hour_start_time,
FORMAT(cast({{ dt_next_hour_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as  next_hour_start_time,
ts.alarm_service,
case when {{ now_time }} < {{ dt_next_hour_start_time }} then DATEDIFF(ss,{{ dt_hour_start_time }},DATEADD(ss,1,{{ now_time }})) else 3600 end as sys_run_duration,  -- 小时内理论运行时长
-- case when {{ now_time }} < {{ dt_next_hour_start_time }} then unix_timestamp(date_format(DATE_ADD({{ now_time }}, INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s'))-unix_timestamp ({{ dt_hour_start_time }}) else 3600 end as sys_run_duration,  -- 小时内理论运行时长
COALESCE(te.sys_error_duration,0) as sys_error_duration,
COALESCE(te.sys_error_num,0) as sys_error_num
from
-- 系统服务集合
(select
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
-- 系统小时内故障时长（去重）、故障次数
left join
(select
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as sys_error_duration,
count(distinct t.error_id) as sys_error_num
from
(select
alarm_service as sys_name,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(SUBSTRING(FORMAT(start_time,'yyyy-MM-dd HH:mm:ss'),15,2) as int)*60 + cast(SUBSTRING(FORMAT(start_time,'yyyy-MM-dd HH:mm:ss'),18,2) as int)+1 as start_seq_lag,
-- cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when FORMAT(end_time,'yyyy-MM-dd HH:mm:ss')=FORMAT(cast({{ now_time }} as datetime),'yyyy-MM-dd HH:00:00') then 3600 else cast(SUBSTRING(FORMAT(end_time,'yyyy-MM-dd HH:mm:ss'),15,2) as int)*60+cast(SUBSTRING(FORMAT(end_time,'yyyy-MM-dd HH:mm:ss'),18,2) as int)+1 end as end_seq_lag
-- case when end_time=date_format({{ now_time }}, '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag
-- 小时内故障集合
from
(select alarm_service,
       id as error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time >= {{ dt_hour_start_time }} then start_time else {{ dt_hour_start_time }} end as start_time,
	   -- GREATEST(start_time,{{ dt_hour_start_time }}) AS start_time,
	   case when COALESCE(end_time,{{ now_time }}) <= {{ dt_next_hour_start_time }} then COALESCE(end_time,{{ now_time }}) else {{ dt_next_hour_start_time }} end as end_time
	   -- LEAST(COALESCE(end_time,{{ now_time }}),{{ dt_next_hour_start_time }}) AS end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
        (start_time >= {{ dt_hour_start_time }} and start_time < {{ dt_next_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }}) or
        (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) < {{ dt_next_hour_start_time }}) or
        (start_time < {{ dt_hour_start_time }} and coalesce(end_time, {{ now_time }}) >= {{ dt_next_hour_start_time }})
    )
-- order by alarm_service,original_start_time asc
)t)t
-- 生成3600序列
left join (select seq_list from qt_smartreport.qtr_dim_hour_seconds_sequence where seq_list >=1 and seq_list <= 3600) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)te on te.alarm_service=ts.alarm_service
{% endif %}