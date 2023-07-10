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
tbr.robot_code,
COALESCE(t1.uptime_state_duration,0) as uptime_state_duration,
COALESCE(t1.loading_busy_state_duration,0) as loading_busy_state_duration,
COALESCE(t1.empty_busy_state_duration,0) as empty_busy_state_duration,
COALESCE(t1.busy_state_duration,0) as busy_state_duration,
COALESCE(t1.charging_state_duration,0) as charging_state_duration,
COALESCE(t1.idle_state_duration,0) as idle_state_duration,
COALESCE(t1.locked_state_duration,0) as locked_state_duration,
COALESCE(t1.error_state_duration,0) as error_state_duration,
(UNIX_TIMESTAMP(LEAST(@dt_next_hour_start_time,@now_time))-UNIX_TIMESTAMP(@dt_hour_start_time))-(COALESCE(t1.loading_busy_state_duration,0)+COALESCE(t1.empty_busy_state_duration,0)+COALESCE(t1.charging_state_duration,0)+COALESCE(t1.idle_state_duration,0)+COALESCE(t1.locked_state_duration,0)+COALESCE(t1.error_state_duration,0)) as offline_duration  -- 离线时长
from 
-- 机器人集合
(select distinct robot_code from phoenix_basic.basic_robot)tbr
-- 机器人小时内各种状态的时长
left join 
(select 
ts.robot_code,
sum(case when ts.is_uptime_state = 1 then ts.stat_duration end)       as uptime_state_duration,  -- 开动时长
sum(case when ts.is_loading_busy_state = 1 then ts.stat_duration end) as loading_busy_state_duration,  -- 利用时长
sum(case when ts.is_empty_busy_state = 1 then ts.stat_duration end)   as empty_busy_state_duration,   -- 空闲作业时长
sum(case when ts.is_busy_state = 1 then ts.stat_duration end)         as busy_state_duration,  -- 搬运作业时长
sum(case when ts.is_charging_state = 1 then ts.stat_duration end)     as charging_state_duration, -- 充电时长
sum(case when ts.is_idle_state = 1 then ts.stat_duration end)         as idle_state_duration,  -- 空闲时长
sum(case when ts.is_locked_state = 1 then ts.stat_duration end)       as locked_state_duration,  --  锁定时长
sum(case when ts.is_error_state = 1 then ts.stat_duration end)        as error_state_duration   -- 异常时长
-- 机器人状态判断
from 
(select 
t.robot_code,
t.state_id,
t.online_state,
t.work_state,
t.job_sn,
case when (t.is_error != 1 and t.work_state in ('BUSY', 'CHARGING')) or ((t.work_state = 'ERROR' or t.is_error = 1) and t.job_sn is not null)then 1 else 0 end  as   is_uptime_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and ((tjh.job_sn is not null and tjh.job_type = 'CUSTOMIZE') or (tj.job_sn is not null and tj.job_type = 'CUSTOMIZE')) then 1 else 0 end as is_loading_busy_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and ((tjh.job_sn is not null and tjh.job_type != 'CUSTOMIZE') or (tj.job_sn is not null and tj.job_type != 'CUSTOMIZE')) then 1 else 0 end as is_empty_busy_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'BUSY' then 1 else 0 end as  is_busy_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'IDLE' then 1 else 0 end as  is_idle_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'CHARGING' then 1 else 0 end as is_charging_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'LOCKED' then 1 else 0 end as is_locked_state,
case when t.is_error = 1 or t.work_state = 'ERROR' then 1 else 0 end  as  is_error_state,
t.duration,
t.stat_state_duration	as  stat_duration			 				   
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
UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, LEAST(@dt_next_hour_start_time,@now_time)))-UNIX_TIMESTAMP(@dt_hour_start_time) as stat_state_duration  -- 每个机器人计算小时之前的最后一条状态在该小时内持续时长（秒）			
from 
-- 找到每个机器人此小时前最后一条记录
(select 
robot_code, max(id) as before_the_hour_last_id 
from phoenix_rms.robot_state_history
where create_time < @dt_hour_start_time
group by robot_code)t1         
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
-- 判断小时内最开始的一段时间内的状态
left join 
(select 
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >=  @dt_hour_start_time and create_time < @dt_next_hour_start_time
group by robot_code)t3 on t3.robot_code=t1.robot_code    -- 找到每个机器人此小时第一条记录
-- 判断小时内发生的记录的持续时长
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
case when t5.the_hour_last_id is not null then UNIX_TIMESTAMP(LEAST(@dt_next_hour_start_time,@now_time))-UNIX_TIMESTAMP(t4.create_time) else t4.duration / 1000 end as stat_state_duration  -- 每个机器人在计算小时内的每条状态持续时长（最后一条要做特殊处理）（秒）
from 
-- 每个机器人计算小时之内的状态变化数据
(select *
from phoenix_rms.robot_state_history 
where create_time >= @dt_hour_start_time and create_time < @dt_next_hour_start_time)t4 
-- 找到每个机器人在计算小时内的最后一条状态变化数据
left join 
(select 
robot_code, 
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time   
from phoenix_rms.robot_state_history
where create_time >= @dt_hour_start_time and create_time < @dt_next_hour_start_time
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)t 
left join (select job_sn, job_type from phoenix_rms.job_history) tjh on tjh.job_sn = t.job_sn
left join (select job_sn, job_type from phoenix_rms.job) tj on tj.job_sn = t.job_sn
)ts 
group by ts.robot_code)t1 on t1.robot_code=tbr.robot_code


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
tbr.robot_code,
COALESCE(t1.uptime_state_duration,0) as uptime_state_duration,
COALESCE(t1.loading_busy_state_duration,0) as loading_busy_state_duration,
COALESCE(t1.empty_busy_state_duration,0) as empty_busy_state_duration,
COALESCE(t1.busy_state_duration,0) as busy_state_duration,
COALESCE(t1.charging_state_duration,0) as charging_state_duration,
COALESCE(t1.idle_state_duration,0) as idle_state_duration,
COALESCE(t1.locked_state_duration,0) as locked_state_duration,
COALESCE(t1.error_state_duration,0) as error_state_duration,
(datediff(ms,@dt_hour_start_time,(case when @dt_next_hour_start_time <= @now_time then @dt_next_hour_start_time else @now_time end ))/cast(1000 as decimal))-(COALESCE(t1.loading_busy_state_duration,0)+COALESCE(t1.empty_busy_state_duration,0)+COALESCE(t1.charging_state_duration,0)+COALESCE(t1.idle_state_duration,0)+COALESCE(t1.locked_state_duration,0)+COALESCE(t1.error_state_duration,0)) as offline_duration  -- 离线时长
--   (UNIX_TIMESTAMP(LEAST(@dt_next_hour_start_time,@now_time))-UNIX_TIMESTAMP(@dt_hour_start_time))-(COALESCE(t1.loading_busy_state_duration,0)+COALESCE(t1.empty_busy_state_duration,0)+COALESCE(t1.charging_state_duration,0)+COALESCE(t1.idle_state_duration,0)+COALESCE(t1.locked_state_duration,0)+COALESCE(t1.error_state_duration,0)) as offline_duration  -- 离线时长
from
-- 机器人集合
(select distinct robot_code from phoenix_basic.dbo.basic_robot)tbr
-- 机器人小时内各种状态的时长
left join
(select
ts.robot_code,
sum(case when ts.is_uptime_state = 1 then ts.stat_duration end)       as uptime_state_duration,  -- 开动时长
sum(case when ts.is_loading_busy_state = 1 then ts.stat_duration end) as loading_busy_state_duration,  -- 利用时长
sum(case when ts.is_empty_busy_state = 1 then ts.stat_duration end)   as empty_busy_state_duration,   -- 空闲作业时长
sum(case when ts.is_busy_state = 1 then ts.stat_duration end)         as busy_state_duration,  -- 搬运作业时长
sum(case when ts.is_charging_state = 1 then ts.stat_duration end)     as charging_state_duration, -- 充电时长
sum(case when ts.is_idle_state = 1 then ts.stat_duration end)         as idle_state_duration,  -- 空闲时长
sum(case when ts.is_locked_state = 1 then ts.stat_duration end)       as locked_state_duration,  --  锁定时长
sum(case when ts.is_error_state = 1 then ts.stat_duration end)        as error_state_duration   -- 异常时长
-- 机器人状态判断
from
(select
t.robot_code,
t.state_id,
t.online_state,
t.work_state,
t.job_sn,
case when (t.is_error != 1 and t.work_state in ('BUSY', 'CHARGING')) or ((t.work_state = 'ERROR' or t.is_error = 1) and t.job_sn is not null)then 1 else 0 end  as   is_uptime_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and ((tjh.job_sn is not null and tjh.job_type = 'CUSTOMIZE') or (tj.job_sn is not null and tj.job_type = 'CUSTOMIZE')) then 1 else 0 end as is_loading_busy_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and ((tjh.job_sn is not null and tjh.job_type != 'CUSTOMIZE') or (tj.job_sn is not null and tj.job_type != 'CUSTOMIZE')) then 1 else 0 end as is_empty_busy_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'BUSY' then 1 else 0 end as  is_busy_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'IDLE' then 1 else 0 end as  is_idle_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'CHARGING' then 1 else 0 end as is_charging_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'LOCKED' then 1 else 0 end as is_locked_state,
case when t.is_error = 1 or t.work_state = 'ERROR' then 1 else 0 end  as  is_error_state,
t.duration,
t.stat_state_duration	as  stat_duration
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
t2.duration /cast(1000 as decimal) as                           duration,
case when @dt_next_hour_start_time <= @now_time then DATEDIFF(ms,@dt_hour_start_time,coalesce(t3.the_hour_first_create_time, @dt_next_hour_start_time))/cast(1000 as decimal) else DATEDIFF(ms,@dt_hour_start_time,coalesce(t3.the_hour_first_create_time, @now_time))/cast(1000 as decimal) end as stat_state_duration  -- 每个机器人计算小时之前的最后一条状态在该小时内持续时长（秒）
-- UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, LEAST(@dt_next_hour_start_time,@now_time)))-UNIX_TIMESTAMP(@dt_hour_start_time) as stat_state_duration  -- 每个机器人计算小时之前的最后一条状态在该小时内持续时长（秒）
from
-- 找到每个机器人此小时前最后一条记录
(select
robot_code, max(id) as before_the_hour_last_id
from phoenix_rms.dbo.robot_state_history
where create_time < @dt_hour_start_time
group by robot_code)t1
left join phoenix_rms.dbo.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
-- 判断小时内最开始的一段时间内的状态
left join
(select
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.dbo.robot_state_history
where create_time >=  @dt_hour_start_time and create_time < @dt_next_hour_start_time
group by robot_code)t3 on t3.robot_code=t1.robot_code    -- 找到每个机器人此小时第一条记录
-- 判断小时内发生的记录的持续时长
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
t4.duration /cast(1000 as decimal) as           duration,
case when t5.the_hour_last_id is not null and @dt_next_hour_start_time <= @now_time then DATEDIFF(ms,t4.create_time,@dt_next_hour_start_time)/cast(1000 as decimal) when t5.the_hour_last_id is not null and @dt_next_hour_start_time > @now_time then DATEDIFF(ms,t4.create_time,@now_time)/cast(1000 as decimal) else t4.duration /cast(1000 as decimal) end as stat_state_duration -- 每个机器人在计算小时内的每条状态持续时长（最后一条要做特殊处理）（秒）
-- case when t5.the_hour_last_id is not null then UNIX_TIMESTAMP(LEAST(@dt_next_hour_start_time,@now_time))-UNIX_TIMESTAMP(t4.create_time) else t4.duration /cast(1000 as decimal) end as stat_state_duration  -- 每个机器人在计算小时内的每条状态持续时长（最后一条要做特殊处理）（秒）
from
-- 每个机器人计算小时之内的状态变化数据
(select *
from phoenix_rms.dbo.robot_state_history
where create_time >= @dt_hour_start_time and create_time < @dt_next_hour_start_time)t4
-- 找到每个机器人在计算小时内的最后一条状态变化数据
left join
(select
robot_code,
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time
from phoenix_rms.dbo.robot_state_history
where create_time >= @dt_hour_start_time and create_time < @dt_next_hour_start_time
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)t
left join (select job_sn, job_type from phoenix_rms.dbo.job_history) tjh on tjh.job_sn = t.job_sn
left join (select job_sn, job_type from phoenix_rms.dbo.job) tj on tj.job_sn = t.job_sn
)ts
group by ts.robot_code)t1 on t1.robot_code=tbr.robot_code








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
tbr.robot_code,
COALESCE(t1.uptime_state_duration,0) as uptime_state_duration,
COALESCE(t1.loading_busy_state_duration,0) as loading_busy_state_duration,
COALESCE(t1.empty_busy_state_duration,0) as empty_busy_state_duration,
COALESCE(t1.busy_state_duration,0) as busy_state_duration,
COALESCE(t1.charging_state_duration,0) as charging_state_duration,
COALESCE(t1.idle_state_duration,0) as idle_state_duration,
COALESCE(t1.locked_state_duration,0) as locked_state_duration,
COALESCE(t1.error_state_duration,0) as error_state_duration,
(UNIX_TIMESTAMP(LEAST({{ dt_next_hour_start_time }},{{ now_time }}))-UNIX_TIMESTAMP({{ dt_hour_start_time }}))-(COALESCE(t1.loading_busy_state_duration,0)+COALESCE(t1.empty_busy_state_duration,0)+COALESCE(t1.charging_state_duration,0)+COALESCE(t1.idle_state_duration,0)+COALESCE(t1.locked_state_duration,0)+COALESCE(t1.error_state_duration,0)) as offline_duration  -- 离线时长
from
-- 机器人集合
(select distinct robot_code from phoenix_basic.basic_robot)tbr
-- 机器人小时内各种状态的时长
left join
(select
ts.robot_code,
sum(case when ts.is_uptime_state = 1 then ts.stat_duration end)       as uptime_state_duration,  -- 开动时长
sum(case when ts.is_loading_busy_state = 1 then ts.stat_duration end) as loading_busy_state_duration,  -- 利用时长
sum(case when ts.is_empty_busy_state = 1 then ts.stat_duration end)   as empty_busy_state_duration,   -- 空闲作业时长
sum(case when ts.is_busy_state = 1 then ts.stat_duration end)         as busy_state_duration,  -- 搬运作业时长
sum(case when ts.is_charging_state = 1 then ts.stat_duration end)     as charging_state_duration, -- 充电时长
sum(case when ts.is_idle_state = 1 then ts.stat_duration end)         as idle_state_duration,  -- 空闲时长
sum(case when ts.is_locked_state = 1 then ts.stat_duration end)       as locked_state_duration,  --  锁定时长
sum(case when ts.is_error_state = 1 then ts.stat_duration end)        as error_state_duration   -- 异常时长
-- 机器人状态判断
from
(select
t.robot_code,
t.state_id,
t.online_state,
t.work_state,
t.job_sn,
case when (t.is_error != 1 and t.work_state in ('BUSY', 'CHARGING')) or ((t.work_state = 'ERROR' or t.is_error = 1) and t.job_sn is not null)then 1 else 0 end  as   is_uptime_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and ((tjh.job_sn is not null and tjh.job_type = 'CUSTOMIZE') or (tj.job_sn is not null and tj.job_type = 'CUSTOMIZE')) then 1 else 0 end as is_loading_busy_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and ((tjh.job_sn is not null and tjh.job_type != 'CUSTOMIZE') or (tj.job_sn is not null and tj.job_type != 'CUSTOMIZE')) then 1 else 0 end as is_empty_busy_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'BUSY' then 1 else 0 end as  is_busy_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'IDLE' then 1 else 0 end as  is_idle_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'CHARGING' then 1 else 0 end as is_charging_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'LOCKED' then 1 else 0 end as is_locked_state,
case when t.is_error = 1 or t.work_state = 'ERROR' then 1 else 0 end  as  is_error_state,
t.duration,
t.stat_state_duration	as  stat_duration
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
UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, LEAST({{ dt_next_hour_start_time }},{{ now_time }})))-UNIX_TIMESTAMP({{ dt_hour_start_time }}) as stat_state_duration  -- 每个机器人计算小时之前的最后一条状态在该小时内持续时长（秒）
from
-- 找到每个机器人此小时前最后一条记录
(select
robot_code, max(id) as before_the_hour_last_id
from phoenix_rms.robot_state_history
where create_time < {{ dt_hour_start_time }}
group by robot_code)t1
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
-- 判断小时内最开始的一段时间内的状态
left join
(select
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >=  {{ dt_hour_start_time }} and create_time < {{ dt_next_hour_start_time }}
group by robot_code)t3 on t3.robot_code=t1.robot_code    -- 找到每个机器人此小时第一条记录
-- 判断小时内发生的记录的持续时长
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
case when t5.the_hour_last_id is not null then UNIX_TIMESTAMP(LEAST({{ dt_next_hour_start_time }},{{ now_time }}))-UNIX_TIMESTAMP(t4.create_time) else t4.duration / 1000 end as stat_state_duration  -- 每个机器人在计算小时内的每条状态持续时长（最后一条要做特殊处理）（秒）
from
-- 每个机器人计算小时之内的状态变化数据
(select *
from phoenix_rms.robot_state_history
where create_time >= {{ dt_hour_start_time }} and create_time < {{ dt_next_hour_start_time }})t4
-- 找到每个机器人在计算小时内的最后一条状态变化数据
left join
(select
robot_code,
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time
from phoenix_rms.robot_state_history
where create_time >= {{ dt_hour_start_time }} and create_time < {{ dt_next_hour_start_time }}
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)t
left join (select job_sn, job_type from phoenix_rms.job_history) tjh on tjh.job_sn = t.job_sn
left join (select job_sn, job_type from phoenix_rms.job) tj on tj.job_sn = t.job_sn
)ts
group by ts.robot_code)t1 on t1.robot_code=tbr.robot_code
{% elif db_type=="SQLSERVER" %}
-- sqlserver逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
FORMAT(cast({{ dt_hour_start_time }} as datetime),'yyyy-MM-dd') as date_value,
FORMAT(cast({{ dt_hour_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as hour_start_time,
FORMAT(cast({{ dt_next_hour_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as  next_hour_start_time,
tbr.robot_code,
COALESCE(t1.uptime_state_duration,0) as uptime_state_duration,
COALESCE(t1.loading_busy_state_duration,0) as loading_busy_state_duration,
COALESCE(t1.empty_busy_state_duration,0) as empty_busy_state_duration,
COALESCE(t1.busy_state_duration,0) as busy_state_duration,
COALESCE(t1.charging_state_duration,0) as charging_state_duration,
COALESCE(t1.idle_state_duration,0) as idle_state_duration,
COALESCE(t1.locked_state_duration,0) as locked_state_duration,
COALESCE(t1.error_state_duration,0) as error_state_duration,
(datediff(ms,{{ dt_hour_start_time }},(case when {{ dt_next_hour_start_time }} <= {{ now_time }} then {{ dt_next_hour_start_time }} else {{ now_time }} end ))/cast(1000 as decimal))-(COALESCE(t1.loading_busy_state_duration,0)+COALESCE(t1.empty_busy_state_duration,0)+COALESCE(t1.charging_state_duration,0)+COALESCE(t1.idle_state_duration,0)+COALESCE(t1.locked_state_duration,0)+COALESCE(t1.error_state_duration,0)) as offline_duration  -- 离线时长
--   (UNIX_TIMESTAMP(LEAST({{ dt_next_hour_start_time }},{{ now_time }}))-UNIX_TIMESTAMP({{ dt_hour_start_time }}))-(COALESCE(t1.loading_busy_state_duration,0)+COALESCE(t1.empty_busy_state_duration,0)+COALESCE(t1.charging_state_duration,0)+COALESCE(t1.idle_state_duration,0)+COALESCE(t1.locked_state_duration,0)+COALESCE(t1.error_state_duration,0)) as offline_duration  -- 离线时长
from
-- 机器人集合
(select distinct robot_code from phoenix_basic.basic_robot)tbr
-- 机器人小时内各种状态的时长
left join
(select
ts.robot_code,
sum(case when ts.is_uptime_state = 1 then ts.stat_duration end)       as uptime_state_duration,  -- 开动时长
sum(case when ts.is_loading_busy_state = 1 then ts.stat_duration end) as loading_busy_state_duration,  -- 利用时长
sum(case when ts.is_empty_busy_state = 1 then ts.stat_duration end)   as empty_busy_state_duration,   -- 空闲作业时长
sum(case when ts.is_busy_state = 1 then ts.stat_duration end)         as busy_state_duration,  -- 搬运作业时长
sum(case when ts.is_charging_state = 1 then ts.stat_duration end)     as charging_state_duration, -- 充电时长
sum(case when ts.is_idle_state = 1 then ts.stat_duration end)         as idle_state_duration,  -- 空闲时长
sum(case when ts.is_locked_state = 1 then ts.stat_duration end)       as locked_state_duration,  --  锁定时长
sum(case when ts.is_error_state = 1 then ts.stat_duration end)        as error_state_duration   -- 异常时长
-- 机器人状态判断
from
(select
t.robot_code,
t.state_id,
t.online_state,
t.work_state,
t.job_sn,
case when (t.is_error != 1 and t.work_state in ('BUSY', 'CHARGING')) or ((t.work_state = 'ERROR' or t.is_error = 1) and t.job_sn is not null)then 1 else 0 end  as   is_uptime_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and ((tjh.job_sn is not null and tjh.job_type = 'CUSTOMIZE') or (tj.job_sn is not null and tj.job_type = 'CUSTOMIZE')) then 1 else 0 end as is_loading_busy_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and ((tjh.job_sn is not null and tjh.job_type != 'CUSTOMIZE') or (tj.job_sn is not null and tj.job_type != 'CUSTOMIZE')) then 1 else 0 end as is_empty_busy_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'BUSY' then 1 else 0 end as  is_busy_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'IDLE' then 1 else 0 end as  is_idle_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'CHARGING' then 1 else 0 end as is_charging_state,
case when t.is_error != 1 and t.online_state = 'REGISTERED' and t.work_state = 'LOCKED' then 1 else 0 end as is_locked_state,
case when t.is_error = 1 or t.work_state = 'ERROR' then 1 else 0 end  as  is_error_state,
t.duration,
t.stat_state_duration	as  stat_duration
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
t2.duration /cast(1000 as decimal) as                           duration,
case when {{ dt_next_hour_start_time }} <= {{ now_time }} then DATEDIFF(ms,{{ dt_hour_start_time }},coalesce(t3.the_hour_first_create_time, {{ dt_next_hour_start_time }}))/cast(1000 as decimal) else DATEDIFF(ms,{{ dt_hour_start_time }},coalesce(t3.the_hour_first_create_time, {{ now_time }}))/cast(1000 as decimal) end as stat_state_duration  -- 每个机器人计算小时之前的最后一条状态在该小时内持续时长（秒）
-- UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, LEAST({{ dt_next_hour_start_time }},{{ now_time }})))-UNIX_TIMESTAMP({{ dt_hour_start_time }}) as stat_state_duration  -- 每个机器人计算小时之前的最后一条状态在该小时内持续时长（秒）
from
-- 找到每个机器人此小时前最后一条记录
(select
robot_code, max(id) as before_the_hour_last_id
from phoenix_rms.robot_state_history
where create_time < {{ dt_hour_start_time }}
group by robot_code)t1
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
-- 判断小时内最开始的一段时间内的状态
left join
(select
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >=  {{ dt_hour_start_time }} and create_time < {{ dt_next_hour_start_time }}
group by robot_code)t3 on t3.robot_code=t1.robot_code    -- 找到每个机器人此小时第一条记录
-- 判断小时内发生的记录的持续时长
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
t4.duration /cast(1000 as decimal) as           duration,
case when t5.the_hour_last_id is not null and {{ dt_next_hour_start_time }} <= {{ now_time }} then DATEDIFF(ms,t4.create_time,{{ dt_next_hour_start_time }})/cast(1000 as decimal) when t5.the_hour_last_id is not null and {{ dt_next_hour_start_time }} > {{ now_time }} then DATEDIFF(ms,t4.create_time,{{ now_time }})/cast(1000 as decimal) else t4.duration /cast(1000 as decimal) end as stat_state_duration -- 每个机器人在计算小时内的每条状态持续时长（最后一条要做特殊处理）（秒）
-- case when t5.the_hour_last_id is not null then UNIX_TIMESTAMP(LEAST({{ dt_next_hour_start_time }},{{ now_time }}))-UNIX_TIMESTAMP(t4.create_time) else t4.duration /cast(1000 as decimal) end as stat_state_duration  -- 每个机器人在计算小时内的每条状态持续时长（最后一条要做特殊处理）（秒）
from
-- 每个机器人计算小时之内的状态变化数据
(select *
from phoenix_rms.robot_state_history
where create_time >= {{ dt_hour_start_time }} and create_time < {{ dt_next_hour_start_time }})t4
-- 找到每个机器人在计算小时内的最后一条状态变化数据
left join
(select
robot_code,
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time
from phoenix_rms.robot_state_history
where create_time >= {{ dt_hour_start_time }} and create_time < {{ dt_next_hour_start_time }}
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)t
left join (select job_sn, job_type from phoenix_rms.job_history) tjh on tjh.job_sn = t.job_sn
left join (select job_sn, job_type from phoenix_rms.job) tj on tj.job_sn = t.job_sn
)ts
group by ts.robot_code)t1 on t1.robot_code=tbr.robot_code
{% endif %}