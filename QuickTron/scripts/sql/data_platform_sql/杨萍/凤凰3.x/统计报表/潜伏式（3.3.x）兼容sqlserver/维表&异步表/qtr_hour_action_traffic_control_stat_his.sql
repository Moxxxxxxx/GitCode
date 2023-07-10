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
tja.action_uid,  -- Action的ID
tja.action_begin_time,  -- Action创建时间
tja.action_end_time,    -- Action结束时间
tja.robot_code,    -- 机器人编码
tja.job_sn,        -- 机器人任务编码
COALESCE(jnh.job_type,jnr.job_type) as job_type,  -- 任务类型
case when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code else 'unknown' end  order_start_point,  -- 搬运作业单起始点
case when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code else 'unknown' end   order_target_point,  -- 搬运作业单目标点
tas.start_code as action_start_code,    -- Action起始点
tas.target_code as action_target_code,    -- Action目标点
cast(tas.estimate_distance/1000 as decimal(20,10)) as  estimate_distance,  -- 预估行驶距离（米）
cast(tas.rts_map_distance/1000 as decimal(20,10)) as actual_move_distance,  -- 实际行驶距离(米)
cast(case when COALESCE(tas.estimate_distance,0)!=0 then (tas.rts_map_distance/1000-tas.estimate_distance/1000)/(tas.estimate_distance/1000) end as decimal(20,10)) as detour_ratio,   -- 绕路距离比例
cast(case when tas.is_loading = 0 then tas.rts_map_distance/1000 end as decimal(20,10)) as empty_move_distance,  -- 空车移动距离（米）
case when tas.is_loading = 0 then (tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as empty_move_time,  -- 空车移动时长（秒）
cast(case when tas.is_loading = 0 and (tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) !=0 then (tas.rts_map_distance/1000)/(tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as decimal(20,10)) as empty_move_speed,  -- 空车移动速度（米/秒）
cast(case when tas.is_loading = 1 then tas.rts_map_distance/1000 end as decimal(20,10)) as loading_move_distance,  -- 带载移动距离（米）
case when tas.is_loading = 1 then (tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as loading_move_time,  -- 带载移动时长（秒）
cast(case when tas.is_loading = 1 and (tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) !=0 then (tas.rts_map_distance/1000)/(tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as decimal(20,10)) as loading_move_speed,  -- 带载移动速度（米/秒）
tas.parking_count,     -- 交控停车次数
tas.parking_time/1000 as parking_time,     -- 交控停车总时长（秒）
tas.deadlock_count ,   -- 死锁次数
tas.deadlock_time/1000 as  deadlock_time,    -- 死锁总时长（秒）
tas.quit_drl_count,   -- 退出区控次数
tas.drive_arc_count,    -- 弧形次数
tja.rotate_count as robot_rotate_count,  -- 机器人旋转次数
tja.rack_rotate_times as rack_rotate_count,   --  货架旋转次数 
from_unixtime(tas.lock_start_timestamp/1000) as lock_start_time,  -- 锁闭开始时间
from_unixtime(tas.lock_end_timestamp/1000) as lock_end_time  -- 锁闭结束时间
from phoenix_rms.job_action_statistics_data tja 
inner join phoenix_rts.action_statistics_data tas on tas.action_uid = tja.action_uid   -- 可能出现起始点和目标点一样的action，rts那边就不写数据了
left join phoenix_rss.transport_order_carrier_job tj on tj.job_sn = tja.job_sn
left join phoenix_rss.transport_order t on tj.order_id = t.id
left join phoenix_rms.job_history jnh on jnh.job_sn= tja.job_sn
left join phoenix_rms.job jnr on jnr.job_sn= tja.job_sn
where tja.action_end_time >= @dt_hour_start_time and tja.action_end_time < @dt_next_hour_start_time



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
tja.action_uid,  -- Action的ID
tja.action_begin_time,  -- Action创建时间
tja.action_end_time,    -- Action结束时间
tja.robot_code,    -- 机器人编码
tja.job_sn,        -- 机器人任务编码
COALESCE(jnh.job_type,jnr.job_type) as job_type,  -- 任务类型
case when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code else 'unknown' end  order_start_point,  -- 搬运作业单起始点
case when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code else 'unknown' end   order_target_point,  -- 搬运作业单目标点
tas.start_code as action_start_code,    -- Action起始点
tas.target_code as action_target_code,    -- Action目标点
cast(tas.estimate_distance/cast(1000 as decimal) as decimal(20,10)) as  estimate_distance,  -- 预估行驶距离（米）
cast(tas.rts_map_distance/cast(1000 as decimal) as decimal(20,10)) as actual_move_distance,  -- 实际行驶距离(米)
cast(case when COALESCE(tas.estimate_distance,0)!=0 then (tas.rts_map_distance/cast(1000 as decimal)-tas.estimate_distance/cast(1000 as decimal))/(tas.estimate_distance/cast(1000 as decimal)) end as decimal(20,10)) as detour_ratio,   -- 绕路距离比例
cast(case when tas.is_loading = 0 then tas.rts_map_distance/cast(1000 as decimal) end as decimal(20,10)) as empty_move_distance,  -- 空车移动距离（米）
case when tas.is_loading = 0 then (tas.lock_end_timestamp/cast(1000 as decimal)-tas.lock_start_timestamp/cast(1000 as decimal)) end as empty_move_time,  -- 空车移动时长（秒）
cast(case when tas.is_loading = 0 and (tas.lock_end_timestamp/cast(1000 as decimal)-tas.lock_start_timestamp/cast(1000 as decimal)) !=0 then (tas.rts_map_distance/cast(1000 as decimal))/(tas.lock_end_timestamp/cast(1000 as decimal)-tas.lock_start_timestamp/cast(1000 as decimal)) end as decimal(20,10)) as empty_move_speed,  -- 空车移动速度（米/秒）
cast(case when tas.is_loading = 1 then tas.rts_map_distance/cast(1000 as decimal) end as decimal(20,10)) as loading_move_distance,  -- 带载移动距离（米）
case when tas.is_loading = 1 then (tas.lock_end_timestamp/cast(1000 as decimal)-tas.lock_start_timestamp/cast(1000 as decimal)) end as loading_move_time,  -- 带载移动时长（秒）
cast(case when tas.is_loading = 1 and (tas.lock_end_timestamp/cast(1000 as decimal)-tas.lock_start_timestamp/cast(1000 as decimal)) !=0 then (tas.rts_map_distance/cast(1000 as decimal))/(tas.lock_end_timestamp/cast(1000 as decimal)-tas.lock_start_timestamp/cast(1000 as decimal)) end as decimal(20,10)) as loading_move_speed,  -- 带载移动速度（米/秒）
tas.parking_count,     -- 交控停车次数
tas.parking_time/cast(1000 as decimal) as parking_time,     -- 交控停车总时长（秒）
tas.deadlock_count ,   -- 死锁次数
tas.deadlock_time/cast(1000 as decimal) as  deadlock_time,    -- 死锁总时长（秒）
tas.quit_drl_count,   -- 退出区控次数
tas.drive_arc_count,    -- 弧形次数
tja.rotate_count as robot_rotate_count,  -- 机器人旋转次数
tja.rack_rotate_times as rack_rotate_count,   --  货架旋转次数 
DATEADD(S,tas.lock_start_timestamp/cast(1000 as decimal),'1970-01-01 08:00:00') as lock_start_time,  -- 锁闭开始时间
-- from_unixtime(tas.lock_start_timestamp/1000) as lock_start_time,  -- 锁闭开始时间
DATEADD(S,tas.lock_end_timestamp/cast(1000 as decimal),'1970-01-01 08:00:00') as lock_end_time  -- 锁闭结束时间
-- from_unixtime(tas.lock_end_timestamp/1000) as lock_end_time  -- 锁闭结束时间
from phoenix_rms.dbo.job_action_statistics_data tja 
inner join phoenix_rts.dbo.action_statistics_data tas on tas.action_uid = tja.action_uid   -- 可能出现起始点和目标点一样的action，rts那边就不写数据了
left join phoenix_rss.dbo.transport_order_carrier_job tj on tj.job_sn = tja.job_sn
left join phoenix_rss.dbo.transport_order t on tj.order_id = t.id
left join phoenix_rms.dbo.job_history jnh on jnh.job_sn= tja.job_sn
left join phoenix_rms.dbo.job jnr on jnr.job_sn= tja.job_sn
where tja.action_end_time >= @dt_hour_start_time and tja.action_end_time < @dt_next_hour_start_time





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
tja.action_uid,  -- Action的ID
tja.action_begin_time,  -- Action创建时间
tja.action_end_time,    -- Action结束时间
tja.robot_code,    -- 机器人编码
tja.job_sn,        -- 机器人任务编码
COALESCE(jnh.job_type,jnr.job_type) as job_type,  -- 任务类型
case when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code else 'unknown' end  order_start_point,  -- 搬运作业单起始点
case when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code else 'unknown' end   order_target_point,  -- 搬运作业单目标点
tas.start_code as action_start_code,    -- Action起始点
tas.target_code as action_target_code,    -- Action目标点
cast(tas.estimate_distance/1000 as decimal(20,10)) as  estimate_distance,  -- 预估行驶距离（米）
cast(tas.rts_map_distance/1000 as decimal(20,10)) as actual_move_distance,  -- 实际行驶距离(米)
cast(case when COALESCE(tas.estimate_distance,0)!=0 then (tas.rts_map_distance/1000-tas.estimate_distance/1000)/(tas.estimate_distance/1000) end as decimal(20,10)) as detour_ratio,   -- 绕路距离比例
cast(case when tas.is_loading = 0 then tas.rts_map_distance/1000 end as decimal(20,10)) as empty_move_distance,  -- 空车移动距离（米）
case when tas.is_loading = 0 then (tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as empty_move_time,  -- 空车移动时长（秒）
cast(case when tas.is_loading = 0 and (tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) !=0 then (tas.rts_map_distance/1000)/(tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as decimal(20,10)) as empty_move_speed,  -- 空车移动速度（米/秒）
cast(case when tas.is_loading = 1 then tas.rts_map_distance/1000 end as decimal(20,10)) as loading_move_distance,  -- 带载移动距离（米）
case when tas.is_loading = 1 then (tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as loading_move_time,  -- 带载移动时长（秒）
cast(case when tas.is_loading = 1 and (tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) !=0 then (tas.rts_map_distance/1000)/(tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as decimal(20,10)) as loading_move_speed,  -- 带载移动速度（米/秒）
tas.parking_count,     -- 交控停车次数
tas.parking_time/1000 as parking_time,     -- 交控停车总时长（秒）
tas.deadlock_count ,   -- 死锁次数
tas.deadlock_time/1000 as  deadlock_time,    -- 死锁总时长（秒）
tas.quit_drl_count,   -- 退出区控次数
tas.drive_arc_count,    -- 弧形次数
tja.rotate_count as robot_rotate_count,  -- 机器人旋转次数
tja.rack_rotate_times as rack_rotate_count,   --  货架旋转次数 
from_unixtime(tas.lock_start_timestamp/1000) as lock_start_time,  -- 锁闭开始时间
from_unixtime(tas.lock_end_timestamp/1000) as lock_end_time  -- 锁闭结束时间
from phoenix_rms.job_action_statistics_data tja
inner join phoenix_rts.action_statistics_data tas on tas.action_uid = tja.action_uid   -- 可能出现起始点和目标点一样的action，rts那边就不写数据了
left join phoenix_rss.transport_order_carrier_job tj on tj.job_sn = tja.job_sn
left join phoenix_rss.transport_order t on tj.order_id = t.id
left join phoenix_rms.job_history jnh on jnh.job_sn= tja.job_sn
left join phoenix_rms.job jnr on jnr.job_sn= tja.job_sn
where tja.action_end_time >= {{ dt_hour_start_time }} and tja.action_end_time < {{ dt_next_hour_start_time }}
{% elif db_type=="SQLSERVER" %}
-- sqlserver逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
FORMAT(cast({{ dt_hour_start_time }} as datetime),'yyyy-MM-dd') as date_value,
FORMAT(cast({{ dt_hour_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as hour_start_time,
FORMAT(cast({{ dt_next_hour_start_time }} as datetime), 'yyyy-MM-dd HH:00:00.0000000') as  next_hour_start_time,
tja.action_uid,  -- Action的ID
tja.action_begin_time,  -- Action创建时间
tja.action_end_time,    -- Action结束时间
tja.robot_code,    -- 机器人编码
tja.job_sn,        -- 机器人任务编码
COALESCE(jnh.job_type,jnr.job_type) as job_type,  -- 任务类型
case when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code else 'unknown' end  order_start_point,  -- 搬运作业单起始点
case when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code else 'unknown' end   order_target_point,  -- 搬运作业单目标点
tas.start_code as action_start_code,    -- Action起始点
tas.target_code as action_target_code,    -- Action目标点
cast(tas.estimate_distance/cast(1000 as decimal) as decimal(20,10)) as  estimate_distance,  -- 预估行驶距离（米）
cast(tas.rts_map_distance/cast(1000 as decimal) as decimal(20,10)) as actual_move_distance,  -- 实际行驶距离(米)
cast(case when COALESCE(tas.estimate_distance,0)!=0 then (tas.rts_map_distance/cast(1000 as decimal)-tas.estimate_distance/cast(1000 as decimal))/(tas.estimate_distance/cast(1000 as decimal)) end as decimal(20,10)) as detour_ratio,   -- 绕路距离比例
cast(case when tas.is_loading = 0 then tas.rts_map_distance/cast(1000 as decimal) end as decimal(20,10)) as empty_move_distance,  -- 空车移动距离（米）
case when tas.is_loading = 0 then (tas.lock_end_timestamp/cast(1000 as decimal)-tas.lock_start_timestamp/cast(1000 as decimal)) end as empty_move_time,  -- 空车移动时长（秒）
cast(case when tas.is_loading = 0 and (tas.lock_end_timestamp/cast(1000 as decimal)-tas.lock_start_timestamp/cast(1000 as decimal)) !=0 then (tas.rts_map_distance/cast(1000 as decimal))/(tas.lock_end_timestamp/cast(1000 as decimal)-tas.lock_start_timestamp/cast(1000 as decimal)) end as decimal(20,10)) as empty_move_speed,  -- 空车移动速度（米/秒）
cast(case when tas.is_loading = 1 then tas.rts_map_distance/cast(1000 as decimal) end as decimal(20,10)) as loading_move_distance,  -- 带载移动距离（米）
case when tas.is_loading = 1 then (tas.lock_end_timestamp/cast(1000 as decimal)-tas.lock_start_timestamp/cast(1000 as decimal)) end as loading_move_time,  -- 带载移动时长（秒）
cast(case when tas.is_loading = 1 and (tas.lock_end_timestamp/cast(1000 as decimal)-tas.lock_start_timestamp/cast(1000 as decimal)) !=0 then (tas.rts_map_distance/cast(1000 as decimal))/(tas.lock_end_timestamp/cast(1000 as decimal)-tas.lock_start_timestamp/cast(1000 as decimal)) end as decimal(20,10)) as loading_move_speed,  -- 带载移动速度（米/秒）
tas.parking_count,     -- 交控停车次数
tas.parking_time/cast(1000 as decimal) as parking_time,     -- 交控停车总时长（秒）
tas.deadlock_count ,   -- 死锁次数
tas.deadlock_time/cast(1000 as decimal) as  deadlock_time,    -- 死锁总时长（秒）
tas.quit_drl_count,   -- 退出区控次数
tas.drive_arc_count,    -- 弧形次数
tja.rotate_count as robot_rotate_count,  -- 机器人旋转次数
tja.rack_rotate_times as rack_rotate_count,   --  货架旋转次数 
DATEADD(S,tas.lock_start_timestamp/cast(1000 as decimal),'1970-01-01 08:00:00') as lock_start_time,  -- 锁闭开始时间
-- from_unixtime(tas.lock_start_timestamp/1000) as lock_start_time,  -- 锁闭开始时间
DATEADD(S,tas.lock_end_timestamp/cast(1000 as decimal),'1970-01-01 08:00:00') as lock_end_time  -- 锁闭结束时间
-- from_unixtime(tas.lock_end_timestamp/1000) as lock_end_time  -- 锁闭结束时间
from phoenix_rms.job_action_statistics_data tja
inner join phoenix_rts.action_statistics_data tas on tas.action_uid = tja.action_uid   -- 可能出现起始点和目标点一样的action，rts那边就不写数据了
left join phoenix_rss.transport_order_carrier_job tj on tj.job_sn = tja.job_sn
left join phoenix_rss.transport_order t on tj.order_id = t.id
left join phoenix_rms.job_history jnh on jnh.job_sn= tja.job_sn
left join phoenix_rms.job jnr on jnr.job_sn= tja.job_sn
where tja.action_end_time >= {{ dt_hour_start_time }} and tja.action_end_time < {{ dt_next_hour_start_time }}
{% endif %}