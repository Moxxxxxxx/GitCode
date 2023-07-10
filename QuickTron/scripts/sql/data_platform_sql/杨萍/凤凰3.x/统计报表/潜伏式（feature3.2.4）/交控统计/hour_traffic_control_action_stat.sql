select
tt.action_begin_time,  -- Action创建时间
tt.action_uid,  -- Action的ID
tt.job_sn,        -- 机器人任务编码
tt.job_type,   -- 任务类型
COALESCE(l.line_name, '未配置')     AS    line_name, -- 路线
tt.action_start_code,    -- Action起始点
tt.action_target_code,    -- Action目标点
tt.detour_ratio,   -- 绕路距离比例
tt.estimate_distance,  -- 预估行驶距离（米）
tt.actual_move_distance,  -- 实际行驶距离(米)
tt.empty_move_distance,  -- 空车移动距离（米）
tt.empty_move_time,  -- 空车移动时长（秒）
tt.empty_move_speed,  -- 空车移动速度（米/秒）
tt.loading_move_distance,  -- 带载移动距离（米）
tt.loading_move_time,  -- 带载移动时长（秒）
tt.loading_move_speed,  -- 带载移动速度（米/秒）
tt.parking_count,     -- 交控停车次数
tt.parking_time,     -- 交控停车总时长（秒）
tt.deadlock_count ,   -- 死锁次数
tt.deadlock_time,    -- 解死锁时长（秒）
tt.robot_rotate_count,  -- 机器人旋转次数
tt.rack_rotate_count,   -- 货架旋转次数
tt.drive_arc_count,    -- 弧形次数
tt.action_end_time,    -- Action结束时间
tt.lock_start_time,   -- 锁闭开始时间
tt.lock_end_time,    -- 锁闭结束时间
unix_timestamp(tt.lock_end_time)-unix_timestamp(tt.lock_start_time) as lock_time  -- 实际行驶时间 = 锁闭结束时间 - 锁闭开始时间
from 
(select 
date_value,hour_start_time,next_hour_start_time,action_uid,action_begin_time,action_end_time,robot_code,job_sn,job_type,order_start_point,order_target_point,action_start_code,action_target_code,estimate_distance,actual_move_distance,detour_ratio,empty_move_distance,empty_move_time,empty_move_speed,loading_move_distance,loading_move_time,loading_move_speed,parking_count,parking_time,deadlock_count,deadlock_time,quit_drl_count,drive_arc_count,robot_rotate_count,rack_rotate_count,lock_start_time,lock_end_time
from qt_smartreport.qt_hour_action_traffic_control_stat_his t
where t.hour_start_time BETWEEN  {start_time}  AND  {end_time}  
union all 
select 
date_value,hour_start_time,next_hour_start_time,action_uid,action_begin_time,action_end_time,robot_code,job_sn,job_type,order_start_point,order_target_point,action_start_code,action_target_code,estimate_distance,actual_move_distance,detour_ratio,empty_move_distance,empty_move_time,empty_move_speed,loading_move_distance,loading_move_time,loading_move_speed,parking_count,parking_time,deadlock_count,deadlock_time,quit_drl_count,drive_arc_count,robot_rotate_count,rack_rotate_count,lock_start_time,lock_end_time
from 
(select 
date( {now_hour_start_time} ) as date_value,
date_format( {now_hour_start_time} , '%Y-%m-%d %H:00:00') as hour_start_time,
date_format( {now_next_hour_start_time} , '%Y-%m-%d %H:00:00') as next_hour_start_time,
tja.action_uid,  -- Action的ID
tja.action_begin_time,  -- Action创建时间
tja.action_end_time,    -- Action结束时间
tja.robot_code,    -- 机器人编码
tja.job_sn,        -- 机器人任务编码
COALESCE(jnh.job_type,jnr.job_type) as job_type,  -- 任务类型
case when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code else 'unknow' end  order_start_point,  -- 搬运作业单起始点
case when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code else 'unknow' end   order_target_point,  -- 搬运作业单目标点
tas.start_code as action_start_code,    -- Action起始点
tas.target_code as action_target_code,    -- Action目标点
tas.estimate_distance/1000 as  estimate_distance,  -- 预估行驶距离（米）
tas.rts_map_distance/1000 as actual_move_distance,  -- 实际行驶距离(米)
case when COALESCE(tas.estimate_distance,0)!=0 then (tas.rts_map_distance/1000-tas.estimate_distance/1000)/(tas.estimate_distance/1000) end as detour_ratio,   -- 绕路距离比例
case when tas.is_loading = 0 then tas.rts_map_distance/1000 end as empty_move_distance,  -- 空车移动距离（米）
case when tas.is_loading = 0 then (tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as empty_move_time,  -- 空车移动时长（秒）
case when tas.is_loading = 0 then (tas.rts_map_distance/1000)/(tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as empty_move_speed,  -- 空车移动速度（米/秒）
case when tas.is_loading = 1 then tas.rts_map_distance/1000 end as loading_move_distance,  -- 带载移动距离（米）
case when tas.is_loading = 1 then (tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as loading_move_time,  -- 带载移动时长（秒）
case when tas.is_loading = 1 then (tas.rts_map_distance/1000)/(tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as loading_move_speed,  -- 带载移动速度（米/秒）
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
where tja.action_end_time >=  {now_hour_start_time}  and tja.action_end_time <  {now_next_hour_start_time} )t 
where t.hour_start_time BETWEEN  {start_time}  AND  {end_time} 
)tt
LEFT JOIN
 (SELECT DISTINCT tmp1.id AS line_id
                , tmp1.line_name
                , tmp1.estimate_move_time_consuming
                , tmp2.start_point_code
                , tmp3.target_point_code
  FROM qt_smartreport.carry_job_line_info_v4 tmp1
           LEFT JOIN
       qt_smartreport.carry_job_start_point_code_v4 tmp2
       ON
           tmp1.id = tmp2.line_id
           LEFT JOIN
       qt_smartreport.carry_job_target_point_code_v4 tmp3
       ON
           tmp1.id = tmp3.line_id) l
 ON
             tt.order_start_point = l.start_point_code
         AND
             tt.order_target_point = l.target_point_code




######################################################################################################################################
---  检查
######################################################################################################################################

set @now_time=sysdate();   --  当前时间
set @now_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00.000000000');  -- 当天开始时间
set @now_end_time=date_format(sysdate(), '%Y-%m-%d 23:59:59.999999999');   -- 当天结束时间
set @next_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00.000000000'); --  明天开始时间
set @now_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @now_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间
set @now_week_start_time= date_format(DATE_SUB(sysdate(),INTERVAL WEEKDAY(sysdate()) + 0 DAY), '%Y-%m-%d 00:00:00'); -- 当前一周的开始时间
set @now_next_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) -7 DAY), '%Y-%m-%d 00:00:00'); --  下一周的开始时间
set @pre_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');  -- 前一小时开始时间  
set @pre_hour_end_time=date_format(sysdate(), '%Y-%m-%d %H:00:00'); -- 前一小时结束时间
set @pre_day_start_time=date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000');  -- 前一天开始时间
set @pre_day_end_time=date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 23:59:59.999999999');  -- 前一天结束时间
set @pre_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) +7 DAY), '%Y-%m-%d 00:00:00.000000000'); -- 前一周开始时间
set @pre_week_end_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) +1 DAY), '%Y-%m-%d 23:59:59.999999999'); -- 前一周结束时间 
set @start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00.000000000');  -- 筛选框开始时间  默认当天开始时间
set @end_time = date_format(sysdate(), '%Y-%m-%d %H:59:59.999999999');  --  筛选框结束时间  默认当前小时结束时间
select @now_time,@now_start_time,@now_end_time,@next_start_time,@now_hour_start_time,@now_next_hour_start_time,@now_week_start_time,@now_next_week_start_time,@pre_hour_start_time,@pre_hour_end_time,@pre_day_start_time,@pre_day_end_time,@pre_week_start_time,@pre_week_end_time,@start_time,@end_time;

 
select
tt.action_begin_time,  -- Action创建时间
tt.action_uid,  -- Action的ID
tt.job_sn,        -- 机器人任务编码
tt.job_type,   -- 任务类型
COALESCE(l.line_name, '未配置')     AS    line_name, -- 路线
tt.action_start_code,    -- Action起始点
tt.action_target_code,    -- Action目标点
tt.detour_ratio,   -- 绕路距离比例
tt.estimate_distance,  -- 预估行驶距离（米）
tt.actual_move_distance,  -- 实际行驶距离(米)
tt.empty_move_distance,  -- 空车移动距离（米）
tt.empty_move_time,  -- 空车移动时长（秒）
tt.empty_move_speed,  -- 空车移动速度（米/秒）
tt.loading_move_distance,  -- 带载移动距离（米）
tt.loading_move_time,  -- 带载移动时长（秒）
tt.loading_move_speed,  -- 带载移动速度（米/秒）
tt.parking_count,     -- 交控停车次数
tt.parking_time,     -- 交控停车总时长（秒）
tt.deadlock_count ,   -- 死锁次数
tt.deadlock_time,    -- 解死锁时长（秒）
tt.robot_rotate_count,  -- 机器人旋转次数
tt.rack_rotate_count,   -- 货架旋转次数
tt.drive_arc_count,    -- 弧形次数
tt.action_end_time,    -- Action结束时间
tt.lock_start_time,   -- 锁闭开始时间
tt.lock_end_time,    -- 锁闭结束时间
unix_timestamp(tt.lock_end_time)-unix_timestamp(tt.lock_start_time) as lock_time  -- 实际行驶时间 = 锁闭结束时间 - 锁闭开始时间
from 
(select 
date_value,hour_start_time,next_hour_start_time,action_uid,action_begin_time,action_end_time,robot_code,job_sn,job_type,order_start_point,order_target_point,action_start_code,action_target_code,estimate_distance,actual_move_distance,detour_ratio,empty_move_distance,empty_move_time,empty_move_speed,loading_move_distance,loading_move_time,loading_move_speed,parking_count,parking_time,deadlock_count,deadlock_time,quit_drl_count,drive_arc_count,robot_rotate_count,rack_rotate_count,lock_start_time,lock_end_time
from qt_smartreport.qt_hour_action_traffic_control_stat_his t
where t.hour_start_time BETWEEN @start_time AND @end_time 
union all 
select 
date_value,hour_start_time,next_hour_start_time,action_uid,action_begin_time,action_end_time,robot_code,job_sn,job_type,order_start_point,order_target_point,action_start_code,action_target_code,estimate_distance,actual_move_distance,detour_ratio,empty_move_distance,empty_move_time,empty_move_speed,loading_move_distance,loading_move_time,loading_move_speed,parking_count,parking_time,deadlock_count,deadlock_time,quit_drl_count,drive_arc_count,robot_rotate_count,rack_rotate_count,lock_start_time,lock_end_time
from 
(select 
date(@now_hour_start_time) as date_value,
date_format(@now_hour_start_time, '%Y-%m-%d %H:00:00') as hour_start_time,
date_format(@now_next_hour_start_time, '%Y-%m-%d %H:00:00') as next_hour_start_time,
tja.action_uid,  -- Action的ID
tja.action_begin_time,  -- Action创建时间
tja.action_end_time,    -- Action结束时间
tja.robot_code,    -- 机器人编码
tja.job_sn,        -- 机器人任务编码
COALESCE(jnh.job_type,jnr.job_type) as job_type,  -- 任务类型
case when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code else 'unknow' end  order_start_point,  -- 搬运作业单起始点
case when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code else 'unknow' end   order_target_point,  -- 搬运作业单目标点
tas.start_code as action_start_code,    -- Action起始点
tas.target_code as action_target_code,    -- Action目标点
tas.estimate_distance/1000 as  estimate_distance,  -- 预估行驶距离（米）
tas.rts_map_distance/1000 as actual_move_distance,  -- 实际行驶距离(米)
case when COALESCE(tas.estimate_distance,0)!=0 then (tas.rts_map_distance/1000-tas.estimate_distance/1000)/(tas.estimate_distance/1000) end as detour_ratio,   -- 绕路距离比例
case when tas.is_loading = 0 then tas.rts_map_distance/1000 end as empty_move_distance,  -- 空车移动距离（米）
case when tas.is_loading = 0 then (tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as empty_move_time,  -- 空车移动时长（秒）
case when tas.is_loading = 0 then (tas.rts_map_distance/1000)/(tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as empty_move_speed,  -- 空车移动速度（米/秒）
case when tas.is_loading = 1 then tas.rts_map_distance/1000 end as loading_move_distance,  -- 带载移动距离（米）
case when tas.is_loading = 1 then (tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as loading_move_time,  -- 带载移动时长（秒）
case when tas.is_loading = 1 then (tas.rts_map_distance/1000)/(tas.lock_end_timestamp/1000-tas.lock_start_timestamp/1000) end as loading_move_speed,  -- 带载移动速度（米/秒）
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
where tja.action_end_time >= @now_hour_start_time and tja.action_end_time < @now_next_hour_start_time)t 
where t.hour_start_time BETWEEN @start_time AND @end_time
)tt
LEFT JOIN
 (SELECT DISTINCT tmp1.id AS line_id
                , tmp1.line_name
                , tmp1.estimate_move_time_consuming
                , tmp2.start_point_code
                , tmp3.target_point_code
  FROM qt_smartreport.carry_job_line_info_v4 tmp1
           LEFT JOIN
       qt_smartreport.carry_job_start_point_code_v4 tmp2
       ON
           tmp1.id = tmp2.line_id
           LEFT JOIN
       qt_smartreport.carry_job_target_point_code_v4 tmp3
       ON
           tmp1.id = tmp3.line_id) l
 ON
             tt.order_start_point = l.start_point_code
         AND
             tt.order_target_point = l.target_point_code

