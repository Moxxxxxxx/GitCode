-- 用于：统计报表->现场交控统计->交控统计

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
tt.lock_start_time,  -- 锁闭开始时间
tt.lock_end_time,  -- 锁闭结束时间
unix_timestamp(tt.lock_end_time)-unix_timestamp(tt.lock_start_time) as lock_time  -- 实际行驶时间 = 锁闭结束时间 - 锁闭开始时间
from qt_smartreport.qtr_hour_action_traffic_control_stat_his tt
LEFT JOIN
(SELECT DISTINCT tmp1.id AS line_id
                , tmp1.line_name
                , tmp1.estimate_move_time_consuming
                , tmp2.start_point_code
                , tmp3.target_point_code
  FROM qt_smartreport.carry_job_line_info_v4 tmp1
  LEFT JOIN qt_smartreport.carry_job_start_point_code_v4 tmp2 ON tmp1.id = tmp2.line_id
  LEFT JOIN qt_smartreport.carry_job_target_point_code_v4 tmp3 ON tmp1.id = tmp3.line_id)l
 ON tt.order_start_point = l.start_point_code AND tt.order_target_point = l.target_point_code
where tt.hour_start_time BETWEEN {start_time}  AND  {end_time}
 
 
 
 
 


#############################################################################################
---  检查
#############################################################################################
-- { now_time }
-- { start_time }
-- { end_time }
set @now_time = sysdate(); --  当前时间
set @start_time = date_format(sysdate(), '%Y-%m-%d 00:00:00.000000000'); -- 筛选框开始时间  默认当天开始时间
set @end_time = date_format(sysdate(), '%Y-%m-%d %H:59:59.999999999'); --  筛选框结束时间  默认当前小时结束时间
select @now_time, @start_time, @end_time;

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
tt.lock_start_time,  -- 锁闭开始时间
tt.lock_end_time,  -- 锁闭结束时间
unix_timestamp(tt.lock_end_time)-unix_timestamp(tt.lock_start_time) as lock_time  -- 实际行驶时间 = 锁闭结束时间 - 锁闭开始时间
from qt_smartreport.qtr_hour_action_traffic_control_stat_his tt
LEFT JOIN
(SELECT DISTINCT tmp1.id AS line_id
                , tmp1.line_name
                , tmp1.estimate_move_time_consuming
                , tmp2.start_point_code
                , tmp3.target_point_code
  FROM qt_smartreport.carry_job_line_info_v4 tmp1
  LEFT JOIN qt_smartreport.carry_job_start_point_code_v4 tmp2 ON tmp1.id = tmp2.line_id
  LEFT JOIN qt_smartreport.carry_job_target_point_code_v4 tmp3 ON tmp1.id = tmp3.line_id)l
 ON tt.order_start_point = l.start_point_code AND tt.order_target_point = l.target_point_code
where tt.hour_start_time BETWEEN @start_time AND @end_time