-- 表1：qt_smartreport.qtr_hour_action_traffic_control_stat_his

-- step1:删除相关数据（qtr_hour_action_traffic_control_stat_his）
DELETE
FROM qt_smartreport.qtr_hour_action_traffic_control_stat_his
where hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');	


-- step2:插入相关数据（qtr_hour_action_traffic_control_stat_his）
insert into qt_smartreport.qtr_hour_action_traffic_control_stat_his(create_time,update_time,date_value,hour_start_time,next_hour_start_time,action_uid,action_begin_time,action_end_time,robot_code,job_sn,job_type,order_start_point,order_target_point,action_start_code,action_target_code,estimate_distance,actual_move_distance,detour_ratio,empty_move_distance,empty_move_time,empty_move_speed,loading_move_distance,loading_move_time,loading_move_speed,parking_count,parking_time,deadlock_count,deadlock_time,quit_drl_count,drive_arc_count,robot_rotate_count,rack_rotate_count)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
date(DATE_ADD(sysdate(), INTERVAL -1 HOUR)) as date_value,
date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') as hour_start_time,
date_format(sysdate(), '%Y-%m-%d %H:00:00') as next_hour_start_time,
tja.action_uid as action_uid,  -- Action的ID
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
case when COALESCE(tas.estimate_distance,0)!=0 then (tas.rts_map_distance/1000 - tas.estimate_distance/1000)/(tas.estimate_distance/1000) end as detour_ratio,   -- 绕路距离比例
case when tja.is_loading = 0 then tas.rts_map_distance/1000 end as empty_move_distance,  -- 空车移动距离（米）
case when tja.is_loading = 0 then unix_timestamp(tja.action_end_time) - unix_timestamp(tja.action_begin_time) end as empty_move_time,  -- 空车移动时长（秒）
case when tja.is_loading = 0 then (tas.rts_map_distance/1000)/(unix_timestamp(tja.action_end_time) - unix_timestamp(tja.action_begin_time)) end as empty_move_speed,  -- 空车移动速度（米/秒）
case when tja.is_loading = 1 then tas.rts_map_distance/1000 end as loading_move_distance,  -- 带载移动距离（米）
case when tja.is_loading = 1 then unix_timestamp(tja.action_end_time) - unix_timestamp(tja.action_begin_time) end as loading_move_time,  -- 带载移动时长（秒）
case when tja.is_loading = 1 then (tas.rts_map_distance/1000)/(unix_timestamp(tja.action_end_time) - unix_timestamp(tja.action_begin_time)) end as loading_move_speed,  -- 带载移动速度（米/秒）
tas.parking_count,     -- 交控停车次数
tas.parking_time/1000 as parking_time,     -- 交控停车总时长（秒）
tas.deadlock_count ,   -- 死锁次数
tas.deadlock_time/1000 as  deadlock_time,    -- 死锁总时长（秒）
tas.quit_drl_count,   -- 退出区控次数
tas.drive_arc_count,    -- 弧形次数
tja.rotate_count as robot_rotate_count,  -- 机器人旋转次数
tja.rack_rotate_times as rack_rotate_count   --  货架旋转次数 
from phoenix_rms.job_action_statistics_data tja 
left join phoenix_rts.action_statistics_data tas on tas.action_uid = tja.action_uid 
left join phoenix_rss.transport_order_carrier_job tj on tj.job_sn = tja.job_sn
left join phoenix_rss.transport_order t on tj.order_id = t.id
left join phoenix_rms.job_history jnh on jnh.job_sn= tja.job_sn
left join phoenix_rms.job jnr on jnr.job_sn= tja.job_sn
where tja.action_end_time >= date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and tja.action_end_time < date_format(sysdate(), '%Y-%m-%d %H:00:00')