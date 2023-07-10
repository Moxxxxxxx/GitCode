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
date(DATE_ADD(@now_time, INTERVAL -1 HOUR)) as date_value,
date_format(DATE_ADD(@now_time, INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') as hour_start_time, 
date_format(@now_time, '%Y-%m-%d %H:00:00')  as next_hour_start_time,
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
tja.actual_move_distance*1000 as actual_move_distance,  -- 实际行驶距离(米)
(tja.actual_move_distance*1000)/(tas.estimate_distance/1000) as detour_ratio,   -- 绕路距离比例
case when tja.is_loading = 0 then tja.actual_move_distance*1000 end as empty_move_distance,  -- 空车移动距离（米）
case when tja.is_loading = 0 then unix_timestamp(tja.action_end_time) - unix_timestamp(tja.action_begin_time) end as empty_move_time,  -- 空车移动时长（秒）
case when tja.is_loading = 0 then (tja.actual_move_distance*1000)/(unix_timestamp(tja.action_end_time) - unix_timestamp(tja.action_begin_time)) end as empty_move_speed,  -- 空车移动速度（米/秒）
case when tja.is_loading = 1 then tja.actual_move_distance*1000 end as loading_move_distance,  -- 带载移动距离（米）
case when tja.is_loading = 1 then unix_timestamp(tja.action_end_time) - unix_timestamp(tja.action_begin_time) end as loading_move_time,  -- 带载移动时长（秒）
case when tja.is_loading = 1 then (tja.actual_move_distance*1000)/(unix_timestamp(tja.action_end_time) - unix_timestamp(tja.action_begin_time)) end as loading_move_speed,  -- 带载移动速度（米/秒）
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
where tja.action_end_time >= @pre_hour_start_time and tja.action_end_time < @pre_hour_end_time



##########################################################################################
##########################################################################################


# step1:建表（qt_hour_action_traffic_control_stat_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_hour_action_traffic_control_stat_his
(
    `id`                          int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`                  date      NOT NULL COMMENT '日期',
    `hour_start_time`             datetime  NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time`        datetime  NOT NULL COMMENT '下一个小时开始时间',
    `action_uid`                  varchar(255)        DEFAULT NULL COMMENT 'Action的ID',	
    `action_begin_time`           datetime(6)        DEFAULT NULL COMMENT 'Action创建时间',	
    `action_end_time`             datetime(6)        DEFAULT NULL COMMENT 'Action结束时间',	
    `robot_code`                  varchar(255)       DEFAULT NULL COMMENT '机器人编码',
    `job_sn`                      varchar(255)       DEFAULT NULL COMMENT '机器人任务编码',	
    `job_type`                    varchar(255)       DEFAULT NULL COMMENT '任务类型',
    `order_start_point`           varchar(255)       DEFAULT NULL COMMENT '搬运作业单起始点',
    `order_target_point`          varchar(255)       DEFAULT NULL COMMENT '搬运作业单目标点',
    `action_start_code`           varchar(255)       DEFAULT NULL COMMENT 'Action起始点',
    `action_target_code`          varchar(255)       DEFAULT NULL COMMENT 'Action目标点',	
    `estimate_distance`           decimal(65, 20)    DEFAULT NULL COMMENT '预估行驶距离（米）',	
    `actual_move_distance`        decimal(65, 20)    DEFAULT NULL COMMENT '实际行驶距离（米）',	
    `detour_ratio`                decimal(65, 20)    DEFAULT NULL COMMENT '绕路距离比例',	
    `empty_move_distance`         decimal(65, 20)    DEFAULT NULL COMMENT '空车移动距离（米）',	
    `empty_move_time`             decimal(65, 20)    DEFAULT NULL COMMENT '空车移动时长（秒）',	
    `empty_move_speed`            decimal(65, 20)    DEFAULT NULL COMMENT '空车移动速度（米/秒）',	
    `loading_move_distance`       decimal(65, 20)    DEFAULT NULL COMMENT '带载移动距离（米）',	
    `loading_move_time`           decimal(65, 20)    DEFAULT NULL COMMENT '带载移动时长（秒）',	
    `loading_move_speed`          decimal(65, 20)    DEFAULT NULL COMMENT '带载移动速度（米/秒）',		
    `parking_count`               bigint(20)    DEFAULT NULL COMMENT '交控停车次数',	
    `parking_time`                decimal(65, 20)    DEFAULT NULL COMMENT '交控停车总时长（秒）',		
    `deadlock_count`               bigint(20)    DEFAULT NULL COMMENT '死锁次数',	
    `deadlock_time`           decimal(65, 20)    DEFAULT NULL COMMENT '死锁总时长（秒）',	
    `quit_drl_count`               bigint(20)    DEFAULT NULL COMMENT '退出区控次数',	
    `drive_arc_count`               bigint(20)    DEFAULT NULL COMMENT '弧形次数',
    `robot_rotate_count`               bigint(20)    DEFAULT NULL COMMENT '机器人旋转次数',
    `rack_rotate_count`               bigint(20)    DEFAULT NULL COMMENT '货架旋转次数',	
    `created_time`                timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`                timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_hour_start_time (`hour_start_time`),
    key idx_next_hour_start_time (`next_hour_start_time`),
    key idx_action_uid (`action_uid`),	
    key idx_action_begin_time (`action_begin_time`),
    key idx_action_end_time (`action_end_time`),
    key idx_robot_code (`robot_code`),	
    key idx_job_sn (`job_sn`),
    key idx_job_type (`job_type`)	
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='小时内Action统计明细（H+1）';
	
	
	

# step2:删除相关数据（qt_hour_action_traffic_control_stat_his）
DELETE
FROM qt_smartreport.qt_hour_action_traffic_control_stat_his
where hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');	



# step3:插入相关数据（qt_hour_action_traffic_control_stat_his）
insert into qt_smartreport.qt_hour_action_traffic_control_stat_his(date_value,hour_start_time,next_hour_start_time,action_uid,action_begin_time,action_end_time,robot_code,job_sn,job_type,order_start_point,order_target_point,action_start_code,action_target_code,estimate_distance,actual_move_distance,detour_ratio,empty_move_distance,empty_move_time,empty_move_speed,loading_move_distance,loading_move_time,loading_move_speed,parking_count,parking_time,deadlock_count,deadlock_time,quit_drl_count,drive_arc_count,robot_rotate_count,rack_rotate_count)
select 
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


