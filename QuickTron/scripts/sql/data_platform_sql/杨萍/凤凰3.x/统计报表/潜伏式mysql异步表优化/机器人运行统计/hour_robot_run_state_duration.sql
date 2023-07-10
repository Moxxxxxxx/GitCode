-- 用于：统计报表->机器人统计->充电统计->机器人运行统计 
 
select
t.hour_start_time as hour_value,
t.robot_code,
brt.robot_type_code,
brt.robot_type_name,
COALESCE(t.uptime_state_duration, 0) / (UNIX_TIMESTAMP(LEAST(t.next_hour_start_time,{ now_time }))-UNIX_TIMESTAMP(t.hour_start_time)) as uptime_state_rate,
t.uptime_state_duration,
(UNIX_TIMESTAMP(LEAST(t.next_hour_start_time,{ now_time }))-UNIX_TIMESTAMP(t.hour_start_time)) as uptime_state_rate_fenmu,
COALESCE(t.loading_busy_state_duration, 0) / (UNIX_TIMESTAMP(LEAST(t.next_hour_start_time,{ now_time }))-UNIX_TIMESTAMP(t.hour_start_time)) as utilization_rate,
t.loading_busy_state_duration as utilization_duration,
(UNIX_TIMESTAMP(LEAST(t.next_hour_start_time,{ now_time }))-UNIX_TIMESTAMP(t.hour_start_time)) as utilization_rate_fenmu,
t.loading_busy_state_duration,
t.empty_busy_state_duration,
t.charging_state_duration,
t.idle_state_duration,
t.locked_state_duration,
t.error_state_duration,
t.offline_duration
from qt_smartreport.qtr_hour_robot_state_duration_his t
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where  t.hour_start_time BETWEEN { start_time } and { end_time }





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
t.hour_start_time as hour_value,
t.robot_code,
brt.robot_type_code,
brt.robot_type_name,	   
COALESCE(t.uptime_state_duration, 0) / (UNIX_TIMESTAMP(LEAST(t.next_hour_start_time,@now_time))-UNIX_TIMESTAMP(t.hour_start_time)) as uptime_state_rate,
t.uptime_state_duration,
(UNIX_TIMESTAMP(LEAST(t.next_hour_start_time,@now_time))-UNIX_TIMESTAMP(t.hour_start_time)) as uptime_state_rate_fenmu,
COALESCE(t.loading_busy_state_duration, 0) / (UNIX_TIMESTAMP(LEAST(t.next_hour_start_time,@now_time))-UNIX_TIMESTAMP(t.hour_start_time)) as utilization_rate,	   
t.loading_busy_state_duration as utilization_duration,
(UNIX_TIMESTAMP(LEAST(t.next_hour_start_time,@now_time))-UNIX_TIMESTAMP(t.hour_start_time)) as utilization_rate_fenmu,
t.loading_busy_state_duration,
t.empty_busy_state_duration,
t.charging_state_duration,
t.idle_state_duration,	 
t.locked_state_duration,	   
t.error_state_duration,	   
t.offline_duration
from qt_smartreport.qtr_hour_robot_state_duration_his t 
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where  t.hour_start_time BETWEEN @start_time and @end_time