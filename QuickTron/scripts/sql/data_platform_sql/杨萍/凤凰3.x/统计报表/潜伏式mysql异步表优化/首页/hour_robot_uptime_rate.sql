-- 用于：首页->当日机器人开动率分时趋势

select
t.hour_start_time as x,  -- 小时
ROUND(coalesce(sum(t1.uptime_state_duration) / sum(t1.uptime_state_rate_fenmu), 0)*100,2) as y  -- 小时开动率
from
-- 当天小时维表
(select
DATE_FORMAT(concat(date({ now_time }),' ',hour_start_time), '%Y-%m-%d %H:00:00')  as hour_start_time,
DATE_ADD(DATE_FORMAT(concat(date({ now_time }),' ',hour_start_time), '%Y-%m-%d %H:00:00'), INTERVAL 60 MINUTE) as next_hour_start_time
from qt_smartreport.qtr_dim_hour)t
-- 当天各小时的开动时长、开动率分母
left join
(select
t.hour_start_time as hour_value,
COALESCE(t.uptime_state_duration, 0) / (UNIX_TIMESTAMP(LEAST(t.next_hour_start_time,{ now_time }))-UNIX_TIMESTAMP(t.hour_start_time)) as uptime_state_rate,
t.uptime_state_duration,
(UNIX_TIMESTAMP(LEAST(t.next_hour_start_time,{ now_time }))-UNIX_TIMESTAMP(t.hour_start_time)) as uptime_state_rate_fenmu
from qt_smartreport.qtr_hour_robot_state_duration_his t
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where  t.hour_start_time  >= { now_start_time })t1 on t1.hour_value = t.hour_start_time
group by t.hour_start_time








#############################################################################################
---  检查
#############################################################################################
-- { now_time }
-- { start_time }
-- { end_time }
-- { now_start_time }
set @now_time = sysdate(); --  当前时间
set @start_time = date_format(sysdate(), '%Y-%m-%d 00:00:00.000000000'); -- 筛选框开始时间  默认当天开始时间
set @end_time = date_format(sysdate(), '%Y-%m-%d %H:59:59.999999999'); --  筛选框结束时间  默认当前小时结束时间
set @now_start_time = date_format(sysdate(), '%Y-%m-%d 00:00:00.000000000');  -- 当天开始时间
select @now_time, @start_time, @end_time,@now_start_time;


select 
t.hour_start_time as x,  -- 小时
ROUND(coalesce(sum(t1.uptime_state_duration) / sum(t1.uptime_state_rate_fenmu), 0)*100,2) as y  -- 小时开动率
from 
-- 当天小时维表
(select
DATE_FORMAT(concat(date(@now_time),' ',hour_start_time), '%Y-%m-%d %H:00:00')  as hour_start_time,
DATE_ADD(DATE_FORMAT(concat(date(@now_time),' ',hour_start_time), '%Y-%m-%d %H:00:00'), INTERVAL 60 MINUTE) as next_hour_start_time
from qt_smartreport.qtr_dim_hour)t
-- 当天各小时的开动时长、开动率分母
left join 
(select 
t.hour_start_time as hour_value,   
COALESCE(t.uptime_state_duration, 0) / (UNIX_TIMESTAMP(LEAST(t.next_hour_start_time,@now_time))-UNIX_TIMESTAMP(t.hour_start_time)) as uptime_state_rate,
t.uptime_state_duration,
(UNIX_TIMESTAMP(LEAST(t.next_hour_start_time,@now_time))-UNIX_TIMESTAMP(t.hour_start_time)) as uptime_state_rate_fenmu
from qt_smartreport.qtr_hour_robot_state_duration_his t 
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where  t.hour_start_time  >= @now_start_time)t1 on t1.hour_value = t.hour_start_time
group by t.hour_start_time