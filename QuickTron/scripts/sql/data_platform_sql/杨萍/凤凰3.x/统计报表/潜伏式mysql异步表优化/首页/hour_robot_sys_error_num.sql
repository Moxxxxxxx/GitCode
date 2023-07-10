-- 用于：首页->当日告警分时统计 

select
t.hour_start_time               as hour_value,
COALESCE(t1.robot_error_num, 0) as robot_error_num,
COALESCE(t1.sys_error_num, 0)   as sys_error_num
from
-- 当天小时维表
(select
DATE_FORMAT(concat(date({ now_time }),' ',hour_start_time), '%Y-%m-%d %H:00:00')  as hour_start_time,
DATE_ADD(DATE_FORMAT(concat(date({ now_time }),' ',hour_start_time), '%Y-%m-%d %H:00:00'), INTERVAL 60 MINUTE) as next_hour_start_time
from qt_smartreport.qtr_dim_hour)t
-- 当天机器人新增故障数（收敛后）、系统新增故障数
left join
(select
DATE_FORMAT(start_time, '%Y-%m-%d %H:00:00') as hour_value,
count(distinct error_id) as robot_error_num,
null as sys_error_num
from qt_smartreport.qtr_day_robot_error_list_his
where start_time >= { now_start_time }
group by hour_value
union all
select
DATE_FORMAT(start_time, '%Y-%m-%d %H:00:00') as hour_value,
null as robot_error_num,
count(distinct error_id) as sys_error_num
from qt_smartreport.qtr_day_sys_error_list_his
where start_time >= { now_start_time }
group by hour_value)t1 on t1.hour_value = t.hour_start_time
order by hour_value asc






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
t.hour_start_time               as hour_value,
COALESCE(t1.robot_error_num, 0) as robot_error_num,
COALESCE(t1.sys_error_num, 0)   as sys_error_num
from 
-- 当天小时维表
(select 
DATE_FORMAT(concat(date(@now_time),' ',hour_start_time), '%Y-%m-%d %H:00:00')  as hour_start_time,
DATE_ADD(DATE_FORMAT(concat(date(@now_time),' ',hour_start_time), '%Y-%m-%d %H:00:00'), INTERVAL 60 MINUTE) as next_hour_start_time
from qt_smartreport.qtr_dim_hour)t 
-- 当天机器人新增故障数（收敛后）、系统新增故障数
left join 
(select 
DATE_FORMAT(start_time, '%Y-%m-%d %H:00:00') as hour_value,
count(distinct error_id) as robot_error_num,
null as sys_error_num
from qt_smartreport.qtr_day_robot_error_list_his
where start_time >= @now_start_time
group by hour_value
union all 
select 
DATE_FORMAT(start_time, '%Y-%m-%d %H:00:00') as hour_value,
null as robot_error_num,
count(distinct error_id) as sys_error_num
from qt_smartreport.qtr_day_sys_error_list_his
where start_time >= @now_start_time
group by hour_value)t1 on t1.hour_value = t.hour_start_time
order by hour_value asc




