-- 用于：统计报表->项目概览->故障机器人TOP5


select 
robot_code,
count(distinct error_id) as breakdown_num
from qt_smartreport.qtr_day_robot_error_list_his
where start_time >= { now_start_time }
group by robot_code
order by breakdown_num desc
limit 5





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
robot_code,
count(distinct error_id) as breakdown_num
from qt_smartreport.qtr_day_robot_error_list_his
where start_time >= @now_start_time
group by robot_code
order by breakdown_num desc
limit 5