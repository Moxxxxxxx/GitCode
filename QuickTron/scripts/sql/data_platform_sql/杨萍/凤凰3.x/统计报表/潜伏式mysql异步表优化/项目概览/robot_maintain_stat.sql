-- 用于：统计报表->项目概览->历史机器人维修统计

select 
count(distinct robot_code) as total_maintenance_robot_num,  -- 累计维修车数
COALESCE(avg(unix_timestamp(coalesce(end_time,{ now_time })) - unix_timestamp(start_time)),0) as avg_maintenance_time,  -- 平均维修时长（秒）
count(distinct id) as total_maintenance_num  -- 累计维修次数
from phoenix_rms.robot_maintain_record





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
count(distinct robot_code) as total_maintenance_robot_num,  -- 累计维修车数
COALESCE(avg(unix_timestamp(coalesce(end_time,@now_time)) - unix_timestamp(start_time)),0) as avg_maintenance_time,  -- 平均维修时长（秒）
count(distinct id) as total_maintenance_num  -- 累计维修次数
from phoenix_rms.robot_maintain_record
