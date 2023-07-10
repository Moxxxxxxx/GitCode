-- 用于：统计报表->项目概览->人工介入恢复方式分布

select `method` as recovery_method,  -- 恢复方式
count(distinct id) as recovery_num   -- 恢复数量
from phoenix_rms.robot_recovery_record
where `method` != '自恢复' and `result` = 1
and start_time >= { now_start_time }
group by `method`



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


select `method` as recovery_method,  -- 恢复方式
count(distinct id) as recovery_num   -- 恢复数量
from phoenix_rms.robot_recovery_record
where `method` != '自恢复' and `result` = 1
and start_time >= @now_start_time
group by `method`