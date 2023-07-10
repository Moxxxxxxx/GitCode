-- 用于：统计报表->项目概览->机器人自恢复统计 

select 
    count(distinct id) as automatic_recovery_num    -- 自恢复次数
    ,count(distinct case when `result` = 1 then id end)  as automatic_recovery_success_num  -- 成功次数
    ,ROUND(count(distinct case when `result` = 1 then id end)/count(distinct id),4) AS automatic_recovery_success_rate  -- 成功占比
    ,count(distinct case when `result` = 0 then id end)  as automatic_recovery_fail_num   -- 失败次数
    ,ROUND(count(distinct case when `result` = 0 then id end)/count(distinct id),4) AS automatic_recovery_fail_rate -- 失败占比
    ,ROUND(avg(case when `result`=1 then unix_timestamp(coalesce(end_time,{ now_time }))- unix_timestamp(start_time) end)*1000,3) as automatic_recovery_avg_time  --  平均自恢复时长
from phoenix_rms.robot_recovery_record
where `method` = '自恢复' 
and start_time >= { now_start_time }





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
    count(distinct id) as automatic_recovery_num    -- 自恢复次数
    ,count(distinct case when `result` = 1 then id end)  as automatic_recovery_success_num  -- 成功次数
    ,ROUND(count(distinct case when `result` = 1 then id end)/count(distinct id),4) AS automatic_recovery_success_rate  -- 成功占比
    ,count(distinct case when `result` = 0 then id end)  as automatic_recovery_fail_num   -- 失败次数
    ,ROUND(count(distinct case when `result` = 0 then id end)/count(distinct id),4) AS automatic_recovery_fail_rate -- 失败占比
    ,ROUND(avg(case when `result`=1 then unix_timestamp(coalesce(end_time,@now_time))- unix_timestamp(start_time) end)*1000,3) as automatic_recovery_avg_time  --  平均自恢复时长
from phoenix_rms.robot_recovery_record
where `method` = '自恢复' 
and start_time >= @now_start_time

