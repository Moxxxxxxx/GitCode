-- 用于：统计报表->项目概览->机器人维修明细

SELECT
    rmr.robot_code,  -- 机器人编码
    brt.robot_type_code,  -- 机器人类型编码
    brt.robot_type_name,  -- 机器人类型名称
    rmr.id as  maintain_id,   -- 维修id
    rmr.start_time as maintain_start_time,  -- 开始维修时间
    rmr.end_time as maintain_end_time,  -- 维修结束时间
    unix_timestamp(coalesce(rmr.end_time,{ now_time })) - unix_timestamp(rmr.start_time) as  maintain_duration,  -- 维修时长（秒）
    rmr.reason as maintain_reason  --  维修原因
from phoenix_rms.robot_maintain_record rmr
left join phoenix_basic.basic_robot br on br.robot_code = rmr.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
ORDER BY coalesce(rmr.end_time,'2222-01-01 00:00:00') DESC, rmr.start_time DESC




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


SELECT
    rmr.robot_code,  -- 机器人编码
    brt.robot_type_code,  -- 机器人类型编码
    brt.robot_type_name,  -- 机器人类型名称
    rmr.id as  maintain_id,   -- 维修id
    rmr.start_time as maintain_start_time,  -- 开始维修时间
    rmr.end_time as maintain_end_time,  -- 维修结束时间
    unix_timestamp(coalesce(rmr.end_time,@now_time)) - unix_timestamp(rmr.start_time) as  maintain_duration,  -- 维修时长（秒）
    rmr.reason as maintain_reason  --  维修原因
from phoenix_rms.robot_maintain_record rmr
left join phoenix_basic.basic_robot br on br.robot_code = rmr.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
ORDER BY coalesce(rmr.end_time,'2222-01-01 00:00:00') DESC, rmr.start_time DESC