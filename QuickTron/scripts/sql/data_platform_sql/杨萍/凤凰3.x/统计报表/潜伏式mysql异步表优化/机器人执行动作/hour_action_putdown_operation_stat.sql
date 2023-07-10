-- 用于：统计报表->机器人统计->机器人执行动作统计->降下

select 
'putdown' as operation_type,  -- 执行动作操作类型
t.robot_code,   -- 机器人编码
brt.robot_type_code,  -- 机器人类型编码
brt.robot_type_name,  -- 机器人类型
t.action_uid,   -- Action的ID
t.action_begin_time,  -- Action创建时间
t.action_end_time,  -- Action结束时间
t.job_sn,   -- 机器人任务编码
COALESCE(t.before_putdown_cost_time,0) + COALESCE(t.do_putdown_cost_time,0) as putdown_cost_time, -- 降下总耗时（秒）
case when t.is_rectification=1 then '是' else '否' end as is_rectification,  -- 降下是否纠偏     
t.before_putdown_cost_time,  -- 降下前确认耗时（秒）
t.do_putdown_cost_time     -- 纯降下耗时（秒）
from qt_smartreport.qtr_hour_action_putdown_operation_his t
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where t.action_begin_time BETWEEN {start_time}  AND  {end_time}


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
'putdown' as operation_type,  -- 执行动作操作类型
t.robot_code,   -- 机器人编码
brt.robot_type_code,  -- 机器人类型编码
brt.robot_type_name,  -- 机器人类型
t.action_uid,   -- Action的ID
t.action_begin_time,  -- Action创建时间
t.action_end_time,  -- Action结束时间
t.job_sn,   -- 机器人任务编码
COALESCE(t.before_putdown_cost_time,0) + COALESCE(t.do_putdown_cost_time,0) as putdown_cost_time, -- 降下总耗时（秒）
case when t.is_rectification=1 then '是' else '否' end as is_rectification,  -- 降下是否纠偏     
t.before_putdown_cost_time,  -- 降下前确认耗时（秒）
t.do_putdown_cost_time     -- 纯降下耗时（秒）
from qt_smartreport.qtr_hour_action_putdown_operation_his t
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where t.action_begin_time BETWEEN @start_time AND @end_time