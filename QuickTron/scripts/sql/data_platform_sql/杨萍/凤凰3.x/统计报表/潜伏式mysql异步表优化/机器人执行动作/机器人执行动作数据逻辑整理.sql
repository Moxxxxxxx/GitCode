doRackCheckWithUpCameraBeforeLiftUp  -- 顶升前确定
DoLiftUp  -- 顶升
doRackCheckWithUpCameraAfterLiftUp -- 顶升后确定
doGuideBeforePutDown  -- 降下前确定
DoPutDown  -- 降下
terminalGuide -- 末端引导


select * from phoenix_rms.job_action_operation_record
where operation_name in ('doRackCheckWithUpCameraBeforeLiftUp','DoLiftUp','doRackCheckWithUpCameraAfterLiftUp')
select * from phoenix_rms.job_action_operation_record
where operation_name in ('doGuideBeforePutDown','DoPutDown')
select * from phoenix_rms.job_action_operation_record
where operation_name='terminalGuide'



-- part1:顶升
set @now_time=sysdate();   --  当前时间
set @now_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @now_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间
select @now_time,@now_hour_start_time,@now_next_hour_start_time;

select 
action_uid,  -- action的ID
action_begin_time,  -- action开始时间
action_end_time,  -- action结束时间
robot_code,   -- 机器人编码
job_sn,       -- 机器人任务编码
action_code,  -- action编码
is_loading,   -- action是否带载（action结束时）
do_rack_check_with_upcamera_before_liftup_start_time as before_liftup_start_time, -- 顶升前确定开始时间
do_rack_check_with_upcamera_before_liftup_end_time as before_liftup_end_time, -- 顶升前确定结束时间
UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_end_time) - UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_start_time) as before_liftup_cost_time,  -- 顶升前确定耗时
do_liftup_start_time,  -- 顶升动作开始时间
do_liftup_end_time,  -- 顶升动作结束时间
UNIX_TIMESTAMP(do_liftup_end_time) - UNIX_TIMESTAMP(do_liftup_start_time) as do_liftup_cost_time,  -- 顶升动作耗时
do_rack_check_with_upcamera_after_liftup_start_time as after_liftup_start_time,  -- 顶升后确定开始时间
do_rack_check_with_upcamera_after_liftup_end_time as after_liftup_end_time,  -- 顶升后确定结束时间
UNIX_TIMESTAMP(do_rack_check_with_upcamera_after_liftup_end_time) - UNIX_TIMESTAMP(do_rack_check_with_upcamera_after_liftup_start_time) as after_liftup_cost_time,  -- 顶升后确定耗时
case when (UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_end_time) - UNIX_TIMESTAMP(do_rack_check_with_upcamera_before_liftup_start_time))>0.5 then 1 else 0 end is_rectification  -- 顶升时否发生纠偏(机器人顶升前确定时长大于等于0.5s，则算该次顶升动作发生过纠偏)
from 
(select 
t1.action_uid,
t1.action_begin_time,
t1.action_end_time,
t1.job_sn ,
t1.is_loading,
t1.action_code, 
t1.robot_code, 
max(case when t2.operation_name='doRackCheckWithUpCameraBeforeLiftUp' then t2.start_time end) as do_rack_check_with_upcamera_before_liftup_start_time,
max(case when t2.operation_name='doRackCheckWithUpCameraBeforeLiftUp' then t2.end_time end) as do_rack_check_with_upcamera_before_liftup_end_time,
max(case when t2.operation_name='DoLiftUp' then t2.start_time end) as do_liftup_start_time,
max(case when t2.operation_name='DoLiftUp' then t2.end_time end) as do_liftup_end_time,
max(case when t2.operation_name='doRackCheckWithUpCameraAfterLiftUp' then t2.start_time end) as do_rack_check_with_upcamera_after_liftup_start_time,
max(case when t2.operation_name='doRackCheckWithUpCameraAfterLiftUp' then t2.end_time end) as do_rack_check_with_upcamera_after_liftup_end_time
from phoenix_rms.job_action_statistics_data t1
inner join phoenix_rms.job_action_operation_record t2 
on t2.action_uid =t1.action_uid and t2.operation_name in ('doRackCheckWithUpCameraBeforeLiftUp','DoLiftUp','doRackCheckWithUpCameraAfterLiftUp')
-- where action_end_time >= @now_hour_start_time and action_end_time < @now_next_hour_start_time
group by t1.action_uid,t1.action_begin_time,t1.action_end_time,t1.job_sn ,t1.is_loading,t1.action_code, t1.robot_code)t 




-- part2:降下 

set @now_time=sysdate();   --  当前时间
set @now_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @now_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间
select @now_time,@now_hour_start_time,@now_next_hour_start_time;

select 
action_uid,  -- action的ID
action_begin_time,  -- action开始时间
action_end_time,  -- action结束时间
robot_code,   -- 机器人编码
job_sn,       -- 机器人任务编码
action_code,  -- action编码
is_loading,   -- action是否带载（action结束时）
do_guide_before_putdown_start_time as before_putdown_start_time, -- 降下前确定开始时间
do_guide_before_putdown_end_time as before_putdown_end_time, -- 降下前确定结束时间
UNIX_TIMESTAMP(do_guide_before_putdown_end_time) - UNIX_TIMESTAMP(do_guide_before_putdown_start_time) as before_putdown_cost_time,  -- 降下前确定耗时
do_putdown_start_time,  -- 降下动作开始时间
do_putdown_end_time,  -- 降下动作结束时间
UNIX_TIMESTAMP(do_putdown_end_time) - UNIX_TIMESTAMP(do_putdown_start_time) as do_liftup_cost_time,  -- 降下动作耗时
case when (UNIX_TIMESTAMP(do_guide_before_putdown_end_time) - UNIX_TIMESTAMP(do_guide_before_putdown_start_time))>0.5 then 1 else 0 end is_rectification  -- 降下时否发生纠偏(机器人降下前确定时长大于等于0.5s，则算该次降下动作发生过纠偏)
from 
(select 
t1.action_uid,
t1.action_begin_time,
t1.action_end_time,
t1.job_sn ,
t1.is_loading,
t1.action_code, 
t1.robot_code, 
max(case when t2.operation_name='doGuideBeforePutDown' then t2.start_time end) as do_guide_before_putdown_start_time,
max(case when t2.operation_name='doGuideBeforePutDown' then t2.end_time end) as do_guide_before_putdown_end_time,
max(case when t2.operation_name='DoPutDown' then t2.start_time end) as do_putdown_start_time,
max(case when t2.operation_name='DoPutDown' then t2.end_time end) as do_putdown_end_time
from phoenix_rms.job_action_statistics_data t1
inner join phoenix_rms.job_action_operation_record t2 
on t2.action_uid =t1.action_uid and t2.operation_name in ('doGuideBeforePutDown','DoPutDown')
-- where action_end_time >= @now_hour_start_time and action_end_time < @now_next_hour_start_time
group by t1.action_uid,t1.action_begin_time,t1.action_end_time,t1.job_sn ,t1.is_loading,t1.action_code, t1.robot_code)t 



-- part3:末端引导 

set @now_time=sysdate();   --  当前时间
set @now_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @now_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间
select @now_time,@now_hour_start_time,@now_next_hour_start_time;

select 
action_uid,  -- action的ID
action_begin_time,  -- action开始时间
action_end_time,  -- action结束时间
robot_code,   -- 机器人编码
job_sn,       -- 机器人任务编码
action_code,  -- action编码
is_loading,   -- action是否带载（action结束时）
terminal_guide_start_time, -- 末端引导开始时间
terminal_guide_end_time, -- 末端引导开始时间结束时间
UNIX_TIMESTAMP(terminal_guide_end_time)-UNIX_TIMESTAMP(terminal_guide_start_time) as terminal_guide_cost_time  -- 末端引导耗时
from 
(select 
t1.action_uid,
t1.action_begin_time,
t1.action_end_time,
t1.job_sn ,
t1.is_loading,
t1.action_code, 
max(case when t2.operation_name='terminalGuide' then t2.start_time end) as terminal_guide_start_time,
max(case when t2.operation_name='terminalGuide' then t2.end_time end) as terminal_guide_end_time
from phoenix_rms.job_action_statistics_data t1
inner join phoenix_rms.job_action_operation_record t2 
on t2.action_uid =t1.action_uid and t2.operation_name in ('terminalGuide')
-- where action_end_time >= @now_hour_start_time and action_end_time < @now_next_hour_start_time
group by t1.action_uid,t1.action_begin_time,t1.action_end_time,t1.job_sn ,t1.is_loading,t1.action_code, t1.robot_code)t