set @now_time=sysdate();   --  当前时间
set @dt_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @dt_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间
set @dt_week_start_time=date_format(DATE_SUB(sysdate(),INTERVAL WEEKDAY(sysdate()) + 0 DAY), '%Y-%m-%d 00:00:00'); -- 当前一周的开始时间
set @dt_next_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) -7 DAY), '%Y-%m-%d 00:00:00'); --  下一周的开始时间
select @now_time,@dt_hour_start_time,@dt_next_hour_start_time,@dt_day_start_time,@dt_next_day_start_time,@dt_week_start_time,@dt_next_week_start_time;



-- 插入数据（mysql参数）
-- insert into qt_smartreport.qtr_hour_action_putdown_operation_his(create_time,update_time,date_value,hour_start_time,next_hour_start_time,action_uid,action_begin_time,action_end_time,robot_code,job_sn,action_code,is_loading,before_putdown_start_time,before_putdown_end_time,before_putdown_cost_time,do_putdown_start_time,do_putdown_end_time,do_putdown_cost_time,is_rectification)

select 
@now_time as create_time,
@now_time as update_time,
date(@dt_hour_start_time) as date_value,
@dt_hour_start_time as hour_start_time,
@dt_next_hour_start_time as next_hour_start_time,
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
UNIX_TIMESTAMP(do_putdown_end_time) - UNIX_TIMESTAMP(do_putdown_start_time) as do_putdown_cost_time,  -- 降下动作耗时
case when (UNIX_TIMESTAMP(do_guide_before_putdown_end_time) - UNIX_TIMESTAMP(do_guide_before_putdown_start_time))>0.5 then 1 else 0 end is_rectification  -- 降下时否发生纠偏(机器人降下前确定时长大于等于0.5s，则算该次降下动作发生过纠偏)
from 
(select 
t1.action_uid,
t1.action_begin_time,
t1.action_end_time,
t1.job_sn ,
case when t1.is_loading=1 then 1 else 0 end as is_loading,
t1.action_code, 
t1.robot_code, 
max(case when t2.operation_name='doGuideBeforePutDown' then t2.start_time end) as do_guide_before_putdown_start_time,
max(case when t2.operation_name='doGuideBeforePutDown' then t2.end_time end) as do_guide_before_putdown_end_time,
max(case when t2.operation_name='DoPutDown' then t2.start_time end) as do_putdown_start_time,
max(case when t2.operation_name='DoPutDown' then t2.end_time end) as do_putdown_end_time
from phoenix_rms.job_action_statistics_data t1
inner join phoenix_rms.job_action_operation_record t2 
on t2.action_uid =t1.action_uid and t2.operation_name in ('doGuideBeforePutDown','DoPutDown')
where t1.action_end_time >= @dt_hour_start_time and t1.action_end_time < @dt_next_hour_start_time
group by t1.action_uid,t1.action_begin_time,t1.action_end_time,t1.job_sn ,t1.is_loading,t1.action_code, t1.robot_code)t 


--------------------------------------------------------------------------------------------------------------------------
			
-- 插入数据（异步表）qt_smartreport.qtr_hour_action_putdown_operation_his	
-- {{ dt_relative_time(dt) }}
-- {{ now_time }}
-- {{ dt_hour_start_time }}
-- {{ dt_next_hour_start_time }}
-- {{ dt_day_start_time }}
-- {{ dt_next_day_start_time }}
-- {{ dt_week_start_time }}
-- {{ dt_next_week_start_time }}	



-- 定义时间参数
{% set now_time=datetime.datetime.now().strftime("'%Y-%m-%d %H:%M:%S.000000'") %}  -- 客观当前时间
{% set dt_hour_start_time=dt_relative_time(dt,default="%Y-%m-%d %H:00:00.000000") %}   -- dt所在小时的开始时间
{% set dt_next_hour_start_time=dt_relative_time(dt,hours=1,default="%Y-%m-%d %H:00:00.000000") %}  -- dt所在小时的下一个小时的开始时间
{% set dt_day_start_time=dt_relative_time(dt,default="%Y-%m-%d 00:00:00.000000") %}  -- dt所在天的开始时间
{% set dt_next_day_start_time=dt_relative_time(dt,days=1,default="%Y-%m-%d 00:00:00.000000") %}  -- dt所在天的下一天的开始时间
{% set dt_week_start_time=(dt - datetime.timedelta(days=dt.now().weekday())).strftime("'%Y-%m-%d 00:00:00.000000'") %}  -- dt所在周的开始时间
{% set dt_next_week_start_time=(dt + datetime.timedelta(days=7-dt.now().weekday())).strftime("'%Y-%m-%d 00:00:00.000000'") %}  -- dt所在周的下一周的开始时间




-- 插入逻辑 
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
date({{ dt_hour_start_time }}) as date_value,
{{ dt_hour_start_time }} as hour_start_time,
{{ dt_next_hour_start_time }} as next_hour_start_time,
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
UNIX_TIMESTAMP(do_putdown_end_time) - UNIX_TIMESTAMP(do_putdown_start_time) as do_putdown_cost_time,  -- 降下动作耗时
case when (UNIX_TIMESTAMP(do_guide_before_putdown_end_time) - UNIX_TIMESTAMP(do_guide_before_putdown_start_time))>0.5 then 1 else 0 end is_rectification  -- 降下时否发生纠偏(机器人降下前确定时长大于等于0.5s，则算该次降下动作发生过纠偏)
from
(select
t1.action_uid,
t1.action_begin_time,
t1.action_end_time,
t1.job_sn ,
case when t1.is_loading=1 then 1 else 0 end as is_loading,
t1.action_code,
t1.robot_code,
max(case when t2.operation_name='doGuideBeforePutDown' then t2.start_time end) as do_guide_before_putdown_start_time,
max(case when t2.operation_name='doGuideBeforePutDown' then t2.end_time end) as do_guide_before_putdown_end_time,
max(case when t2.operation_name='DoPutDown' then t2.start_time end) as do_putdown_start_time,
max(case when t2.operation_name='DoPutDown' then t2.end_time end) as do_putdown_end_time
from phoenix_rms.job_action_statistics_data t1
inner join phoenix_rms.job_action_operation_record t2
on t2.action_uid =t1.action_uid and t2.operation_name in ('doGuideBeforePutDown','DoPutDown')
where t1.action_end_time >= {{ dt_hour_start_time }} and t1.action_end_time < {{ dt_next_hour_start_time }}
group by t1.action_uid,t1.action_begin_time,t1.action_end_time,t1.job_sn ,t1.is_loading,t1.action_code, t1.robot_code)t