select
t.create_sys_error_num as new_breakdown_num,    --  新增故障次数
case
when COALESCE(t.create_sys_error_num,0) = 0 then 0
when COALESCE(t.create_sys_error_num,0) != 0 and COALESCE(t.create_order_num,0) = 0 then concat(create_sys_error_num,'/','0')
when COALESCE(t.create_sys_error_num,0) != 0 and COALESCE(t.create_order_num,0) >= COALESCE(t.create_sys_error_num,0) then concat('1','/',round(t.create_order_num/t.create_sys_error_num))
when COALESCE(t.create_sys_error_num,0) != 0 and COALESCE(t.create_order_num,0) < COALESCE(t.create_sys_error_num,0) then concat(t.create_sys_error_num/t.create_order_num,'/','1')
else 0 end as sys_breakdown_carry_order_rate,  -- 故障率（搬运作业单）
case
when COALESCE(t.create_sys_error_num,0) = 0 then 0
when COALESCE(t.create_sys_error_num,0) != 0 and COALESCE(t.create_job_num,0) = 0 then concat(create_sys_error_num,'/','0')
when COALESCE(t.create_sys_error_num,0) != 0 and COALESCE(t.create_job_num,0) >= COALESCE(t.create_sys_error_num,0) then concat('1','/',round(t.create_job_num/t.create_sys_error_num))
when COALESCE(t.create_sys_error_num,0) != 0 and COALESCE(t.create_job_num,0) < COALESCE(t.create_sys_error_num,0) then concat(t.create_sys_error_num/t.create_job_num,'/','1')
else 0 end as sys_breakdown_carry_task_rate,  -- 故障率（机器人任务）
case when COALESCE(t.end_error_num,0) != 0 then COALESCE(t.end_error_time,0)/COALESCE(t.end_error_num,0) else null end as mttr,  -- MTTR
case when COALESCE(t.the_day_error_num,0) != 0 then (COALESCE(t.the_day_theory_run_duration,0)-COALESCE(t.the_day_error_duration,0))/COALESCE(t.the_day_error_num,0) else null end as mtbf,  -- MTBF
case when COALESCE(t.accum_error_num,0) != 0 then (COALESCE(t.accum_theory_run_duration,0)-COALESCE(t.accum_error_duration,0))/COALESCE(t.accum_error_num,0) else null end as  cumul_mtbf   -- 累计MTBF
from
(
select
max(create_sys_error_num) as create_sys_error_num,
max(create_order_num) as create_order_num,
max(create_job_num) as create_job_num,
max(end_error_num) as end_error_num,
max(end_error_time) as end_error_time,
max(the_day_theory_run_duration) as the_day_theory_run_duration,
max(the_day_error_duration) as the_day_error_duration,
max(accum_theory_run_duration) as accum_theory_run_duration,
max(accum_error_duration) as accum_error_duration,
max(the_day_error_num) as the_day_error_num,
max(accum_error_num) as accum_error_num
from
(
-- part1:全场新增的系统故障
select
count(distinct id) as create_sys_error_num,  --  当天新增系统故障次数
null               as create_order_num,  --  当天新增作业单数
null               as create_job_num,    --  当天新增作业单对应的任务数
null               as end_error_num,     --   当天结束的系统故障次数
null               as end_error_time,     --  当天结束的系统故障所对应的故障时长
null as the_day_theory_run_duration,   -- 当天理论运行时长
null as the_day_error_duration,    -- 当天故障时长
null as accum_theory_run_duration,  -- 累计理论运行时长
null as accum_error_duration,        -- 理论故障时长
null as the_day_error_num,    -- 当天参与计算的故障数
null as accum_error_num       -- 累计参与计算的故障数
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
and alarm_level >= 3
and start_time >= {now_start_time}
and start_time < {next_start_time}
--  part2:全场新增的作业单任务
union all
select
null                         as create_sys_error_num,
count(distinct tor.order_no) as create_order_num,
count(distinct tocj.job_sn)  as create_job_num,
null                         as end_error_num,
null                         as end_error_time,
null as the_day_theory_run_duration,   -- 当天理论运行时长
null as the_day_error_duration,    -- 当天故障时长
null as accum_theory_run_duration,  -- 累计理论运行时长
null as accum_error_duration,        -- 理论故障时长
null as the_day_error_num,    -- 当天参与计算的故障数
null as accum_error_num       -- 累计参与计算的故障数
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id
where tor.create_time >= {now_start_time}
and tor.create_time < {next_start_time}
--  part3:全场结束的系统故障
union all
select
null     as create_sys_error_num,
null     as create_order_num,
null     as create_job_num,
count(distinct id)    as end_error_num,
sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as end_error_time,
null as the_day_theory_run_duration,   -- 当天理论运行时长
null as the_day_error_duration,    -- 当天故障时长
null as accum_theory_run_duration,  -- 累计理论运行时长
null as accum_error_duration,        -- 理论故障时长
null as the_day_error_num,    -- 当天参与计算的故障数
null as accum_error_num       -- 累计参与计算的故障数
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
and alarm_level >= 3
and end_time is not null
and end_time >= {now_start_time}
and end_time < {next_start_time}
--  part4:全场系统当天的理论运行时长、故障时长（时间去重）、累计的理论运行时长、累计的故障时长（时间去重）
union all
select
null as create_sys_error_num,
null as create_order_num,
null as create_job_num,
null as end_error_num,
null as end_error_time,
COALESCE(t1.theory_run_duration,0) + COALESCE(t2.theory_run_duration,0) as the_day_theory_run_duration,
COALESCE(t1.error_duration,0) + COALESCE(t2.error_duration,0) as the_day_error_duration,
COALESCE(t1.accum_theory_run_duration,0) + COALESCE(t2.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t1.accum_error_duration,0) + COALESCE(t2.error_duration,0) as accum_error_duration,
null as the_day_error_num,    -- 当天参与计算的故障数
null as accum_error_num       -- 累计参与计算的故障数
from
--  全场系统当天当前小时之前每个小时的理论运行时长、故障时长（时间去重）
(select
alarm_service,
sum(theory_run_duration) as theory_run_duration,
sum(error_duration)  as error_duration,
sum(case when hour_start_time=DATE_ADD({now_hour_start_time}, INTERVAL -1 HOUR) then accum_theory_run_duration end) as accum_theory_run_duration,
sum(case when hour_start_time=DATE_ADD({now_hour_start_time}, INTERVAL -1 HOUR) then accum_error_duration end) as accum_error_duration
from
(SELECT
alarm_service,
hour_start_time,
theory_run_duration,
error_duration,
accum_theory_run_duration,
accum_error_duration
from qt_smartreport.qt_hour_sys_error_mtbf_his
where alarm_service = 'ALL_SYS'
and hour_start_time >= {now_start_time} and hour_start_time < {next_start_time})t
group by alarm_service)t1
--  全场系统当前小时的理论运行时长、故障时长（时间去重）
left join
(select
ts.alarm_service,
-- date_format({now_hour_start_time}, '%Y-%m-%d %H:00:00') as hour_start_time,
unix_timestamp(date_format(DATE_ADD({now_time} , INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s'))-unix_timestamp (date_format({now_time} , '%Y-%m-%d %H:00:00'))  as theory_run_duration,
COALESCE(te.sys_error_duration,0) as error_duration
from
(select
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
inner join
(select
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as sys_error_duration,
count(distinct t.error_id) as sys_error_num
from
--  当前小时参与计算的系统故障
(select
alarm_service as sys_name,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when end_time=date_format({now_time} , '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag
from
(select alarm_service,
       id as error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time<{now_hour_start_time} then {now_hour_start_time} else start_time end start_time,
	   case when COALESCE(end_time,{now_time} )>={now_next_hour_start_time} then {now_next_hour_start_time} else COALESCE(end_time,{now_time}  ) end as end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
         coalesce(end_time, {now_time} ) < {now_next_hour_start_time}) or
        (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
         coalesce(end_time, {now_time} ) >= {now_next_hour_start_time}) or
        (start_time < {now_hour_start_time} and coalesce(end_time, {now_time} ) >= {now_hour_start_time} and
         coalesce(end_time, {now_time} ) < {now_next_hour_start_time}) or
        (start_time < {now_hour_start_time} and coalesce(end_time, {now_time} ) >= {now_next_hour_start_time})
    )
order by alarm_service,original_start_time asc)t)t
left join
-- 一个小时3600秒序列
(select
@num:=@num+1 as seq_list
from qt_smartreport.qt_dim_hour_seconds_sequence t,(SELECT @num := 0) as i
) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)te on te.alarm_service=ts.alarm_service and ts.alarm_service='ALL_SYS'
)t2 on t2.alarm_service=t1.alarm_service
--  part5:全场系统当天参与计算的系统故障次数、累计的参与计算的系统故障次数
union all
select
null as create_sys_error_num,
null as create_order_num,
null as create_job_num,
null as end_error_num,
null as end_error_time,
null as the_day_theory_run_duration,   -- 当天理论运行时长
null as the_day_error_duration,    -- 当天故障时长
null as accum_theory_run_duration,  -- 累计理论运行时长
null as accum_error_duration,        -- 理论故障时长
count(distinct case when date(hour_start_time)=CURRENT_DATE() then error_id end) as the_day_error_num,
count(distinct error_id) as accum_error_num
from
(
--  全场系统当前小时之前每个小时参与计算的故障集合
SELECT hour_start_time,error_id
from qt_smartreport.qt_hour_sys_error_list_his
--  全场系统当前小时参与计算的故障集合
union all
select
date_format( {now_hour_start_time} , '%Y-%m-%d %H:00:00') as hour_start_time,id as error_id
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
         coalesce(end_time, {now_time} ) < {now_next_hour_start_time}) or
        (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
         coalesce(end_time, {now_time} ) >= {now_next_hour_start_time}) or
        (start_time < {now_hour_start_time} and coalesce(end_time, {now_time} ) >= {now_hour_start_time} and
         coalesce(end_time, {now_time} ) < {now_next_hour_start_time}) or
        (start_time < {now_hour_start_time} and coalesce(end_time, {now_time} ) >= {now_next_hour_start_time})
    )
)t
)t
)t





######################################################################################################################################
---  检查
######################################################################################################################################

-- {now_start_time}  -- 当天开始时间
-- {now_end_time}    -- 当天结束时间
-- {now_time}        --  当前时间
-- {next_start_time}    --  明天开始时间
-- {now_hour_start_time}      --  当前小时开始时间
-- {now_next_hour_start_time}  -- 下一个小时开始时间
-- {now_week_start_time}  -- 当前一周的开始时间
-- {now_next_week_start_time}  --  下一周的开始时间
-- {start_time}  -- 筛选框开始时间  默认当天开始时间
-- {end_time}   --  筛选框结束时间  默认当前小时结束时间


set @now_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00.000000000');
set @now_end_time=date_format(sysdate(), '%Y-%m-%d 23:59:59.999999999');
set @now_time=sysdate();
set @next_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00.000000000');
set @now_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');
set @now_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00');
set @now_week_start_time= date_format(DATE_SUB(sysdate(),INTERVAL WEEKDAY(sysdate()) + 0 DAY), '%Y-%m-%d 00:00:00');
set @now_next_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) -7 DAY), '%Y-%m-%d 00:00:00');
set @start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00.000000000');
set @end_time = date_format(sysdate(), '%Y-%m-%d %H:59:59.999999999');
select  @now_start_time,@now_end_time,@now_time,@next_start_time,@now_hour_start_time,@now_next_hour_start_time,@now_week_start_time,@now_next_week_start_time,@start_time,@end_time;
 

	
 
select 
t.create_sys_error_num as new_breakdown_num,    --  新增故障次数
case 
when COALESCE(t.create_sys_error_num,0) = 0 then 0
when COALESCE(t.create_sys_error_num,0) != 0 and COALESCE(t.create_order_num,0) = 0 then concat(create_sys_error_num,'/','0')
when COALESCE(t.create_sys_error_num,0) != 0 and COALESCE(t.create_order_num,0) >= COALESCE(t.create_sys_error_num,0) then concat('1','/',round(t.create_order_num/t.create_sys_error_num))
when COALESCE(t.create_sys_error_num,0) != 0 and COALESCE(t.create_order_num,0) < COALESCE(t.create_sys_error_num,0) then concat(t.create_sys_error_num/t.create_order_num,'/','1')
else 0 end as sys_breakdown_carry_order_rate,  -- 故障率（搬运作业单）
case 
when COALESCE(t.create_sys_error_num,0) = 0 then 0
when COALESCE(t.create_sys_error_num,0) != 0 and COALESCE(t.create_job_num,0) = 0 then concat(create_sys_error_num,'/','0')
when COALESCE(t.create_sys_error_num,0) != 0 and COALESCE(t.create_job_num,0) >= COALESCE(t.create_sys_error_num,0) then concat('1','/',round(t.create_job_num/t.create_sys_error_num))
when COALESCE(t.create_sys_error_num,0) != 0 and COALESCE(t.create_job_num,0) < COALESCE(t.create_sys_error_num,0) then concat(t.create_sys_error_num/t.create_job_num,'/','1')
else 0 end as sys_breakdown_carry_task_rate,  -- 故障率（机器人任务）
case when COALESCE(t.end_error_num,0) != 0 then COALESCE(t.end_error_time,0)/COALESCE(t.end_error_num,0) else null end as mttr,  -- MTTR
case when COALESCE(t.the_day_error_num,0) != 0 then (COALESCE(t.the_day_theory_run_duration,0)-COALESCE(t.the_day_error_duration,0))/COALESCE(t.the_day_error_num,0) else null end as mtbf,  -- MTBF
case when COALESCE(t.accum_error_num,0) != 0 then (COALESCE(t.accum_theory_run_duration,0)-COALESCE(t.accum_error_duration,0))/COALESCE(t.accum_error_num,0) else null end as  cumul_mtbf   -- 累计MTBF
from  
( 
select 
max(create_sys_error_num) as create_sys_error_num,
max(create_order_num) as create_order_num,
max(create_job_num) as create_job_num,
max(end_error_num) as end_error_num,
max(end_error_time) as end_error_time,
max(the_day_theory_run_duration) as the_day_theory_run_duration,
max(the_day_error_duration) as the_day_error_duration,
max(accum_theory_run_duration) as accum_theory_run_duration,
max(accum_error_duration) as accum_error_duration,
max(the_day_error_num) as the_day_error_num,
max(accum_error_num) as accum_error_num
from  
( 
-- part1:全场新增的系统故障
select 
count(distinct id) as create_sys_error_num,  --  当天新增系统故障次数
null               as create_order_num,  --  当天新增作业单数
null               as create_job_num,    --  当天新增作业单对应的任务数
null               as end_error_num,     --   当天结束的系统故障次数
null               as end_error_time,     --  当天结束的系统故障所对应的故障时长
null as the_day_theory_run_duration,   -- 当天理论运行时长
null as the_day_error_duration,    -- 当天故障时长
null as accum_theory_run_duration,  -- 累计理论运行时长
null as accum_error_duration,        -- 理论故障时长
null as the_day_error_num,    -- 当天参与计算的故障数
null as accum_error_num       -- 累计参与计算的故障数
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
and alarm_level >= 3
and start_time >= @now_start_time 
and start_time < @next_start_time 
--  part2:全场新增的作业单任务
union all 
select 
null                         as create_sys_error_num,
count(distinct tor.order_no) as create_order_num,
count(distinct tocj.job_sn)  as create_job_num,
null                         as end_error_num,
null                         as end_error_time,
null as the_day_theory_run_duration,   -- 当天理论运行时长
null as the_day_error_duration,    -- 当天故障时长
null as accum_theory_run_duration,  -- 累计理论运行时长
null as accum_error_duration,        -- 理论故障时长
null as the_day_error_num,    -- 当天参与计算的故障数
null as accum_error_num       -- 累计参与计算的故障数
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj on tocj.order_id = tor.id 
where tor.create_time >= @now_start_time
and tor.create_time < @next_start_time	
--  part3:全场结束的系统故障
union all
select 
null     as create_sys_error_num,
null     as create_order_num,
null     as create_job_num,
count(distinct id)    as end_error_num,
sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as end_error_time,
null as the_day_theory_run_duration,   -- 当天理论运行时长
null as the_day_error_duration,    -- 当天故障时长
null as accum_theory_run_duration,  -- 累计理论运行时长
null as accum_error_duration,        -- 理论故障时长
null as the_day_error_num,    -- 当天参与计算的故障数
null as accum_error_num       -- 累计参与计算的故障数
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
and alarm_level >= 3
and end_time is not null
and end_time >= @now_start_time 
and end_time < @next_start_time
--  part4:全场系统当天的理论运行时长、故障时长（时间去重）、累计的理论运行时长、累计的故障时长（时间去重）
union all 
select 
null as create_sys_error_num,
null as create_order_num,
null as create_job_num,
null as end_error_num,
null as end_error_time,
COALESCE(t1.theory_run_duration,0) + COALESCE(t2.theory_run_duration,0) as the_day_theory_run_duration,
COALESCE(t1.error_duration,0) + COALESCE(t2.error_duration,0) as the_day_error_duration,
COALESCE(t1.accum_theory_run_duration,0) + COALESCE(t2.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t1.accum_error_duration,0) + COALESCE(t2.error_duration,0) as accum_error_duration,
null as the_day_error_num,    -- 当天参与计算的故障数
null as accum_error_num       -- 累计参与计算的故障数
from 
--  全场系统当天当前小时之前每个小时的理论运行时长、故障时长（时间去重）
(select 
alarm_service,
sum(theory_run_duration) as theory_run_duration,
sum(error_duration)  as error_duration,
sum(case when hour_start_time=DATE_ADD(@now_hour_start_time, INTERVAL -1 HOUR) then accum_theory_run_duration end) as accum_theory_run_duration,
sum(case when hour_start_time=DATE_ADD(@now_hour_start_time, INTERVAL -1 HOUR) then accum_error_duration end) as accum_error_duration
from 
(SELECT
alarm_service,
hour_start_time,
theory_run_duration,
error_duration,
accum_theory_run_duration,
accum_error_duration 
from qt_smartreport.qt_hour_sys_error_mtbf_his
where alarm_service = 'ALL_SYS'
and hour_start_time >= @now_start_time and hour_start_time < @next_start_time)t
group by alarm_service)t1 
--  全场系统当前小时的理论运行时长、故障时长（时间去重）
left join 
(select 
ts.alarm_service,
-- date_format(@now_hour_start_time, '%Y-%m-%d %H:00:00') as hour_start_time,
unix_timestamp(date_format(DATE_ADD(@now_time , INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s'))-unix_timestamp (date_format(@now_time , '%Y-%m-%d %H:00:00'))  as theory_run_duration,
COALESCE(te.sys_error_duration,0) as error_duration
from 
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
inner join 
(select 
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as sys_error_duration,
count(distinct t.error_id) as sys_error_num
from 
--  当前小时参与计算的系统故障
(select 
alarm_service as sys_name,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when end_time=date_format(@now_time , '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag 
from 
(select alarm_service,
       id as error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time<@now_hour_start_time then @now_hour_start_time else start_time end start_time,
	   case when COALESCE(end_time,@now_time )>=@now_next_hour_start_time then @now_next_hour_start_time else COALESCE(end_time,@now_time  ) end as end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
         coalesce(end_time, @now_time ) < @now_next_hour_start_time) or
        (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
         coalesce(end_time, @now_time ) >= @now_next_hour_start_time) or
        (start_time < @now_hour_start_time and coalesce(end_time, @now_time ) >= @now_hour_start_time and
         coalesce(end_time, @now_time ) < @now_next_hour_start_time) or
        (start_time < @now_hour_start_time and coalesce(end_time, @now_time ) >= @now_next_hour_start_time)
    )
order by alarm_service,original_start_time asc)t)t
left join 
-- 一个小时3600秒序列
(select 
@num:=@num+1 as seq_list
from qt_smartreport.qt_dim_hour_seconds_sequence t,(SELECT @num := 0) as i
) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)te on te.alarm_service=ts.alarm_service and ts.alarm_service='ALL_SYS'
)t2 on t2.alarm_service=t1.alarm_service
--  part5:全场系统当天参与计算的系统故障次数、累计的参与计算的系统故障次数
union all 
select 
null as create_sys_error_num,
null as create_order_num,
null as create_job_num,
null as end_error_num,
null as end_error_time,
null as the_day_theory_run_duration,   -- 当天理论运行时长
null as the_day_error_duration,    -- 当天故障时长
null as accum_theory_run_duration,  -- 累计理论运行时长
null as accum_error_duration,        -- 理论故障时长
count(distinct case when date(hour_start_time)=CURRENT_DATE() then error_id end) as the_day_error_num, 
count(distinct error_id) as accum_error_num
from  
( 
--  全场系统当前小时之前每个小时参与计算的故障集合
SELECT hour_start_time,error_id  
from qt_smartreport.qt_hour_sys_error_list_his
--  全场系统当前小时参与计算的故障集合
union all 
select 
date_format( @now_hour_start_time , '%Y-%m-%d %H:00:00') as hour_start_time,id as error_id
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
         coalesce(end_time, @now_time ) < @now_next_hour_start_time) or
        (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
         coalesce(end_time, @now_time ) >= @now_next_hour_start_time) or
        (start_time < @now_hour_start_time and coalesce(end_time, @now_time ) >= @now_hour_start_time and
         coalesce(end_time, @now_time ) < @now_next_hour_start_time) or
        (start_time < @now_hour_start_time and coalesce(end_time, @now_time ) >= @now_next_hour_start_time)
    )
)t 
)t
)t 

