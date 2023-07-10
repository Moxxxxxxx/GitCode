-- 新增故障次数 ：create_sys_error_num
-- 故障率（搬运作业单） ：使用 create_sys_error_num 和 create_order_num   注意数据的3种展示
-- 订单量 ： create_order_num
-- 故障率（机器人任务）： 使用 create_sys_error_num 和 create_job_num  注意数据的3种展示
-- 任务量 ： create_job_num
-- OEE = (sys_run_time-sys_error_time)/sys_run_time
-- MTBF = (sys_run_time-sys_error_time)/sys_error_num
-- MTTR = end_error_time/end_sys_error_num



select
ts.alarm_service,  -- 系统服务模块
COALESCE(t1.create_sys_error_num,0) as create_sys_error_num,   -- 新增故障次数
COALESCE(t1.end_sys_error_num,0) as end_sys_error_num,     -- 结束故障次数
COALESCE(t1.end_error_time,0) as end_error_time,           -- 结束故障时间
COALESCE(t1.sys_error_num,0) as sys_error_num,        -- 时间段内持续的故障次数
COALESCE(t3.sys_error_time,0) as sys_error_time,        -- 持续故障时长（去重）
COALESCE(t2.create_order_num,0) as create_order_num,         -- 新增订单量
COALESCE(t2.create_job_num,0) as create_job_num,            -- 新人任务量
COALESCE(t3.sys_run_time,0) as sys_run_time           -- 理论运行时长
from
(select
distinct module as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server'))ts
-- part1:筛选小时内的新增故障次数、结束故障次数、结束故障时间、时间段内持续的故障次数
left join
(select
alarm_service,
count(distinct case when tb.start_time BETWEEN {start_time} and {end_time} then tb.error_id end) as create_sys_error_num,  -- 新增故障次数
count(distinct case when tb.end_time is not null and tb.end_time BETWEEN {start_time} and {end_time} then tb.error_id end) as end_sys_error_num,     -- 结束故障次数
sum(case when tb.end_time is not null and tb.end_time BETWEEN {start_time} and {end_time} then unix_timestamp(tb.end_time)-unix_timestamp(tb.start_time) end) as end_error_time,        -- 结束故障时间
count(distinct tb.error_id) as sys_error_num         -- 时间段内持续的故障次数
from
(select alarm_service,id as error_id,start_time,end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= {start_time} and start_time <= {end_time} and
         coalesce(end_time, {now_time} ) <= {end_time}) or
        (start_time >= {start_time} and start_time <= {end_time} and
         coalesce(end_time, {now_time} ) > {end_time}) or
        (start_time < {start_time} and coalesce(end_time, {now_time} ) >= {start_time} and
         coalesce(end_time, {now_time} ) <= {end_time}) or
        (start_time < {start_time} and coalesce(end_time, {now_time} ) > {end_time})
    ))tb
group by alarm_service
)t1 on t1.alarm_service = ts.alarm_service
-- part2:筛选小时内的新增订单量、新增任务量
left join
(select
count(distinct tor.order_no)                      as create_order_num,      -- 新增订单量
count(distinct tocj.job_sn)                       as create_job_num        -- 新增任务量
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj
on tocj.order_id = tor.id
where tor.create_time BETWEEN {start_time} and {end_time})t2 on 1
-- part3:筛选小时内的系统持续故障时长（去重）、理论运行时长
left join
(select
t.alarm_service,
sum(t.error_duration) as sys_error_time,        -- 持续故障时长（去重）
sum(t.theory_run_duration) as sys_run_time           -- 理论运行时长
from
(select alarm_service,hour_start_time ,sys_run_duration as theory_run_duration ,sys_error_duration as error_duration
from qt_smartreport.qt_hour_sys_error_duration_his
where alarm_service !='ALL_SYS'
and hour_start_time BETWEEN {start_time} and {end_time}
union all
select
alarm_service,hour_start_time,theory_run_duration,error_duration
from
(select
ts.alarm_service,
date_format({now_hour_start_time}, '%Y-%m-%d %H:00:00') as hour_start_time,
unix_timestamp(date_format(DATE_ADD({now_time} , INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s'))-unix_timestamp (date_format({now_time} , '%Y-%m-%d %H:00:00'))  as theory_run_duration,
COALESCE(te.sys_error_duration,0) as error_duration
from
(select
distinct module as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server'))ts
left join
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
	   case when COALESCE(end_time,{now_time} )>={now_next_hour_start}_time then {now_next_hour_start}_time else COALESCE(end_time,{now_time}  ) end as end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start}_time and
         coalesce(end_time, {now_time} ) < {now_next_hour_start}_time) or
        (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start}_time and
         coalesce(end_time, {now_time} ) >= {now_next_hour_start}_time) or
        (start_time < {now_hour_start_time} and coalesce(end_time, {now_time} ) >= {now_hour_start_time} and
         coalesce(end_time, {now_time} ) < {now_next_hour_start}_time) or
        (start_time < {now_hour_start_time} and coalesce(end_time, {now_time} ) >= {now_next_hour_start}_time)
    )
order by alarm_service,original_start_time asc)t)t
left join
-- 一个小时3600秒序列
(select
@num:=@num+1 as seq_list
from qt_smartreport.qt_dim_hour_seconds_sequence t,(SELECT @num := 0) as i
) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)te on te.alarm_service=ts.alarm_service and ts.alarm_service!='ALL_SYS')t
where t.hour_start_time BETWEEN {start_time} and {end_time})t
group by t.alarm_service)t3 on t3.alarm_service=ts.alarm_service







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


-- 新增故障次数 ：create_sys_error_num
-- 故障率（搬运作业单） ：使用 create_sys_error_num 和 create_order_num   注意数据的3种展示
-- 订单量 ： create_order_num
-- 故障率（机器人任务）： 使用 create_sys_error_num 和 create_job_num  注意数据的3种展示
-- 任务量 ： create_job_num
-- OEE = (sys_run_time-sys_error_time)/sys_run_time
-- MTBF = (sys_run_time-sys_error_time)/sys_error_num
-- MTTR = end_error_time/end_sys_error_num



select 
ts.alarm_service,  -- 系统服务模块
COALESCE(t1.create_sys_error_num,0) as create_sys_error_num,   -- 新增故障次数
COALESCE(t1.end_sys_error_num,0) as end_sys_error_num,     -- 结束故障次数
COALESCE(t1.end_error_time,0) as end_error_time,           -- 结束故障时间
COALESCE(t1.sys_error_num,0) as sys_error_num,        -- 时间段内持续的故障次数
COALESCE(t3.sys_error_time,0) as sys_error_time,        -- 持续故障时长（去重）
COALESCE(t2.create_order_num,0) as create_order_num,         -- 新增订单量
COALESCE(t2.create_job_num,0) as create_job_num,            -- 新人任务量
COALESCE(t3.sys_run_time,0) as sys_run_time           -- 理论运行时长
from 
(select 
distinct module as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server'))ts 
-- part1:筛选小时内的新增故障次数、结束故障次数、结束故障时间、时间段内持续的故障次数
left join 
(select 
alarm_service,
count(distinct case when tb.start_time BETWEEN @start_time and @end_time then tb.error_id end) as create_sys_error_num,  -- 新增故障次数
count(distinct case when tb.end_time is not null and tb.end_time BETWEEN @start_time and @end_time then tb.error_id end) as end_sys_error_num,     -- 结束故障次数
sum(case when tb.end_time is not null and tb.end_time BETWEEN @start_time and @end_time then unix_timestamp(tb.end_time)-unix_timestamp(tb.start_time) end) as end_error_time,        -- 结束故障时间
count(distinct tb.error_id) as sys_error_num         -- 时间段内持续的故障次数
from 
(select alarm_service,id as error_id,start_time,end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= @start_time and start_time <= @end_time and
         coalesce(end_time, @now_time ) <= @end_time) or
        (start_time >= @start_time and start_time <= @end_time and
         coalesce(end_time, @now_time ) > @end_time) or
        (start_time < @start_time and coalesce(end_time, @now_time ) >= @start_time and
         coalesce(end_time, @now_time ) <= @end_time) or
        (start_time < @start_time and coalesce(end_time, @now_time ) > @end_time)
    ))tb
group by alarm_service	
)t1 on t1.alarm_service = ts.alarm_service 
-- part2:筛选小时内的新增订单量、新增任务量
left join 
(select 
count(distinct tor.order_no)                      as create_order_num,      -- 新增订单量
count(distinct tocj.job_sn)                       as create_job_num        -- 新增任务量
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj
on tocj.order_id = tor.id
where tor.create_time BETWEEN @start_time and @end_time)t2 on 1
-- part3:筛选小时内的系统持续故障时长（去重）、理论运行时长
left join 
(select 
t.alarm_service,
sum(t.error_duration) as sys_error_time,        -- 持续故障时长（去重）
sum(t.theory_run_duration) as sys_run_time           -- 理论运行时长
from 
(select alarm_service,hour_start_time ,sys_run_duration as theory_run_duration ,sys_error_duration as error_duration  
from qt_smartreport.qt_hour_sys_error_duration_his
where alarm_service !='ALL_SYS'
and hour_start_time BETWEEN @start_time and @end_time
union all 
select 
alarm_service,hour_start_time,theory_run_duration,error_duration
from 
(select 
ts.alarm_service,
date_format(@now_hour_start_time, '%Y-%m-%d %H:00:00') as hour_start_time,
unix_timestamp(date_format(DATE_ADD(@now_time , INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s'))-unix_timestamp (date_format(@now_time , '%Y-%m-%d %H:00:00'))  as theory_run_duration,
COALESCE(te.sys_error_duration,0) as error_duration
from 
(select 
distinct module as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server'))ts
left join 
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
WITH ROLLUP)te on te.alarm_service=ts.alarm_service and ts.alarm_service!='ALL_SYS')t 
where t.hour_start_time BETWEEN @start_time and @end_time)t
group by t.alarm_service)t3 on t3.alarm_service=ts.alarm_service