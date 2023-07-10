-- 新增故障次数 ：create_robot_error_num
-- 故障率（搬运作业单） ：使用 create_robot_error_num 和 create_order_num   注意数据的3种展示
-- 订单量 ： create_order_num
-- 故障率（机器人任务）： 使用 create_robot_error_num 和 create_job_num  注意数据的3种展示
-- 机器人任务量 ： create_job_num
-- OEE = (robot_run_time-robot_error_time)/robot_run_time
-- MTBF = (robot_run_time-robot_error_time)/robot_error_num
-- MTTR = end_error_time/end_robot_error_num


SELECT
COALESCE(max(td.create_robot_error_num),0) as create_robot_error_num,   -- 新增故障次数
COALESCE(max(td.end_robot_error_num),0) as end_robot_error_num,     -- 结束故障次数
COALESCE(max(td.end_error_time),0) as  end_error_time,           -- 结束故障时间
COALESCE(max(td.robot_error_num),0) as  robot_error_num,        -- 时间段内持续的故障次数
COALESCE(max(td.robot_error_time),0) as  robot_error_time,        -- 持续故障时长
COALESCE(max(td.create_order_num),0) as create_order_num,         -- 新增订单量
COALESCE(max(td.create_job_num),0) as create_job_num,            -- 新增任务量
COALESCE(max(td.robot_run_time),0) as  robot_run_time           -- 理论运行时长
from
(
-- 筛选小时内机器人新增故障次数、结束故障次数、已结束故障时长
select
count(distinct case when tb.start_time BETWEEN {start_time} and {end_time} then tb.error_id end) as create_robot_error_num,
count(distinct case when tb.end_time is not null and tb.end_time BETWEEN {start_time} and {end_time} then tb.error_id end) as end_robot_error_num,
sum(case when tb.end_time is not null and tb.end_time BETWEEN {start_time} and {end_time} and date_format(tb.end_time, '%Y-%m-%d %H:00:00')=date_format(tb.hour_start_time, '%Y-%m-%d %H:00:00') then unix_timestamp(tb.end_time)-unix_timestamp(tb.start_time) end) as end_error_time,
count(distinct tb.error_id) as robot_error_num,
sum(unix_timestamp(stat_end_time)-unix_timestamp(stat_start_time)) as robot_error_time,
null as create_order_num,
null as create_job_num,
null as robot_run_time
from
(select t.hour_start_time,t.error_id,bn.start_time,bn.end_time,t.stat_start_time,t.stat_end_time
from qt_smartreport.qt_hour_robot_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
where t.hour_start_time BETWEEN {start_time} and {end_time}
union
select t.hour_start_time,t.error_id,bn.start_time,bn.end_time,t.stat_start_time,t.stat_end_time
from
(select date_format({now_hour_start_time}, '%Y-%m-%d %H:00:00') as hour_start_time,
       t1.id                                     as error_id,
       t1.error_code,
       t1.start_time,
       t1.end_time,
       t1.warning_spec,
       t1.alarm_module,
       t1.alarm_service,
       t1.alarm_type,
       t1.alarm_level,
       t1.alarm_detail,
       t1.param_value,
       t1.job_order,
       t1.robot_job,
       t1.robot_code,
       t1.device_code,
       t1.server_code,
       t1.transport_object,
	   case when t1.start_time < {now_hour_start_time} then {now_hour_start_time} else t1.start_time end as stat_start_time,
	   case when t1.end_time is null then sysdate() else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
               coalesce(end_time, {now_time}) < {now_next_hour_start_time}) or
              (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
               coalesce(end_time, {now_time}) >= {now_next_hour_start_time}) or
              (start_time < {now_hour_start_time} and coalesce(end_time, {now_time}) >= {now_hour_start_time} and
               coalesce(end_time, {now_time}) < {now_next_hour_start_time}) or
              (start_time < {now_hour_start_time} and coalesce(end_time, {now_time}) >= {now_next_hour_start_time})
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
                              coalesce(end_time, {now_time}) < {now_next_hour_start_time}) or
                             (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
                              coalesce(end_time, {now_time}) >= {now_next_hour_start_time}) or
                             (start_time < {now_hour_start_time} and coalesce(end_time, {now_time}) >= {now_hour_start_time} and
                              coalesce(end_time, {now_time}) < {now_next_hour_start_time}) or
                             (start_time < {now_hour_start_time} and coalesce(end_time, {now_time}) >= {now_next_hour_start_time})
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
					left join phoenix_basic.basic_notification bn on bn.id=t.error_id
					inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
					where t.hour_start_time BETWEEN {start_time} and {end_time})tb
union all
-- 所筛选小时内机器人新增
select
null as create_robot_error_num,
null as end_robot_error_num,
null as end_error_time,
null as robot_error_num,
null as robot_error_time,
count(distinct tor.order_no)                      as create_order_num,
count(distinct tocj.job_sn)                       as create_job_num,
null as robot_run_time
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj
on tocj.order_id = tor.id
where tor.create_time BETWEEN {start_time} and {end_time}
union all
-- 所筛选小时内机器人理论运行时长
select
null as create_robot_error_num,
null as end_robot_error_num,
null as end_error_time,
null as robot_error_num,
null as robot_error_time,
null as create_order_num,
null as create_job_num,
sum(t.theory_run_duration) as robot_run_time
from
(select robot_code,hour_start_time,theory_run_duration
from qt_smartreport.qt_hour_robot_error_mtbf_his t
where t.hour_start_time between {start_time} and {end_time}
union all
select robot_code,hour_start_time,theory_run_duration
from
(select
ts.robot_code,
date_format({now_hour_start_time}, '%Y-%m-%d %H:00:00') as hour_start_time,
sum(stat_state_duration) as theory_run_duration
from
(select
t1.robot_code,
t2.id              as                           state_id,
t2.create_time     as                           state_create_time,
t2.network_state,
t2.online_state,
t2.work_state,
t2.job_sn,
t2.cause,
t2.is_error,
t2.duration / 1000 as                           duration,
case when {now_time} < {now_next_hour_start_time} then UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, {now_time})) - UNIX_TIMESTAMP({now_hour_start_time}) else UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, {now_next_hour_start_time})) - UNIX_TIMESTAMP({now_hour_start_time}) end stat_state_duration
from
(select
robot_code, max(id) as before_the_hour_last_id
from phoenix_rms.robot_state_history
where create_time < {now_hour_start_time}
group by robot_code)t1
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
left join
(select
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >= {now_hour_start_time} and create_time < {now_next_hour_start_time}
group by robot_code)t3 on t3.robot_code=t1.robot_code

union all

select
t4.robot_code,
t4.id              as           state_id,
t4.create_time     as           state_create_time,
t4.network_state,
t4.online_state,
t4.work_state,
t4.job_sn,
t4.cause,
t4.is_error,
t4.duration / 1000 as           duration,
case when t5.the_hour_last_id is not null and {now_time} >= {now_next_hour_start_time} then UNIX_TIMESTAMP({now_next_hour_start_time})-UNIX_TIMESTAMP(t4.create_time)
when t5.the_hour_last_id is not null and {now_time} < {now_next_hour_start_time} then UNIX_TIMESTAMP({now_time}) - UNIX_TIMESTAMP(t4.create_time)
else t4.duration / 1000 end stat_state_duration
from
(select
*
from phoenix_rms.robot_state_history
where create_time >= {now_hour_start_time} and create_time < {now_next_hour_start_time})t4
left join
(select
robot_code,
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time
from phoenix_rms.robot_state_history
where create_time >= {now_hour_start_time} and create_time < {now_next_hour_start_time}
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code)t
where t.hour_start_time between {start_time} and {end_time})t
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
)td







######################################################################################################################################
---  检查
######################################################################################################################################
-- 新增故障次数 ：create_robot_error_num
-- 故障率（搬运作业单） ：使用 create_robot_error_num 和 create_order_num   注意数据的3种展示
-- 订单量 ： create_order_num
-- 故障率（机器人任务）： 使用 create_robot_error_num 和 create_job_num  注意数据的3种展示
-- 机器人任务量 ： create_job_num
-- OEE = (robot_run_time-robot_error_time)/robot_run_time
-- MTBF = (robot_run_time-robot_error_time)/robot_error_num
-- MTTR = end_error_time/end_robot_error_num


-- {now_start_time}  -- 当天开始时间
-- {now_end_time}    -- 当天结束时间
-- {now_time}        --  当前时间
-- {next_start_time}    --  明天开始时间
-- {now_hour_start_time}     --  当前小时开始时间
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
 



SELECT
COALESCE(max(td.create_robot_error_num),0) as create_robot_error_num,   -- 新增故障次数
COALESCE(max(td.end_robot_error_num),0) as end_robot_error_num,     -- 结束故障次数
COALESCE(max(td.end_error_time),0) as  end_error_time,           -- 结束故障时间
COALESCE(max(td.robot_error_num),0) as  robot_error_num,        -- 时间段内持续的故障次数
COALESCE(max(td.robot_error_time),0) as  robot_error_time,        -- 持续故障时长
COALESCE(max(td.create_order_num),0) as create_order_num,         -- 新增订单量
COALESCE(max(td.create_job_num),0) as create_job_num,            -- 新人任务量
COALESCE(max(td.robot_run_time),0) as  robot_run_time           -- 理论运行时长
from
(
-- 筛选小时内机器人新增故障次数、结束故障次数、已结束故障时长
select
count(distinct case when tb.start_time BETWEEN @start_time and @end_time then tb.error_id end) as create_robot_error_num,
count(distinct case when tb.end_time is not null and tb.end_time BETWEEN @start_time and @end_time then tb.error_id end) as end_robot_error_num,
sum(case when tb.end_time is not null and tb.end_time BETWEEN @start_time and @end_time and date_format(tb.end_time, '%Y-%m-%d %H:00:00')=date_format(tb.hour_start_time, '%Y-%m-%d %H:00:00') then unix_timestamp(tb.end_time)-unix_timestamp(tb.start_time) end) as end_error_time,
count(distinct tb.error_id) as robot_error_num,
sum(unix_timestamp(stat_end_time)-unix_timestamp(stat_start_time)) as robot_error_time,
null as create_order_num,
null as create_job_num,
null as robot_run_time
from
(select t.hour_start_time,t.error_id,bn.start_time,bn.end_time,t.stat_start_time,t.stat_end_time
from qt_smartreport.qt_hour_robot_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
where t.hour_start_time BETWEEN @start_time and @end_time
union
select t.hour_start_time,t.error_id,bn.start_time,bn.end_time,t.stat_start_time,t.stat_end_time
from
(select date_format(@now_hour_start_time, '%Y-%m-%d %H:00:00') as hour_start_time,
       t1.id                                     as error_id,
       t1.error_code,
       t1.start_time,
       t1.end_time,
       t1.warning_spec,
       t1.alarm_module,
       t1.alarm_service,
       t1.alarm_type,
       t1.alarm_level,
       t1.alarm_detail,
       t1.param_value,
       t1.job_order,
       t1.robot_job,
       t1.robot_code,
       t1.device_code,
       t1.server_code,
       t1.transport_object,
	   case when t1.start_time < @now_hour_start_time then @now_hour_start_time  else t1.start_time end as stat_start_time,
	   case when t1.end_time is null then sysdate() else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
               coalesce(end_time, @now_time) < @now_next_hour_start_time) or
              (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
               coalesce(end_time, @now_time) >= @now_next_hour_start_time) or
              (start_time < @now_hour_start_time and coalesce(end_time,  @now_time ) >= @now_hour_start_time and
               coalesce(end_time, @now_time) < @now_next_hour_start_time) or
              (start_time < @now_hour_start_time and coalesce(end_time,  @now_time ) >= @now_next_hour_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             ( start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
                              coalesce(end_time, @now_time) < @now_next_hour_start_time ) or
                             ( start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
                              coalesce(end_time, @now_time) >= @now_next_hour_start_time ) or
                             ( start_time < @now_hour_start_time and coalesce( end_time, @now_time ) >= @now_hour_start_time and
                              coalesce( end_time, @now_time ) < @now_next_hour_start_time ) or
                             ( start_time < @now_hour_start_time and coalesce(end_time, @now_time ) >= @now_next_hour_start_time )
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
					left join phoenix_basic.basic_notification bn on bn.id=t.error_id
					inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
					where t.hour_start_time BETWEEN @start_time and @end_time )tb
union all
-- 所筛选小时内机器人新增
select
null as create_robot_error_num,
null as end_robot_error_num,
null as end_error_time,
null as robot_error_num,
null as robot_error_time,
count(distinct tor.order_no)                      as create_order_num,
count(distinct tocj.job_sn)                       as create_job_num,
null as robot_run_time
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj
on tocj.order_id = tor.id
where tor.create_time BETWEEN @start_time and @end_time
union all
-- 所筛选小时内机器人理论运行时长
select
null as create_robot_error_num,
null as end_robot_error_num,
null as end_error_time,
null as robot_error_num,
null as robot_error_time,
null as create_order_num,
null as create_job_num,
sum(t.theory_run_duration) as robot_run_time
from
(select robot_code,hour_start_time,theory_run_duration
from qt_smartreport.qt_hour_robot_error_mtbf_his t
where t.hour_start_time between @start_time and @end_time
union all
select robot_code,hour_start_time,theory_run_duration
from
(select
ts.robot_code,
date_format(@now_hour_start_time, '%Y-%m-%d %H:00:00') as hour_start_time,
sum(stat_state_duration) as theory_run_duration
from
(select
t1.robot_code,
t2.id              as                           state_id,
t2.create_time     as                           state_create_time,
t2.network_state,
t2.online_state,
t2.work_state,
t2.job_sn,
t2.cause,
t2.is_error,
t2.duration / 1000 as                           duration,
case when @now_time < @now_next_hour_start_time then UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time,  @now_time )) - UNIX_TIMESTAMP( @now_hour_start_time) else UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time,  @now_next_hour_start_time )) - UNIX_TIMESTAMP( @now_hour_start_time ) end stat_state_duration
from
(select
robot_code, max(id) as before_the_hour_last_id
from phoenix_rms.robot_state_history
where create_time < @now_hour_start_time
group by robot_code)t1
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
left join
(select
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >= @now_hour_start_time and create_time < @now_next_hour_start_time
group by robot_code)t3 on t3.robot_code=t1.robot_code

union all

select
t4.robot_code,
t4.id              as           state_id,
t4.create_time     as           state_create_time,
t4.network_state,
t4.online_state,
t4.work_state,
t4.job_sn,
t4.cause,
t4.is_error,
t4.duration / 1000 as           duration,
case when t5.the_hour_last_id is not null and @now_time >= @now_next_hour_start_time then UNIX_TIMESTAMP( @now_next_hour_start_time )-UNIX_TIMESTAMP(t4.create_time)
when t5.the_hour_last_id is not null and  @now_time  < @now_next_hour_start_time then UNIX_TIMESTAMP( @now_time ) - UNIX_TIMESTAMP(t4.create_time)
else t4.duration / 1000 end stat_state_duration
from
(select
*
from phoenix_rms.robot_state_history
where create_time >=  @now_hour_start_time and create_time <  @now_next_hour_start_time )t4
left join
(select
robot_code,
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time
from phoenix_rms.robot_state_history
where create_time >=  @now_hour_start_time and create_time <  @now_next_hour_start_time 
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code)t
where t.hour_start_time between @start_time and  @end_time )t
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
)td


