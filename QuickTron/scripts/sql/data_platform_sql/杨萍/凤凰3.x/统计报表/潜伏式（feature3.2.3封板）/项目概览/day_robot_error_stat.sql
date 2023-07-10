select '全场'                                            as stat_type,
       null                                              as robot_type_code,
       '全场'                                            as robot_type_name,               -- 机器人类型
       coalesce(sum(t.create_robot_error_num), 0)        as breakdown_num,                 -- 故障次数
       case
	       when COALESCE(SUM(t.create_robot_error_num), 0) = 0 then 0
	       when COALESCE(SUM(t.create_robot_error_num), 0) != 0 and COALESCE(SUM(t.create_order_num), 0) = 0 then concat(coalesce(sum(t.create_robot_error_num), 0), '/', '0')
           when COALESCE(SUM(t.create_robot_error_num), 0) != 0 and COALESCE(SUM(t.create_order_num), 0) >= COALESCE(SUM(t.create_robot_error_num), 0) then concat('1', '/',round(COALESCE(SUM(t.create_order_num), 0) /COALESCE(SUM(t.create_robot_error_num), 0)))
           when COALESCE(SUM(t.create_robot_error_num), 0) != 0 and COALESCE(SUM(t.create_order_num), 0) < COALESCE(SUM(t.create_robot_error_num), 0) then concat(round(COALESCE(SUM(t.create_robot_error_num), 0) / COALESCE(SUM(t.create_order_num), 0)), '/', '1') else 0
	   end as order_breakdown_rate,-- 故障率（搬运作业单）
       case
           when COALESCE(SUM(t.create_order_num), 0) = 0 then COALESCE(SUM(t.create_robot_error_num), 0) / 1 else COALESCE(SUM(t.create_robot_error_num), 0) /COALESCE(SUM(t.create_order_num), 0)
	   end AS order_breakdown_rate_sort,     -- 故障率（搬运作业单）排序
       coalesce(sum(t.create_order_num), 0)    as order_num,                     -- 订单量
       case
           when COALESCE(SUM(t.create_robot_error_num), 0) = 0 then 0
           when COALESCE(SUM(t.create_robot_error_num), 0) != 0 and COALESCE(SUM(t.create_job_num), 0) = 0 then concat(coalesce(sum(t.create_robot_error_num), 0), '/', '0')
           when COALESCE(SUM(t.create_robot_error_num), 0) != 0 and COALESCE(SUM(t.create_job_num), 0) >= COALESCE(SUM(t.create_robot_error_num), 0) then concat('1', '/',round(COALESCE(SUM(t.create_job_num), 0) /COALESCE(SUM(t.create_robot_error_num), 0)))
           when COALESCE(SUM(t.create_robot_error_num), 0) != 0 and COALESCE(SUM(t.create_job_num), 0) < COALESCE(SUM(t.create_robot_error_num), 0) then concat(round(COALESCE(SUM(t.create_robot_error_num), 0) / COALESCE(SUM(t.create_job_num), 0)), '/', '1')else 0
	   end   as carry_job_breakdown_rate,      -- 故障率（搬运任务）
       case
           when COALESCE(SUM(t.create_job_num), 0) = 0 then COALESCE(SUM(t.create_robot_error_num), 0) / 1 else COALESCE(SUM(t.create_robot_error_num), 0) /COALESCE(SUM(t.create_job_num), 0) end as carry_job_breakdown_rate_sort, -- 故障率（搬运任务）排序
       COALESCE(sum(t.create_job_num), 0) as carry_job_num,                 -- 搬运任务数
       coalesce(sum(t.the_day_run_time), 0)  as the_day_run_time,
       coalesce(sum(t.the_day_error_time), 0) as the_day_error_time,
       ROUND((coalesce(sum(t.the_day_run_time), 0) - coalesce(sum(t.the_day_error_time), 0)) /coalesce(sum(t.the_day_run_time), 0), 4) as oee,
       COALESCE(sum(t.end_error_num), 0)   as end_error_num,
       coalesce(sum(t.end_error_time), 0)  as end_error_time,
       coalesce(sum(t.end_error_time), 0) / COALESCE(sum(t.end_error_num), 0)   as mttr,
	   case when t.the_day_error_num != 0 then (COALESCE(t.the_day_run_time,0)-COALESCE(t.the_day_error_time,0))/t.the_day_error_num end as mtbf,
	   case when t.robot_error_num_his !=0 then ((COALESCE(t.before_theory_run_duration,0)+COALESCE(t.the_day_run_time,0))-(COALESCE(t.before_error_duration,0)+COALESCE(t.the_day_error_time,0)))/t.robot_error_num_his end as cumul_mtbf
from (SELECT
COALESCE(max(td.create_robot_error_num),0) as create_robot_error_num,   -- 新增故障次数
COALESCE(max(td.create_order_num),0) as create_order_num,         -- 新增订单量
COALESCE(max(td.create_job_num),0) as create_job_num,            -- 新人任务量
COALESCE(max(td.end_robot_error_num),0) as end_error_num,     -- 结束故障次数
COALESCE(max(td.end_error_time),0) as  end_error_time,           -- 结束故障时间
COALESCE(max(td.robot_run_time),0) as  the_day_run_time,           -- 理论运行时长
COALESCE(max(td.robot_error_time),0) as  the_day_error_time,        -- 持续故障时长
COALESCE(max(td.robot_error_num),0) as  the_day_error_num,        -- 时间段内持续的故障次数
COALESCE(max(td.before_theory_run_duration),0) as before_theory_run_duration, -- 全场机器人当天之前理论运行时长
COALESCE(max(td.before_error_duration),0) as  before_error_duration,      -- 全场机器人当天之前故障时长
COALESCE(max(td.robot_error_num_his),0) as robot_error_num_his         -- 历史全场机器人故障收敛后集合
from
(
-- 筛选小时内机器人新增故障次数、结束故障次数、已结束故障时长
select
count(distinct case when tb.start_time >= {now_start_time} and tb.start_time < {next_start_time} then tb.error_id end) as create_robot_error_num,
count(distinct case when tb.end_time is not null and tb.end_time >= {now_start_time} and tb.end_time < {next_start_time} then tb.error_id end) as end_robot_error_num,
sum(case when tb.end_time is not null and tb.end_time >= {now_start_time} and tb.end_time < {next_start_time} and date_format(tb.end_time, '%Y-%m-%d %H:00:00')=date_format(tb.hour_start_time, '%Y-%m-%d %H:00:00') then unix_timestamp(tb.end_time)-unix_timestamp(tb.start_time) end) as end_error_time,
count(distinct tb.error_id) as robot_error_num,
sum(unix_timestamp(stat_end_time)-unix_timestamp(stat_start_time)) as robot_error_time,
null as create_order_num,
null as create_job_num,
null as robot_run_time,
null as before_theory_run_duration,
null as before_error_duration,
null as robot_error_num_his
from
(select t.hour_start_time,t.error_id,bn.start_time,bn.end_time,t.stat_start_time,t.stat_end_time
from qt_smartreport.qt_hour_robot_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
where t.hour_start_time >= {now_start_time} and t.hour_start_time < {next_start_time}
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
	   case when t1.start_time < {now_hour_start_time} then {now_hour_start_time}  else t1.start_time end as stat_start_time,
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
              (start_time < {now_hour_start_time} and coalesce(end_time,  {now_time} ) >= {now_hour_start_time} and
               coalesce(end_time, {now_time}) < {now_next_hour_start_time}) or
              (start_time < {now_hour_start_time} and coalesce(end_time,  {now_time} ) >= {now_next_hour_start_time})
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             ( start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
                              coalesce(end_time, {now_time}) < {now_next_hour_start_time} ) or
                             ( start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
                              coalesce(end_time, {now_time}) >= {now_next_hour_start_time} ) or
                             ( start_time < {now_hour_start_time} and coalesce( end_time, {now_time} ) >= {now_hour_start_time} and
                              coalesce( end_time, {now_time} ) < {now_next_hour_start_time} ) or
                             ( start_time < {now_hour_start_time} and coalesce(end_time, {now_time} ) >= {now_next_hour_start_time} )
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
					left join phoenix_basic.basic_notification bn on bn.id=t.error_id
					inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
					where t.hour_start_time >= {now_start_time} and t.hour_start_time < {next_start_time} )tb
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
null as robot_run_time,
null as before_theory_run_duration,
null as before_error_duration,
null as robot_error_num_his
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj
on tocj.order_id = tor.id
where tor.create_time >= {now_start_time} and tor.create_time < {next_start_time}
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
sum(t.theory_run_duration) as robot_run_time,
null as before_theory_run_duration,
null as before_error_duration,
null as robot_error_num_his
from
(select robot_code,hour_start_time,theory_run_duration
from qt_smartreport.qt_hour_robot_error_mtbf_his t
where t.hour_start_time >= {now_start_time} and t.hour_start_time < {next_start_time}
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
case when {now_time} < {now_next_hour_start_time} then UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time,  {now_time} )) - UNIX_TIMESTAMP( {now_hour_start_time}) else UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time,  {now_next_hour_start_time} )) - UNIX_TIMESTAMP( {now_hour_start_time} ) end stat_state_duration
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
case when t5.the_hour_last_id is not null and {now_time} >= {now_next_hour_start_time} then UNIX_TIMESTAMP( {now_next_hour_start_time} )-UNIX_TIMESTAMP(t4.create_time)
when t5.the_hour_last_id is not null and  {now_time}  < {now_next_hour_start_time} then UNIX_TIMESTAMP( {now_time} ) - UNIX_TIMESTAMP(t4.create_time)
else t4.duration / 1000 end stat_state_duration
from
(select
*
from phoenix_rms.robot_state_history
where create_time >=  {now_hour_start_time} and create_time <  {now_next_hour_start_time} )t4
left join
(select
robot_code,
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time
from phoenix_rms.robot_state_history
where create_time >=  {now_hour_start_time} and create_time <  {now_next_hour_start_time}
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code)t
where t.hour_start_time >= {now_start_time} and t.hour_start_time < {next_start_time}  )t
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
union all
-- 全场机器人当天之前理论运行时长、全场机器人当天之前故障时长
select
null as create_robot_error_num,
null as end_robot_error_num,
null as end_error_time,
null as robot_error_num,
null as robot_error_time,
null as create_order_num,
null as create_job_num,
null as robot_run_time,
sum(t.accum_theory_run_duration) as before_theory_run_duration, -- 全场机器人当天之前理论运行时长
sum(t.accum_error_duration) as before_error_duration,   -- 全场机器人当天之前故障时长
null as robot_error_num_his
from qt_smartreport.qt_day_robot_error_mtbf_his t
inner join phoenix_basic.basic_robot br on br.robot_code = t.robot_code and br.usage_state = 'using'
where t.date_value = date(date_add( {now_start_time} , interval -1 day))
union all
-- 全场机器人累计故障收敛后集合
select
null as create_robot_error_num,
null as end_robot_error_num,
null as end_error_time,
null as robot_error_num,
null as robot_error_time,
null as create_order_num,
null as create_job_num,
null as robot_run_time,
null as before_theory_run_duration, -- 全场机器人当天之前理论运行时长
null as before_error_duration,      -- 全场机器人当天之前故障时长
count(distinct tr.error_id) as robot_error_num_his         -- 历史全场机器人故障收敛后集合
from
(select distinct t.robot_code,t.error_id  -- 机器人当前小时之前故障收敛后集合
from qt_smartreport.qt_hour_robot_error_list_his t
inner join phoenix_basic.basic_robot br on br.robot_code = t.robot_code and br.usage_state = 'using'
union
select distinct t.robot_code,t.error_id  -- 当前小时机器人故障收敛后集合
from
(select
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
               coalesce(end_time, sysdate()) <  {now_next_hour_start_time} ) or
              (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
               coalesce(end_time, sysdate()) >=  {now_next_hour_start_time} ) or
              (start_time < {now_hour_start_time} and coalesce(end_time, sysdate()) >= {now_hour_start_time} and
               coalesce(end_time, sysdate()) <  {now_next_hour_start_time} ) or
              (start_time < {now_hour_start_time} and coalesce(end_time, sysdate()) >=  {now_next_hour_start_time} )
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
                              coalesce(end_time, sysdate()) <  {now_next_hour_start_time} ) or
                             (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
                              coalesce(end_time, sysdate()) >=  {now_next_hour_start_time} ) or
                             (start_time < {now_hour_start_time} and coalesce(end_time, sysdate()) >= {now_hour_start_time} and
                              coalesce(end_time, sysdate()) <  {now_next_hour_start_time} ) or
                             (start_time < {now_hour_start_time} and coalesce(end_time, sysdate()) >= {now_next_hour_start_time} )
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
         inner join phoenix_basic.basic_robot br on br.robot_code = t.robot_code and br.usage_state = 'using')tr
)td
) t

union all
select '各类机器人'                                                        as stat_type,
       t.robot_type_code,
       t.robot_type_name,  -- 机器人类型
       coalesce(sum(t.create_robot_error_num), 0)                          as breakdown_num, -- 故障次数
	case
     when COALESCE(SUM(t.create_robot_error_num),0)=0 then 0
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_order_num),0)=0 then concat(coalesce(sum(t.create_robot_error_num), 0), '/', '0')
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_order_num),0)>=COALESCE(SUM(t.create_robot_error_num),0) then concat('1','/',round(COALESCE(SUM(t.create_order_num),0)/COALESCE(SUM(t.create_robot_error_num),0)))
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_order_num),0)<COALESCE(SUM(t.create_robot_error_num),0) then concat(round(COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_order_num),0)),'/','1')
	 else 0 end as order_breakdown_rate, -- 故障率（搬运作业单）
    case when COALESCE(SUM(t.create_order_num),0)=0 then COALESCE(SUM(t.create_robot_error_num),0)/1 else COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_order_num),0) end AS order_breakdown_rate_sort, -- 故障率（搬运作业单）排序
       coalesce(sum(t.create_order_num), 0)                                as order_num, -- 订单量
	 case
     when COALESCE(SUM(t.create_robot_error_num),0)=0 then 0
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_job_num),0)=0 then concat(coalesce(sum(t.create_robot_error_num), 0), '/', '0')
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_job_num),0)>=COALESCE(SUM(t.create_robot_error_num),0) then concat('1','/',round(COALESCE(SUM(t.create_job_num),0)/COALESCE(SUM(t.create_robot_error_num),0)))
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_job_num),0)<COALESCE(SUM(t.create_robot_error_num),0) then concat(round(COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_job_num),0)),'/','1')
	 else 0 end as carry_job_breakdown_rate, -- 故障率（搬运任务）
    case when COALESCE(SUM(t.create_job_num),0)=0 then COALESCE(SUM(t.create_robot_error_num),0)/1 else COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_job_num),0) end AS carry_job_breakdown_rate_sort, -- 故障率（搬运任务）排序
       COALESCE(sum(t.create_job_num), 0)                                  as carry_job_num, -- 搬运任务数
       coalesce(sum(t.the_day_run_time), 0)                                as the_day_run_time,
       coalesce(sum(t.the_day_error_time), 0)                              as the_day_error_time,
       (coalesce(sum(t.the_day_run_time), 0) - coalesce(sum(t.the_day_error_time), 0)) /
       coalesce(sum(t.the_day_run_time), 0)                                as oee,
       COALESCE(sum(t.end_error_num), 0)                                   as end_error_num,
       coalesce(sum(t.end_error_time), 0)                                  as end_error_time,
       coalesce(sum(t.end_error_time), 0) /
       COALESCE(sum(t.end_error_num), 0)                                   as mttr,
    case when t.the_day_error_num != 0 then (COALESCE(t.the_day_run_time,0)-COALESCE(t.the_day_error_time,0))/t.the_day_error_num end as mtbf,
	case when t.robot_error_num_his != 0 then ((COALESCE(t.before_theory_run_duration,0)+COALESCE(t.the_day_run_time,0))-(COALESCE(t.before_error_duration,0)+COALESCE(t.the_day_error_time,0)))/t.robot_error_num_his end as cumul_mtbf
from
(SELECT
br.robot_type_code,
br.robot_type_name,
COALESCE(t1.create_robot_error_num,0) as create_robot_error_num,   -- 新增故障次数
COALESCE(t2.create_order_num,0) as create_order_num,         -- 新增订单量
COALESCE(t2.create_job_num,0) as create_job_num,            -- 新人任务量
COALESCE(t1.end_robot_error_num,0) as end_error_num,     -- 结束故障次数
COALESCE(t1.end_error_time,0) as  end_error_time,           -- 结束故障时间
COALESCE(t3.robot_run_time,0) as  the_day_run_time,           -- 理论运行时长
COALESCE(t1.robot_error_time,0) as  the_day_error_time,        -- 持续故障时长
COALESCE(t1.robot_error_num,0) as  the_day_error_num,        -- 时间段内持续的故障次数
COALESCE(t4.before_theory_run_duration,0) as  before_theory_run_duration,
COALESCE(t4.before_error_duration,0) as  before_error_duration,
COALESCE(t5.robot_error_num_his,0) as robot_error_num_his
from
(select distinct brt.robot_type_code,brt.robot_type_name
from phoenix_basic.basic_robot br
inner join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id and br.usage_state = 'using')br
--  小时内机器人新增故障次数、结束故障次数、已结束故障时长
left join
(select
tb.robot_type_code,tb.robot_type_name,
count(distinct case when tb.start_time >= {now_start_time} and tb.start_time < {next_start_time} then tb.error_id end) as create_robot_error_num,
count(distinct case when tb.end_time is not null and tb.end_time >= {now_start_time} and tb.end_time < {next_start_time} then tb.error_id end) as end_robot_error_num,
sum(case when tb.end_time is not null and tb.end_time >= {now_start_time} and tb.end_time < {next_start_time} and date_format(tb.end_time, '%Y-%m-%d %H:00:00')=date_format(tb.hour_start_time, '%Y-%m-%d %H:00:00') then unix_timestamp(tb.end_time)-unix_timestamp(tb.start_time) end) as end_error_time,
count(distinct tb.error_id) as robot_error_num,
sum(unix_timestamp(stat_end_time)-unix_timestamp(stat_start_time)) as robot_error_time
from
(select brt.robot_type_code,brt.robot_type_name,t.hour_start_time,t.error_id,bn.start_time,bn.end_time,t.stat_start_time,t.stat_end_time
from qt_smartreport.qt_hour_robot_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where t.hour_start_time >= {now_start_time} and t.hour_start_time < {next_start_time}
union
select brt.robot_type_code,brt.robot_type_name,t.hour_start_time,t.error_id,bn.start_time,bn.end_time,t.stat_start_time,t.stat_end_time
from
(select date_format( {now_hour_start_time}  , '%Y-%m-%d %H:00:00') as hour_start_time,
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
	   case when t1.start_time <  {now_hour_start_time}   then  {now_hour_start_time}   else t1.start_time end as stat_start_time,
	   case when t1.end_time is null or t1.end_time >=  {now_next_hour_start_time}   then  {now_next_hour_start_time}   else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >=  {now_hour_start_time}   and start_time <  {now_next_hour_start_time}   and
               coalesce(end_time,  {now_time}  ) <  {now_next_hour_start_time}  ) or
              (start_time >=  {now_hour_start_time}   and start_time <  {now_next_hour_start_time}   and
               coalesce(end_time,  {now_time}  ) >=  {now_next_hour_start_time}  ) or
              (start_time <  {now_hour_start_time}   and coalesce(end_time,  {now_time}  ) >=  {now_hour_start_time}   and
               coalesce(end_time,  {now_time}  ) <  {now_next_hour_start_time}  ) or
              (start_time <  {now_hour_start_time}   and coalesce(end_time,  {now_time}  ) >=  {now_next_hour_start_time}  )
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >=  {now_hour_start_time}   and start_time <  {now_next_hour_start_time}   and
                              coalesce(end_time,  {now_time}  ) <  {now_next_hour_start_time}  ) or
                             (start_time >=  {now_hour_start_time}   and start_time <  {now_next_hour_start_time}   and
                              coalesce(end_time,  {now_time}  ) >=  {now_next_hour_start_time}  ) or
                             (start_time <  {now_hour_start_time}   and coalesce(end_time,  {now_time}  ) >=  {now_hour_start_time}   and
                              coalesce(end_time,  {now_time}  ) <  {now_next_hour_start_time}  ) or
                             (start_time <  {now_hour_start_time}   and coalesce(end_time,  {now_time}  ) >=  {now_next_hour_start_time}  )
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
					left join phoenix_basic.basic_notification bn on bn.id=t.error_id
					inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
                    left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
					where t.hour_start_time >= {now_start_time} and t.hour_start_time < {next_start_time}  )tb
group by tb.robot_type_code,tb.robot_type_name)t1 on t1.robot_type_code=br.robot_type_code
-- 小时内机器人新增
left join
(select
brt.robot_type_code,brt.robot_type_name,
count(distinct tor.order_no)                      as create_order_num,
count(distinct tocj.job_sn)                       as create_job_num
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj
on tocj.order_id = tor.id
inner join phoenix_basic.basic_robot br on br.robot_code=tocj.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tor.create_time >= {now_start_time} and tor.create_time < {next_start_time}
group by brt.robot_type_code,brt.robot_type_name)t2 on t2.robot_type_code=br.robot_type_code
-- 小时内机器人理论运行时长
left join
(select
brt.robot_type_code,brt.robot_type_name,sum(t.theory_run_duration) as robot_run_time
from
(select robot_code,hour_start_time,theory_run_duration
from qt_smartreport.qt_hour_robot_error_mtbf_his t
where t.hour_start_time >= {now_start_time} and t.hour_start_time < {next_start_time}
union all
select robot_code,hour_start_time,theory_run_duration
from
(select
ts.robot_code,
date_format( {now_hour_start_time}  , '%Y-%m-%d %H:00:00') as hour_start_time,
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
case when  {now_time}   <  {now_next_hour_start_time}   then UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time,  {now_time}  )) - UNIX_TIMESTAMP( {now_hour_start_time}  ) else UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time,  {now_next_hour_start_time}  )) - UNIX_TIMESTAMP( {now_hour_start_time}  ) end stat_state_duration
from
(select
robot_code, max(id) as before_the_hour_last_id
from phoenix_rms.robot_state_history
where create_time <  {now_hour_start_time}
group by robot_code)t1
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
left join
(select
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >=  {now_hour_start_time}   and create_time <  {now_next_hour_start_time}
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
case when t5.the_hour_last_id is not null and  {now_time}   >=  {now_next_hour_start_time}   then UNIX_TIMESTAMP( {now_next_hour_start_time}  )-UNIX_TIMESTAMP(t4.create_time)
when t5.the_hour_last_id is not null and  {now_time}   <  {now_next_hour_start_time}   then UNIX_TIMESTAMP( {now_time}  ) - UNIX_TIMESTAMP(t4.create_time)
else t4.duration / 1000 end stat_state_duration
from
(select
*
from phoenix_rms.robot_state_history
where create_time >=  {now_hour_start_time}   and create_time <  {now_next_hour_start_time}  )t4
left join
(select
robot_code,
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time
from phoenix_rms.robot_state_history
where create_time >=  {now_hour_start_time}   and create_time <  {now_next_hour_start_time}
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code)t
where t.hour_start_time >= {now_start_time} and t.hour_start_time < {next_start_time}  )t
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
group by brt.robot_type_code,brt.robot_type_name)t3 on t3.robot_type_code=br.robot_type_code
-- 当天之前理论运行时长、全场机器人当天之前故障时长
left join
(select
brt.robot_type_code,brt.robot_type_name,
sum(t.accum_theory_run_duration) as before_theory_run_duration, -- 全场机器人当天之前理论运行时长
sum(t.accum_error_duration) as before_error_duration,   -- 全场机器人当天之前故障时长
null as robot_error_num_his
from qt_smartreport.qt_day_robot_error_mtbf_his t
inner join phoenix_basic.basic_robot br on br.robot_code = t.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where t.date_value = date(date_add( {now_start_time} , interval -1 day))
group by brt.robot_type_code,brt.robot_type_name)t4 on t4.robot_type_code=br.robot_type_code
-- 各类机器人累计故障收敛后集合
left join
(select
brt.robot_type_code,brt.robot_type_name,
count(distinct tr.error_id) as robot_error_num_his         -- 历史全场机器人故障收敛后集合
from
(select distinct t.robot_code,t.error_id  -- 机器人当前小时之前故障收敛后集合
from qt_smartreport.qt_hour_robot_error_list_his t
union
select distinct t.robot_code,t.error_id  -- 当前小时机器人故障收敛后集合
from
(select
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
	   case when t1.end_time is null or t1.end_time >= {now_next_hour_start_time} then {now_next_hour_start_time} else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
               coalesce(end_time, sysdate()) <  {now_next_hour_start_time} ) or
              (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
               coalesce(end_time, sysdate()) >=  {now_next_hour_start_time} ) or
              (start_time < {now_hour_start_time} and coalesce(end_time, sysdate()) >= {now_hour_start_time} and
               coalesce(end_time, sysdate()) <  {now_next_hour_start_time} ) or
              (start_time < {now_hour_start_time} and coalesce(end_time, sysdate()) >=  {now_next_hour_start_time} )
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
                              coalesce(end_time, sysdate()) <  {now_next_hour_start_time} ) or
                             (start_time >= {now_hour_start_time} and start_time < {now_next_hour_start_time} and
                              coalesce(end_time, sysdate()) >=  {now_next_hour_start_time} ) or
                             (start_time < {now_hour_start_time} and coalesce(end_time, sysdate()) >= {now_hour_start_time} and
                              coalesce(end_time, sysdate()) <  {now_next_hour_start_time} ) or
                             (start_time < {now_hour_start_time} and coalesce(end_time, sysdate()) >= {now_next_hour_start_time} )
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
		 )tr
		 inner join phoenix_basic.basic_robot br on br.robot_code = tr.robot_code and br.usage_state = 'using'
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
group by brt.robot_type_code,brt.robot_type_name)t5 on t5.robot_type_code=br.robot_type_code

) t
group by t.robot_type_code, t.robot_type_name
















######################################################################################################################################
---  检查
######################################################################################################################################


--   {now_start_time}    -- 当天开始时间
--   {now_end_time}      -- 当天结束时间
--   {now_time}          --  当前时间
--   {next_start_time}      --  明天开始时间
--   {now_hour_start_time}       --  当前小时开始时间
--   {now_next_hour_start_time}    -- 下一个小时开始时间
--   {now_week_start_time}    -- 当前一周的开始时间
--   {now_next_week_start_time}    --  下一周的开始时间
--   {start_time}    -- 筛选框开始时间  默认当天开始时间
--   {end_time}     --  筛选框结束时间  默认当前小时结束时间



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
 


select '全场'                                            as stat_type,
       null                                              as robot_type_code,
       '全场'                                            as robot_type_name,               -- 机器人类型
       coalesce(sum(t.create_robot_error_num), 0)        as breakdown_num,                 -- 故障次数
       case
	       when COALESCE(SUM(t.create_robot_error_num), 0) = 0 then 0
	       when COALESCE(SUM(t.create_robot_error_num), 0) != 0 and COALESCE(SUM(t.create_order_num), 0) = 0 then concat(coalesce(sum(t.create_robot_error_num), 0), '/', '0')
           when COALESCE(SUM(t.create_robot_error_num), 0) != 0 and COALESCE(SUM(t.create_order_num), 0) >= COALESCE(SUM(t.create_robot_error_num), 0) then concat('1', '/',round(COALESCE(SUM(t.create_order_num), 0) /COALESCE(SUM(t.create_robot_error_num), 0)))
           when COALESCE(SUM(t.create_robot_error_num), 0) != 0 and COALESCE(SUM(t.create_order_num), 0) < COALESCE(SUM(t.create_robot_error_num), 0) then concat(round(COALESCE(SUM(t.create_robot_error_num), 0) / COALESCE(SUM(t.create_order_num), 0)), '/', '1') else 0
	   end as order_breakdown_rate,-- 故障率（搬运作业单）
       case
           when COALESCE(SUM(t.create_order_num), 0) = 0 then COALESCE(SUM(t.create_robot_error_num), 0) / 1 else COALESCE(SUM(t.create_robot_error_num), 0) /COALESCE(SUM(t.create_order_num), 0)
	   end AS order_breakdown_rate_sort,     -- 故障率（搬运作业单）排序
       coalesce(sum(t.create_order_num), 0)    as order_num,                     -- 订单量
       case
           when COALESCE(SUM(t.create_robot_error_num), 0) = 0 then 0
           when COALESCE(SUM(t.create_robot_error_num), 0) != 0 and COALESCE(SUM(t.create_job_num), 0) = 0 then concat(coalesce(sum(t.create_robot_error_num), 0), '/', '0')
           when COALESCE(SUM(t.create_robot_error_num), 0) != 0 and COALESCE(SUM(t.create_job_num), 0) >= COALESCE(SUM(t.create_robot_error_num), 0) then concat('1', '/',round(COALESCE(SUM(t.create_job_num), 0) /COALESCE(SUM(t.create_robot_error_num), 0)))
           when COALESCE(SUM(t.create_robot_error_num), 0) != 0 and COALESCE(SUM(t.create_job_num), 0) < COALESCE(SUM(t.create_robot_error_num), 0) then concat(round(COALESCE(SUM(t.create_robot_error_num), 0) / COALESCE(SUM(t.create_job_num), 0)), '/', '1')else 0
	   end   as carry_job_breakdown_rate,      -- 故障率（搬运任务）
       case
           when COALESCE(SUM(t.create_job_num), 0) = 0 then COALESCE(SUM(t.create_robot_error_num), 0) / 1 else COALESCE(SUM(t.create_robot_error_num), 0) /COALESCE(SUM(t.create_job_num), 0) end as carry_job_breakdown_rate_sort, -- 故障率（搬运任务）排序
       COALESCE(sum(t.create_job_num), 0) as carry_job_num,                 -- 搬运任务数
       coalesce(sum(t.the_day_run_time), 0)  as the_day_run_time,
       coalesce(sum(t.the_day_error_time), 0) as the_day_error_time,
       ROUND((coalesce(sum(t.the_day_run_time), 0) - coalesce(sum(t.the_day_error_time), 0)) /coalesce(sum(t.the_day_run_time), 0), 4) as oee,
       COALESCE(sum(t.end_error_num), 0)   as end_error_num,
       coalesce(sum(t.end_error_time), 0)  as end_error_time,
       coalesce(sum(t.end_error_time), 0) / COALESCE(sum(t.end_error_num), 0)   as mttr,
	   case when t.the_day_error_num != 0 then (COALESCE(t.the_day_run_time,0)-COALESCE(t.the_day_error_time,0))/t.the_day_error_num end as mtbf,
	   case when t.robot_error_num_his !=0 then ((COALESCE(t.before_theory_run_duration,0)+COALESCE(t.the_day_run_time,0))-(COALESCE(t.before_error_duration,0)+COALESCE(t.the_day_error_time,0)))/t.robot_error_num_his end as cumul_mtbf
from (SELECT
COALESCE(max(td.create_robot_error_num),0) as create_robot_error_num,   -- 新增故障次数
COALESCE(max(td.create_order_num),0) as create_order_num,         -- 新增订单量
COALESCE(max(td.create_job_num),0) as create_job_num,            -- 新人任务量
COALESCE(max(td.end_robot_error_num),0) as end_error_num,     -- 结束故障次数
COALESCE(max(td.end_error_time),0) as  end_error_time,           -- 结束故障时间
COALESCE(max(td.robot_run_time),0) as  the_day_run_time,           -- 理论运行时长
COALESCE(max(td.robot_error_time),0) as  the_day_error_time,        -- 持续故障时长
COALESCE(max(td.robot_error_num),0) as  the_day_error_num,        -- 时间段内持续的故障次数
COALESCE(max(td.before_theory_run_duration),0) as before_theory_run_duration, -- 全场机器人当天之前理论运行时长
COALESCE(max(td.before_error_duration),0) as  before_error_duration,      -- 全场机器人当天之前故障时长
COALESCE(max(td.robot_error_num_his),0) as robot_error_num_his         -- 历史全场机器人故障收敛后集合
from
(
-- 筛选小时内机器人新增故障次数、结束故障次数、已结束故障时长
select
count(distinct case when tb.start_time >= @now_start_time and tb.start_time < @next_start_time then tb.error_id end) as create_robot_error_num,  
count(distinct case when tb.end_time is not null and tb.end_time >= @now_start_time and tb.end_time < @next_start_time then tb.error_id end) as end_robot_error_num,
sum(case when tb.end_time is not null and tb.end_time >= @now_start_time and tb.end_time < @next_start_time and date_format(tb.end_time, '%Y-%m-%d %H:00:00')=date_format(tb.hour_start_time, '%Y-%m-%d %H:00:00') then unix_timestamp(tb.end_time)-unix_timestamp(tb.start_time) end) as end_error_time,
count(distinct tb.error_id) as robot_error_num,
sum(unix_timestamp(stat_end_time)-unix_timestamp(stat_start_time)) as robot_error_time,
null as create_order_num,
null as create_job_num,
null as robot_run_time,
null as before_theory_run_duration,
null as before_error_duration,
null as robot_error_num_his
from
(select t.hour_start_time,t.error_id,bn.start_time,bn.end_time,t.stat_start_time,t.stat_end_time
from qt_smartreport.qt_hour_robot_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
where t.hour_start_time >= @now_start_time and t.hour_start_time < @next_start_time
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
	   case when t1.end_time is null or t1.end_time >= @now_next_hour_start_time then @now_next_hour_start_time else t1.end_time end as stat_end_time
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
					where t.hour_start_time >= @now_start_time and t.hour_start_time < @next_start_time )tb
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
null as robot_run_time,
null as before_theory_run_duration,
null as before_error_duration,
null as robot_error_num_his
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj
on tocj.order_id = tor.id
where tor.create_time >= @now_start_time and tor.create_time < @next_start_time
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
sum(t.theory_run_duration) as robot_run_time,
null as before_theory_run_duration,
null as before_error_duration,
null as robot_error_num_his
from
(select robot_code,hour_start_time,theory_run_duration
from qt_smartreport.qt_hour_robot_error_mtbf_his t
where t.hour_start_time >= @now_start_time and t.hour_start_time < @next_start_time 
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
where t.hour_start_time >= @now_start_time and t.hour_start_time < @next_start_time  )t
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
union all 
-- 全场机器人当天之前理论运行时长、全场机器人当天之前故障时长
select 
null as create_robot_error_num,
null as end_robot_error_num,
null as end_error_time,
null as robot_error_num,
null as robot_error_time,
null as create_order_num,
null as create_job_num,
null as robot_run_time,
sum(t.accum_theory_run_duration) as before_theory_run_duration, -- 全场机器人当天之前理论运行时长
sum(t.accum_error_duration) as before_error_duration,   -- 全场机器人当天之前故障时长
null as robot_error_num_his
from qt_smartreport.qt_day_robot_error_mtbf_his t
inner join phoenix_basic.basic_robot br on br.robot_code = t.robot_code and br.usage_state = 'using'
where t.date_value = date(date_add( @now_start_time , interval -1 day))
union all 
-- 全场机器人累计故障收敛后集合
select 
null as create_robot_error_num,
null as end_robot_error_num,
null as end_error_time,
null as robot_error_num,
null as robot_error_time,
null as create_order_num,
null as create_job_num,
null as robot_run_time,
null as before_theory_run_duration, -- 全场机器人当天之前理论运行时长
null as before_error_duration,      -- 全场机器人当天之前故障时长
count(distinct tr.error_id) as robot_error_num_his         -- 历史全场机器人故障收敛后集合
from
(select distinct t.robot_code,t.error_id  -- 机器人当前小时之前故障收敛后集合
from qt_smartreport.qt_hour_robot_error_list_his t
inner join phoenix_basic.basic_robot br on br.robot_code = t.robot_code and br.usage_state = 'using'
union
select distinct t.robot_code,t.error_id  -- 当前小时机器人故障收敛后集合
from
(select
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
	   case when t1.start_time < @now_hour_start_time then @now_hour_start_time else t1.start_time end as stat_start_time,
	   case when t1.end_time is null or t1.end_time >= @now_next_hour_start_time then @now_next_hour_start_time else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
               coalesce(end_time, sysdate()) <  @now_next_hour_start_time ) or
              (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
               coalesce(end_time, sysdate()) >=  @now_next_hour_start_time ) or
              (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_hour_start_time and
               coalesce(end_time, sysdate()) <  @now_next_hour_start_time ) or
              (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >=  @now_next_hour_start_time )
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
                              coalesce(end_time, sysdate()) <  @now_next_hour_start_time ) or
                             (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
                              coalesce(end_time, sysdate()) >=  @now_next_hour_start_time ) or
                             (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_hour_start_time and
                              coalesce(end_time, sysdate()) <  @now_next_hour_start_time ) or
                             (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_next_hour_start_time )
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
         inner join phoenix_basic.basic_robot br on br.robot_code = t.robot_code and br.usage_state = 'using')tr
)td
) t

union all 
select '各类机器人'                                                        as stat_type,
       t.robot_type_code,
       t.robot_type_name,  -- 机器人类型
       coalesce(sum(t.create_robot_error_num), 0)                          as breakdown_num, -- 故障次数
	case
     when COALESCE(SUM(t.create_robot_error_num),0)=0 then 0
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_order_num),0)=0 then concat(coalesce(sum(t.create_robot_error_num), 0), '/', '0')
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_order_num),0)>=COALESCE(SUM(t.create_robot_error_num),0) then concat('1','/',round(COALESCE(SUM(t.create_order_num),0)/COALESCE(SUM(t.create_robot_error_num),0)))
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_order_num),0)<COALESCE(SUM(t.create_robot_error_num),0) then concat(round(COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_order_num),0)),'/','1')
	 else 0 end as order_breakdown_rate, -- 故障率（搬运作业单）
    case when COALESCE(SUM(t.create_order_num),0)=0 then COALESCE(SUM(t.create_robot_error_num),0)/1 else COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_order_num),0) end AS order_breakdown_rate_sort, -- 故障率（搬运作业单）排序
       coalesce(sum(t.create_order_num), 0)                                as order_num, -- 订单量
	 case
     when COALESCE(SUM(t.create_robot_error_num),0)=0 then 0
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_job_num),0)=0 then concat(coalesce(sum(t.create_robot_error_num), 0), '/', '0')
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_job_num),0)>=COALESCE(SUM(t.create_robot_error_num),0) then concat('1','/',round(COALESCE(SUM(t.create_job_num),0)/COALESCE(SUM(t.create_robot_error_num),0)))
	 when COALESCE(SUM(t.create_robot_error_num),0)!=0 and COALESCE(SUM(t.create_job_num),0)<COALESCE(SUM(t.create_robot_error_num),0) then concat(round(COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_job_num),0)),'/','1')
	 else 0 end as carry_job_breakdown_rate, -- 故障率（搬运任务）
    case when COALESCE(SUM(t.create_job_num),0)=0 then COALESCE(SUM(t.create_robot_error_num),0)/1 else COALESCE(SUM(t.create_robot_error_num),0)/COALESCE(SUM(t.create_job_num),0) end AS carry_job_breakdown_rate_sort, -- 故障率（搬运任务）排序
       COALESCE(sum(t.create_job_num), 0)                                  as carry_job_num, -- 搬运任务数
       coalesce(sum(t.the_day_run_time), 0)                                as the_day_run_time,
       coalesce(sum(t.the_day_error_time), 0)                              as the_day_error_time,
       (coalesce(sum(t.the_day_run_time), 0) - coalesce(sum(t.the_day_error_time), 0)) /
       coalesce(sum(t.the_day_run_time), 0)                                as oee,
       COALESCE(sum(t.end_error_num), 0)                                   as end_error_num,
       coalesce(sum(t.end_error_time), 0)                                  as end_error_time,
       coalesce(sum(t.end_error_time), 0) /
       COALESCE(sum(t.end_error_num), 0)                                   as mttr,
    case when t.the_day_error_num != 0 then (COALESCE(t.the_day_run_time,0)-COALESCE(t.the_day_error_time,0))/t.the_day_error_num end as mtbf,
	case when t.robot_error_num_his != 0 then ((COALESCE(t.before_theory_run_duration,0)+COALESCE(t.the_day_run_time,0))-(COALESCE(t.before_error_duration,0)+COALESCE(t.the_day_error_time,0)))/t.robot_error_num_his end as cumul_mtbf
from 
(SELECT
br.robot_type_code,
br.robot_type_name,
COALESCE(t1.create_robot_error_num,0) as create_robot_error_num,   -- 新增故障次数
COALESCE(t2.create_order_num,0) as create_order_num,         -- 新增订单量
COALESCE(t2.create_job_num,0) as create_job_num,            -- 新人任务量
COALESCE(t1.end_robot_error_num,0) as end_error_num,     -- 结束故障次数
COALESCE(t1.end_error_time,0) as  end_error_time,           -- 结束故障时间
COALESCE(t3.robot_run_time,0) as  the_day_run_time,           -- 理论运行时长
COALESCE(t1.robot_error_time,0) as  the_day_error_time,        -- 持续故障时长
COALESCE(t1.robot_error_num,0) as  the_day_error_num,        -- 时间段内持续的故障次数
COALESCE(t4.before_theory_run_duration,0) as  before_theory_run_duration,
COALESCE(t4.before_error_duration,0) as  before_error_duration,
COALESCE(t5.robot_error_num_his,0) as robot_error_num_his 
from
(select distinct brt.robot_type_code,brt.robot_type_name
from phoenix_basic.basic_robot br
inner join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id and br.usage_state = 'using')br
--  小时内机器人新增故障次数、结束故障次数、已结束故障时长
left join
(select
tb.robot_type_code,tb.robot_type_name,
count(distinct case when tb.start_time >= @now_start_time and tb.start_time < @next_start_time then tb.error_id end) as create_robot_error_num,  
count(distinct case when tb.end_time is not null and tb.end_time >= @now_start_time and tb.end_time < @next_start_time then tb.error_id end) as end_robot_error_num,
sum(case when tb.end_time is not null and tb.end_time >= @now_start_time and tb.end_time < @next_start_time and date_format(tb.end_time, '%Y-%m-%d %H:00:00')=date_format(tb.hour_start_time, '%Y-%m-%d %H:00:00') then unix_timestamp(tb.end_time)-unix_timestamp(tb.start_time) end) as end_error_time,
count(distinct tb.error_id) as robot_error_num,
sum(unix_timestamp(stat_end_time)-unix_timestamp(stat_start_time)) as robot_error_time
from
(select brt.robot_type_code,brt.robot_type_name,t.hour_start_time,t.error_id,bn.start_time,bn.end_time,t.stat_start_time,t.stat_end_time
from qt_smartreport.qt_hour_robot_error_list_his t
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where t.hour_start_time >= @now_start_time and t.hour_start_time < @next_start_time
union
select brt.robot_type_code,brt.robot_type_name,t.hour_start_time,t.error_id,bn.start_time,bn.end_time,t.stat_start_time,t.stat_end_time
from
(select date_format( @now_hour_start_time  , '%Y-%m-%d %H:00:00') as hour_start_time,
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
	   case when t1.start_time <  @now_hour_start_time   then  @now_hour_start_time   else t1.start_time end as stat_start_time,
	   case when t1.end_time is null or t1.end_time >=  @now_next_hour_start_time   then  @now_next_hour_start_time   else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >=  @now_hour_start_time   and start_time <  @now_next_hour_start_time   and
               coalesce(end_time,  @now_time  ) <  @now_next_hour_start_time  ) or
              (start_time >=  @now_hour_start_time   and start_time <  @now_next_hour_start_time   and
               coalesce(end_time,  @now_time  ) >=  @now_next_hour_start_time  ) or
              (start_time <  @now_hour_start_time   and coalesce(end_time,  @now_time  ) >=  @now_hour_start_time   and
               coalesce(end_time,  @now_time  ) <  @now_next_hour_start_time  ) or
              (start_time <  @now_hour_start_time   and coalesce(end_time,  @now_time  ) >=  @now_next_hour_start_time  )
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >=  @now_hour_start_time   and start_time <  @now_next_hour_start_time   and
                              coalesce(end_time,  @now_time  ) <  @now_next_hour_start_time  ) or
                             (start_time >=  @now_hour_start_time   and start_time <  @now_next_hour_start_time   and
                              coalesce(end_time,  @now_time  ) >=  @now_next_hour_start_time  ) or
                             (start_time <  @now_hour_start_time   and coalesce(end_time,  @now_time  ) >=  @now_hour_start_time   and
                              coalesce(end_time,  @now_time  ) <  @now_next_hour_start_time  ) or
                             (start_time <  @now_hour_start_time   and coalesce(end_time,  @now_time  ) >=  @now_next_hour_start_time  )
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t
					left join phoenix_basic.basic_notification bn on bn.id=t.error_id
					inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
                    left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
					where t.hour_start_time >= @now_start_time and t.hour_start_time < @next_start_time  )tb
group by tb.robot_type_code,tb.robot_type_name)t1 on t1.robot_type_code=br.robot_type_code
-- 小时内机器人新增
left join
(select
brt.robot_type_code,brt.robot_type_name,
count(distinct tor.order_no)                      as create_order_num,
count(distinct tocj.job_sn)                       as create_job_num
from phoenix_rss.transport_order tor
left join phoenix_rss.transport_order_carrier_job tocj
on tocj.order_id = tor.id
inner join phoenix_basic.basic_robot br on br.robot_code=tocj.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tor.create_time >= @now_start_time and tor.create_time < @next_start_time
group by brt.robot_type_code,brt.robot_type_name)t2 on t2.robot_type_code=br.robot_type_code
-- 小时内机器人理论运行时长
left join
(select
brt.robot_type_code,brt.robot_type_name,sum(t.theory_run_duration) as robot_run_time
from
(select robot_code,hour_start_time,theory_run_duration
from qt_smartreport.qt_hour_robot_error_mtbf_his t
where t.hour_start_time >= @now_start_time and t.hour_start_time < @next_start_time 
union all
select robot_code,hour_start_time,theory_run_duration
from
(select
ts.robot_code,
date_format( @now_hour_start_time  , '%Y-%m-%d %H:00:00') as hour_start_time,
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
case when  @now_time   <  @now_next_hour_start_time   then UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time,  @now_time  )) - UNIX_TIMESTAMP( @now_hour_start_time  ) else UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time,  @now_next_hour_start_time  )) - UNIX_TIMESTAMP( @now_hour_start_time  ) end stat_state_duration
from
(select
robot_code, max(id) as before_the_hour_last_id
from phoenix_rms.robot_state_history
where create_time <  @now_hour_start_time  
group by robot_code)t1
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
left join
(select
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >=  @now_hour_start_time   and create_time <  @now_next_hour_start_time  
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
case when t5.the_hour_last_id is not null and  @now_time   >=  @now_next_hour_start_time   then UNIX_TIMESTAMP( @now_next_hour_start_time  )-UNIX_TIMESTAMP(t4.create_time)
when t5.the_hour_last_id is not null and  @now_time   <  @now_next_hour_start_time   then UNIX_TIMESTAMP( @now_time  ) - UNIX_TIMESTAMP(t4.create_time)
else t4.duration / 1000 end stat_state_duration
from
(select
*
from phoenix_rms.robot_state_history
where create_time >=  @now_hour_start_time   and create_time <  @now_next_hour_start_time  )t4
left join
(select
robot_code,
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time
from phoenix_rms.robot_state_history
where create_time >=  @now_hour_start_time   and create_time <  @now_next_hour_start_time  
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code)t
where t.hour_start_time >= @now_start_time and t.hour_start_time < @next_start_time  )t
inner join phoenix_basic.basic_robot br on br.robot_code=t.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
group by brt.robot_type_code,brt.robot_type_name)t3 on t3.robot_type_code=br.robot_type_code
-- 当天之前理论运行时长、全场机器人当天之前故障时长
left join 
(select 
brt.robot_type_code,brt.robot_type_name,
sum(t.accum_theory_run_duration) as before_theory_run_duration, -- 全场机器人当天之前理论运行时长
sum(t.accum_error_duration) as before_error_duration,   -- 全场机器人当天之前故障时长
null as robot_error_num_his
from qt_smartreport.qt_day_robot_error_mtbf_his t
inner join phoenix_basic.basic_robot br on br.robot_code = t.robot_code and br.usage_state = 'using'
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where t.date_value = date(date_add( @now_start_time , interval -1 day))
group by brt.robot_type_code,brt.robot_type_name)t4 on t4.robot_type_code=br.robot_type_code 
-- 各类机器人累计故障收敛后集合
left join 
(select 
brt.robot_type_code,brt.robot_type_name,
count(distinct tr.error_id) as robot_error_num_his         -- 历史全场机器人故障收敛后集合
from
(select distinct t.robot_code,t.error_id  -- 机器人当前小时之前故障收敛后集合
from qt_smartreport.qt_hour_robot_error_list_his t
union
select distinct t.robot_code,t.error_id  -- 当前小时机器人故障收敛后集合
from
(select
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
	   case when t1.start_time < @now_hour_start_time then @now_hour_start_time else t1.start_time end as stat_start_time,
	   case when t1.end_time is null or t1.end_time >= @now_next_hour_start_time then @now_next_hour_start_time else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
               coalesce(end_time, sysdate()) <  @now_next_hour_start_time ) or
              (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
               coalesce(end_time, sysdate()) >=  @now_next_hour_start_time ) or
              (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_hour_start_time and
               coalesce(end_time, sysdate()) <  @now_next_hour_start_time ) or
              (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >=  @now_next_hour_start_time )
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
                              coalesce(end_time, sysdate()) <  @now_next_hour_start_time ) or
                             (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
                              coalesce(end_time, sysdate()) >=  @now_next_hour_start_time ) or
                             (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_hour_start_time and
                              coalesce(end_time, sysdate()) <  @now_next_hour_start_time ) or
                             (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_next_hour_start_time )
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id)t 
		 )tr
		 inner join phoenix_basic.basic_robot br on br.robot_code = tr.robot_code and br.usage_state = 'using'
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id			 
group by brt.robot_type_code,brt.robot_type_name)t5 on t5.robot_type_code=br.robot_type_code 

) t
group by t.robot_type_code, t.robot_type_name

