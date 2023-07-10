--  MTBF = (sum(theory_run_duration)-sum(error_duration))/sum(error_num)
-- 累计MTBF = (sum(accum_theory_run_duration)-sum(accum_error_duration))/sum(accum_error_num)


-- part1: 当周之前的各周数据
select
'week' as stat_time_type,
date_format(date_value, '%Y-%m-%d 00:00:00') as stat_time_value,
alarm_service,
theory_run_duration,   -- 理论运行时长
error_duration,        -- 故障时长
error_num,             -- 故障次数
accum_theory_run_duration,  --  累计理论运行时长
accum_error_duration,      -- 累计故障时长
accum_error_num             -- 累计故障次数
from qt_smartreport.qt_week_sys_error_mtbf_his
where date_value BETWEEN   {start_time}  AND   {end_time} 

-- part2: 当周数据
union all
select
'week' as stat_time_type,
date_format(date_value, '%Y-%m-%d 00:00:00') as stat_time_value,
alarm_service,
theory_run_duration,   -- 理论运行时长
error_duration,        -- 故障时长
error_num,             -- 故障次数
accum_theory_run_duration,  --  累计理论运行时长
accum_error_duration,      -- 累计故障时长
accum_error_num             -- 累计故障次数
from
(select
DATE_SUB(current_date() ,INTERVAL WEEKDAY(current_date()) + 0 DAY) as date_value,
ts.alarm_service,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t1.error_duration,0) as error_duration,
COALESCE(t3.error_num,0) as error_num,
COALESCE(t2.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t2.accum_error_duration,0)+COALESCE(t1.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num
from
(select
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
-- part1:当周的系统理论运行时长、故障时长（去重）
left join
(select
alarm_service,
sum(theory_run_duration) as theory_run_duration,
sum(error_duration) as error_duration
from
(select
date_value,
hour_start_time,
alarm_service,
theory_run_duration,
error_duration
from qt_smartreport.qt_hour_sys_error_mtbf_his
where hour_start_time >=  {now_week_start_time}  and hour_start_time <  {now_next_week_start_time} 
union all
select
date( {now_hour_start_time} ) as date_value,
date_format( {now_hour_start_time} , '%Y-%m-%d %H:00:00') as hour_start_time,
ts.alarm_service,
unix_timestamp(date_format(DATE_ADD( {now_time}  , INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s'))-unix_timestamp (date_format( {now_time}  , '%Y-%m-%d %H:00:00'))  as theory_run_duration,
COALESCE(t1.sys_error_duration,0) as error_duration
from
(select
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
--  当前小时参与计算的系统故障数、故障时长（去重）
left join
(select
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as sys_error_duration
from
(select
alarm_service as sys_name,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when end_time=date_format( {now_time}  , '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag
from
(select alarm_service,
       id as error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time< {now_hour_start_time}  then  {now_hour_start_time}  else start_time end start_time,
	   case when COALESCE(end_time, {now_time}  )>= {now_next_hour_start_time}  then  {now_next_hour_start_time}  else COALESCE(end_time, {now_time}   ) end as end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >=  {now_hour_start_time}  and start_time <  {now_next_hour_start_time}  and
         coalesce(end_time,  {now_time}  ) <  {now_next_hour_start_time} ) or
        (start_time >=  {now_hour_start_time}  and start_time <  {now_next_hour_start_time}  and
         coalesce(end_time,  {now_time}  ) >=  {now_next_hour_start_time} ) or
        (start_time <  {now_hour_start_time}  and coalesce(end_time,  {now_time}  ) >=  {now_hour_start_time}  and
         coalesce(end_time,  {now_time}  ) <  {now_next_hour_start_time} ) or
        (start_time <  {now_hour_start_time}  and coalesce(end_time,  {now_time}  ) >=  {now_next_hour_start_time} )
    )
order by alarm_service,original_start_time asc)t)t
left join
-- 一个小时3600秒序列
(select
@num:=@num+1 as seq_list
from qt_smartreport.qt_dim_hour_seconds_sequence t,(SELECT @num := 0) as i
) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)t1 on t1.alarm_service=ts.alarm_service)t
group by alarm_service)t1 on t1.alarm_service=ts.alarm_service
-- part2: 前一周的累计理论运行时长、累计故障时长（去重）
left join
(select
alarm_service,accum_theory_run_duration,accum_error_duration
from qt_smartreport.qt_week_sys_error_mtbf_his
where date_value= DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) +7 DAY))t2 on t2.alarm_service=ts.alarm_service
-- part3: 当周参与计算的故障数、累计参与计算的故障数
left join
(select
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct case when date_value=DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) +0 DAY) then error_id end) as error_num,
count(distinct error_id) as accum_error_num
from
(select
date_value,alarm_service ,error_id
from qt_smartreport.qt_week_sys_error_list_his
union all
select distinct DATE_SUB(date_value,INTERVAL WEEKDAY(date_value) + 0 DAY) as date_value,alarm_service ,error_id
from qt_smartreport.qt_hour_sys_error_list_his
where hour_start_time >=  {now_week_start_time}  and hour_start_time <  {now_next_week_start_time} 
union all
select date(DATE_SUB( {now_hour_start_time} ,INTERVAL WEEKDAY( {now_hour_start_time} ) + 0 DAY)) as date_value,alarm_service ,id as error_id
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >=  {now_hour_start_time}  and start_time <  {now_next_hour_start_time}  and
         coalesce(end_time,  {now_time}  ) <  {now_next_hour_start_time} ) or
        (start_time >=  {now_hour_start_time}  and start_time <  {now_next_hour_start_time}  and
         coalesce(end_time,  {now_time}  ) >=  {now_next_hour_start_time} ) or
        (start_time <  {now_hour_start_time}  and coalesce(end_time,  {now_time}  ) >=  {now_hour_start_time}  and
         coalesce(end_time,  {now_time}  ) <  {now_next_hour_start_time} ) or
        (start_time <  {now_hour_start_time}  and coalesce(end_time,  {now_time}  ) >=  {now_next_hour_start_time} )
    )
)te
group by alarm_service
WITH ROLLUP
)t3 on t3.alarm_service_name=ts.alarm_service)t
where t.date_value BETWEEN   {start_time}  AND   {end_time} 





######################################################################################################################################
---  检查
######################################################################################################################################

--  MTBF = (sum(theory_run_duration)-sum(error_duration))/sum(error_num)
-- 累计MTBF = (sum(accum_theory_run_duration)-sum(accum_error_duration))/sum(accum_error_num)


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


-- part1: 当周之前的各周数据
select 
'week' as stat_time_type,
date_format(date_value, '%Y-%m-%d 00:00:00') as stat_time_value,
alarm_service,  
theory_run_duration,   -- 理论运行时长
error_duration,        -- 故障时长
error_num,             -- 故障次数
accum_theory_run_duration,  --  累计理论运行时长
accum_error_duration,      -- 累计故障时长
accum_error_num             -- 累计故障次数
from qt_smartreport.qt_week_sys_error_mtbf_his
where date_value BETWEEN  @start_time AND  @end_time

-- part2: 当周数据 
union all 
select 
'week' as stat_time_type,
date_format(date_value, '%Y-%m-%d 00:00:00') as stat_time_value,
alarm_service,  
theory_run_duration,   -- 理论运行时长
error_duration,        -- 故障时长
error_num,             -- 故障次数
accum_theory_run_duration,  --  累计理论运行时长
accum_error_duration,      -- 累计故障时长
accum_error_num             -- 累计故障次数
from 
(select 
DATE_SUB(current_date() ,INTERVAL WEEKDAY(current_date()) + 0 DAY) as date_value,
ts.alarm_service,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t1.error_duration,0) as error_duration,
COALESCE(t3.error_num,0) as error_num,
COALESCE(t2.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t2.accum_error_duration,0)+COALESCE(t1.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num
from 
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
-- part1:当周的系统理论运行时长、故障时长（去重）
left join 
(select 
alarm_service,
sum(theory_run_duration) as theory_run_duration,
sum(error_duration) as error_duration
from 
(select 
date_value,
hour_start_time,
alarm_service,
theory_run_duration,
error_duration
from qt_smartreport.qt_hour_sys_error_mtbf_his
where hour_start_time >= @now_week_start_time and hour_start_time < @now_next_week_start_time
union all 
select 
date(@now_hour_start_time) as date_value,
date_format(@now_hour_start_time, '%Y-%m-%d %H:00:00') as hour_start_time,
ts.alarm_service,
unix_timestamp(date_format(DATE_ADD(@now_time , INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s'))-unix_timestamp (date_format(@now_time , '%Y-%m-%d %H:00:00'))  as theory_run_duration,
COALESCE(t1.sys_error_duration,0) as error_duration
from 
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
--  当前小时参与计算的系统故障数、故障时长（去重）
left join 
(select 
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as sys_error_duration
from 
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
WITH ROLLUP)t1 on t1.alarm_service=ts.alarm_service)t 
group by alarm_service)t1 on t1.alarm_service=ts.alarm_service
-- part2: 前一周的累计理论运行时长、累计故障时长（去重）
left join 
(select 
alarm_service,accum_theory_run_duration,accum_error_duration  
from qt_smartreport.qt_week_sys_error_mtbf_his
where date_value= DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) +7 DAY))t2 on t2.alarm_service=ts.alarm_service
-- part3: 当周参与计算的故障数、累计参与计算的故障数
left join 
(select 
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct case when date_value=DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) +0 DAY) then error_id end) as error_num,
count(distinct error_id) as accum_error_num
from 
(select 
date_value,alarm_service ,error_id
from qt_smartreport.qt_week_sys_error_list_his
union all 
select distinct DATE_SUB(date_value,INTERVAL WEEKDAY(date_value) + 0 DAY) as date_value,alarm_service ,error_id
from qt_smartreport.qt_hour_sys_error_list_his
where hour_start_time >= @now_week_start_time and hour_start_time < @now_next_week_start_time
union all 
select date(DATE_SUB(@now_hour_start_time,INTERVAL WEEKDAY(@now_hour_start_time) + 0 DAY)) as date_value,alarm_service ,id as error_id
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
)te 	
group by alarm_service
WITH ROLLUP
)t3 on t3.alarm_service_name=ts.alarm_service)t 
where t.date_value BETWEEN  @start_time AND  @end_time

