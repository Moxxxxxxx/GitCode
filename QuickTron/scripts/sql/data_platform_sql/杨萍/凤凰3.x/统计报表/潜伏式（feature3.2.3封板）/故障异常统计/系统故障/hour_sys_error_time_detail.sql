select 
current_date() as date_value,
date_format(sysdate(), '%Y-%m-%d %H:00:00') as hour_start_time,
date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00') as next_hour_start_time,
ts.alarm_service,
unix_timestamp(date_format(DATE_ADD(sysdate(), INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s'))-unix_timestamp (date_format(sysdate(), '%Y-%m-%d %H:00:00'))  as sys_run_duration,
COALESCE(te.sys_error_duration,0) as the_hour_cost_seconds,
COALESCE(te.sys_error_num,0) as sys_error_num
from 
(select 
module as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module)ts
left join 
(select 
alarm_service,
count(distinct se.seq_list) as sys_error_duration,
count(distinct t.error_id) as sys_error_num
from 
(select 
alarm_service,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when end_time=date_format(sysdate(), '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag 
from 
(select alarm_service,
       id as error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time<date_format(sysdate(), '%Y-%m-%d %H:00:00') then date_format(sysdate(), '%Y-%m-%d %H:00:00') else start_time end start_time,
	   case when COALESCE(end_time,sysdate())>=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00') then date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00') else COALESCE(end_time,sysdate()) end as end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= date_format(sysdate(), '%Y-%m-%d %H:00:00') and start_time < date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00') and
         coalesce(end_time, sysdate()) < date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00')) or
        (start_time >= date_format(sysdate(), '%Y-%m-%d %H:00:00') and start_time < date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00') and
         coalesce(end_time, sysdate()) >= date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00')) or
        (start_time < date_format(sysdate(), '%Y-%m-%d %H:00:00') and coalesce(end_time, sysdate()) >= date_format(sysdate(), '%Y-%m-%d %H:00:00') and
         coalesce(end_time, sysdate()) < date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00')) or
        (start_time < date_format(sysdate(), '%Y-%m-%d %H:00:00') and coalesce(end_time, sysdate()) >= date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'))
    )
order by alarm_service,original_start_time asc)t)t
left join (
select 
@num:=@num+1 as seq_list
from qt_smartreport.qt_dim_hour_seconds_sequence t,(SELECT @num := 0) as i
) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by alarm_service
)te on te.alarm_service=ts.alarm_service