-- 系统故障集合
set @now_hour_start_time = date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');
set @now_next_hour_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
select @now_hour_start_time, @now_next_hour_start_time;


select alarm_service,
       id as error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time<@now_hour_start_time then @now_hour_start_time else start_time end start_time,
	   case when COALESCE(end_time,sysdate())>=@now_next_hour_start_time then @now_next_hour_start_time else COALESCE(end_time,sysdate()) end as end_time,
	   UNIX_TIMESTAMP(case when COALESCE(end_time,sysdate())>=@now_next_hour_start_time then @now_next_hour_start_time else COALESCE(end_time,sysdate()) end)-UNIX_TIMESTAMP(case when start_time<@now_hour_start_time then @now_hour_start_time else start_time end) as the_hour_keep_suration
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
         coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
        (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
         coalesce(end_time, sysdate()) >= @now_next_hour_start_time) or
        (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_hour_start_time and
         coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
        (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_next_hour_start_time)
    )
order by alarm_service,original_start_time asc


##########################################################

-- 各模块系统故障时间段重叠时长
set @now_hour_start_time = date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');
set @now_next_hour_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
select @now_hour_start_time, @now_next_hour_start_time;


select 
date(@now_hour_start_time) as date_value,
@now_hour_start_time as hour_start_time,
@now_next_hour_start_time as next_hour_start_time,
ts.alarm_service,
3600 as sys_run_duration,
COALESCE(te.sys_error_duration,0) as sys_error_duration,
COALESCE(te.sys_error_num,0) as sys_error_num
from 
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
left join 
(select 
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as sys_error_duration,
count(distinct t.error_id) as sys_error_num
from 
(select 
alarm_service as sys_name,
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
	   case when start_time<@now_hour_start_time then @now_hour_start_time else start_time end start_time,
	   case when COALESCE(end_time,sysdate())>=@now_next_hour_start_time then @now_next_hour_start_time else COALESCE(end_time,sysdate()) end as end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
         coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
        (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
         coalesce(end_time, sysdate()) >= @now_next_hour_start_time) or
        (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_hour_start_time and
         coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
        (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_next_hour_start_time)
    )
order by alarm_service,original_start_time asc)t)t
left join (
select 
@num:=@num+1 as seq_list
from 				 
(select a.seq				 
from 				 
(SELECT 1 as seq UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )a
join 
(SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )b on 1 )t,(SELECT @num := 0) as i
) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)te on te.alarm_service=ts.alarm_service



-- 各模块系统故障时间段重叠时长（当前小时）
set @now_hour_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
set @now_next_hour_start_time = date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00');
select @now_hour_start_time, @now_next_hour_start_time;


select 
date(@now_hour_start_time) as date_value,
@now_hour_start_time as hour_start_time,
@now_next_hour_start_time as next_hour_start_time,
ts.alarm_service,
unix_timestamp(date_format(DATE_ADD(sysdate(), INTERVAL 1 SECOND), '%Y-%m-%d %H:%i:%s'))-unix_timestamp (date_format(sysdate(), '%Y-%m-%d %H:00:00'))  as sys_run_duration,
COALESCE(te.sys_error_duration,0) as sys_error_duration,
COALESCE(te.sys_error_num,0) as sys_error_num
from 
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
left join 
(select 
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as sys_error_duration,
count(distinct t.error_id) as sys_error_num
from 
(select 
alarm_service as sys_name,
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
	   case when start_time<@now_hour_start_time then @now_hour_start_time else start_time end start_time,
	   case when COALESCE(end_time,sysdate())>=@now_next_hour_start_time then @now_next_hour_start_time else COALESCE(end_time,sysdate()) end as end_time
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
         coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
        (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
         coalesce(end_time, sysdate()) >= @now_next_hour_start_time) or
        (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_hour_start_time and
         coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
        (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_next_hour_start_time)
    )
order by alarm_service,original_start_time asc)t)t
left join (
select 
@num:=@num+1 as seq_list
from 				 
(select a.seq				 
from 				 
(SELECT 1 as seq UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )a
join 
(SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )b on 1 )t,(SELECT @num := 0) as i
) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)te on te.alarm_service=ts.alarm_service





########################################################################################################################
########################################################################################################################

set @now_hour_start_time = date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');
set @now_next_hour_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
select @now_hour_start_time, @now_next_hour_start_time;


select 
date(@now_hour_start_time) as date_value,
@now_hour_start_time as hour_start_time,
@now_next_hour_start_time as next_hour_start_time,
ts.alarm_service,
3600 as sys_run_duration,
COALESCE(te.sys_error_duration,0) as sys_error_duration,
COALESCE(te.sys_error_num,0) as sys_error_num
from 
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
left join 
(select 
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as sys_error_duration,
count(distinct t.error_id) as sys_error_num
from 
(select 
alarm_service as sys_name,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when end_time=date_format(sysdate(), '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag 
from 
(select alarm_service,
       error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time<@now_hour_start_time then @now_hour_start_time else start_time end start_time,
	   case when COALESCE(end_time,sysdate())>=@now_next_hour_start_time then @now_next_hour_start_time else COALESCE(end_time,sysdate()) end as end_time
from qt_smartreport.qt_hour_sys_error_list_his
where hour_start_time = @now_hour_start_time
order by alarm_service,original_start_time asc)t)t
left join (
select 
@num:=@num+1 as seq_list
from 				 
(select a.seq				 
from 				 
(SELECT 1 as seq UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )a
join 
(SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )b on 1 )t,(SELECT @num := 0) as i
) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)te on te.alarm_service=ts.alarm_service
########################################################################################################################
########################################################################################################################
-- 计算前一个小时系统故障集合
set @now_hour_start_time = date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');
set @now_next_hour_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
select @now_hour_start_time, @now_next_hour_start_time;

select 
date(DATE_ADD(sysdate(), INTERVAL -1 HOUR)) as date_value,
date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') as hour_start_time,
date_format(sysdate(), '%Y-%m-%d %H:00:00') as next_hour_start_time,
id                                     as error_id,
error_code,
start_time,
end_time,
warning_spec,
alarm_module,
alarm_service,
alarm_type,
alarm_level,
alarm_detail,
param_value,
job_order,
robot_job,
robot_code,
device_code,
server_code,
transport_object    
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
         coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
        (start_time >= @now_hour_start_time and start_time < @now_next_hour_start_time and
         coalesce(end_time, sysdate()) >= @now_next_hour_start_time) or
        (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_hour_start_time and
         coalesce(end_time, sysdate()) < @now_next_hour_start_time) or
        (start_time < @now_hour_start_time and coalesce(end_time, sysdate()) >= @now_next_hour_start_time)
    )

########################################################################################################################
########################################################################################################################

-- 计算前一个小时的MTBF、累计MTBF 

set @now_hour_start_time = date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');
set @now_next_hour_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
select @now_hour_start_time, @now_next_hour_start_time;


select 
date(@now_hour_start_time) as date_value,
@now_hour_start_time as hour_start_time,
@now_next_hour_start_time as next_hour_start_time,
ts.alarm_service,
3600 as theory_run_duration,
COALESCE(te.error_duration,0) as error_duration,
COALESCE(te.error_num,0) as error_num,
case when COALESCE(te.error_num,0) != 0 then (3600 - COALESCE(te.error_duration,0)) / COALESCE(te.error_num,0) else null end as mtbf,
COALESCE(t2.accum_theory_run_duration,0) + 3600 as accum_theory_run_duration,
COALESCE(t2.accum_error_duration,0)+COALESCE(te.error_duration,0) as accum_error_duration,
COALESCE(t1.accum_error_num,0) as accum_error_num,
case when COALESCE(t1.accum_error_num,0) != 0 then ((COALESCE(t2.accum_theory_run_duration,0) + 3600)-(COALESCE(t2.accum_error_duration,0)+COALESCE(te.error_duration,0)))/COALESCE(t1.accum_error_num,0) else null end as accum_mtbf
from 
-- 参与计算的所有系统
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
-- 各系统小时内故障时长、故障次数
left join 
(select 
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as error_duration,
count(distinct t.error_id) as error_num
from 
(select 
alarm_service as sys_name,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when end_time=date_format(sysdate(), '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag 
from 
(select alarm_service,
       error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time<@now_hour_start_time then @now_hour_start_time else start_time end start_time,
	   case when COALESCE(end_time,sysdate())>=@now_next_hour_start_time then @now_next_hour_start_time else COALESCE(end_time,sysdate()) end as end_time
from qt_smartreport.qt_hour_sys_error_list_his
where hour_start_time = @now_hour_start_time
order by alarm_service,original_start_time asc)t)t
left join (
select 
@num:=@num+1 as seq_list
from 				 
(select a.seq				 
from 				 
(SELECT 1 as seq UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )a
join 
(SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )b on 1 )t,(SELECT @num := 0) as i
) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)te on te.alarm_service=ts.alarm_service
-- 各系统累计故障次数
left join 
(select 
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct error_id) as accum_error_num
FROM qt_smartreport.qt_hour_sys_error_list_his
where hour_start_time<=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00')
group by alarm_service
WITH ROLLUP)t1 on t1.alarm_service_name=ts.alarm_service
-- 各系统累计故障时长 
left join 
(select alarm_service,accum_theory_run_duration,accum_error_duration 
from qt_smartreport.qt_hour_sys_error_mtbf_his
where hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -2 HOUR), '%Y-%m-%d %H:00:00'))t2 on t2.alarm_service=ts.alarm_service




########################################################################################################################
########################################################################################################################





########################################################################################################################
########################################################################################################################


# step1:建表（qt_hour_sys_error_list_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_hour_sys_error_list_his
(
    `id`                   bigint(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`           date         NOT NULL COMMENT '日期',
    `hour_start_time`      datetime     NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time` datetime     NOT NULL COMMENT '下一个小时开始时间',
    `error_id`             bigint(20)   NOT NULL COMMENT '故障通知ID',
    `error_code`           varchar(255) NOT NULL COMMENT '故障码',
    `start_time`           datetime(6)           DEFAULT NULL COMMENT '开始时间-告警触发时间',
    `end_time`             datetime(6)           DEFAULT NULL COMMENT '结束时间-告警结束时间',
    `warning_spec`         varchar(255)          DEFAULT NULL COMMENT '故障分类',
    `alarm_module`         varchar(255)          DEFAULT NULL COMMENT '告警模块-外设、系统、服务、机器人',
    `alarm_service`        varchar(255)          DEFAULT NULL COMMENT '告警服务',
    `alarm_type`           varchar(255)          DEFAULT NULL COMMENT '告警对象类型',
    `alarm_level`          int(11)               DEFAULT NULL COMMENT '告警级别',
    `alarm_detail`         varchar(255)          DEFAULT NULL COMMENT '故障详情',
    `param_value`          varchar(255)          DEFAULT NULL COMMENT '参数值',
    `job_order`            varchar(255)          DEFAULT NULL COMMENT '关联作业单',
    `robot_job`            varchar(255)          DEFAULT NULL COMMENT '关联机器人任务',
    `robot_code`           varchar(255)          DEFAULT NULL COMMENT '关联机器人编号',
    `device_code`          varchar(255)          DEFAULT NULL COMMENT '关联设备编码',
    `server_code`          varchar(255)          DEFAULT NULL COMMENT '关联服务器',
    `transport_object`     varchar(255)          DEFAULT NULL COMMENT '关联搬运对象',
    `created_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`         timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_hour_start_time (`hour_start_time`),
    key idx_next_hour_start_time (`next_hour_start_time`),
    key idx_error_id (`error_id`),
    key idx_error_code (`error_code`),
    key idx_start_time (`start_time`),
    key idx_end_time (`end_time`),
    key idx_alarm_service (`alarm_service`),
    key idx_warning_spec (`warning_spec`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='系统小时维度故障集合（H+1）';
			


# step2:删除相关数据（qt_hour_sys_error_list_his）
DELETE
FROM qt_smartreport.qt_hour_sys_error_list_his
where hour_start_time = date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');	




# step3:插入相关数据（qt_hour_sys_error_list_his）
insert into qt_smartreport.qt_hour_sys_error_list_his(date_value,hour_start_time,next_hour_start_time, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object)
select 
date(DATE_ADD(sysdate(), INTERVAL -1 HOUR)) as date_value,
date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') as hour_start_time,
date_format(sysdate(), '%Y-%m-%d %H:00:00') as next_hour_start_time,
id                                     as error_id,
error_code,
start_time,
end_time,
warning_spec,
alarm_module,
alarm_service,
alarm_type,
alarm_level,
alarm_detail,
param_value,
job_order,
robot_job,
robot_code,
device_code,
server_code,
transport_object    
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and start_time < date_format(sysdate(), '%Y-%m-%d %H:00:00') and
         coalesce(end_time, sysdate()) < date_format(sysdate(), '%Y-%m-%d %H:00:00')) or
        (start_time >= date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and start_time < date_format(sysdate(), '%Y-%m-%d %H:00:00') and
         coalesce(end_time, sysdate()) >= date_format(sysdate(), '%Y-%m-%d %H:00:00')) or
        (start_time < date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and coalesce(end_time, sysdate()) >= date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and
         coalesce(end_time, sysdate()) < date_format(sysdate(), '%Y-%m-%d %H:00:00')) or
        (start_time < date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and coalesce(end_time, sysdate()) >= date_format(sysdate(), '%Y-%m-%d %H:00:00'))
    )
	
	
								
				
# step4:建表（qt_hour_sys_error_mtbf_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_hour_sys_error_mtbf_his
(
    `id`                        bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`                date       NOT NULL COMMENT '日期',
    `hour_start_time`           datetime   NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time`      datetime   NOT NULL COMMENT '下一个小时开始时间',
    `alarm_service`        varchar(255)             NOT NULL COMMENT '告警服务',
    `theory_run_duration`       decimal(65, 30)     DEFAULT NULL COMMENT '在该小时内理论运行时长（秒）',
    `error_duration`            decimal(65, 30)     DEFAULT NULL COMMENT '在该小时内故障时长（秒）',
    `error_num`                 bigint(10)          DEFAULT NULL COMMENT '在该小时内参与计算的故障数',
    `mtbf`                      decimal(65, 30)     DEFAULT NULL COMMENT 'mtbf（秒）',
    `accum_theory_run_duration` decimal(65, 30)     DEFAULT NULL COMMENT '累计理论运行时长（秒）',
    `accum_error_duration`      decimal(65, 30)     DEFAULT NULL COMMENT '累计故障时长（秒）',
    `accum_error_num`           bigint(10)          DEFAULT NULL COMMENT '累计参与计算的故障数',
    `accum_mtbf`                decimal(65, 30)     DEFAULT NULL COMMENT '累计mtbf（秒）',
    `created_time`              timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`              timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_hour_start_time (`hour_start_time`),
    key idx_next_hour_start_time (`next_hour_start_time`),
    key idx_alarm_service (`alarm_service`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='系统小时维度mtbf（H+1）';
	
	

# step5:删除相关数据（qt_hour_sys_error_mtbf_his）
DELETE
FROM qt_smartreport.qt_hour_sys_error_mtbf_his
where hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');	


					
					
# step6:插入相关数据（qt_hour_sys_error_mtbf_his）
insert into qt_smartreport.qt_hour_sys_error_mtbf_his(date_value,hour_start_time,next_hour_start_time,alarm_service,theory_run_duration,error_duration,error_num,mtbf,accum_theory_run_duration,accum_error_duration,accum_error_num,accum_mtbf)	
select 
date(DATE_ADD(sysdate(), INTERVAL -1 HOUR)) as date_value,
date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') as hour_start_time,
date_format(sysdate(), '%Y-%m-%d %H:00:00') as next_hour_start_time,
ts.alarm_service,
3600 as theory_run_duration,
COALESCE(te.error_duration,0) as error_duration,
COALESCE(te.error_num,0) as error_num,
case when COALESCE(te.error_num,0) != 0 then (3600 - COALESCE(te.error_duration,0)) / COALESCE(te.error_num,0) else null end as mtbf,
COALESCE(t2.accum_theory_run_duration,0) + 3600 as accum_theory_run_duration,
COALESCE(t2.accum_error_duration,0)+COALESCE(te.error_duration,0) as accum_error_duration,
COALESCE(t1.accum_error_num,0) as accum_error_num,
case when COALESCE(t1.accum_error_num,0) != 0 then ((COALESCE(t2.accum_theory_run_duration,0) + 3600)-(COALESCE(t2.accum_error_duration,0)+COALESCE(te.error_duration,0)))/COALESCE(t1.accum_error_num,0) else null end as accum_mtbf
from 
-- 参与计算的所有系统
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
-- 各系统小时内故障时长、故障次数
left join 
(select 
COALESCE(sys_name,'ALL_SYS') as alarm_service,
count(distinct se.seq_list) as error_duration,
count(distinct t.error_id) as error_num
from 
(select 
alarm_service as sys_name,
error_id,
original_start_time,
original_end_time,
start_time,
end_time,
cast(substr(start_time,15,2) as UNSIGNED)*60+cast(substr(start_time,18,2) as UNSIGNED)+1 as start_seq_lag,
case when end_time=date_format(sysdate(), '%Y-%m-%d %H:00:00') then 3600 else cast(substr(end_time,15,2) as UNSIGNED)*60+cast(substr(end_time,18,2) as UNSIGNED)+1 end as end_seq_lag 
from 
(select alarm_service,
       error_id,
       start_time as original_start_time,
       end_time as original_end_time,
	   case when start_time<date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') then date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') else start_time end start_time,
	   case when COALESCE(end_time,sysdate()) >= date_format(sysdate(), '%Y-%m-%d %H:00:00') then date_format(sysdate(), '%Y-%m-%d %H:00:00') else COALESCE(end_time,sysdate()) end as end_time
from qt_smartreport.qt_hour_sys_error_list_his
where hour_start_time = date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00')
order by alarm_service,original_start_time asc)t)t
left join (
select 
@num:=@num+1 as seq_list
from 				 
(select a.seq				 
from 				 
(SELECT 1 as seq UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )a
join 
(SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49 UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54 UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59 UNION SELECT 60 )b on 1 )t,(SELECT @num := 0) as i
) se on se.seq_list>=t.start_seq_lag and  se.seq_list<=t.end_seq_lag
group by sys_name
WITH ROLLUP)te on te.alarm_service=ts.alarm_service
-- 各系统累计故障次数
left join 
(select 
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct error_id) as accum_error_num
FROM qt_smartreport.qt_hour_sys_error_list_his
where hour_start_time<=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00')
group by alarm_service
WITH ROLLUP)t1 on t1.alarm_service_name=ts.alarm_service
-- 各系统累计故障时长 
left join 
(select alarm_service,accum_theory_run_duration,accum_error_duration 
from qt_smartreport.qt_hour_sys_error_mtbf_his
where hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -2 HOUR), '%Y-%m-%d %H:00:00'))t2 on t2.alarm_service=ts.alarm_service