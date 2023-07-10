
-- 半小时内自恢复成功的故障

set @from_dttm='2023-01-01 00:00:00';
set @to_dttm='2023-02-11 00:00:00';


select distinct bn.id as error_id,bn.robot_code,bn.error_code,bn.start_time   
from 
(select id,robot_code,error_code,start_time   
from phoenix_basic.basic_notification
where alarm_module ='robot' and alarm_level >= 3
AND  start_time >= @from_dttm
and start_time< @to_dttm
)bn 
inner join 
-- 机器人自恢复成功的故障list
(select 
robot_code,
`method`,
error_codes,
`result`,
start_time,
end_time
from phoenix_rms.robot_recovery_record 
where `method` ='自恢复' and `result`=1
AND  start_time >= @from_dttm
and start_time< @to_dttm
AND start_time<=from_unixtime(UNIX_TIMESTAMP('2023-02-08 00:00:00')+30*60)
)tsh on tsh.robot_code =bn.robot_code and tsh.error_codes=bn.error_code  and tsh.start_time >=bn.start_time and UNIX_TIMESTAMP(tsh.start_time)-UNIX_TIMESTAMP(bn.start_time)<=30*60



-----------------------------------------------------------

-- 自恢复成功的故障但是在

set @from_dttm='2023-02-01 00:00:00';
set @to_dttm='2023-02-11 00:00:00';


select 
t1.*
from 
(select 
robot_code,
`method`,
error_codes,
`result`,
start_time,
end_time
from phoenix_rms.robot_recovery_record 
where `method` ='自恢复' and `result`=1
AND  start_time >= @from_dttm
and start_time< @to_dttm)
)t1 
left join 
(
select distinct bn.id as error_id,bn.robot_code,bn.error_code,bn.start_time   
from 
(select id,robot_code,error_code,start_time   
from phoenix_basic.basic_notification
where alarm_module ='robot' and alarm_level >= 3
AND  start_time >= @from_dttm
and start_time< @to_dttm
)bn 
inner join 
-- 机器人自恢复成功的故障list
(select 
robot_code,
`method`,
error_codes,
`result`,
start_time,
end_time
from phoenix_rms.robot_recovery_record 
where `method` ='自恢复' and `result`=1
AND  start_time >= @from_dttm
and start_time< @to_dttm
AND start_time<=from_unixtime(UNIX_TIMESTAMP('2023-02-08 00:00:00')+30*60)
)tsh on tsh.robot_code =bn.robot_code and tsh.error_codes=bn.error_code  and tsh.start_time >=bn.start_time and UNIX_TIMESTAMP(tsh.start_time)-UNIX_TIMESTAMP(bn.start_time)<=30*60
)t2 on t2.robot_code=t1.robot_code and t2.error_code=t1.error_codes
where t2.robot_code is null 


--------------------------------------

select 
t1.*
from 
(select *
from phoenix_rms.robot_recovery_record
where `method` ='自恢复' and `result`=1
and start_time >='2023-02-10 00:00:00')t1 
left join 
(select *
from phoenix_basic.basic_notification
where alarm_module ='robot'
and start_time >='2023-02-10 00:00:00')t2 on t2.robot_code=t1.robot_code and t2.error_code=t1.error_codes and UNIX_TIMESTAMP(t1.start_time)-UNIX_TIMESTAMP(t2.start_time)<=30*60
where t2.error_code is null 



-----------------------

select 
bei.error_code ,
bei.alarm_name, 
count(distinct bn.id),
count(distinct case when bn.robot_job is null then bn.id end) no_job_error,
count(distinct case when bn.robot_job is not null then bn.id end) have_job_error
from phoenix_basic.basic_notification bn 
left join phoenix_basic.basic_error_info bei on bei.error_code =bn.error_code 
where bn.alarm_module ='robot' 
and bn.alarm_service='DSP'
and bn.start_time >='2023-02-01 00:00:00'
group by 1,2
order by have_job_error desc


-- 2月前10天DSP类带job的故障告警
select 
count(distinct bn.id),
count(distinct case when bn.robot_job is null then bn.id end) no_job_error,
count(distinct case when bn.robot_job is not null then bn.id end) have_job_error
from phoenix_basic.basic_notification bn 
left join phoenix_basic.basic_error_info bei on bei.error_code =bn.error_code 
where bn.alarm_module ='robot' 
and bn.alarm_service='DSP'
and bn.start_time >='2023-02-01 00:00:00'
and bn.start_time <'2023-02-11 00:00:00'



select 
bn.alarm_service,
count(distinct bn.id),
count(distinct case when bn.robot_job is null then bn.id end) no_job_error,
count(distinct case when bn.robot_job is not null then bn.id end) have_job_error
from phoenix_basic.basic_notification bn 
left join phoenix_basic.basic_error_info bei on bei.error_code =bn.error_code 
where bn.alarm_module ='robot' 
-- and bn.alarm_service='DSP'
and bn.start_time >='2023-02-01 00:00:00'
and bn.start_time <'2023-02-11 00:00:00'
group by 1