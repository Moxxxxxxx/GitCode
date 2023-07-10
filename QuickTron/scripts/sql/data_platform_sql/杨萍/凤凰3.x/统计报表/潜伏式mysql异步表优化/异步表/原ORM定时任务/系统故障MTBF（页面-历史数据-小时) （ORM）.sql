-- 表1：qt_smartreport.qtr_hour_sys_error_list_his


-- step1:删除相关数据（qtr_hour_sys_error_list_his）
DELETE
FROM qt_smartreport.qtr_hour_sys_error_list_his
where hour_start_time = date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');


-- step2:插入相关数据（qtr_hour_sys_error_list_his）
insert into qt_smartreport.qtr_hour_sys_error_list_his(create_time,update_time,date_value,hour_start_time,next_hour_start_time, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
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




-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_hour_sys_error_list_his;
insert into qt_smartreport.qtr_hour_sys_error_list_his(create_time,update_time,date_value,hour_start_time,next_hour_start_time, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object)
select created_time as create_time,updated_time as update_time,date_value,hour_start_time,next_hour_start_time, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object
from qt_smartreport.qt_hour_sys_error_list_his;	




--------------------------------------------------------------------------------
-- 表2：qt_smartreport.qtr_hour_sys_error_mtbf_his

-- step1:删除相关数据（qtr_hour_sys_error_mtbf_his）
DELETE
FROM qt_smartreport.qtr_hour_sys_error_mtbf_his
where hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');	


-- step2:插入相关数据（qtr_hour_sys_error_mtbf_his）
insert into qt_smartreport.qtr_hour_sys_error_mtbf_his(create_time,update_time,date_value,hour_start_time,next_hour_start_time,alarm_service,theory_run_duration,error_duration,error_num,mtbf,accum_theory_run_duration,accum_error_duration,accum_error_num,accum_mtbf)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
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
from qt_smartreport.qtr_hour_sys_error_list_his
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
FROM qt_smartreport.qtr_hour_sys_error_list_his
where hour_start_time<=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00')
group by alarm_service
WITH ROLLUP)t1 on t1.alarm_service_name=ts.alarm_service
-- 各系统累计故障时长 
left join 
(select alarm_service,accum_theory_run_duration,accum_error_duration 
from qt_smartreport.qtr_hour_sys_error_mtbf_his
where hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -2 HOUR), '%Y-%m-%d %H:00:00'))t2 on t2.alarm_service=ts.alarm_service



-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_hour_sys_error_mtbf_his;
insert into qt_smartreport.qtr_hour_sys_error_mtbf_his(create_time,update_time,date_value,hour_start_time,next_hour_start_time,alarm_service,theory_run_duration,error_duration,error_num,mtbf,accum_theory_run_duration,accum_error_duration,accum_error_num,accum_mtbf)
select created_time as create_time,updated_time as update_time,date_value,hour_start_time,next_hour_start_time,alarm_service,theory_run_duration,error_duration,error_num,mtbf,accum_theory_run_duration,accum_error_duration,accum_error_num,accum_mtbf
from qt_smartreport.qt_hour_sys_error_mtbf_his;
