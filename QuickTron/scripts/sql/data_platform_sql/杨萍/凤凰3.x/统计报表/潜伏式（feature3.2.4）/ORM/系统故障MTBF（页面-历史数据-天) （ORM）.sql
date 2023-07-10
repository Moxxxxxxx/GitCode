-- 表1：qt_smartreport.qtr_day_sys_error_list_his


-- step1:删除相关数据（qtr_day_sys_error_list_his）
DELETE
FROM qt_smartreport.qtr_day_sys_error_list_his
where date_value = date_add(CURRENT_DATE(), interval -1 day);


-- step2:插入相关数据（qtr_day_sys_error_list_his）
insert into qt_smartreport.qtr_day_sys_error_list_his(create_time,update_time,date_value, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object)
select 
distinct    -- 一定要记得对之前小时维度的故障集合去重
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
date_add(CURRENT_DATE(), interval -1 day) as date_value,
t.error_id,
bn.error_code,
bn.start_time,
bn.end_time,
bn.warning_spec,
bn.alarm_module,
bn.alarm_service,
bn.alarm_type,
bn.alarm_level,
bn.alarm_detail,
bn.param_value,
bn.job_order,
bn.robot_job,
bn.robot_code,
bn.device_code,
bn.server_code,
bn.transport_object 
from qt_smartreport.qtr_hour_sys_error_list_his t 
left join phoenix_basic.basic_notification bn on bn.id=t.error_id
where t.date_value=date_add(current_date(), interval -1 day)



-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_day_sys_error_list_his;
insert into qt_smartreport.qtr_day_sys_error_list_his(create_time,update_time,date_value, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object)
select created_time as create_time,updated_time as update_time,date_value, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object
from qt_smartreport.qt_day_sys_error_list_his;	



--------------------------------------------------------------------------------
-- 表2：qt_smartreport.qtr_day_sys_error_mtbf_his

-- step1:删除相关数据（qtr_day_sys_error_mtbf_his）
DELETE
FROM qt_smartreport.qtr_day_sys_error_mtbf_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);



-- step2:插入相关数据（qtr_day_sys_error_mtbf_his）
insert into qt_smartreport.qtr_day_sys_error_mtbf_his(create_time,update_time,date_value,alarm_service,theory_run_duration,error_duration,error_num,mtbf,accum_theory_run_duration,accum_error_duration,accum_error_num,accum_mtbf)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
date_add(CURRENT_DATE(), interval -1 day) as date_value,
ts.alarm_service,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t1.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
case when COALESCE(t2.error_num,0) != 0 then (COALESCE(t1.theory_run_duration,0)-COALESCE(t1.error_duration,0))/t2.error_num else null end as mtbf,
COALESCE(t4.accum_theory_run_duration,0) + COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0) + COALESCE(t1.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num,
case when COALESCE(t3.accum_error_num,0) != 0 then ((COALESCE(t4.accum_theory_run_duration,0) + COALESCE(t1.theory_run_duration,0))-(COALESCE(t4.accum_error_duration,0) + COALESCE(t1.error_duration,0)))/t3.accum_error_num else null end as accum_mtbf
from 
-- 参与计算的所有系统
(select 
COALESCE(module,'ALL_SYS') as alarm_service
from phoenix_basic.basic_error_info
where alarm_module in ('system', 'server')
group by module
WITH rollup)ts
-- 各系统前一天理论运行时长、故障时长（时间段上去重）
left join 
(select 
date_value,
alarm_service,
sum(theory_run_duration) as theory_run_duration,
sum(error_duration) as  error_duration
from qt_smartreport.qtr_hour_sys_error_mtbf_his
where date_value=date_add(current_date(), interval -1 day)
group by date_value,alarm_service)t1 on t1.alarm_service=ts.alarm_service
-- 各系统前一天参与计算的故障数
left join 
(select 
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct error_id) as error_num
from qt_smartreport.qtr_day_sys_error_list_his
where date_value=date_add(current_date(), interval -1 day)
group by alarm_service)t2 on t2.alarm_service_name=ts.alarm_service
-- 各系统历史累计参与计算的故障数
left join 
(select 
COALESCE(alarm_service,'ALL_SYS') as alarm_service_name,
count(distinct error_id) as accum_error_num
from qt_smartreport.qtr_day_sys_error_list_his
where date_value<=date_add(current_date(), interval -1 day)
group by alarm_service)t3 on t3.alarm_service_name=ts.alarm_service
-- 各系统前前一天累计理论运行时长、累计故障时长（时间段上去重）
left join  
(select 
alarm_service,accum_theory_run_duration,accum_error_duration  
from qt_smartreport.qtr_day_sys_error_mtbf_his
where date_value=date_add(current_date(), interval -2 day))t4 on t4.alarm_service=ts.alarm_service	



-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_day_sys_error_mtbf_his;
insert into qt_smartreport.qtr_day_sys_error_mtbf_his(create_time,update_time,date_value,alarm_service,theory_run_duration,error_duration,error_num,mtbf,accum_theory_run_duration,accum_error_duration,accum_error_num,accum_mtbf)
select created_time as create_time,updated_time as update_time,date_value,alarm_service,theory_run_duration,error_duration,error_num,mtbf,accum_theory_run_duration,accum_error_duration,accum_error_num,accum_mtbf
from qt_smartreport.qt_day_sys_error_mtbf_his;