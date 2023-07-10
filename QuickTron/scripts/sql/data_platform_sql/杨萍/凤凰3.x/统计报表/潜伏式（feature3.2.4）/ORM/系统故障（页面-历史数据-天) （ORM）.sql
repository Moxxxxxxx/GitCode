-- 表1：qt_smartreport.qtr_day_sys_error_detail_his


-- step1:删除相关数据（qtr_day_sys_error_detail_his）
DELETE
FROM qt_smartreport.qtr_day_sys_error_detail_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);

-- step2:插入相关数据（qtr_day_sys_error_detail_his）
insert into qt_smartreport.qtr_day_sys_error_detail_his(create_time,update_time,date_value, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
date_add(current_date(), interval -1 day) as date_value,
id                                        as error_id,
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
              (start_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000') and
               start_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000') and
               coalesce(end_time, sysdate()) < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')) or
              (start_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000') and
               start_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000') and
               coalesce(end_time, sysdate()) >= date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')) or
              (start_time < date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000') and
               coalesce(end_time, sysdate()) >=
               date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000') and
               coalesce(end_time, sysdate()) < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')) or
              (start_time < date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000') and
               coalesce(end_time, sysdate()) >= date_format(current_date(), '%Y-%m-%d 00:00:00.000000000'))
          )



-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_day_sys_error_detail_his;
insert into qt_smartreport.qtr_day_sys_error_detail_his(create_time,update_time,date_value, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object)
select created_time as create_time,updated_time as update_time,date_value, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object
from qt_smartreport.qt_day_sys_error_detail_his;


--------------------------------------------------------------------------------
-- 表2：qt_smartreport.qtr_day_sys_end_error_detail_his

-- step1:删除相关数据（qtr_day_sys_end_error_detail_his）
DELETE
FROM qt_smartreport.qtr_day_sys_end_error_detail_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);


-- step2:插入相关数据（qtr_day_sys_end_error_detail_his）
insert into qt_smartreport.qtr_day_sys_end_error_detail_his(create_time,update_time,date_value, error_id, error_code, start_time, end_time,warning_spec, alarm_module, alarm_service, alarm_type,alarm_level, alarm_detail, param_value, job_order,robot_job, robot_code, device_code, server_code,transport_object)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
date_add(current_date(), interval -1 day) as date_value,
id                                        as error_id,
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
  and end_time is not null
  and end_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')
  and end_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')	


-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_day_sys_end_error_detail_his;
insert into qt_smartreport.qtr_day_sys_end_error_detail_his(create_time,update_time,date_value, error_id, error_code, start_time, end_time,warning_spec, alarm_module, alarm_service, alarm_type,alarm_level, alarm_detail, param_value, job_order,robot_job, robot_code, device_code, server_code,transport_object)
select created_time as create_time,updated_time as update_time,date_value, error_id, error_code, start_time, end_time,warning_spec, alarm_module, alarm_service, alarm_type,alarm_level, alarm_detail, param_value, job_order,robot_job, robot_code, device_code, server_code,transport_object
from qt_smartreport.qt_day_sys_end_error_detail_his;


