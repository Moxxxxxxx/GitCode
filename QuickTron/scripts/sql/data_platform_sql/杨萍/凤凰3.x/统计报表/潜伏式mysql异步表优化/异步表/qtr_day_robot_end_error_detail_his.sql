set @now_time=sysdate();   --  当前时间
set @dt_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @dt_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间
set @dt_week_start_time=date_format(DATE_SUB(sysdate(),INTERVAL WEEKDAY(sysdate()) + 0 DAY), '%Y-%m-%d 00:00:00'); -- 当前一周的开始时间
set @dt_next_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) -7 DAY), '%Y-%m-%d 00:00:00'); --  下一周的开始时间
select @now_time,@dt_hour_start_time,@dt_next_hour_start_time,@dt_day_start_time,@dt_next_day_start_time,@dt_week_start_time,@dt_next_week_start_time;


-- 插入数据（mysql参数）
insert into qt_smartreport.qtr_day_robot_end_error_detail_his(create_time,update_time,date_value, error_id, error_code, start_time, end_time,warning_spec, alarm_module, alarm_service, alarm_type,alarm_level, alarm_detail, param_value, job_order,robot_job, robot_code, device_code, server_code,transport_object)
select 
@now_time as create_time,
@now_time as update_time,
date(@dt_day_start_time) as date_value,
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
where alarm_module = 'robot'
  and alarm_level >= 3
  and end_time is not null
  and end_time >= @dt_day_start_time
  and end_time < @dt_next_day_start_time
  


--------------------------------------------------------------------------------------------------------------------------
			
-- 插入数据（异步表）qt_smartreport.qtr_day_robot_end_error_detail_his	
-- {{ dt_relative_time(dt) }}
-- {{ now_time }}
-- {{ dt_hour_start_time }}
-- {{ dt_next_hour_start_time }}
-- {{ dt_day_start_time }}
-- {{ dt_next_day_start_time }}
-- {{ dt_week_start_time }}
-- {{ dt_next_week_start_time }}	

-- 定义时间参数
{% set now_time=datetime.datetime.now().strftime("'%Y-%m-%d %H:%M:%S.000000000'") %}  -- 客观当前时间
{% set dt_hour_start_time=dt_relative_time(dt,default="%Y-%m-%d %H:00:00.000000000") %}   -- dt所在小时的开始时间
{% set dt_next_hour_start_time=dt_relative_time(dt,hours=1,default="%Y-%m-%d %H:00:00.000000000") %}  -- dt所在小时的下一个小时的开始时间
{% set dt_day_start_time=dt_relative_time(dt,default="%Y-%m-%d 00:00:00.000000000") %}  -- dt所在天的开始时间
{% set dt_next_day_start_time=dt_relative_time(dt,days=1,default="%Y-%m-%d 00:00:00.000000000") %}  -- dt所在天的下一天的开始时间
{% set dt_week_start_time=(dt - datetime.timedelta(days=dt.now().weekday())).strftime("'%Y-%m-%d 00:00:00.000000000'") %}  -- dt所在周的开始时间
{% set dt_next_week_start_time=(dt + datetime.timedelta(days=7-dt.now().weekday())).strftime("'%Y-%m-%d 00:00:00.000000000'") %}  -- dt所在周的下一周的开始时间		


-- 插入逻辑  
select 
{{ now_time }} as create_time,
{{ now_time }} as update_time,
date({{ dt_day_start_time }}) as date_value,
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
where alarm_module = 'robot'
  and alarm_level >= 3
  and end_time is not null
  and end_time >= {{ dt_day_start_time }}
  and end_time < {{ dt_next_day_start_time }}