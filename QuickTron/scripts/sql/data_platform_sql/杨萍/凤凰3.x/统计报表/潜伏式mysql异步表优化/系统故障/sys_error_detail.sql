-- 用于：统计报表->故障异常统计->系统故障统计

select 
id                                                                         as error_id,
error_code,
start_time                                                                 as error_start_time,
end_time                                                                   as error_end_time,
unix_timestamp(COALESCE(end_time, { now_time })) - unix_timestamp(start_time) as error_time,
alarm_level,
alarm_detail,
alarm_service,
warning_spec,
robot_code,
robot_job,
job_order
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
and alarm_level >= 3
and start_time BETWEEN { start_time } and { end_time }




#############################################################################################
---  检查
#############################################################################################
-- { now_time }
-- { start_time }
-- { end_time }
set @now_time = sysdate(); --  当前时间
set @start_time = date_format(sysdate(), '%Y-%m-%d 00:00:00.000000000'); -- 筛选框开始时间  默认当天开始时间
set @end_time = date_format(sysdate(), '%Y-%m-%d %H:59:59.999999999'); --  筛选框结束时间  默认当前小时结束时间
select @now_time, @start_time, @end_time;


select 
id                                                                         as error_id,
error_code,
start_time                                                                 as error_start_time,
end_time                                                                   as error_end_time,
unix_timestamp(COALESCE(end_time, @now_time)) - unix_timestamp(start_time) as error_time,
alarm_level,
alarm_detail,
alarm_service,
warning_spec,
robot_code,
robot_job,
job_order
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
and alarm_level >= 3
and start_time BETWEEN @start_time and @end_time