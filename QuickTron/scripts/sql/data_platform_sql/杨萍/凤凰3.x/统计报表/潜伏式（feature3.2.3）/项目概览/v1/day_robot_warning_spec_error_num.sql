select warning_spec,
       error_code,
       alarm_detail       as error_detail,
       count(distinct id) as breakdown_num
from ({tb}) t -- day_robot_error_detail.sql
where start_time BETWEEN {now_start_time} and {now_end_time}
group by warning_spec, error_code, alarm_detail
order by breakdown_num desc
limit 5












-----------------------------------------------------------------------------
set @now_start_time = '2022-08-24 00:00:00.000000000';
set @now_end_time = '2022-08-24 23:59:59.999999999';


select warning_spec,
error_code,
alarm_detail as error_detail,
count(distinct id) as error_num 
from qt_smartreport.qt_day_basic_notification_temp
where start_time BETWEEN @now_start_time and @now_end_time
group by warning_spec,error_code,alarm_detail