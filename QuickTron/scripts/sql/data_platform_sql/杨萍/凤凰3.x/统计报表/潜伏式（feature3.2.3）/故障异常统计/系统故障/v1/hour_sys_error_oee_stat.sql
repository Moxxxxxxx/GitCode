select t.hour_value,
       t.alarm_service,
       3600                             as sys_run_time,
       COALESCE(sum(sys_error_time), 0) as sys_error_time
from (select hour_start_time       as hour_value,
             alarm_service,
             the_hour_cost_seconds as sys_error_time
      from qt_smartreport.qt_hour_sys_error_time_detail_his
      where hour_start_time BETWEEN {start_time} and {end_time}
      union all
      select hour_start_time       as hour_value,
             alarm_service,
             the_hour_cost_seconds as sys_error_time
      from ({tb_hour_sys_error_time_detail}) tb -- hour_sys_error_time_detail.sql							  
     ) t
group by t.hour_value, t.alarm_service
