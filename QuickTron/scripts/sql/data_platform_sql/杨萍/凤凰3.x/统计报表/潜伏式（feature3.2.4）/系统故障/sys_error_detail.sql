select id                                                                         as error_id,
       error_code,
       start_time                                                                 as error_start_time,
       end_time                                                                   as error_end_time,
       unix_timestamp(COALESCE(end_time, sysdate())) - unix_timestamp(start_time) as error_time,
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
  and start_time BETWEEN {start_time} and {end_time}