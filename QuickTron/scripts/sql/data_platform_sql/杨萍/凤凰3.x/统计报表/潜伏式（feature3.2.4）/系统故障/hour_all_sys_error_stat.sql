select t1.hour_value,
       'ALL_SYS' AS alarm_service,
       COALESCE(t2.create_sys_error_num, 0) as create_sys_error_num,
       COALESCE(t3.create_order_num, 0)     as create_order_num,
       COALESCE(t3.create_job_num, 0)       as create_job_num,
       COALESCE(t1.sys_run_time, 0)         as sys_run_time,
       COALESCE(t1.sys_error_time, 0)       as sys_error_time,
       COALESCE(t4.end_sys_error_num, 0)    as end_sys_error_num,
       COALESCE(t4.end_sys_error_time, 0)   as end_sys_error_time
from (select hour_start_time    as hour_value,
             sys_run_duration   as sys_run_time,
             sys_error_duration as sys_error_time
      from qt_smartreport.qt_hour_sys_error_duration_his
      where alarm_service = 'ALL_SYS'
        and hour_start_time BETWEEN {start_time} and {end_time}
      union 
      select hour_start_time    as hour_value,
             sys_run_duration   as sys_run_time,
             the_hour_cost_seconds as sys_error_time
      from ({tb_hour_all_sys_error_time_detail}) tb -- hour_all_sys_error_time_detail.sql	
     ) t1
         left join
     (select DATE_FORMAT(start_time, '%Y-%m-%d %H:00:00') as hour_value,
             count(distinct id)                           as create_sys_error_num
      from phoenix_basic.basic_notification
      where alarm_module in ('system', 'server')
        and alarm_level >= 3
        and start_time BETWEEN {start_time} and {end_time}
		group by hour_value) t2
     on t2.hour_value = t1.hour_value
         left join
     (select DATE_FORMAT(tor.create_time, '%Y-%m-%d %H:00:00') as hour_value,
                                 count(distinct tor.order_no)                      as create_order_num,
                                 count(distinct tocj.job_sn)                       as create_job_num
                          from phoenix_rss.transport_order tor
                                   left join phoenix_rss.transport_order_carrier_job tocj
                                             on tocj.order_id = tor.id 
                              where tor.create_time BETWEEN {start_time} and {end_time}
                          group by hour_value) t3
     on t3.hour_value = t1.hour_value
         left join
     (select DATE_FORMAT(end_time, '%Y-%m-%d %H:00:00')                                      as hour_value,
             count(distinct id)                                                              as end_sys_error_num,
             sum(unix_timestamp(COALESCE(end_time, sysdate())) - unix_timestamp(start_time)) as end_sys_error_time
      from phoenix_basic.basic_notification
      where alarm_module in ('system', 'server')
        and alarm_level >= 3
        and end_time is not null
        and end_time BETWEEN {start_time} and {end_time}
      group by hour_value) t4 on t4.hour_value = t1.hour_value