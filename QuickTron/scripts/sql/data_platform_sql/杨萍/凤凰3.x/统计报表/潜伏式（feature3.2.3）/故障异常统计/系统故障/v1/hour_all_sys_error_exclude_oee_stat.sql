select t.hour_value,
       COALESCE(sum(create_sys_error_num), 0) as create_sys_error_num,
       COALESCE(sum(create_order_num), 0)     as create_order_num,
       COALESCE(sum(create_job_num), 0)       as create_job_num,
       COALESCE(sum(end_sys_error_num), 0)    as end_sys_error_num,
       COALESCE(sum(end_sys_error_time), 0)   as end_sys_error_time
from (select DATE_FORMAT(start_time, '%Y-%m-%d %H:00:00') as hour_value,
             count(distinct id)                           as create_sys_error_num,
             null                                         as create_order_num,
             null                                         as create_job_num,
             null                                         as end_sys_error_num,
             null                                         as end_sys_error_time
      from phoenix_basic.basic_notification
      where alarm_module in ('system', 'server')
        and alarm_level >= 3
        and start_time BETWEEN {start_time} and {end_time}
      group by hour_value
      union all
      select DATE_FORMAT(tor.create_time, '%Y-%m-%d %H:00:00') as hour_value,
             null                                              as create_sys_error_num,
             count(distinct tor.order_no)                      as create_order_num,
             count(distinct tocj.job_sn)                       as create_job_num,
             null                                              as end_sys_error_num,
             null                                              as end_sys_error_time
      from phoenix_rss.transport_order tor
               left join phoenix_rss.transport_order_carrier_job tocj
                         on tocj.order_no = tor.order_no
      where tor.order_state != 'CANCELED'
        and tor.create_time BETWEEN {start_time} and {end_time}		  
      union all
      select DATE_FORMAT(end_time, '%Y-%m-%d %H:00:00')                 as hour_value,
             count(distinct id)                                         as create_sys_error_num,
             null                                                       as create_order_num,
             null                                                       as create_job_num,
             count(distinct id)                                         as end_sys_error_num,
             sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as end_sys_error_time
      from phoenix_basic.basic_notification
      where alarm_module in ('system', 'server')
        and alarm_level >= 3
        and end_time BETWEEN {start_time} and {end_time}
      group by hour_value) t
group by t.hour_value
