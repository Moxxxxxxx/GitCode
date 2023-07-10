select t.hour_value,
       t.alarm_service,
       COALESCE(sum(create_sys_error_num), 0) as create_sys_error_num,
       COALESCE(sum(create_order_num), 0)     as create_order_num,
       COALESCE(sum(create_job_num), 0)       as create_job_num,
       COALESCE(sum(end_sys_error_num), 0)    as end_sys_error_num,
       COALESCE(sum(end_sys_error_time), 0)   as end_sys_error_time
from (select DATE_FORMAT(start_time, '%Y-%m-%d %H:00:00') as hour_value,
             alarm_service,
             count(distinct id)                           as create_sys_error_num,
             null                                         as create_order_num,
             null                                         as create_job_num,
             null                                         as end_sys_error_num,
             null                                         as end_sys_error_time
      from phoenix_basic.basic_notification
      where alarm_module in ('system', 'server')
        and alarm_level >= 3
        and start_time BETWEEN {start_time} and {end_time}
      group by hour_value, alarm_service
      union all
      select t2.hour_value,
             t1.alarm_service,
             null as create_sys_error_num,
             t2.create_order_num,
             t2.create_job_num,
             null as end_sys_error_num,
             null as end_sys_error_time
      from (select distinct module as alarm_service from phoenix_basic.basic_error_info) t1
               left join (select DATE_FORMAT(tor.create_time, '%Y-%m-%d %H:00:00') as hour_value,
                                 count(distinct tor.order_no)                      as create_order_num,
                                 count(distinct tocj.job_sn)                       as create_job_num
                          from phoenix_rss.transport_order tor
                                   left join phoenix_rss.transport_order_carrier_job tocj
                                             on tocj.order_no = tor.order_no
                          where tor.order_state != 'CANCELED'
                            and tor.create_time BETWEEN {start_time} and {end_time}
                          group by hour_value) t2 on 1
      union all
      select DATE_FORMAT(end_time, '%Y-%m-%d %H:00:00')                 as hour_value,
             alarm_service,
             count(distinct id)                                         as create_sys_error_num,
             null                                                       as create_order_num,
             null                                                       as create_job_num,
             count(distinct id)                                         as end_sys_error_num,
             sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as end_sys_error_time
      from phoenix_basic.basic_notification
      where alarm_module in ('system', 'server')
        and alarm_level >= 3
        and end_time BETWEEN {start_time} and {end_time}
      group by hour_value, alarm_service) t
group by t.hour_value, t.alarm_service