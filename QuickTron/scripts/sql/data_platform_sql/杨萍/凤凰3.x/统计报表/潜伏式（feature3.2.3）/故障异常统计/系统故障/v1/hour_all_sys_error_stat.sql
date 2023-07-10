select t1.hour_value,
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
      union all
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
                                             on tocj.order_no = tor.order_no
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









###########################老版逻辑###########################

select t.hour_value,
       COALESCE(sum(create_sys_error_num), 0) as create_sys_error_num,
       COALESCE(sum(create_order_num), 0)     as create_order_num,
       COALESCE(sum(create_job_num), 0)       as create_job_num,
       3600                                   as sys_run_time,
       COALESCE(sum(sys_error_time), 0)       as sys_error_time,
       COALESCE(sum(end_sys_error_num), 0)    as end_sys_error_num,
       COALESCE(sum(end_sys_error_time), 0)   as end_sys_error_time
from (select DATE_FORMAT(start_time, '%Y-%m-%d %H:00:00') as hour_value,
             count(distinct id)                           as create_sys_error_num,
             null                                         as create_order_num,
             null                                         as create_job_num,
             null                                         as sys_run_time,
             null                                         as sys_error_time,
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
             null                                              as sys_run_time,
             null                                              as sys_error_time,
             null                                              as end_sys_error_num,
             null                                              as end_sys_error_time
      from phoenix_rss.transport_order tor
               left join phoenix_rss.transport_order_carrier_job tocj
                         on tocj.order_no = tor.order_no
      where  tor.create_time BETWEEN {start_time} and {end_time}
      union all
      select hour_start_time       as value_time,
             null                  as create_sys_error_num,
             null                  as create_order_num,
             null                  as create_job_num,
             null                  as sys_run_time,
             the_hour_cost_seconds as sys_error_time,
             null                  as end_sys_error_num,
             null                  as end_sys_error_time
      from qt_smartreport.qt_hour_all_sys_error_time_detail_his
      where hour_start_time BETWEEN {start_time} and {end_time}
      union all
      select hour_start_time       as value_time,
             null                  as create_sys_error_num,
             null                  as create_order_num,
             null                  as create_job_num,
             null                  as sys_run_time,
             the_hour_cost_seconds as sys_error_time,
             null                  as end_sys_error_num,
             null                  as end_sys_error_time
      from ({tb_hour_all_sys_error_time_detail}) tb -- hour_all_sys_error_time_detail.sql	  		  
      union all
      select DATE_FORMAT(end_time, '%Y-%m-%d %H:00:00')                 as hour_value,
             null                                                       as create_sys_error_num,
             null                                                       as create_order_num,
             null                                                       as create_job_num,
             null                                                       as sys_run_time,
             null                                                       as sys_error_time,
             count(distinct id)                                         as end_sys_error_num,
             sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as end_sys_error_time
      from phoenix_basic.basic_notification
      where alarm_module in ('system', 'server')
        and alarm_level >= 3
        and end_time BETWEEN {start_time} and {end_time}
      group by hour_value) t
group by t.hour_value











---------------------------------------------------------------------------------------------
set @now_start_time = date_format(current_date(), '%Y-%m-%d 00:00:00.000000000');
set @now_end_time = date_format(current_date(), '%Y-%m-%d 23:59:59.999999999');
set @next_start_time = date_format(date_add(current_date(), interval 1 day), '%Y-%m-%d 00:00:00.000000000');
set @start_time = '2022-09-06 00:00:00.000000000';
set @end_time = '2022-09-06 23:59:59.999999999';


select t.hour_value,
       COALESCE(sum(create_sys_error_num), 0) as create_sys_error_num,
       COALESCE(sum(create_order_num), 0)     as create_order_num,
       COALESCE(sum(create_job_num), 0)       as create_job_num,
       3600                                   as sys_run_time,
       COALESCE(sum(sys_error_time), 0)       as sys_error_time,
       COALESCE(sum(end_sys_error_num), 0)    as end_sys_error_num,
       COALESCE(sum(end_sys_error_time), 0)   as end_sys_error_time
from (select DATE_FORMAT(start_time, '%Y-%m-%d %H:00:00') as hour_value,
             count(distinct id)                           as create_sys_error_num,
             null                                         as create_order_num,
             null                                         as create_job_num,
             null                                         as sys_run_time,
             null                                         as sys_error_time,
             null                                         as end_sys_error_num,
             null                                         as end_sys_error_time
      from phoenix_basic.basic_notification
      where alarm_module in ('system', 'server')
        and alarm_level >= 3
        and start_time BETWEEN @start_time and @end_time
      group by hour_value
      union all
      select DATE_FORMAT(tor.create_time, '%Y-%m-%d %H:00:00') as hour_value,
             null                                              as create_sys_error_num,
             count(distinct tor.order_no)                      as create_order_num,
             count(distinct tocj.job_sn)                       as create_job_num,
             null                                              as sys_run_time,
             null                                              as sys_error_time,
             null                                              as end_sys_error_num,
             null                                              as end_sys_error_time
      from phoenix_rss.transport_order tor
               left join phoenix_rss.transport_order_carrier_job tocj
                         on tocj.order_no = tor.order_no
      where tor.order_state != 'CANCELED'
        and tor.create_time BETWEEN @start_time and @end_time
      union all
      select hour_start_time       as value_time,
             null                  as create_sys_error_num,
             null                  as create_order_num,
             null                  as create_job_num,
             null                  as sys_run_time,
             the_hour_cost_seconds as sys_error_time,
             null                  as end_sys_error_num,
             null                  as end_sys_error_time
      from qt_smartreport.qt_hour_all_sys_error_time_detail_his
      where hour_start_time BETWEEN @start_time and @end_time
      union all
      select hour_start_time       as value_time,
             null                  as create_sys_error_num,
             null                  as create_order_num,
             null                  as create_job_num,
             null                  as sys_run_time,
             the_hour_cost_seconds as sys_error_time,
             null                  as end_sys_error_num,
             null                  as end_sys_error_time
      from ({tb_hour_all_sys_error_time_detail}) tb -- hour_all_sys_error_time_detail.sql	  		  
      union all
      select DATE_FORMAT(end_time, '%Y-%m-%d %H:00:00')                 as hour_value,
             count(distinct id)                                         as create_sys_error_num,
             null                                                       as create_order_num,
             null                                                       as create_job_num,
             null                                                       as sys_run_time,
             null                                                       as sys_error_time,
             count(distinct id)                                         as end_sys_error_num,
             sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as end_sys_error_time
      from phoenix_basic.basic_notification
      where alarm_module in ('system', 'server')
        and alarm_level >= 3
        and end_time BETWEEN @start_time and @end_time
      group by hour_value) t
group by t.hour_value