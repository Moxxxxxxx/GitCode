select tr.hour_value,
       tr.robot_code,
       brt.robot_type_code,
       brt.robot_type_name,
       COALESCE(sum(create_robot_error_num), 0) as create_robot_error_num,
       COALESCE(sum(create_order_num), 0)       as create_order_num,
       COALESCE(sum(create_job_num), 0)         as create_job_num,
       COALESCE(sum(robot_run_time), 0)         as robot_run_time,
       COALESCE(sum(robot_error_time), 0)       as robot_error_time,
	   COALESCE(sum(robot_error_num), 0)        as robot_error_num,
       COALESCE(sum(end_robot_error_num), 0)    as end_robot_error_num,
       COALESCE(sum(end_robot_error_time), 0)   as end_robot_error_time
from (select DATE_FORMAT(start_time, '%Y-%m-%d %H:00:00') as hour_value,
             t1.robot_code,
             count(distinct t1.error_id)                  as create_robot_error_num,
             null                                         as create_order_num,
             null                                         as create_job_num,
             null                                         as robot_run_time,
             null                                         as robot_error_time,
			 null                                         as robot_error_num,
             null                                         as end_robot_error_num,
             null                                         as end_robot_error_time
      from (select distinct robot_code,
                            error_id,
                            start_time
            from (select robot_code, error_id, start_time
                  from qt_smartreport.qt_day_robot_error_detail_his
                  where start_time BETWEEN {start_time} and {end_time}
                  union all
                  select robot_code, id as error_id, start_time
                  from ({tb_day_robot_error_detail}) tb
                  where start_time BETWEEN {start_time} and {end_time}) t) t1 -- day_robot_error_detail.sql
      group by hour_value, t1.robot_code
      union all
      select DATE_FORMAT(tor.create_time, '%Y-%m-%d %H:00:00') as hour_value,
             tocj.robot_code,
             null                                              as create_robot_error_num,
             count(distinct tor.order_no)                      as create_order_num,
             count(distinct tocj.job_sn)                       as create_job_num,
             null                                              as robot_run_time,
             null                                              as robot_error_time,
			 null                                              as robot_error_num,
             null                                              as end_robot_error_num,
             null                                              as end_robot_error_time
      from phoenix_rss.transport_order tor
               inner join phoenix_rss.transport_order_carrier_job tocj
                          on tocj.order_id = tor.id and tocj.robot_code is not null and
                             tocj.robot_code <> ''
      where tor.create_time BETWEEN {start_time} and {end_time}
      group by hour_value, tocj.robot_code
      union all
      select hour_value,
             robot_code,
             null                  as create_robot_error_num,
             null                  as create_order_num,
             null                  as create_job_num,
             null                  as robot_run_time,
             sum(robot_error_time) as robot_error_time,
			 sum(robot_error_num)  as robot_error_num,
             null                  as end_robot_error_num,
             null                  as end_robot_error_time
      from (select hour_start_time            as hour_value,
                   robot_code,
				   count(distinct error_id) as robot_error_num,
                   sum(the_hour_cost_seconds) as robot_error_time
            from qt_smartreport.qt_hour_robot_error_time_detail_his
            where hour_start_time BETWEEN {start_time} and {end_time}
            group by hour_start_time, robot_code
            union all
            select hour_start_time            as hour_value,
                   robot_code,
				   count(distinct id) as  robot_error_num,
                   sum(the_hour_cost_seconds) as robot_error_time
            from ({tb_hour_robot_error_time_detail}) tb -- hour_robot_error_time_detail.sql
            where hour_start_time BETWEEN {start_time} and {end_time}
            group by hour_start_time, robot_code) t
      group by hour_value, robot_code
      union all
      select DATE_FORMAT(t.end_time, '%Y-%m-%d %H:00:00')                   as hour_value,
             t.robot_code,
             null                                                           as create_robot_error_num,
             null                                                           as create_order_num,
             null                                                           as create_job_num,
             null                                                           as robot_run_time,
             null                                                           as robot_error_time,
			 null                                                           as robot_error_num,
             count(distinct t.error_id)                                     as end_robot_error_num,
             sum(unix_timestamp(t.end_time) - unix_timestamp(t.start_time)) as end_robot_error_time
      from (select t.robot_code, t.error_id, t.start_time, bn.end_time
            from qt_smartreport.qt_day_robot_error_detail_his t
                     inner join phoenix_basic.basic_notification bn
                                on bn.id = t.error_id and bn.end_time is not null and
                                   bn.end_time BETWEEN {start_time} and {end_time}
            union
            select tb.robot_code, tb.id AS error_id, tb.start_time, bn.end_time
            from ({tb_day_robot_error_detail}) tb -- day_robot_error_detail.sql
                     inner join phoenix_basic.basic_notification bn on bn.id = tb.id and bn.end_time is not null and
                                                                       bn.end_time BETWEEN {start_time} and {end_time}) t
      group by hour_value, t.robot_code
      union all
      select hour_start_time    as hour_value,
             robot_code,
             null               as create_robot_error_num,
             null               as create_order_num,
             null               as create_job_num,
             sum(stat_duration) as robot_run_time,
             null               as robot_error_time,
			 null               as robot_error_num,
             null               as end_robot_error_num,
             null               as end_robot_error_time
      from qt_smartreport.qt_hour_robot_state_detail_duration_his
      where hour_start_time BETWEEN {start_time} and {end_time}
        and (online_state = 'REGISTERED' or work_state = 'ERROR' or is_error = 1)
      group by hour_start_time, robot_code
      union all
      select hour_start_time    as hour_value,
             robot_code,
             null               as create_robot_error_num,
             null               as create_order_num,
             null               as create_job_num,
             sum(stat_duration) as robot_run_time,
             null               as robot_error_time,
			 null               as robot_error_num,
             null               as end_robot_error_num,
             null               as end_robot_error_time
      from ({tb_hour_robot_state_detail_duration}) tb -- hour_robot_state_detail_duration.sql
      where hour_start_time BETWEEN {start_time} and {end_time}
        and (online_state = 'REGISTERED' or work_state = 'ERROR' or is_error = 1)
      group by hour_start_time, robot_code) tr
         inner join phoenix_basic.basic_robot br on br.robot_code = tr.robot_code
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
group by tr.hour_value, tr.robot_code, brt.robot_type_code, brt.robot_type_name
