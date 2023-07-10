select DATE_FORMAT(t.hour_start_time, '%Y-%m-%d %H:00:00')           as x,
       ROUND(coalesce(sum(t.uptime_state_duration) / sum(t.total_time), 0)*100,2) as y
from (select t1.robot_code,
             t1.hour_start_time,
             t1.next_hour_start_time,
             COALESCE(t2.uptime_state_duration, 0) as uptime_state_duration,
             case
                 when HOUR(t1.hour_start_time) = HOUR(sysdate())
                     then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t1.hour_start_time)
                 else 3600 end                     as total_time
      from (select t1.hour_start_time,
                   t1.next_hour_start_time,
                   t2.robot_code
            from (select th.hour_start_time,
                         th.next_hour_start_time
                  from (select date_format(concat(current_date(), ' ', hour_start_time),
                                           '%Y-%m-%d %H:00:00') as hour_start_time,
                               date_format(case
                                               when hour_start_time = '23:00:00'
                                                   then concat(
                                                       date_add(current_date(), interval 1 day),
                                                       ' ', next_hour_start_time)
                                               else concat(current_date(), ' ', next_hour_start_time) end,
                                           '%Y-%m-%d %H:00:00')    next_hour_start_time
                        from qt_smartreport.qt_dim_hour) th
                 ) t1
                     left join
                 (select br.robot_code
                  from phoenix_basic.basic_robot br
                  where br.usage_state = 'using') t2 on 1) t1
               left join (select tb.robot_code,
                                 tb.hour_start_time,
                                 tb.next_hour_start_time,
                                 sum(stat_duration) as uptime_state_duration
                          from ({tb_hour_robot_state_detail_duration}) tb -- hour_robot_state_detail_duration.sql
                          where (tb.is_error != 1 and tb.work_state in ('BUSY', 'CHARGING'))
                             or ((tb.work_state = 'ERROR' or tb.is_error = 1) and tb.job_sn is not null)
                          group by tb.robot_code, tb.hour_start_time, tb.next_hour_start_time) t2
                         on t2.robot_code = t1.robot_code and t2.hour_start_time = t1.hour_start_time and
                            t2.next_hour_start_time = t1.next_hour_start_time) t
group by 1