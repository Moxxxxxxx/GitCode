select DATE_FORMAT(t.hour_start_time, '%Y-%m-%d %H:00:00')       as hour_value,
       coalesce(sum(t.loading_busy_time) / sum(t.total_time), 0) as utilization_rate
from (select t1.robot_code,
             t1.hour_start_time,
             t1.next_hour_start_time,
             COALESCE(t2.loading_busy_time, 0) as loading_busy_time,
             case
                 when HOUR(t1.hour_start_time) = HOUR(sysdate())
                     then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t1.hour_start_time)
                 else 3600 end                 as total_time
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
                  where th.hour_start_time <= sysdate()) t1
                     left join
                 (select br.robot_code
                  from phoenix_basic.basic_robot br
                  where br.usage_state = 'using') t2 on 1) t1
               left join (select tb.robot_code,
                                 tb.hour_start_time,
                                 tb.next_hour_start_time,
                                 sum(stat_duration) as loading_busy_time
                          from ({tb_hour_robot_state_detail_duration}) tb -- hour_robot_state_detail_duration.sql
                                   left join (select job_sn, job_type from phoenix_rms.job_history) tjh
                                             on tjh.job_sn = tb.job_sn
                                   left join (select job_sn, job_type from phoenix_rms.job) tj on tj.job_sn = tb.job_sn
                          where tb.online_state = 'REGISTERED'
                            and tb.work_state = 'BUSY'
							and tb.is_error != 1
                            and ((tjh.job_sn is not null and tjh.job_type = 'CUSTOMIZE') or
                                 (tj.job_sn is not null and tj.job_type = 'CUSTOMIZE'))
                          group by tb.robot_code, tb.hour_start_time, tb.next_hour_start_time) t2
                         on t2.robot_code = t1.robot_code and t2.hour_start_time = t1.hour_start_time and
                            t2.next_hour_start_time = t1.next_hour_start_time) t
group by hour_value	