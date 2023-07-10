select t1.hour_start_time,
       t1.next_hour_start_time,
       t2.*,
       case
           when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
                t2.stat_end_time < t1.next_hour_start_time then UNIX_TIMESTAMP(t2.stat_end_time) -
                                                                UNIX_TIMESTAMP(t2.stat_start_time)
           when t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
                t2.stat_end_time >= t1.next_hour_start_time
               then UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t2.stat_start_time)
           when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and
                t2.stat_end_time < t1.next_hour_start_time
               then UNIX_TIMESTAMP(t2.stat_end_time) - UNIX_TIMESTAMP(t1.hour_start_time)
           when t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.next_hour_start_time
               then UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t1.hour_start_time)
           end the_hour_cost_seconds
from (select th.hour_start_time,
             th.next_hour_start_time
      from (select date_format(concat(current_date(), ' ', hour_start_time),
                               '%Y-%m-%d %H:00:00') as hour_start_time,
                   date_format(case
                                   when hour_start_time = '23:00:00' then concat(
                                           date_add(current_date(), interval 1 day), ' ',
                                           next_hour_start_time)
                                   else concat(current_date(), ' ', next_hour_start_time) end,
                               '%Y-%m-%d %H:00:00')    next_hour_start_time
            from qt_smartreport.qt_dim_hour) th
      where th.hour_start_time <= sysdate()) t1
         inner join
     (select t.*,
             case when t.start_time < {now_start_time} then {now_start_time} else t.start_time end stat_start_time,
             coalesce(t.end_time, sysdate()) as                                                    stat_end_time
      from ({tb_day_robot_error_detail}) t) t2 on -- day_robot_error_detail.sql
         ((t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
           t2.stat_end_time < t1.next_hour_start_time)
                 or (t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
                     t2.stat_end_time >= t1.next_hour_start_time)
                 or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and
                     t2.stat_end_time < t1.next_hour_start_time)
                 or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.next_hour_start_time))