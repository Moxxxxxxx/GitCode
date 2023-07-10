select CURRENT_DATE()               as                           date_value,
       t1.robot_code,
       t1.hour_start_time,
       t1.next_hour_start_time,
       t2.id              as                           state_id,
       t2.create_time     as                           state_create_time,
       t2.network_state,
       t2.online_state,
       t2.work_state,
       t2.job_sn,
       t2.cause,
       t2.is_error, 
       t2.duration / 1000 as                           duration,
       case
           when sysdate() < t1.next_hour_start_time then
                   UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, sysdate())) -
                   UNIX_TIMESTAMP(t1.hour_start_time)
           else UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, t1.next_hour_start_time)) -
                UNIX_TIMESTAMP(t1.hour_start_time) end stat_duration
from (select t.robot_code, t1.hour_start_time, t1.next_hour_start_time, max(t.id) as before_day_last_id
      from (select * from phoenix_rms.robot_state_history where create_time>=date_add(current_date(), interval -1 day)) t
               inner join (select br.robot_code,
                                  t.hour_start_time,
                                  t.next_hour_start_time
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
                                 where th.hour_start_time <= sysdate()) t
                                    left join phoenix_basic.basic_robot br on 1) t1
                          on t1.robot_code = t.robot_code and t.create_time < t1.hour_start_time
      group by t.robot_code, t1.hour_start_time, t1.next_hour_start_time) t1
         left join phoenix_rms.robot_state_history t2 on t2.robot_code = t1.robot_code and t2.id = t1.before_day_last_id
         left join (select t.robot_code,
                           t1.hour_start_time,
                           t1.next_hour_start_time,
                           min(create_time) as the_hour_first_create_time
                    from phoenix_rms.robot_state_history t
                             inner join (select br.robot_code,
                                                t.hour_start_time,
                                                t.next_hour_start_time
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
                                               where th.hour_start_time <= sysdate()) t
                                                  left join phoenix_basic.basic_robot br on 1) t1
                                        on t1.robot_code = t.robot_code and t.create_time >= t1.hour_start_time and
                                           t.create_time < t1.next_hour_start_time
                    group by t.robot_code, t1.hour_start_time, t1.next_hour_start_time) t3
                   on t3.robot_code = t1.robot_code and t3.hour_start_time = t1.hour_start_time and
                      t3.next_hour_start_time = t1.next_hour_start_time
union all
select CURRENT_DATE()               as           date_value,
       t.robot_code,
       t.hour_start_time,
       t.next_hour_start_time,
       t4.id              as           state_id,
       t4.create_time     as           state_create_time,
       t4.network_state,
       t4.online_state,
       t4.work_state,
       t4.job_sn,
       t4.cause,
       t4.is_error,
       t4.duration / 1000 as           duration,
       case
           when t5.the_hour_last_id is not null and sysdate() >= t.next_hour_start_time
               then UNIX_TIMESTAMP(t.next_hour_start_time) - UNIX_TIMESTAMP(t4.create_time)
           when t5.the_hour_last_id is not null and sysdate() < t.next_hour_start_time
               then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t4.create_time)
           else t4.duration / 1000 end stat_duration
from phoenix_rms.robot_state_history t4
         inner join
     (select br.robot_code,
             t.hour_start_time,
             t.next_hour_start_time
      from (select th.hour_start_time,
                   th.next_hour_start_time
            from (select date_format(concat(current_date(), ' ', hour_start_time),
                                     '%Y-%m-%d %H:00:00') as hour_start_time,
                         date_format(case
                                         when hour_start_time = '23:00:00' then concat(
                                                 date_add(current_date(), interval 1 day), ' ', next_hour_start_time)
                                         else concat(current_date(), ' ', next_hour_start_time) end,
                                     '%Y-%m-%d %H:00:00')    next_hour_start_time
                  from qt_smartreport.qt_dim_hour) th
            where th.hour_start_time <= sysdate()) t
               left join phoenix_basic.basic_robot br on 1) t
     on t4.robot_code = t.robot_code and t4.create_time >= t.hour_start_time and
        t4.create_time < t.next_hour_start_time
         left join (select t.robot_code,
                           t.hour_start_time,
                           t.next_hour_start_time,
                           max(t1.id)          as the_hour_last_id,
                           max(t1.create_time) as the_hour_last_create_time
                    from (select br.robot_code,
                                 t.hour_start_time,
                                 t.next_hour_start_time
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
                                where th.hour_start_time <= sysdate()) t
                                   left join phoenix_basic.basic_robot br on 1) t
                             inner join phoenix_rms.robot_state_history t1
                                        on t1.robot_code = t.robot_code and t1.create_time >= t.hour_start_time and
                                           t1.create_time < t.next_hour_start_time
                    group by t.robot_code, t.hour_start_time, t.next_hour_start_time) t5
                   on t5.robot_code = t4.robot_code and t5.the_hour_last_id = t4.id				   