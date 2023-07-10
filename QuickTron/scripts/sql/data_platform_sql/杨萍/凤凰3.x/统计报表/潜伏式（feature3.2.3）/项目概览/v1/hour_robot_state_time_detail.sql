select t1.hour_start_time,
       t1.next_hour_start_time,
       t2.id as state_id,
       t2.robot_code,
       t2.create_time,
       t2.network_state,
       t2.online_state,
       t2.work_state,
       t2.job_sn,
       t2.cause,
       t2.next_id,
       t2.next_create_time,
       t2.start_time,
       t2.end_time,
       case
           when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                t2.end_time < t1.next_hour_start_time then UNIX_TIMESTAMP(t2.end_time) - UNIX_TIMESTAMP(t2.start_time)
           when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                t2.end_time >= t1.next_hour_start_time
               then UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t2.start_time)
           when t2.start_time < t1.hour_start_time and t2.end_time >= t1.hour_start_time and
                t2.end_time < t1.next_hour_start_time
               then UNIX_TIMESTAMP(t2.end_time) - UNIX_TIMESTAMP(t1.hour_start_time)
           when t2.start_time < t1.hour_start_time and t2.end_time >= t1.next_hour_start_time
               then UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t1.hour_start_time)
           end  the_hour_cost_seconds
from (select h.hour_start_time,
             h.next_hour_start_time
      from (select th.day_hours                               as hour_start_time,
                   DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
            from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(current_date(), '%Y-%m-%d 00:00:00'), INTERVAL
                                              (-(@u := @u + 1)) HOUR), '%Y-%m-%d %H:00:00') as day_hours
                  FROM (SELECT a
                        FROM (SELECT '1' AS a UNION SELECT '2' UNION SELECT '3' UNION SELECT '4') AS a
                                 JOIN(SELECT '1'
                                      UNION
                                      SELECT '2'
                                      UNION
                                      SELECT '3'
                                      UNION
                                      SELECT '4'
                                      UNION
                                      SELECT '5'
                                      UNION
                                      SELECT '6') AS b ON 1) AS b,
                       (SELECT @u := -1) AS i) th) h
      where h.hour_start_time <= sysdate()) t1
         inner join
     (select t.*,
             case when t.create_time < {now_start_time} then {now_start_time} else t.create_time end start_time,
             coalesce(t.next_create_time, sysdate()) as                                              end_time
      from ({tb}) t) t2 on
         ((t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
           t2.end_time < t1.next_hour_start_time)
             or (t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                 t2.end_time >= t1.next_hour_start_time)
             or (t2.start_time < t1.hour_start_time and t2.end_time >= t1.hour_start_time and
                 t2.end_time < t1.next_hour_start_time)
             or (t2.start_time < t1.hour_start_time and t2.end_time >= t1.next_hour_start_time))







































-----------------------------------------------------------------------------------------------------------------

set @now_start_time = '2022-08-24 00:00:00.000000000';
set @now_end_time = '2022-08-24 23:59:59.999999999';




drop table if exists qt_smartreport.qt_hour_robot_state_time_detail_temp;
create table qt_smartreport.qt_hour_robot_state_time_detail_temp
as
select t1.hour_start_time,
       t1.next_hour_start_time,
       t2.id as state_id,
       t2.robot_code,
       t2.create_time,
       t2.network_state,
       t2.online_state,
       t2.work_state,
       t2.job_sn,
       t2.cause,
       t2.next_id,
       t2.next_create_time,
       t2.start_time,
       t2.end_time,
       case
           when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                t2.end_time < t1.next_hour_start_time then UNIX_TIMESTAMP(t2.end_time) - UNIX_TIMESTAMP(t2.start_time)
           when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                t2.end_time >= t1.next_hour_start_time
               then UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t2.start_time)
           when t2.start_time < t1.hour_start_time and t2.end_time >= t1.hour_start_time and
                t2.end_time < t1.next_hour_start_time
               then UNIX_TIMESTAMP(t2.end_time) - UNIX_TIMESTAMP(t1.hour_start_time)
           when t2.start_time < t1.hour_start_time and t2.end_time >= t1.next_hour_start_time
               then UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t1.hour_start_time)
           end  the_hour_cost_seconds
from (select h.hour_start_time,
             h.next_hour_start_time
      from (select th.day_hours                               as hour_start_time,
                   DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
            from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(current_date(), '%Y-%m-%d 00:00:00'), INTERVAL
                                              (-(@u := @u + 1)) HOUR), '%Y-%m-%d %H:00:00') as day_hours
                  FROM (SELECT a
                        FROM (SELECT '1' AS a UNION SELECT '2' UNION SELECT '3' UNION SELECT '4') AS a
                                 JOIN(SELECT '1'
                                      UNION
                                      SELECT '2'
                                      UNION
                                      SELECT '3'
                                      UNION
                                      SELECT '4'
                                      UNION
                                      SELECT '5'
                                      UNION
                                      SELECT '6') AS b ON 1) AS b,
                       (SELECT @u := -1) AS i) th) h
      where h.hour_start_time <= sysdate()) t1
         inner join
     (select t.*,
case when t.create_time<@now_start_time then @now_start_time else t.create_time end start_time,
coalesce(t.next_create_time,sysdate()) as end_time
from qt_smartreport.qt_day_robot_state_change_detail_temp t) t2 on
         ((t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
           t2.end_time < t1.next_hour_start_time)
             or (t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                 t2.end_time >= t1.next_hour_start_time)
             or (t2.start_time < t1.hour_start_time and t2.end_time >= t1.hour_start_time and
                 t2.end_time < t1.next_hour_start_time)
             or (t2.start_time < t1.hour_start_time and t2.end_time >= t1.next_hour_start_time))