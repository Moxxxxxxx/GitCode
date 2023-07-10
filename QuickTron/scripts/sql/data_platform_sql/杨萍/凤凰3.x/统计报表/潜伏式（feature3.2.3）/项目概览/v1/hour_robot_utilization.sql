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
                            and ((tjh.job_sn is not null and tjh.job_type = 'CUSTOMIZE') or
                                 (tj.job_sn is not null and tj.job_type = 'CUSTOMIZE'))
                          group by tb.robot_code, tb.hour_start_time, tb.next_hour_start_time) t2
                         on t2.robot_code = t1.robot_code and t2.hour_start_time = t1.hour_start_time and
                            t2.next_hour_start_time = t1.next_hour_start_time) t
group by hour_value					  



















-----老版本------------------------

select DATE_FORMAT(t.hour_start_time, '%Y-%m-%d %H:00:00') as hour_value,
       coalesce(sum(loading_busy_time) / sum(total_time), 0) as utilization_rate
from (select t.hour_start_time,
             t.robot_code,
             coalesce(sum(t.loading_busy_time), 0) as loading_busy_time,
             (case
                  when HOUR(t.hour_start_time) = HOUR(sysdate())
                      then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t.hour_start_time)
                  else 3600 end)                   as total_time
      from (select tt.hour_start_time,
                   tt.next_hour_start_time,
                   tt.robot_code,
                   case
                       when t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and rjsc.job_sn is not null
                           then t.the_hour_cost_seconds end                          as loading_busy_time,
                   case
                       when t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and rjsc.job_sn is null
                           then t.the_hour_cost_seconds end                          as empty_busy_time,
                   case
                       when t.online_state = 'REGISTERED' and t.work_state = 'IDLE'
                           then t.the_hour_cost_seconds end                          as idle_time,
                   case
                       when t.online_state = 'REGISTERED' and t.work_state = 'CHARGING'
                           then t.the_hour_cost_seconds end                          as charging_time,
                   case
                       when t.online_state = 'REGISTERED' and t.work_state = 'LOCKED'
                           then t.the_hour_cost_seconds end                          as lock_time,
                   case when t.work_state = 'ERROR' then t.the_hour_cost_seconds end as error_time,
                   t.state_id,
                   t.the_hour_cost_seconds
            from (select t1.hour_start_time,
                         t1.next_hour_start_time,
                         t2.robot_code
                  from (select h.hour_start_time,
                               h.next_hour_start_time
                        from (select th.day_hours                               as hour_start_time,
                                     DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
                              from (SELECT DATE_FORMAT(
                                                   DATE_SUB(DATE_FORMAT(current_date(), '%Y-%m-%d 00:00:00'), INTERVAL
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
                           left join
                       (select br.robot_code
                        from phoenix_basic.basic_robot br
                        where br.usage_state = 'using') t2 on 1) tt
                     left join ({tb}) t
            on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and t.robot_code = tt.robot_code
                left join (select DISTINCT job_sn from phoenix_rss.transport_order_carrier_job) rjsc on rjsc.job_sn = t.job_sn) t
      group by t.hour_start_time, t.robot_code) t
group by hour_value
























-----------------------------------------------------------------------------------------------------------------


set @now_start_time = '2022-08-24 00:00:00.000000000';
set @now_end_time = '2022-08-24 23:59:59.999999999';


select 
DATE_FORMAT(t.hour_start_time, '%Y-%m-%d %H:00:00') as hour_value,
coalesce(sum(loading_busy_time)/sum(total_time),0) as utilization_rate
from   
(select t.hour_start_time,
	   t.robot_code,
	   coalesce(sum(t.loading_busy_time), 0)                       as loading_busy_time,
	   (case
                                                    when HOUR(t.hour_start_time) = HOUR(sysdate())
                                                        then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t.hour_start_time)
                                                    else 3600 end) as total_time  
from 
(select tt.hour_start_time,
             tt.next_hour_start_time,
             tt.robot_code,
             case
                 when t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and rjsc.job_sn is not null
                     then t.the_hour_cost_seconds end                          as loading_busy_time,
             case
                 when t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and rjsc.job_sn is null
                     then t.the_hour_cost_seconds end                          as empty_busy_time,
             case
                 when t.online_state = 'REGISTERED' and t.work_state = 'IDLE'
                     then t.the_hour_cost_seconds end                          as idle_time,
             case
                 when t.online_state = 'REGISTERED' and t.work_state = 'CHARGING'
                     then t.the_hour_cost_seconds end                          as charging_time,
             case
                 when t.online_state = 'REGISTERED' and t.work_state = 'LOCKED'
                     then t.the_hour_cost_seconds end                          as lock_time,
             case when t.work_state = 'ERROR' then t.the_hour_cost_seconds end as error_time,
             t.state_id,
             t.the_hour_cost_seconds			 
      from (select t1.hour_start_time,
                   t1.next_hour_start_time,
                   t2.robot_code
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
                     left join
                 (select br.robot_code
                  from phoenix_basic.basic_robot br
						   where br.usage_state='using') t2 on 1) tt
               left join qt_smartreport.qt_hour_robot_state_time_detail_temp t
                         on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and t.robot_code = tt.robot_code 
               left join (select DISTINCT job_sn  from phoenix_rss.transport_order_carrier_job) rjsc on rjsc.job_sn = t.job_sn)t 
			   group by t.hour_start_time,t.robot_code)t
			   group by hour_value