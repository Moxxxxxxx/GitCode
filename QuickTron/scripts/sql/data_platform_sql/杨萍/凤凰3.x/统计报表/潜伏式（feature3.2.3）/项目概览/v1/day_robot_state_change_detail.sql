select t5.id,
       t5.create_time,
       t5.robot_code,
       t5.network_state,
       t5.online_state,
       t5.work_state,
       t5.job_sn,
       t5.cause,
       t6.id            as next_id,
       t6.create_time   as next_create_time,
       t6.network_state as next_network_state,
       t6.online_state  as next_online_state,
       t6.work_state    as next_work_state,
       t6.job_sn        as next_job_sn,
       t6.cause         as next_cause
from (select t2.id,
             t2.create_time,
             t2.robot_code,
             t2.network_state,
             t2.online_state,
             t2.work_state,
             t2.job_sn,
             t2.cause
      from (select robot_code, max(id) as before_day_last_id
            from phoenix_rms.robot_state_history
            where create_time < {now_start_time}
            group by robot_code) t1
               left join phoenix_rms.robot_state_history t2
                         on t2.robot_code = t1.robot_code and t2.id = t1.before_day_last_id) t5
         left join
     (select t4.id,
             t4.create_time,
             t4.robot_code,
             t4.network_state,
             t4.online_state,
             t4.work_state,
             t4.job_sn,
             t4.cause
      from (select robot_code, min(id) as the_day_first_id
            from phoenix_rms.robot_state_history
            where create_time >= {now_start_time}
            group by robot_code) t3
               left join phoenix_rms.robot_state_history t4
                         on t4.robot_code = t3.robot_code and t4.id = t3.the_day_first_id) t6
     on t6.robot_code = t5.robot_code
union all
select t3.id,
       t3.create_time,
       t3.robot_code,
       t3.network_state,
       t3.online_state,
       t3.work_state,
       t3.job_sn,
       t3.cause,
       t4.id            as next_id,
       t4.create_time   as next_create_time,
       t4.network_state as next_network_state,
       t4.online_state  as next_online_state,
       t4.work_state    as next_work_state,
       t4.job_sn        as next_job_sn,
       t4.cause         as next_cause
from (select t1.id,
             t1.create_time,
             t1.robot_code,
             t1.network_state,
             t1.online_state,
             t1.work_state,
             t1.job_sn,
             t1.cause,
             min(t2.id) as next_id
      from phoenix_rms.robot_state_history t1
               left join phoenix_rms.robot_state_history t2
                         on t2.robot_code = t1.robot_code and t2.create_time >= {now_start_time} and
                            t2.create_time > t1.create_time
      where t1.create_time >= {now_start_time}
      group by t1.id,
               t1.create_time,
               t1.robot_code,
               t1.network_state,
               t1.online_state,
               t1.work_state,
               t1.job_sn,
               t1.cause) t3
         left join phoenix_rms.robot_state_history t4 on t4.robot_code = t3.robot_code and t4.id = t3.next_id 
		 
		 
























-----------------------------------------------------------------------------------------------------------------


set @now_start_time = '2022-08-24 00:00:00.000000000';
set @now_end_time = '2022-08-24 23:59:59.999999999';



drop table if exists qt_smartreport.qt_day_robot_state_change_detail_temp;
create table qt_smartreport.qt_day_robot_state_change_detail_temp
as
select t5.id,
       t5.create_time,
       t5.robot_code,
       t5.network_state,
       t5.online_state,
       t5.work_state,
       t5.job_sn,
       t5.cause,
       t6.id            as next_id,
       t6.create_time   as next_create_time,
       t6.network_state as next_network_state,
       t6.online_state  as next_online_state,
       t6.work_state    as next_work_state,
       t6.job_sn        as next_job_sn,
       t6.cause         as next_cause
from (select t2.id,
             t2.create_time,
             t2.robot_code,
             t2.network_state,
             t2.online_state,
             t2.work_state,
             t2.job_sn,
             t2.cause
      from (select robot_code, max(id) as before_day_last_id
            from phoenix_rms.robot_state_history
            where create_time < @now_start_time
            group by robot_code) t1
               left join phoenix_rms.robot_state_history t2
                         on t2.robot_code = t1.robot_code and t2.id = t1.before_day_last_id) t5
         left join
     (select t4.id,
             t4.create_time,
             t4.robot_code,
             t4.network_state,
             t4.online_state,
             t4.work_state,
             t4.job_sn,
             t4.cause
      from (select robot_code, min(id) as the_day_first_id
            from phoenix_rms.robot_state_history
            where create_time >= @now_start_time
            group by robot_code) t3
               left join phoenix_rms.robot_state_history t4
                         on t4.robot_code = t3.robot_code and t4.id = t3.the_day_first_id) t6
     on t6.robot_code = t5.robot_code
union all
select t3.id,
       t3.create_time,
       t3.robot_code,
       t3.network_state,
       t3.online_state,
       t3.work_state,
       t3.job_sn,
       t3.cause,
       t4.id            as next_id,
       t4.create_time   as next_create_time,
       t4.network_state as next_network_state,
       t4.online_state  as next_online_state,
       t4.work_state    as next_work_state,
       t4.job_sn        as next_job_sn,
       t4.cause         as next_cause
from (select t1.id,
             t1.create_time,
             t1.robot_code,
             t1.network_state,
             t1.online_state,
             t1.work_state,
             t1.job_sn,
             t1.cause,
             min(t2.id) as next_id
      from phoenix_rms.robot_state_history t1
               left join phoenix_rms.robot_state_history t2
                         on t2.robot_code = t1.robot_code and t2.create_time >= @now_start_time and
                            t2.create_time > t1.create_time
      where t1.create_time >= @now_start_time
      group by t1.id,
               t1.create_time,
               t1.robot_code,
               t1.network_state,
               t1.online_state,
               t1.work_state,
               t1.job_sn,
               t1.cause) t3
         left join phoenix_rms.robot_state_history t4 on t4.robot_code = t3.robot_code and t4.id = t3.next_id 