select 
       t.hour_start_time as hour_value,
       t.robot_code,
       t.robot_type_code,
       t.robot_type_name,
       t.uptime_state_rate,
       t.uptime_state_duration,
       t.uptime_state_rate_fenmu,
       t.utilization_rate,
       t.utilization_duration,
       t.utilization_rate_fenmu,
       t.loading_busy_state_duration,
       t.empty_busy_state_duration,
       t.charging_state_duration,
       t.idle_state_duration,
       t.locked_state_duration,
       t.error_state_duration,
       t.offline_duration
from qt_smartreport.qt_hour_robot_state_duration_stat_his t
         inner join (select distinct robot_code from phoenix_basic.basic_robot where usage_state = 'using') br
                    on br.robot_code = t.robot_code
where t.hour_start_time BETWEEN {start_time} and {end_time}					
union all
select
       tbr.hour_start_time as hour_value,
       tbr.robot_code,
       tbr.robot_type_code,
       tbr.robot_type_name,
       COALESCE(t1.uptime_state_duration, 0) / 3600                                 as uptime_state_rate,
       COALESCE(t1.uptime_state_duration, 0)                                        as uptime_state_duration,
       3600                                                                         as uptime_state_rate_fenmu,
       COALESCE(t1.loading_busy_state_duration, 0) / 3600                           as utilization_rate,
       COALESCE(t1.loading_busy_state_duration, 0)                                  as utilization_duration,
       3600                                                                         as utilization_rate_fenmu,
       COALESCE(t1.loading_busy_state_duration, 0)                                  as loading_busy_state_duration,
       COALESCE(t1.empty_busy_state_duration, 0)                                    as empty_busy_state_duration,
       COALESCE(t1.charging_state_duration, 0)                                      as charging_state_duration,
       COALESCE(t1.idle_state_duration, 0)                                          as idle_state_duration,
       COALESCE(t1.locked_state_duration, 0)                                        as locked_state_duration,
       COALESCE(t1.error_state_duration, 0)                                         as error_state_duration,
       3600 - COALESCE(t1.loading_busy_state_duration, 0) - COALESCE(t1.empty_busy_state_duration, 0) -
       COALESCE(t1.charging_state_duration, 0) - COALESCE(t1.idle_state_duration, 0) -
       COALESCE(t1.locked_state_duration, 0) - COALESCE(t1.error_state_duration, 0) as offline_duration
from (select t2.robot_code,
             t2.robot_type_code,
             t2.robot_type_name,
             t1.hour_start_time,
             t1.next_hour_start_time
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
           (select br.robot_code, brt.robot_type_code, brt.robot_type_name
            from phoenix_basic.basic_robot br
                     left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
            where br.usage_state = 'using') t2 on 1) tbr
         left join
     (select ts.hour_start_time,
             ts.next_hour_start_time,
             ts.robot_code,
             sum(case when ts.is_uptime_state = 1 then ts.stat_duration end)       as uptime_state_duration,
             sum(case when ts.is_loading_busy_state = 1 then ts.stat_duration end) as loading_busy_state_duration,
             sum(case when ts.is_empty_busy_state = 1 then ts.stat_duration end)   as empty_busy_state_duration,
             sum(case when ts.is_busy_state = 1 then ts.stat_duration end)         as busy_state_duration,
             sum(case when ts.is_charging_state = 1 then ts.stat_duration end)     as charging_state_duration,
             sum(case when ts.is_idle_state = 1 then ts.stat_duration end)         as idle_state_duration,
             sum(case when ts.is_locked_state = 1 then ts.stat_duration end)       as locked_state_duration,
             sum(case when ts.is_error_state = 1 then ts.stat_duration end)        as error_state_duration
      from (select t.hour_start_time,
                   t.next_hour_start_time,
                   t.robot_code,
                   t.state_id,
                   t.online_state,
                   t.work_state,
                   t.job_sn,
                   case
                       when (t.work_state in ('BUSY', 'CHARGING')) or (t.work_state = 'ERROR' and t.job_sn is not null)
                           then 1
                       else 0 end                                                                      is_uptime_state,
                   case
                       when t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and
                            ((tjh.job_sn is not null and tjh.job_type = 'CUSTOMIZE') or
                             (tj.job_sn is not null and tj.job_type = 'CUSTOMIZE')) then 1
                       else 0 end                                                                      is_loading_busy_state,
                   case
                       when t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and
                            ((tjh.job_sn is not null and tjh.job_type != 'CUSTOMIZE') or
                             (tj.job_sn is not null and tj.job_type != 'CUSTOMIZE')) then 1
                       else 0 end                                                                      is_empty_busy_state,
                   case when t.online_state = 'REGISTERED' and t.work_state = 'BUSY' then 1 else 0 end is_busy_state,
                   case when t.online_state = 'REGISTERED' and t.work_state = 'IDLE' then 1 else 0 end is_idle_state,
                   case
                       when t.online_state = 'REGISTERED' and t.work_state = 'CHARGING' then 1
                       else 0 end                                                                      is_charging_state,
                   case
                       when t.online_state = 'REGISTERED' and t.work_state = 'LOCKED' then 1
                       else 0 end                                                                      is_locked_state,
                   case when t.work_state = 'ERROR' then 1 else 0 end                                  is_error_state,
                   t.duration,
                   t.stat_duration
            from ({tb_hour_robot_state_detail_duration}) t  -- hour_robot_state_detail_duration.sql
                     left join (select job_sn, job_type from phoenix_rms.job_history) tjh on tjh.job_sn = t.job_sn
                     left join (select job_sn, job_type from phoenix_rms.job) tj on tj.job_sn = t.job_sn) ts
      group by ts.hour_start_time, ts.next_hour_start_time, ts.robot_code) t1
     on t1.robot_code = tbr.robot_code and t1.hour_start_time = tbr.hour_start_time and
        t1.next_hour_start_time = tbr.next_hour_start_time	  	
where tbr.hour_start_time BETWEEN {start_time} and {end_time}			