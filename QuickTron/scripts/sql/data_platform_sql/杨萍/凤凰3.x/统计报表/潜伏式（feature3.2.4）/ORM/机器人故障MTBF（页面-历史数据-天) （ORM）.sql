-- 表1：qt_smartreport.qtr_day_robot_error_list_his


-- step1:删除相关数据（qtr_day_robot_error_list_his）
DELETE
FROM qt_smartreport.qtr_day_robot_error_list_his
where date_value = date_add(CURRENT_DATE(), interval -1 day);



-- step2:插入相关数据（qtr_day_robot_error_list_his）
insert into qt_smartreport.qtr_day_robot_error_list_his(create_time,update_time,date_value, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object,stat_start_time,stat_end_time)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
date_add(CURRENT_DATE(), interval -1 day) as date_value,
t1.id                                     as error_id,
t1.error_code,
t1.start_time,
t1.end_time,
t1.warning_spec,
t1.alarm_module,
t1.alarm_service,
t1.alarm_type,
t1.alarm_level,
t1.alarm_detail,
t1.param_value,
t1.job_order,
t1.robot_job,
t1.robot_code,
t1.device_code,
t1.server_code,
t1.transport_object,
case when t1.start_time < date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') then date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') else t1.start_time end as stat_start_time,
case when t1.end_time is null or t1.end_time >= date_format(current_date(), '%Y-%m-%d 00:00:00') then date_format(current_date(), '%Y-%m-%d 00:00:00') else t1.end_time end as stat_end_time
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') and start_time < date_format(current_date(), '%Y-%m-%d 00:00:00') and
               coalesce(end_time, sysdate()) < date_format(current_date(), '%Y-%m-%d 00:00:00')) or
              (start_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') and start_time < date_format(current_date(), '%Y-%m-%d 00:00:00') and
               coalesce(end_time, sysdate()) >= date_format(current_date(), '%Y-%m-%d 00:00:00')) or
              (start_time < date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') and coalesce(end_time, sysdate()) >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') and
               coalesce(end_time, sysdate()) < date_format(current_date(), '%Y-%m-%d 00:00:00')) or
              (start_time < date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') and coalesce(end_time, sysdate()) >= date_format(current_date(), '%Y-%m-%d 00:00:00'))
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') and start_time < date_format(current_date(), '%Y-%m-%d 00:00:00') and
                              coalesce(end_time, sysdate()) < date_format(current_date(), '%Y-%m-%d 00:00:00')) or
                             (start_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') and start_time < date_format(current_date(), '%Y-%m-%d 00:00:00') and
                              coalesce(end_time, sysdate()) >= date_format(current_date(), '%Y-%m-%d 00:00:00')) or
                             (start_time < date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') and coalesce(end_time, sysdate()) >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') and
                              coalesce(end_time, sysdate()) < date_format(current_date(), '%Y-%m-%d 00:00:00')) or
                             (start_time < date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') and coalesce(end_time, sysdate()) >= date_format(current_date(), '%Y-%m-%d 00:00:00'))
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id



-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_day_robot_error_list_his;
insert into qt_smartreport.qtr_day_robot_error_list_his(create_time,update_time,date_value, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object,stat_start_time,stat_end_time)
select created_time as create_time,updated_time as update_time,date_value, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object,stat_start_time,stat_end_time
from qt_smartreport.qt_day_robot_error_list_his;



--------------------------------------------------------------------------------
-- 表2：qt_smartreport.qtr_day_robot_error_mtbf_his

-- step1:删除相关数据（qtr_day_robot_error_mtbf_his）
DELETE
FROM qt_smartreport.qtr_day_robot_error_mtbf_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);



-- step2:插入相关数据（qtr_day_robot_error_mtbf_his）
insert into qt_smartreport.qtr_day_robot_error_mtbf_his(create_time,update_time,date_value,robot_code,theory_run_duration,error_duration,error_num,mtbf,accum_theory_run_duration,accum_error_duration,accum_error_num,accum_mtbf)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
date_add(CURRENT_DATE(), interval -1 day) as date_value,
br.robot_code,
COALESCE(t1.theory_run_duration,0) as theory_run_duration,
COALESCE(t2.error_duration,0) as error_duration,
COALESCE(t2.error_num,0) as error_num,
case when COALESCE(t2.error_num,0) != 0 then (COALESCE(t1.theory_run_duration,0)-COALESCE(t2.error_duration,0))/COALESCE(t2.error_num,0) else null end as mtbf,
COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0) as accum_theory_run_duration,
COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0) as accum_error_duration,
COALESCE(t3.accum_error_num,0) as accum_error_num,
case when COALESCE(t3.accum_error_num,0) != 0 then ((COALESCE(t4.accum_theory_run_duration,0)+COALESCE(t1.theory_run_duration,0))-(COALESCE(t4.accum_error_duration,0)+COALESCE(t2.error_duration,0)))/COALESCE(t3.accum_error_num,0) else null end as accum_mtbf
from(select distinct robot_code from phoenix_basic.basic_robot)br
left join 				
(select 
br.robot_code,
COALESCE(t1.theory_run_duration,0) as theory_run_duration
from 
(select distinct robot_code from phoenix_basic.basic_robot)br
left join 
(select 
ts.robot_code,
sum(stat_state_duration) as theory_run_duration
from 
(select 
t1.robot_code,
t2.id              as                           state_id,
t2.create_time     as                           state_create_time,
t2.network_state,
t2.online_state,
t2.work_state,
t2.job_sn,
t2.cause,
t2.is_error, 
t2.duration / 1000 as                           duration,
case when sysdate() < date_format(current_date(), '%Y-%m-%d 00:00:00') then UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, sysdate())) - UNIX_TIMESTAMP(date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00')) else UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, date_format(current_date(), '%Y-%m-%d 00:00:00'))) - UNIX_TIMESTAMP(date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00')) end stat_state_duration				
from 
(select 
robot_code, max(id) as before_the_hour_last_id 
from phoenix_rms.robot_state_history
where create_time < date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00')
group by robot_code)t1 
left join phoenix_rms.robot_state_history t2 on t2.robot_code=t1.robot_code and t2.id=t1.before_the_hour_last_id
left join 
(select 
robot_code, min(create_time) as the_hour_first_create_time
from phoenix_rms.robot_state_history
where create_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') and create_time < date_format(current_date(), '%Y-%m-%d 00:00:00')
group by robot_code)t3 on t3.robot_code=t1.robot_code

union all 

select 
t4.robot_code,	   
t4.id              as           state_id,
t4.create_time     as           state_create_time,
t4.network_state,
t4.online_state,
t4.work_state,
t4.job_sn,
t4.cause,
t4.is_error, 
t4.duration / 1000 as           duration,
case when t5.the_hour_last_id is not null and sysdate() >= date_format(current_date(), '%Y-%m-%d 00:00:00') then UNIX_TIMESTAMP(date_format(current_date(), '%Y-%m-%d 00:00:00'))-UNIX_TIMESTAMP(t4.create_time)
when t5.the_hour_last_id is not null and sysdate() < date_format(current_date(), '%Y-%m-%d 00:00:00') then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t4.create_time)
else t4.duration / 1000 end stat_state_duration
from 
(select 
*
from phoenix_rms.robot_state_history 
where create_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') and create_time < date_format(current_date(), '%Y-%m-%d 00:00:00'))t4 
left join 
(select 
robot_code, 
max(id) as the_hour_last_id,
max(create_time) as the_hour_last_create_time   
from phoenix_rms.robot_state_history
where create_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00') and create_time < date_format(current_date(), '%Y-%m-%d 00:00:00')
group by robot_code)t5 on t5.robot_code=t4.robot_code and t5.the_hour_last_id = t4.id)ts 	
where ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1
group by ts.robot_code)t1 on t1.robot_code=br.robot_code)t1 on t1.robot_code=br.robot_code
left join 
(select robot_code,
sum(unix_timestamp(stat_end_time)-unix_timestamp(stat_start_time)) as error_duration,
count(distinct error_id) as error_num
FROM qt_smartreport.qtr_day_robot_error_list_his
where date_value=date_add(CURRENT_DATE(), interval -1 day)
group by robot_code)t2 on t2.robot_code=br.robot_code		 			
left join 
(select robot_code,count(distinct error_id) as accum_error_num 
FROM qt_smartreport.qtr_day_robot_error_list_his
where date_value<=date_add(CURRENT_DATE(), interval -1 day)
group by robot_code)t3 on t3.robot_code=br.robot_code				
left join 
(select robot_code ,accum_theory_run_duration,accum_error_duration 
from qt_smartreport.qtr_day_robot_error_mtbf_his
where date_value=date_add(CURRENT_DATE(), interval -2 day))t4 on t4.robot_code=br.robot_code




-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_day_robot_error_mtbf_his;
insert into qt_smartreport.qtr_day_robot_error_mtbf_his(create_time,update_time,date_value,robot_code,theory_run_duration,error_duration,error_num,mtbf,accum_theory_run_duration,accum_error_duration,accum_error_num,accum_mtbf)
select created_time as create_time,updated_time as update_time,date_value,robot_code,theory_run_duration,error_duration,error_num,mtbf,accum_theory_run_duration,accum_error_duration,accum_error_num,accum_mtbf
from qt_smartreport.qt_day_robot_error_mtbf_his;



