-- 表1：qt_smartreport.qtr_day_robot_error_detail_his


-- step1:删除相关数据（qtr_day_robot_error_detail_his）
DELETE
FROM qt_smartreport.qtr_day_robot_error_detail_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);


-- step2:插入相关数据（qtr_day_robot_error_detail_his）
insert into qt_smartreport.qtr_day_robot_error_detail_his(create_time,update_time,date_value, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
date_add(current_date(), interval -1 day) as date_value,
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
t1.transport_object
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (
              (start_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000') and
               start_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000') and
               coalesce(end_time, sysdate()) < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')) or
              (start_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000') and
               start_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000') and
               coalesce(end_time, sysdate()) >= date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')) or
              (start_time < date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000') and
               coalesce(end_time, sysdate()) >=
               date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000') and
               coalesce(end_time, sysdate()) < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')) or
              (start_time < date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000') and
               coalesce(end_time, sysdate()) >= date_format(current_date(), '%Y-%m-%d 00:00:00.000000000'))
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (
                             (start_time >=
                              date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000') and
                              start_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000') and
                              coalesce(end_time, sysdate()) <
                              date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')) or
                             (start_time >=
                              date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000') and
                              start_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000') and
                              coalesce(end_time, sysdate()) >=
                              date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')) or
                             (start_time <
                              date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000') and
                              coalesce(end_time, sysdate()) >=
                              date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000') and
                              coalesce(end_time, sysdate()) <
                              date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')) or
                             (start_time <
                              date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000') and
                              coalesce(end_time, sysdate()) >=
                              date_format(current_date(), '%Y-%m-%d 00:00:00.000000000'))
                         )
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id



-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_day_robot_error_detail_his;
insert into qt_smartreport.qtr_day_robot_error_detail_his(create_time,update_time,date_value, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object)
select created_time as create_time,updated_time as update_time,date_value, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object
from qt_smartreport.qt_day_robot_error_detail_his;





--------------------------------------------------------------------------------
-- 表2：qt_smartreport.qtr_hour_robot_error_time_detail_his

-- step1:删除相关数据（qtr_hour_robot_error_time_detail_his）
DELETE
FROM qt_smartreport.qtr_hour_robot_error_time_detail_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);



-- step2:插入相关数据（qtr_hour_robot_error_time_detail_his）
insert into qt_smartreport.qtr_hour_robot_error_time_detail_his(create_time,update_time,date_value,hour_start_time, next_hour_start_time, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object, the_hour_cost_seconds)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
date_add(current_date(), interval -1 day) as date_value,
    t1.hour_start_time,
    t1.next_hour_start_time,
    t2.error_id,
    t2.error_code,
    t2.start_time,
    t2.end_time,
    t2.warning_spec,
    t2.alarm_module,
    t2.alarm_service,
    t2.alarm_type,
    t2.alarm_level,
    t2.alarm_detail,
    t2.param_value,
    t2.job_order,
    t2.robot_job,
    t2.robot_code,
    t2.device_code,
    t2.server_code,
	t2.transport_object,
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
        end                                      the_hour_cost_seconds
from (select th.day_hours                               as hour_start_time,
             DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
      from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(date_add(CURRENT_DATE(), interval -1 day), '%Y-%m-%d 00:00:00'),
                                        INTERVAL
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
                 (SELECT @u := -1) AS i) th) t1
         inner join
     (select t.*,
             case
                 when t.start_time <
                      date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')
                     then date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')
                 else t.start_time end          stat_start_time,
             coalesce(t.end_time, sysdate()) as stat_end_time
      FROM qt_smartreport.qtr_day_robot_error_detail_his t
      where date_value = date_add(CURRENT_DATE(), interval -1 day)) t2 on
         ((t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
           t2.stat_end_time < t1.next_hour_start_time)
             or (t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
                 t2.stat_end_time >= t1.next_hour_start_time)
             or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and
                 t2.stat_end_time < t1.next_hour_start_time)
             or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.next_hour_start_time))





-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_hour_robot_error_time_detail_his;
insert into qt_smartreport.qtr_hour_robot_error_time_detail_his(create_time,update_time,date_value,hour_start_time, next_hour_start_time, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object, the_hour_cost_seconds)
select created_time as create_time,updated_time as update_time,date_value,hour_start_time, next_hour_start_time, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object, the_hour_cost_seconds
from qt_smartreport.qt_hour_robot_error_time_detail_his;




--------------------------------------------------------------------------------
-- 表3：qt_smartreport.qtr_day_robot_end_error_detail_his

-- step1:删除相关数据（qtr_day_robot_end_error_detail_his）
DELETE
FROM qt_smartreport.qtr_day_robot_end_error_detail_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);



-- step2:插入相关数据（qtr_day_robot_end_error_detail_his）
insert into qt_smartreport.qtr_day_robot_end_error_detail_his(create_time,update_time,date_value, error_id, error_code, start_time, end_time,warning_spec, alarm_module, alarm_service, alarm_type,alarm_level, alarm_detail, param_value, job_order,robot_job, robot_code, device_code, server_code,transport_object)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
date_add(current_date(), interval -1 day) as date_value,
       id                                        as error_id,
       error_code,
       start_time,
       end_time,
       warning_spec,
       alarm_module,
       alarm_service,
       alarm_type,
       alarm_level,
       alarm_detail,
       param_value,
       job_order,
       robot_job,
       robot_code,
       device_code,
       server_code,
       transport_object
from phoenix_basic.basic_notification
where alarm_module = 'robot'
  and alarm_level >= 3
  and end_time is not null
  and end_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')
  and end_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')



-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_day_robot_end_error_detail_his;
insert into qt_smartreport.qtr_day_robot_end_error_detail_his(create_time,update_time,date_value, error_id, error_code, start_time, end_time,warning_spec, alarm_module, alarm_service, alarm_type,alarm_level, alarm_detail, param_value, job_order,robot_job, robot_code, device_code, server_code,transport_object)
select created_time as create_time,updated_time as update_time,date_value, error_id, error_code, start_time, end_time,warning_spec, alarm_module, alarm_service, alarm_type,alarm_level, alarm_detail, param_value, job_order,robot_job, robot_code, device_code, server_code,transport_object
from qt_smartreport.qt_day_robot_end_error_detail_his;