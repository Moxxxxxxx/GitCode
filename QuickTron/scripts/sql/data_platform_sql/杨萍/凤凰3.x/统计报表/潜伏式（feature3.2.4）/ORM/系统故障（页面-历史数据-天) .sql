#机器人故障集合：
#1、故障等级>=3
#2、alarm_module in ('system', 'server')


#####################################当天进入计算的数据
set @now_start_time = date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000');
set @now_end_time = date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 23:59:59.999999999');
set @next_start_time = date_format(current_date(), '%Y-%m-%d 00:00:00.000000000');

select date_add(current_date(), interval -1 day) as date_value,
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
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and (
        (start_time >= @now_start_time and start_time < @next_start_time and
         coalesce(end_time, sysdate()) < @next_start_time) or
        (start_time >= @now_start_time and start_time < @next_start_time and
         coalesce(end_time, sysdate()) >= @next_start_time) or
        (start_time < @now_start_time and coalesce(end_time, sysdate()) >= @now_start_time and
         coalesce(end_time, sysdate()) < @next_start_time) or
        (start_time < @now_start_time and coalesce(end_time, sysdate()) >= @next_start_time)
    )

########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################

##按alarm_service对故障时间段进行时间去重


set @now_start_time = date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000');
set @now_end_time = date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 23:59:59.999999999');
set @next_start_time = date_format(current_date(), '%Y-%m-%d 00:00:00.000000000');



select date_add(current_date(), interval -1 day) as date_value,
       t1.hour_start_time,
       t1.next_hour_start_time,
       t2.alarm_service,
       sum(case
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
           end)                                     the_hour_cost_seconds

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
         inner join(select t3.alarm_service,
                           t3.error_id,
                           t3.start_time,
                           t3.end_time,
                           t3.next_error_id,
                           t3.next_error_start_time,
                           case
                               when t3.start_time < @now_start_time then @now_start_time
                               else t3.start_time end                                              stat_start_time,
                           case
                               when COALESCE(t3.end_time, sysdate()) <=
                                    COALESCE(t3.next_error_start_time, t3.end_time, sysdate())
                                   then COALESCE(t3.end_time, sysdate())
                               when COALESCE(t3.end_time, sysdate()) >
                                    COALESCE(t3.next_error_start_time, t3.end_time, sysdate()) and
                                    COALESCE(t3.next_error_start_time, t3.end_time, sysdate()) < @now_start_time
                                   then @now_start_time
                               else COALESCE(t3.next_error_start_time, t3.end_time, sysdate()) end stat_end_time
                    from (select t1.alarm_service,
                                 t1.error_id,
                                 t1.start_time,
                                 t1.end_time,
                                 min(t2.error_id)   as next_error_id,
                                 min(t2.start_time) as next_error_start_time
                          from (select alarm_service,
                                       error_id,
                                       start_time,
                                       end_time
                                from qt_smartreport.qt_day_sys_error_detail_his t
                                where t.date_value = date_add(CURRENT_DATE(), interval -1 day)
                                  and t.alarm_level>=3) t1
                                   left join
                               (select alarm_service,
                                       error_id,
                                       start_time,
                                       end_time
                                from qt_smartreport.qt_day_sys_error_detail_his t
                                where t.date_value = date_add(CURRENT_DATE(), interval -1 day)
                                  and t.alarm_level >=3) t2
                               on t2.alarm_service = t1.alarm_service and t2.start_time > t1.start_time
                          group by t1.alarm_service, t1.error_id, t1.start_time, t1.end_time) t3) t2 on
    ((t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
      t2.stat_end_time < t1.next_hour_start_time)
        or (t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
            t2.stat_end_time >= t1.next_hour_start_time)
        or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and
            t2.stat_end_time < t1.next_hour_start_time)
        or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.next_hour_start_time))
group by t1.hour_start_time,
         t1.next_hour_start_time,
         t2.alarm_service


########################################################################################################################
########################################################################################################################
########################################################################################################################
##将所有系统看作一个整体对故障时间段进行时间去重


set @now_start_time = date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000');
set @now_end_time = date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 23:59:59.999999999');
set @next_start_time = date_format(current_date(), '%Y-%m-%d 00:00:00.000000000');



select date_add(current_date(), interval -1 day) as date_value,
       t1.hour_start_time,
       t1.next_hour_start_time,
       sum(case
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
           end)                                     the_hour_cost_seconds

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
         inner join(select 
                           t3.error_id,
                           t3.start_time,
                           t3.end_time,
                           t3.next_error_id,
                           t3.next_error_start_time,
                           case
                               when t3.start_time < @now_start_time then @now_start_time
                               else t3.start_time end                                              stat_start_time,
                           case
                               when COALESCE(t3.end_time, sysdate()) <=
                                    COALESCE(t3.next_error_start_time, t3.end_time, sysdate())
                                   then COALESCE(t3.end_time, sysdate())
                               when COALESCE(t3.end_time, sysdate()) >
                                    COALESCE(t3.next_error_start_time, t3.end_time, sysdate()) and
                                    COALESCE(t3.next_error_start_time, t3.end_time, sysdate()) < @now_start_time
                                   then @now_start_time
                               else COALESCE(t3.next_error_start_time, t3.end_time, sysdate()) end stat_end_time
                    from (select 
                                 t1.error_id,
                                 t1.start_time,
                                 t1.end_time,
                                 min(t2.error_id)   as next_error_id,
                                 min(t2.start_time) as next_error_start_time
                          from (select 
                                       error_id,
                                       start_time,
                                       end_time
                                from qt_smartreport.qt_day_sys_error_detail_his t
                                where t.date_value = date_add(CURRENT_DATE(), interval -1 day)
                                  and t.alarm_level>=3) t1
                                   left join
                               (select 
                                       error_id,
                                       start_time,
                                       end_time
                                from qt_smartreport.qt_day_sys_error_detail_his t
                                where t.date_value = date_add(CURRENT_DATE(), interval -1 day)
                                  and t.alarm_level>=3) t2
                               on t2.start_time > t1.start_time
                          group by t1.error_id, t1.start_time, t1.end_time) t3) t2 on
    ((t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
      t2.stat_end_time < t1.next_hour_start_time)
        or (t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
            t2.stat_end_time >= t1.next_hour_start_time)
        or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and
            t2.stat_end_time < t1.next_hour_start_time)
        or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.next_hour_start_time))
group by t1.hour_start_time,
         t1.next_hour_start_time




#############################################################################################################
#############################################################################################################
#############################################################################################################



#step1:建表（qt_day_sys_error_detail_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_day_sys_error_detail_his
(
    `id`               bigint(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`       date         NOT NULL COMMENT '日期',
    `error_id`         bigint(20)   NOT NULL COMMENT '故障通知ID',
    `error_code`       varchar(255) NOT NULL COMMENT '故障码',
    `start_time`       datetime(6)           DEFAULT NULL COMMENT '开始时间-告警触发时间',
    `end_time`         datetime(6)           DEFAULT NULL COMMENT '结束时间-告警结束时间',
    `warning_spec`     varchar(255)          DEFAULT NULL COMMENT '故障分类',
    `alarm_module`     varchar(255)          DEFAULT NULL COMMENT '告警模块-外设、系统、服务、机器人',
    `alarm_service`    varchar(255)          DEFAULT NULL COMMENT '告警服务',
    `alarm_type`       varchar(255)          DEFAULT NULL COMMENT '告警对象类型',
    `alarm_level`      int(11)               DEFAULT NULL COMMENT '告警级别',
    `alarm_detail`     varchar(255)          DEFAULT NULL COMMENT '故障详情',
    `param_value`      varchar(255)          DEFAULT NULL COMMENT '参数值',
    `job_order`        varchar(255)          DEFAULT NULL COMMENT '关联作业单',
    `robot_job`        varchar(255)          DEFAULT NULL COMMENT '关联机器人任务',
    `robot_code`       varchar(255)          DEFAULT NULL COMMENT '关联机器人编号',
    `device_code`      varchar(255)          DEFAULT NULL COMMENT '关联设备编码',
    `server_code`      varchar(255)          DEFAULT NULL COMMENT '关联服务器',
    `transport_object` varchar(255)          DEFAULT NULL COMMENT '关联搬运对象',
    `created_time`     timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`     timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_error_id (`error_id`),
    key idx_error_code (`error_code`),
    key idx_start_time (`start_time`),
    key idx_end_time (`end_time`),
    key idx_warning_spec (`warning_spec`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='系统故障结果集（T+1）';
	
		


#step2:删除相关数据（qt_day_sys_error_detail_his）
DELETE
FROM qt_smartreport.qt_day_sys_error_detail_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);




#step3:插入相关数据（qt_day_sys_error_detail_his）
insert into qt_smartreport.qt_day_sys_error_detail_his(date_value, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object)
select date_add(current_date(), interval -1 day) as date_value,
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
where alarm_module in ('system', 'server')
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
          )
		  
		  

#step4:建表（qt_day_sys_end_error_detail_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_day_sys_end_error_detail_his
(
    `id`               bigint(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`       date         NOT NULL COMMENT '日期',
    `error_id`         bigint(20)   NOT NULL COMMENT '故障通知ID',
    `error_code`       varchar(255) NOT NULL COMMENT '故障码',
    `start_time`       datetime(6)           DEFAULT NULL COMMENT '开始时间-告警触发时间',
    `end_time`         datetime(6)           DEFAULT NULL COMMENT '结束时间-告警结束时间',
    `warning_spec`     varchar(255)          DEFAULT NULL COMMENT '故障分类',
    `alarm_module`     varchar(255)          DEFAULT NULL COMMENT '告警模块-外设、系统、服务、机器人',
    `alarm_service`    varchar(255)          DEFAULT NULL COMMENT '告警服务',
    `alarm_type`       varchar(255)          DEFAULT NULL COMMENT '告警对象类型',
    `alarm_level`      int(11)               DEFAULT NULL COMMENT '告警级别',
    `alarm_detail`     varchar(255)          DEFAULT NULL COMMENT '故障详情',
    `param_value`      varchar(255)          DEFAULT NULL COMMENT '参数值',
    `job_order`        varchar(255)          DEFAULT NULL COMMENT '关联作业单',
    `robot_job`        varchar(255)          DEFAULT NULL COMMENT '关联机器人任务',
    `robot_code`       varchar(255)          DEFAULT NULL COMMENT '关联机器人编号',
    `device_code`      varchar(255)          DEFAULT NULL COMMENT '关联设备编码',
    `server_code`      varchar(255)          DEFAULT NULL COMMENT '关联服务器',
    `transport_object` varchar(255)          DEFAULT NULL COMMENT '关联搬运对象',
    `created_time`     timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`     timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_error_id (`error_id`),
    key idx_error_code (`error_code`),
    key idx_start_time (`start_time`),
    key idx_end_time (`end_time`),
    key idx_warning_spec (`warning_spec`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='系统当天结束故障集合（T+1）';
	
		


#step5:删除相关数据（qt_day_sys_end_error_detail_his）
DELETE
FROM qt_smartreport.qt_day_sys_end_error_detail_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);




#step6:插入相关数据（qt_day_sys_end_error_detail_his）
insert into qt_smartreport.qt_day_sys_end_error_detail_his(date_value, error_id, error_code, start_time, end_time,
                                                             warning_spec, alarm_module, alarm_service, alarm_type,
                                                             alarm_level, alarm_detail, param_value, job_order,
                                                             robot_job, robot_code, device_code, server_code,
                                                             transport_object)
select date_add(current_date(), interval -1 day) as date_value,
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
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and end_time is not null
  and end_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')
  and end_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')		  
  
  