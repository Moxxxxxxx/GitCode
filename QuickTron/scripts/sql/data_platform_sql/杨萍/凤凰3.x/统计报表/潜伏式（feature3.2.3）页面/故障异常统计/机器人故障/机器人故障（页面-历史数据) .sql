#机器人类故障收敛规则：
#1、故障等级>=3（现场需要人工介入的机器人故障）
#2、机器人多条故障均没有结束时间or结束时间相同，取第一条


#####################################当天进入计算的数据
set @now_start_time=date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000');
set @now_end_time=date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 23:59:59.999999999');
set @next_start_time=date_format(current_date(), '%Y-%m-%d 00:00:00.000000000');

select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
		and (
		(start_time>=@now_start_time and start_time<@next_start_time and coalesce(end_time, sysdate())<@next_start_time)or 
		(start_time>=@now_start_time and start_time<@next_start_time and coalesce(end_time, sysdate())>=@next_start_time)or 
		(start_time<@now_start_time and coalesce(end_time, sysdate())>=@now_start_time and coalesce(end_time, sysdate())<@next_start_time)or 
		(start_time<@now_start_time and coalesce(end_time, sysdate())>=@next_start_time)
		)

############################当天收敛结果集
set @now_start_time=date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000');
set @now_end_time=date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 23:59:59.999999999');
set @next_start_time=date_format(current_date(), '%Y-%m-%d 00:00:00.000000000');


select date_add(current_date(), interval -1 day) as date_value,
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
              (start_time >= @now_start_time and start_time < @next_start_time and
               coalesce(end_time, sysdate()) < @next_start_time) or
              (start_time >= @now_start_time and start_time < @next_start_time and
               coalesce(end_time, sysdate()) >= @next_start_time) or
              (start_time < @now_start_time and coalesce(end_time, sysdate()) >= @now_start_time and
               coalesce(end_time, sysdate()) < @next_start_time) or
              (start_time < @now_start_time and coalesce(end_time, sysdate()) >= @next_start_time)
          )) t1
         inner join (select robot_code,
                            COALESCE(end_time, '未结束') as end_time,
                            min(id)                      as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
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
                     group by robot_code, COALESCE(end_time, '未结束')) t2
                    on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id


########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################


#step1:建表（qt_day_robot_error_detail_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_day_robot_error_detail_his
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
    key idx_robot_code (`robot_code`),
    key idx_warning_spec (`warning_spec`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类故障收敛结果集（T+1）';
	
		


#step2:删除相关数据（qt_day_robot_error_detail_his）
DELETE
FROM qt_smartreport.qt_day_robot_error_detail_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);




#step3:插入相关数据（qt_day_robot_error_detail_his）
insert into qt_smartreport.qt_day_robot_error_detail_his(date_value, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object)
select date_add(current_date(), interval -1 day) as date_value,
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




#step4:建表（qt_hour_robot_error_time_detail_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_hour_robot_error_time_detail_his
(
    `id`                    int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`            date         NOT NULL COMMENT '日期',
    `hour_start_time`       datetime     NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time`  datetime     NOT NULL COMMENT '下一个小时开始时间',
    `error_id`              bigint(20)   NOT NULL COMMENT '故障通知ID',
    `error_code`            varchar(255) NOT NULL COMMENT '故障码',
    `start_time`            datetime(6)           DEFAULT NULL COMMENT '开始时间-告警触发时间',
    `end_time`              datetime(6)           DEFAULT NULL COMMENT '结束时间-告警结束时间',
    `warning_spec`          varchar(255)          DEFAULT NULL COMMENT '故障分类',
    `alarm_module`          varchar(255)          DEFAULT NULL COMMENT '告警模块-外设、系统、服务、机器人',
    `alarm_service`         varchar(255)          DEFAULT NULL COMMENT '告警服务',
    `alarm_type`            varchar(255)          DEFAULT NULL COMMENT '告警对象类型',
    `alarm_level`           int(11)               DEFAULT NULL COMMENT '告警级别',
    `alarm_detail`          varchar(255)          DEFAULT NULL COMMENT '故障详情',
    `param_value`           varchar(255)          DEFAULT NULL COMMENT '参数值',
    `job_order`             varchar(255)          DEFAULT NULL COMMENT '关联作业单',
    `robot_job`             varchar(255)          DEFAULT NULL COMMENT '关联机器人任务',
    `robot_code`            varchar(255)          DEFAULT NULL COMMENT '关联机器人编号',
    `device_code`           varchar(255)          DEFAULT NULL COMMENT '关联设备编码',
    `server_code`           varchar(255)          DEFAULT NULL COMMENT '关联服务器',
    `transport_object`      varchar(255)          DEFAULT NULL COMMENT '关联搬运对象',
    `the_hour_cost_seconds` decimal(30, 6)        DEFAULT NULL COMMENT '在该小时内时长（秒）',
    `created_time`          timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`          timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_error_id (`error_id`),
    key idx_error_code (`error_code`),
    key idx_robot_code (`robot_code`),
    key idx_warning_spec (`warning_spec`)

)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类收敛故障在小时内持续时长明细（T+1）';
	
	
	
	


#step5:删除相关数据（qt_hour_robot_error_time_detail_his）
DELETE
FROM qt_smartreport.qt_hour_robot_error_time_detail_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);



#step6:插入相关数据(qt_hour_robot_error_time_detail_his)
insert into qt_smartreport.qt_hour_robot_error_time_detail_his(date_value,hour_start_time, next_hour_start_time, error_id, error_code, start_time, end_time,warning_spec,alarm_module,alarm_service,alarm_type,alarm_level,alarm_detail,param_value,job_order,robot_job,robot_code,device_code,server_code,transport_object, the_hour_cost_seconds)
select date_add(current_date(), interval -1 day) as date_value,
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
      FROM qt_smartreport.qt_day_robot_error_detail_his t
      where date_value = date_add(CURRENT_DATE(), interval -1 day)) t2 on
         ((t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
           t2.stat_end_time < t1.next_hour_start_time)
             or (t2.stat_start_time >= t1.hour_start_time and t2.stat_start_time < t1.next_hour_start_time and
                 t2.stat_end_time >= t1.next_hour_start_time)
             or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.hour_start_time and
                 t2.stat_end_time < t1.next_hour_start_time)
             or (t2.stat_start_time < t1.hour_start_time and t2.stat_end_time >= t1.next_hour_start_time))






#step7:建表（qt_day_robot_end_error_detail_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_day_robot_end_error_detail_his
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
    key idx_robot_code (`robot_code`),
    key idx_warning_spec (`warning_spec`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类当天结束故障集合（T+1）';
	
		


#step8:删除相关数据（qt_day_robot_end_error_detail_his）
DELETE
FROM qt_smartreport.qt_day_robot_end_error_detail_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);




#step9:插入相关数据（qt_day_robot_end_error_detail_his）
insert into qt_smartreport.qt_day_robot_end_error_detail_his(date_value, error_id, error_code, start_time, end_time,
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
where alarm_module = 'robot'
  and alarm_level >= 3
  and end_time is not null
  and end_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')
  and end_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')