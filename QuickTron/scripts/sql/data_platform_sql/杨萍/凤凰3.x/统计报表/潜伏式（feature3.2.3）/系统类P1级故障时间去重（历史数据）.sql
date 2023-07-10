------------------------------------------------------------------------------------------------
--step1:建表（qt_notification_sys_p1_detail）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_sys_p1_detail
(
    `id`              int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`      date               DEFAULT NULL COMMENT '日期',
    `notification_id` varchar(100)       DEFAULT NULL COMMENT '通知ID',
    `error_code`      varchar(100)       DEFAULT NULL COMMENT '错误码',
    `alarm_module`    varchar(100)       DEFAULT NULL COMMENT '告警模块',
    `alarm_service`   varchar(100)       DEFAULT NULL COMMENT '告警服务',
    `alarm_type`      varchar(100)       DEFAULT NULL COMMENT '告警分类',
    `alarm_level`     varchar(100)       DEFAULT NULL COMMENT '告警级别',
    `warning_spec`    varchar(100)       DEFAULT NULL COMMENT '告警分类',
    `robot_code`      varchar(100)       DEFAULT NULL COMMENT '机器人编码',
    `job_order`       varchar(100)       DEFAULT NULL COMMENT '作业单编码',
    `device_code`     varchar(100)       DEFAULT NULL COMMENT '设备编码',
    `server_code`     varchar(100)       DEFAULT NULL COMMENT '服务器编码',
    `start_time`      datetime(6)        DEFAULT NULL COMMENT '开始时间-告警触发时间',
    `end_time`        datetime(6)        DEFAULT NULL COMMENT '结束时间-告警结束时间',
    `created_time`    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY `idx_date_value` (`date_value`),
	KEY `idx_warning_spec` (`warning_spec`),
	KEY `idx_start_time` (`start_time`),
	KEY `idx_end_time` (`end_time`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='系统类P1级故障通知明细';	
	
	

	
------------------------------------------------------------------------------------------------
--step2:删除当天相关数据（qt_notification_sys_p1_detail）
DELETE
FROM qt_smartreport.qt_notification_sys_p1_detail
WHERE date_value = date(date_add(sysdate(), interval -1 day));  	




------------------------------------------------------------------------------------------------
--step3:插入当天相关数据(qt_notification_sys_p1_detail)

insert into qt_smartreport.qt_notification_sys_p1_detail(date_value, notification_id, error_code,
                                                         alarm_module, alarm_service, alarm_type, alarm_level,
                                                         warning_spec, robot_code, job_order, device_code,
                                                         server_code, start_time, end_time)
select date(date_add(sysdate(), interval -1 day)) as date_value,
       bn.id                                      as notification_id,

       bn.error_code,
       bn.alarm_module,
       bn.alarm_service,
       bn.alarm_type,
       bn.alarm_level,
       bn.warning_spec,
       bn.robot_code,
       bn.job_order,
       bn.device_code,
       bn.server_code,
       bn.start_time,
       bn.end_time
from phoenix_basic.basic_notification bn
where 1 = 1
  and bn.alarm_module in ('system', 'server', 'device')
--               and bn.alarm_level in (3, 4, 5)			  
  and (((bn.start_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
         bn.start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
         date_format(coalesce(bn.end_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
         date_format(sysdate(), '%Y-%m-%d 00:00:00'))
    or
        (bn.start_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
         bn.start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
         date_format(coalesce(bn.end_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
         date_format(sysdate(), '%Y-%m-%d 00:00:00'))
    or
        (bn.start_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
         date_format(coalesce(bn.end_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
         date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
         date_format(coalesce(bn.end_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
         date_format(sysdate(), '%Y-%m-%d 00:00:00'))
    or
        (bn.start_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
         date_format(coalesce(bn.end_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
         date_format(sysdate(), '%Y-%m-%d 00:00:00'))) or
       (bn.end_time is null and bn.start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00')))
;	   
  

------------------------------------------------------------------------------------------------
--step4:建表（qt_notification_sys_p1_detail_stat_time）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_sys_p1_detail_stat_time
(
    `id`                   int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`           date               DEFAULT NULL COMMENT '日期',
    `hour_start_time`      datetime           DEFAULT NULL COMMENT '小时开始时间',
    `next_hour_start_time` datetime           DEFAULT NULL COMMENT '下一个小时开始时间',
    `notification_id`      varchar(100)       DEFAULT NULL COMMENT '通知ID',
    `error_code`           varchar(100)       DEFAULT NULL COMMENT '错误码',
    `alarm_module`         varchar(100)       DEFAULT NULL COMMENT '告警模块',
    `alarm_service`        varchar(100)       DEFAULT NULL COMMENT '告警服务',
    `alarm_type`           varchar(100)       DEFAULT NULL COMMENT '告警分类',
    `alarm_level`          varchar(100)       DEFAULT NULL COMMENT '告警级别',
    `warning_spec`         varchar(100)       DEFAULT NULL COMMENT '告警分类',
    `robot_code`           varchar(100)       DEFAULT NULL COMMENT '机器人编码',
    `job_order`            varchar(100)       DEFAULT NULL COMMENT '作业单编码',
    `device_code`          varchar(100)       DEFAULT NULL COMMENT '设备编码',
    `server_code`          varchar(100)       DEFAULT NULL COMMENT '服务器编码',
    `start_time`           datetime(6)        DEFAULT NULL COMMENT '开始时间-告警触发时间',
    `end_time`             datetime(6)        DEFAULT NULL COMMENT '结束时间-告警结束时间',
    `stat_start_time`      datetime(6)        DEFAULT NULL COMMENT '时间段内统计开始时间',
    `stat_end_time`        datetime(6)        DEFAULT NULL COMMENT '时间段内统计结束时间',
    `time_type`            varchar(100)       DEFAULT NULL COMMENT '统计维度',
    `created_time`         timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`         timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY `idx_date_value` (`date_value`),
    KEY `idx_warning_spec` (`warning_spec`),
    KEY `idx_start_time` (`start_time`),
    KEY `idx_end_time` (`end_time`),
    KEY `idx_time_type` (`time_type`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='系统类P1级故障通知时间段内明细';	
	
	
	
------------------------------------------------------------------------------------------------
--step5:删除当天相关数据（qt_notification_sys_p1_detail_stat_time）
DELETE
FROM qt_smartreport.qt_notification_sys_p1_detail_stat_time
WHERE date_value = date(date_add(sysdate(), interval -1 day));  	




------------------------------------------------------------------------------------------------
--step6:插入当天相关数据(qt_notification_sys_p1_detail_stat_time)

insert into qt_smartreport.qt_notification_sys_p1_detail_stat_time(date_value, hour_start_time, next_hour_start_time,
                                                                   notification_id, error_code,
                                                                   alarm_module, alarm_service, alarm_type, alarm_level,
                                                                   warning_spec, robot_code, job_order, device_code,
                                                                   server_code, start_time, end_time, stat_start_time,
                                                                   stat_end_time, time_type)
select date_value,
       null                                                     as hour_start_time,
       null                                                     as next_hour_start_time,
       notification_id,
       error_code,
       alarm_module,
       alarm_service,
       alarm_type,
       alarm_level,
       warning_spec,
       robot_code,
       job_order,
       device_code,
       server_code,
       start_time,
       end_time,
       case
           when start_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') then date_format(
                   date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00')
           else start_time end                                  as stat_start_time,
       case
           when end_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') then end_time
           else date_format(sysdate(), '%Y-%m-%d 00:00:00') end as stat_end_time,
       '天'                                                      as time_type
from qt_smartreport.qt_notification_sys_p1_detail
WHERE date_value = date(date_add(sysdate(), interval -1 day))
union all
select t2.date_value,
       t1.hour_start_time,
       t1.next_hour_start_time,
       t2.notification_id,
       t2.error_code,
       t2.alarm_module,
       t2.alarm_service,
       t2.alarm_type,
       t2.alarm_level,
       t2.warning_spec,
       t2.robot_code,
       t2.job_order,
       t2.device_code,
       t2.server_code,
       t2.start_time,
       t2.end_time,
       case when t2.start_time < t1.hour_start_time then t1.hour_start_time else t2.start_time end as stat_start_time,
       case
           when t2.end_time < t1.next_hour_start_time then t2.end_time
           else t1.next_hour_start_time end                                                        as stat_end_time,
       '小时'                                                                                        as time_type
from (select th.day_hours                               as hour_start_time,
             DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
      from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00'),
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
     (select date_value,
             notification_id,
             error_code,
             alarm_module,
             alarm_service,
             alarm_type,
             alarm_level,
             warning_spec,
             robot_code,
             job_order,
             device_code,
             server_code,
             start_time,
             end_time
      from qt_smartreport.qt_notification_sys_p1_detail
      WHERE date_value = date(date_add(sysdate(), interval -1 day))) t2 on
         ((t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
           coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time)
             or (t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                 coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time)
             or (t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.hour_start_time and
                 coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time)
             or (t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time)
             )
;



------------------------------------------------------------------------------------------------
--step7:建表（）


select 
*
from qt_smartreport.qt_notification_sys_p1_detail_stat_time
WHERE date_value = date(date_add(sysdate(), interval -1 day))























select 
date_value,
notification_id,
warning_spec,
start_time,
end_time,
case when start_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') then date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') else start_time end as day_stat_start_time,
case when end_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') then end_time else date_format(sysdate(), '%Y-%m-%d 00:00:00') end as day_stat_end_time,
'天'                                               as time_type
from qt_smartreport.qt_notification_sys_p1_detail
WHERE date_value = date(date_add(sysdate(), interval -1 day))
 












select 
t1.notification_id,
t.warning_spec,
t1.start_time,
t1.end_time,
t1.day_stat_start_time,
t1.day_stat_end_time,

from 
(select 
notification_id,
warning_spec,
start_time,
end_time,
case when start_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') then date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') else start_time end as day_stat_start_time,
case when end_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') then end_time else date_format(sysdate(), '%Y-%m-%d 00:00:00') end as day_stat_end_time
from qt_smartreport.qt_notification_sys_p1_detail
WHERE date_value = date(date_add(sysdate(), interval -1 day)))t1 
left join 
(select 
notification_id,
warning_spec,
start_time,
end_time,
case when start_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') then date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') else start_time end as day_stat_start_time,
case when end_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') then end_time else date_format(sysdate(), '%Y-%m-%d 00:00:00') end as day_stat_end_time
from qt_smartreport.qt_notification_sys_p1_detail
WHERE date_value = date(date_add(sysdate(), interval -1 day)))t2 on t2.warning_spec=t1.warning_spec and t2.day_stat_start_time>=t1.day_stat_start_time