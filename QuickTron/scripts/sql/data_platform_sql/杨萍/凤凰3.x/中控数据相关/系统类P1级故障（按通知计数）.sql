------------------------------------------------------------------------------------------------
--step1:建表（qt_notification_system_module_p1_time_hour_detail）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_system_module_p1_time_hour_detail
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `hour_start_time`           datetime  NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time`      datetime  NOT NULL COMMENT '下一个小时开始时间',
    `notification_id`           varchar(100)       DEFAULT NULL COMMENT '通知ID',
    `error_code`                varchar(100)       DEFAULT NULL COMMENT '错误码',
    `alarm_module`              varchar(100)       DEFAULT NULL COMMENT '告警模块',
    `alarm_service`             varchar(100)       DEFAULT NULL COMMENT '告警服务',
    `alarm_type`                varchar(100)       DEFAULT NULL COMMENT '告警分类',
    `alarm_level`               varchar(100)       DEFAULT NULL COMMENT '告警级别',
    `robot_code`                varchar(100)       DEFAULT NULL COMMENT '机器人编码',
    `first_classification_name` varchar(100)       DEFAULT NULL COMMENT '机器人类型',
    `job_order`                 varchar(100)       DEFAULT NULL COMMENT '作业单编码',
    `the_hour_order_seconds`    decimal(10, 3)     DEFAULT NULL COMMENT '作业单在该小时内时长（秒）',
    `device_code`               varchar(100)       DEFAULT NULL COMMENT '设备编码',
    `server_code`               varchar(100)       DEFAULT NULL COMMENT '服务器编码',
    `object_code`               varchar(100)       DEFAULT NULL COMMENT '故障关联对象编码',
	`object_type`               varchar(100)       DEFAULT NULL COMMENT '故障关联对象类型',
    `start_time`                datetime           DEFAULT NULL COMMENT '开始时间-告警触发时间',
    `end_time`                  datetime           DEFAULT NULL COMMENT '结束时间-告警结束时间',
    `the_hour_cost_seconds`     decimal(10, 3)     DEFAULT NULL COMMENT '在该小时内时长（秒）',
    `created_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='系统类P1级故障通知在小时内耗时明细';	




------------------------------------------------------------------------------------------------
--step2:删除当天相关数据（qt_notification_system_module_p1_time_hour_detail）
DELETE
FROM qt_smartreport.qt_notification_system_module_p1_time_hour_detail
WHERE date_format(hour_start_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  



------------------------------------------------------------------------------------------------
--step3:插入当天相关数据(qt_notification_system_module_p1_time_hour_detail)
insert into qt_smartreport.qt_notification_system_module_p1_time_hour_detail(hour_start_time,next_hour_start_time,notification_id,error_code,alarm_module,alarm_service,alarm_type,alarm_level,robot_code,first_classification_name,job_order,the_hour_order_seconds,device_code,server_code,object_code,object_type,start_time,end_time,the_hour_cost_seconds)
select ta.hour_start_time,
       ta.next_hour_start_time,
       ta.notification_id,
       ta.error_code,
       ta.alarm_module,
       ta.alarm_service,
       ta.alarm_type,
       ta.alarm_level,
       ta.robot_code,
       case
           when brt.first_classification = 'WORKBIN' then '料箱车'
           when brt.first_classification = 'STOREFORKBIN' then '存储一体式'
           when brt.first_classification = 'CARRIER' then '潜伏式'
           when brt.first_classification = 'ROLLER' then '辊筒'
           when brt.first_classification = 'FORKLIFT' then '堆高全向车'
           when brt.first_classification = 'DELIVER' then '投递车'
           when brt.first_classification = 'SC' then '四向穿梭车'
           else brt.first_classification end as first_classification_name,
       ta.job_order,
       tb.the_hour_order_seconds,
       ta.device_code,
       ta.server_code,
       ta.object_code,
	   ta.object_type,
       ta.start_time,
       ta.end_time,
       ta.the_hour_cost_seconds
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             t2.notification_id,
             t2.error_code,
             t2.alarm_module,
             t2.alarm_service,
             t2.alarm_type,
             t2.alarm_level,
             t2.robot_code,
             t2.job_order,
             t2.device_code,
             t2.server_code,
             case
                 when t2.alarm_module = 'server' and t2.alarm_service = 'SERVER' then t2.server_code
                 when t2.alarm_module = 'system' and t2.alarm_type = 'device' then t2.device_code
                 when t2.alarm_module = 'system' and t2.alarm_type = 'job' then t2.job_order
                 when t2.alarm_module = 'system' and t2.alarm_type = 'robot' then t2.robot_code end object_code,
             case
                 when t2.alarm_module = 'server' and t2.alarm_service = 'SERVER' then '服务器'
                 when t2.alarm_module = 'system' and t2.alarm_type = 'device' then '外设'
                 when t2.alarm_module = 'system' and t2.alarm_type = 'job' then '作业单'
                 when t2.alarm_module = 'system' and t2.alarm_type = 'robot' then '机器人' end object_type,				 
             t2.start_time,
             t2.end_time,
             case
                 when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                      coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time then timestampdiff(second,
                                                                                                    t2.start_time,
                                                                                                    coalesce(t2.end_time, sysdate()))
                 when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                      coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time
                     then timestampdiff(second, t2.start_time, t1.next_hour_start_time)
                 when t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.hour_start_time and
                      coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time then timestampdiff(second,
                                                                                                    t1.hour_start_time,
                                                                                                    coalesce(t2.end_time, sysdate()))
                 when t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time
                     then timestampdiff(second, t1.hour_start_time, t1.next_hour_start_time)
                 end                                                                                the_hour_cost_seconds

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
           (select bn.id as notification_id,
                   bn.error_code,
                   bn.alarm_module,
                   bn.alarm_service,
                   bn.alarm_type,
                   bn.alarm_level,
                   bn.robot_code,
                   bn.job_order,
                   bn.device_code,
                   bn.server_code,
                   bn.start_time,
                   bn.end_time
            from phoenix_basic.basic_notification bn
            where 1 = 1
              and bn.alarm_module in ('system', 'server')
              and bn.alarm_level in (3,4,5)
              and ((bn.start_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
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
                    date_format(sysdate(), '%Y-%m-%d 00:00:00')))
           ) t2 on 1) ta
         left join
     (select t1.hour_start_time,
             t1.next_hour_start_time,
             t2.order_id,
             t2.order_type,
             t2.robot_code,
             t2.start_time,
             t2.end_time,
             t2.order_state,
             case
                 when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                      coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time then timestampdiff(second,
                                                                                                    t2.start_time,
                                                                                                    coalesce(t2.end_time, sysdate()))
                 when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                      coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time
                     then timestampdiff(second, t2.start_time, t1.next_hour_start_time)
                 when t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.hour_start_time and
                      coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time then timestampdiff(second,
                                                                                                    t1.hour_start_time,
                                                                                                    coalesce(t2.end_time, sysdate()))
                 when t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time
                     then timestampdiff(second, t1.hour_start_time, t1.next_hour_start_time)
                 end the_hour_order_seconds

      from (select th.day_hours                               as hour_start_time,
                   DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
            from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00'),
                                              INTERVAL
                                              (-(@ho := @ho + 1)) HOUR), '%Y-%m-%d %H:00:00') as day_hours
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
                       (SELECT @ho := -1) AS i) th) t1
               inner join
           (select order_id,
                   order_type,
                   robot_code,
                   create_time as start_time,
                   update_time as end_time,
                   state       as order_state
            from phoenix_rms.transport_order
            where 1 = 1
              and ((create_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                    create_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
                    date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
                    date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                or
                   (create_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                    create_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
                    date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                    date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                or
                   (create_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                    date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                    date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                    date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
                    date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                or
                   (create_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                    date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                    date_format(sysdate(), '%Y-%m-%d 00:00:00')))
           ) t2 on 1
      having the_hour_order_seconds is not null
     ) tb on tb.order_id = ta.job_order and tb.hour_start_time = ta.hour_start_time and
             tb.next_hour_start_time = ta.next_hour_start_time
         left join phoenix_basic.basic_robot br on br.robot_code = ta.robot_code
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
;


--------------------------------
##step4:建表(qt_notification_system_module_p1_object_stat)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_system_module_p1_object_stat
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                datetime  NOT NULL COMMENT '统计时间',
    `date_value`                date               DEFAULT NULL COMMENT '日期',
    `hour_value`                varchar(100)       DEFAULT NULL COMMENT '小时',
    `object_code`                varchar(100)       DEFAULT NULL COMMENT '告警对象编码',
	`object_type`               varchar(100)       DEFAULT NULL COMMENT '故障关联对象类型',	
    `first_classification_name` varchar(100)       DEFAULT NULL COMMENT '机器人类型',
    `add_notification_num`      int(100)           DEFAULT NULL COMMENT '新增告警次数',
    `notification_num`          int(100)           DEFAULT NULL COMMENT '告警次数',
    `notification_time`         int(100)           DEFAULT NULL COMMENT '告警时长（秒）',
    `notification_rate`         decimal(10, 4)     DEFAULT NULL COMMENT '告警率',
    `mtbf`                      decimal(10, 2)     DEFAULT NULL COMMENT 'mtbf',
    `mttr`                      decimal(10, 2)     DEFAULT NULL COMMENT 'mttr',
    `time_type`                 varchar(100)       DEFAULT NULL COMMENT '统计维度',
    `created_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='系统类P1告警通知在时间段内指标统计';	
	
	
--------------------------------
##step5:删除当天相关数据(qt_notification_system_module_p1_object_stat)
DELETE
FROM qt_smartreport.qt_notification_system_module_p1_object_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  




--------------------------------
##step6-1:插入当天相关数据(qt_notification_system_module_p1_object_stat)
#time_type='小时'
#alarm_module = 'system' and alarm_type = 'robot'

insert into qt_smartreport.qt_notification_system_module_p1_object_stat(time_value, date_value, hour_value, object_code,object_type,first_classification_name,add_notification_num, notification_num,notification_time, notification_rate, mtbf, mttr, time_type)
select tt.hour_start_time                                                                                                                         as time_value,
       date(tt.hour_start_time)                                                                                                                   as date_value,
       HOUR(tt.hour_start_time)                                                                                                                   as hour_value,
       tt.robot_code                                                                                                                              as object_code,
	   '机器人' as object_type,
       tt.first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d %H:00:00') = tt.hour_start_time
                                       then t.notification_id end),
                0)                                                                                                                                as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                                                             as notification_num,
       coalesce(sum(t.the_hour_cost_seconds), 0)                                                                                                  as notification_time,
       cast(coalesce(sum(t.the_hour_cost_seconds), 0) / 3600 as decimal(10, 4))                                                                   as notification_rate,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(
                            (3600 - coalesce(sum(t.the_hour_cost_seconds), 0)) /
                            count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2))                                           as mtbf,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(coalesce(sum(t.the_hour_cost_seconds), 0) /
                                                                      count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2)) as mttr,
       '小时'                                                                                                                                       as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             t2.robot_code,
             t2.first_classification_name
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
               left join
           (select br.robot_code,
                   case
                       when brt.first_classification = 'WORKBIN' then '料箱车'
                       when brt.first_classification = 'STOREFORKBIN' then '存储一体式'
                       when brt.first_classification = 'CARRIER' then '潜伏式'
                       when brt.first_classification = 'ROLLER' then '辊筒'
                       when brt.first_classification = 'FORKLIFT' then '堆高全向车'
                       when brt.first_classification = 'DELIVER' then '投递车'
                       when brt.first_classification = 'SC' then '四向穿梭车'
                       else brt.first_classification end as first_classification_name
            from phoenix_basic.basic_robot br
                     left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id) t2 on 1) tt
         left join qt_smartreport.qt_notification_system_module_p1_time_hour_detail t
                   on t.alarm_module = 'system' and t.alarm_type = 'robot' and
                      t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.robot_code = tt.robot_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                       date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, tt.robot_code, first_classification_name
;



--------------------------------
##step6-2:插入当天相关数据(qt_notification_system_module_p1_object_stat)
#time_type='小时'
#alarm_module = 'system' and alarm_type = 'device'

insert into qt_smartreport.qt_notification_system_module_p1_object_stat(time_value, date_value, hour_value, object_code,object_type,first_classification_name,add_notification_num, notification_num,notification_time, notification_rate, mtbf, mttr, time_type)
select tt.hour_start_time                                                                                                                         as time_value,
       date(tt.hour_start_time)                                                                                                                   as date_value,
       HOUR(tt.hour_start_time)                                                                                                                   as hour_value,
       tt.device_code                                                                                                                             as object_code,
	   '外设' as object_type,
       null                                                                                                                                       as first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d %H:00:00') = tt.hour_start_time
                                       then t.notification_id end),
                0)                                                                                                                                as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                                                             as notification_num,
       coalesce(sum(t.the_hour_cost_seconds), 0)                                                                                                  as notification_time,
       cast(coalesce(sum(t.the_hour_cost_seconds), 0) / 3600 as decimal(10, 4))                                                                   as notification_rate,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(
                            (3600 - coalesce(sum(t.the_hour_cost_seconds), 0)) /
                            count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2))                                           as mtbf,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(coalesce(sum(t.the_hour_cost_seconds), 0) /
                                                                      count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2)) as mttr,
       '小时'                                                                                                                                       as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             t2.equipment_code as device_code
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
               left join phoenix_basic.basic_equipment t2 on 1) tt
         left join qt_smartreport.qt_notification_system_module_p1_time_hour_detail t
                   on t.alarm_module = 'system' and t.alarm_type = 'device' and
                      t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.device_code = tt.device_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                         date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, tt.device_code
;



--------------------------------
##step6-3:插入当天相关数据(qt_notification_system_module_p1_object_stat)
#time_type='小时'
#alarm_module = 'system' and alarm_type = 'job'

insert into qt_smartreport.qt_notification_system_module_p1_object_stat(time_value, date_value, hour_value, object_code,object_type,first_classification_name,add_notification_num, notification_num,notification_time, notification_rate, mtbf, mttr, time_type)
select tt.hour_start_time                                                                                                                         as time_value,
       date(tt.hour_start_time)                                                                                                                   as date_value,
       HOUR(tt.hour_start_time)                                                                                                                   as hour_value,
       tt.order_id                                                                                                                                as object_code,
	   '作业单' as object_type,
       null                                                                                                                                       as first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d %H:00:00') = tt.hour_start_time
                                       then t.notification_id end),
                0)                                                                                                                                as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                                                             as notification_num,
       coalesce(sum(t.the_hour_cost_seconds), 0)                                                                                                  as notification_time,
       cast(coalesce(sum(t.the_hour_cost_seconds), 0) /
            coalesce(sum(tt.the_hour_order_seconds), 0) as decimal(10, 4))                                                                        as notification_rate,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(
                            (coalesce(sum(tt.the_hour_order_seconds), 0) - coalesce(sum(t.the_hour_cost_seconds), 0)) /
                            count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2))                                           as mtbf,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(coalesce(sum(t.the_hour_cost_seconds), 0) /
                                                                      count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2)) as mttr,
       '小时'                                                                                                                                       as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             t2.order_id,
             t2.order_type,
             t2.robot_code,
             t2.start_time,
             t2.end_time,
             t2.order_state,
             case
                 when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                      coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time then timestampdiff(second,
                                                                                                    t2.start_time,
                                                                                                    coalesce(t2.end_time, sysdate()))
                 when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                      coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time
                     then timestampdiff(second, t2.start_time, t1.next_hour_start_time)
                 when t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.hour_start_time and
                      coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time then timestampdiff(second,
                                                                                                    t1.hour_start_time,
                                                                                                    coalesce(t2.end_time, sysdate()))
                 when t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time
                     then timestampdiff(second, t1.hour_start_time, t1.next_hour_start_time)
                 end the_hour_order_seconds

      from (select th.day_hours                               as hour_start_time,
                   DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
            from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00'),
                                              INTERVAL
                                              (-(@ho := @ho + 1)) HOUR), '%Y-%m-%d %H:00:00') as day_hours
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
                       (SELECT @ho := -1) AS i) th) t1
               inner join
           (select order_id,
                   order_type,
                   robot_code,
                   create_time as start_time,
                   update_time as end_time,
                   state       as order_state
            from phoenix_rms.transport_order
            where 1 = 1
              and ((create_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                    create_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
                    date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
                    date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                or
                   (create_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                    create_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
                    date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                    date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                or
                   (create_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                    date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                    date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                    date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
                    date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                or
                   (create_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                    date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                    date_format(sysdate(), '%Y-%m-%d 00:00:00')))
           ) t2 on 1
      having the_hour_order_seconds is not null) tt
         left join qt_smartreport.qt_notification_system_module_p1_time_hour_detail t
                   on t.alarm_module = 'system' and t.alarm_type = 'job' and
                      t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.job_order = tt.order_id and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                    date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, tt.order_id
;


--------------------------------
##step6-4:插入当天相关数据(qt_notification_system_module_p1_object_stat)
#time_type='小时'
#alarm_module = 'server' and alarm_service = 'SERVER'

insert into qt_smartreport.qt_notification_system_module_p1_object_stat(time_value, date_value, hour_value, object_code,object_type,first_classification_name,add_notification_num, notification_num,notification_time, notification_rate, mtbf, mttr, time_type)
select tt.hour_start_time                                                                                                                         as time_value,
       date(tt.hour_start_time)                                                                                                                   as date_value,
       HOUR(tt.hour_start_time)                                                                                                                   as hour_value,
       tt.server_code                                                                                                                             as object_code,
	   '服务器' as object_type,
       null                                                                                                                                       as first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d %H:00:00') = tt.hour_start_time
                                       then t.notification_id end),
                0)                                                                                                                                as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                                                             as notification_num,
       coalesce(sum(t.the_hour_cost_seconds), 0)                                                                                                  as notification_time,
       cast(coalesce(sum(t.the_hour_cost_seconds), 0) / 3600 as decimal(10, 4))                                                                   as notification_rate,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(
                            (3600 - coalesce(sum(t.the_hour_cost_seconds), 0)) /
                            count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2))                                           as mtbf,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(coalesce(sum(t.the_hour_cost_seconds), 0) /
                                                                      count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2)) as mttr,
       '小时'                                                                                                                                       as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             t2.server_code
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
               left join (select distinct server_code
                          from phoenix_basic.basic_notification
                          where server_code is not null) t2 on 1) tt
         left join qt_smartreport.qt_notification_system_module_p1_time_hour_detail t
                   on t.alarm_module = 'server' and t.alarm_service = 'SERVER' and
                      t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.server_code = tt.server_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                         date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, tt.server_code
;


---------------------------------------------------------------------------------------
##step6-5:插入当天相关数据(qt_notification_system_module_p1_object_stat)
#time_type='天'
#alarm_module = 'system' and alarm_type = 'robot'

insert into qt_smartreport.qt_notification_system_module_p1_object_stat(time_value, date_value, hour_value, object_code,object_type,first_classification_name,add_notification_num, notification_num,notification_time, notification_rate, mtbf, mttr, time_type)

select date_format(tt.hour_start_time, '%Y-%m-%d')                                                                                                as time_value,
       date(tt.hour_start_time)                                                                                                                   as date_value,
       null                                                                                                                                       as hour_value,
       tt.robot_code                                                                                                                              as object_code,
	   '机器人' as object_type,
       tt.first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d') =
                                        date_format(tt.hour_start_time, '%Y-%m-%d')
                                       then t.notification_id end),
                0)                                                                                                                                as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                                                             as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                                                    as notification_time,
       cast(coalesce(sum(the_hour_cost_seconds), 0) / 3600 / 24 as decimal(10, 4))                                                                as notification_rate,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(
                            (3600 * 24 - coalesce(sum(the_hour_cost_seconds), 0)) /
                            count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2))                                           as mtbf,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(coalesce(sum(the_hour_cost_seconds), 0) /
                                                                      count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2)) as mttr,
       '天'                                                                                                                                        as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             t2.robot_code,
             t2.first_classification_name
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
               left join
           (select br.robot_code,
                   case
                       when brt.first_classification = 'WORKBIN' then '料箱车'
                       when brt.first_classification = 'STOREFORKBIN' then '存储一体式'
                       when brt.first_classification = 'CARRIER' then '潜伏式'
                       when brt.first_classification = 'ROLLER' then '辊筒'
                       when brt.first_classification = 'FORKLIFT' then '堆高全向车'
                       when brt.first_classification = 'DELIVER' then '投递车'
                       when brt.first_classification = 'SC' then '四向穿梭车'
                       else brt.first_classification end as first_classification_name
            from phoenix_basic.basic_robot br
                     left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id) t2 on 1) tt
         left join qt_smartreport.qt_notification_system_module_p1_time_hour_detail t
                   on t.alarm_module = 'system' and t.alarm_type = 'robot' and
                      t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.robot_code = tt.robot_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                       date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, tt.robot_code, first_classification_name
;


---------------------------------------------------------------------------------------
##step6-6:插入当天相关数据(qt_notification_system_module_p1_object_stat)
#time_type='天'
#alarm_module = 'system' and alarm_type = 'device'

insert into qt_smartreport.qt_notification_system_module_p1_object_stat(time_value, date_value, hour_value, object_code,object_type,first_classification_name,add_notification_num, notification_num,notification_time, notification_rate, mtbf, mttr, time_type)

select date_format(tt.hour_start_time, '%Y-%m-%d')                                                                                                as time_value,
       date(tt.hour_start_time)                                                                                                                   as date_value,
       null                                                                                                                                       as hour_value,
       tt.device_code                                                                                                                             as object_code,
	   '外设' as object_type,
       null                                                                                                                                       as first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d') =
                                        date_format(tt.hour_start_time, '%Y-%m-%d')
                                       then t.notification_id end),
                0)                                                                                                                                as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                                                             as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                                                    as notification_time,
       cast(coalesce(sum(the_hour_cost_seconds), 0) / 3600 / 24 as decimal(10, 4))                                                                as notification_rate,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(
                            (3600 * 24 - coalesce(sum(the_hour_cost_seconds), 0)) /
                            count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2))                                           as mtbf,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(coalesce(sum(the_hour_cost_seconds), 0) /
                                                                      count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2)) as mttr,
       '天'                                                                                                                                        as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             t2.equipment_code as device_code
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
               left join phoenix_basic.basic_equipment t2 on 1) tt
         left join qt_smartreport.qt_notification_system_module_p1_time_hour_detail t
                   on t.alarm_module = 'system' and t.alarm_type = 'device' and
                      t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.device_code = tt.device_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                         date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, tt.device_code
;

------------------------------------------------------------
##step6-7:插入当天相关数据(qt_notification_system_module_p1_object_stat)
#time_type='天'
#alarm_module = 'system' and alarm_type = 'job'

insert into qt_smartreport.qt_notification_system_module_p1_object_stat(time_value, date_value, hour_value, object_code,object_type,first_classification_name,add_notification_num, notification_num,notification_time, notification_rate, mtbf, mttr, time_type)
select date_format(tt.hour_start_time, '%Y-%m-%d')                                                                                                as time_value,
       date(tt.hour_start_time)                                                                                                                   as date_value,
       null                                                                                                                                       as hour_value,
       tt.order_id                                                                                                                                as object_code,
	   '作业单' as object_type,
       null                                                                                                                                       as first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d') =
                                        date_format(tt.hour_start_time, '%Y-%m-%d')
                                       then t.notification_id end),
                0)                                                                                                                                as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                                                             as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                                                    as notification_time,
       cast(coalesce(sum(the_hour_cost_seconds), 0) /
            coalesce(sum(tt.the_hour_order_seconds), 0) as decimal(10, 4))                                                                        as notification_rate,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(
                            (coalesce(sum(tt.the_hour_order_seconds), 0) - coalesce(sum(the_hour_cost_seconds), 0)) /
                            count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2))                                           as mtbf,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(coalesce(sum(the_hour_cost_seconds), 0) /
                                                                      count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2)) as mttr,
       '天'                                                                                                                                        as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             t2.order_id,
             t2.order_type,
             t2.robot_code,
             t2.start_time,
             t2.end_time,
             t2.order_state,
             case
                 when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                      coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time then timestampdiff(second,
                                                                                                    t2.start_time,
                                                                                                    coalesce(t2.end_time, sysdate()))
                 when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                      coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time
                     then timestampdiff(second, t2.start_time, t1.next_hour_start_time)
                 when t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.hour_start_time and
                      coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time then timestampdiff(second,
                                                                                                    t1.hour_start_time,
                                                                                                    coalesce(t2.end_time, sysdate()))
                 when t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time
                     then timestampdiff(second, t1.hour_start_time, t1.next_hour_start_time)
                 end the_hour_order_seconds

      from (select th.day_hours                               as hour_start_time,
                   DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
            from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00'),
                                              INTERVAL
                                              (-(@ho := @ho + 1)) HOUR), '%Y-%m-%d %H:00:00') as day_hours
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
                       (SELECT @ho := -1) AS i) th) t1
               inner join
           (select order_id,
                   order_type,
                   robot_code,
                   create_time as start_time,
                   update_time as end_time,
                   state       as order_state
            from phoenix_rms.transport_order
            where 1 = 1
              and ((create_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                    create_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
                    date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
                    date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                or
                   (create_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                    create_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
                    date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                    date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                or
                   (create_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                    date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                    date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                    date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
                    date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                or
                   (create_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                    date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                    date_format(sysdate(), '%Y-%m-%d 00:00:00')))
           ) t2 on 1
      having the_hour_order_seconds is not null) tt
         left join qt_smartreport.qt_notification_system_module_p1_time_hour_detail t
                   on t.alarm_module = 'system' and t.alarm_type = 'job' and
                      t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.job_order = tt.order_id and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                    date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, tt.order_id
;

-----------------------------------------------------------
##step6-8:插入当天相关数据(qt_notification_system_module_p1_object_stat)
#time_type='天'
#alarm_module = 'server' and alarm_service = 'SERVER'

insert into qt_smartreport.qt_notification_system_module_p1_object_stat(time_value, date_value, hour_value, object_code,object_type,first_classification_name,add_notification_num, notification_num,notification_time, notification_rate, mtbf, mttr, time_type)
select date_format(tt.hour_start_time, '%Y-%m-%d')                                                                                                as time_value,
       date(tt.hour_start_time)                                                                                                                   as date_value,
       null                                                                                                                                       as hour_value,
       tt.server_code                                                                                                                             as object_code,
	   '服务器' as object_type,
       null                                                                                                                                       as first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d') =
                                        date_format(tt.hour_start_time, '%Y-%m-%d')
                                       then t.notification_id end),
                0)                                                                                                                                as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                                                             as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                                                    as notification_time,
       cast(coalesce(sum(the_hour_cost_seconds), 0) / 3600 / 24 as decimal(10, 4))                                                                as notification_rate,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(
                            (3600 * 24 - coalesce(sum(the_hour_cost_seconds), 0)) /
                            count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2))                                           as mtbf,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(coalesce(sum(the_hour_cost_seconds), 0) /
                                                                      count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2)) as mttr,
       '天'                                                                                                                                        as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             t2.server_code
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
               left join (select distinct server_code
                          from phoenix_basic.basic_notification
                          where server_code is not null) t2 on 1) tt
         left join qt_smartreport.qt_notification_system_module_p1_time_hour_detail t
                   on t.alarm_module = 'server' and t.alarm_service = 'SERVER' and
                      t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.server_code = tt.server_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                         date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, tt.server_code
;

------------------------------------------------------------------------------------------
--------------------------------
##step7:建表(qt_notification_system_module_p1_stat)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_system_module_p1_stat
(
    `id`                         int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                 datetime  NOT NULL COMMENT '统计时间',
    `date_value`                 date               DEFAULT NULL COMMENT '日期',
    `hour_value`                 varchar(100)       DEFAULT NULL COMMENT '小时',
    `object_num`                 int(100)           DEFAULT NULL COMMENT '告警对象编码数量',
    `robot_num`                  int(100)           DEFAULT NULL COMMENT '项目机器人总数',
    `device_num`                 int(100)           DEFAULT NULL COMMENT '项目设备总数',
    `server_num`                 int(100)           DEFAULT NULL COMMENT '项目服务器总数',
    `order_num`                  int(100)           DEFAULT NULL COMMENT '项目关联作业单总数',
    `the_hour_order_seconds_sum` int(100)           DEFAULT NULL COMMENT '项目关联作业单时长（秒）',
    `add_notification_num`       int(100)           DEFAULT NULL COMMENT '新增告警次数',
    `notification_num`           int(100)           DEFAULT NULL COMMENT '告警次数',
    `notification_time`          int(100)           DEFAULT NULL COMMENT '告警时长（秒）',
    `notification_rate`          decimal(10, 4)     DEFAULT NULL COMMENT '告警率',
    `mtbf`                       decimal(10, 2)     DEFAULT NULL COMMENT 'mtbf',
    `mttr`                       decimal(10, 2)     DEFAULT NULL COMMENT 'mttr',
    `time_type`                  varchar(100)       DEFAULT NULL COMMENT '统计维度',
    `created_time`               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='全场系统类P1级告警通知在时间段内指标统计';	
	
	
--------------------------------
##step8:删除当天相关数据(qt_notification_system_module_p1_stat)
DELETE
FROM qt_smartreport.qt_notification_system_module_p1_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  


--------------------------------
##step9-1:插入当天相关数据(qt_notification_system_module_p1_stat)
#time_type='小时'

insert into qt_smartreport.qt_notification_system_module_p1_stat(time_value, date_value, hour_value, object_num,
                                                                 robot_num, device_num, server_num, order_num,
                                                                 the_hour_order_seconds_sum, add_notification_num,
                                                                 notification_num, notification_time, notification_rate,
                                                                 mtbf, mttr, time_type)
select tt.hour_start_time                                                                                as time_value,
       date(tt.hour_start_time)                                                                          as date_value,
       HOUR(tt.hour_start_time)                                                                          as hour_value,
       tt.robot_num + tt.device_num + tt.server_num + tt.order_num                                       as object_num,
       tt.robot_num,
       tt.device_num,
       tt.server_num,
       tt.order_num,
       tt.the_hour_order_seconds_sum,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d %H:00:00') = tt.hour_start_time
                                       then t.notification_id end),
                0)                                                                                       as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                    as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                           as notification_time,
       cast(coalesce(sum(the_hour_cost_seconds), 0) / (3600 * (tt.robot_num + tt.device_num + tt.server_num) +
                                                       tt.the_hour_order_seconds_sum) as decimal(10, 4)) as notification_rate,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(
                            ((3600 * (tt.robot_num + tt.device_num + tt.server_num) + tt.the_hour_order_seconds_sum) -
                             coalesce(sum(the_hour_cost_seconds), 0)) /
                            count(distinct t.notification_id) as decimal(10, 2))
                else 3600 * (tt.robot_num + tt.device_num + tt.server_num) +
                     tt.the_hour_order_seconds_sum end as decimal(10, 2))                                as mtbf,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(coalesce(sum(the_hour_cost_seconds), 0) /
                                                                      count(distinct t.notification_id) as decimal(10, 2))
                else 0 end as decimal(10, 2))                                                            as mttr,
       '小时'                                                                                              as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             (select count(distinct robot_code) from phoenix_basic.basic_robot)                as robot_num,
             (select count(distinct equipment_code) from phoenix_basic.basic_equipment)        as device_num,
             (select value from phoenix_basic.basic_system_config where name = 'SERVER_COUNT') as server_num,
             coalesce(t2.order_num, 0)                                                         as order_num,
             coalesce(t2.the_hour_order_seconds_sum, 0)                                        as the_hour_order_seconds_sum
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
               left join (
          select hour_start_time,
                 next_hour_start_time,
                 count(distinct order_id)                 as order_num,
                 coalesce(sum(the_hour_order_seconds), 0) as the_hour_order_seconds_sum
          from (select t1.hour_start_time,
                       t1.next_hour_start_time,
                       t2.order_id,
                       t2.order_type,
                       t2.robot_code,
                       t2.start_time,
                       t2.end_time,
                       t2.order_state,
                       case
                           when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                                coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time then timestampdiff(second,
                                                                                                              t2.start_time,
                                                                                                              coalesce(t2.end_time, sysdate()))
                           when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                                coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time
                               then timestampdiff(second, t2.start_time, t1.next_hour_start_time)
                           when t2.start_time < t1.hour_start_time and
                                coalesce(t2.end_time, sysdate()) >= t1.hour_start_time and
                                coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time then timestampdiff(second,
                                                                                                              t1.hour_start_time,
                                                                                                              coalesce(t2.end_time, sysdate()))
                           when t2.start_time < t1.hour_start_time and
                                coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time
                               then timestampdiff(second, t1.hour_start_time, t1.next_hour_start_time)
                           end the_hour_order_seconds

                from (select th.day_hours                               as hour_start_time,
                             DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
                      from (SELECT DATE_FORMAT(
                                           DATE_SUB(DATE_FORMAT(date_add(sysdate(), interval -1 day),
                                                                '%Y-%m-%d 00:00:00'),
                                                    INTERVAL
                                                    (-(@ho := @ho + 1)) HOUR), '%Y-%m-%d %H:00:00') as day_hours
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
                                 (SELECT @ho := -1) AS i) th) t1
                         inner join
                     (select order_id,
                             order_type,
                             robot_code,
                             create_time as start_time,
                             update_time as end_time,
                             state       as order_state
                      from phoenix_rms.transport_order
                      where 1 = 1
                        and ((create_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                              create_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
                              date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
                              date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                          or
                             (create_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                              create_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
                              date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                              date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                          or
                             (create_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                              date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                              date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                              date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
                              date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                          or
                             (create_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                              date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                              date_format(sysdate(), '%Y-%m-%d 00:00:00')))
                     ) t2 on 1
                having the_hour_order_seconds is not null) tt
          group by hour_start_time,
                   next_hour_start_time) t2
                         on t2.hour_start_time = t1.hour_start_time and
                            t2.next_hour_start_time = t1.next_hour_start_time) tt
         left join qt_smartreport.qt_notification_system_module_p1_time_hour_detail t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      date_format(t.hour_start_time, '%Y-%m-%d') =
                      date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, object_num, robot_num, device_num, server_num, order_num,
         the_hour_order_seconds_sum
;


------------------------------------------------------------------------------------------
##step9-2:插入当天相关数据(qt_notification_system_module_p1_stat)
#time_type='天'

insert into qt_smartreport.qt_notification_system_module_p1_stat(time_value, date_value, hour_value, object_num,
                                                                 robot_num, device_num, server_num, order_num,
                                                                 the_hour_order_seconds_sum, add_notification_num,
                                                                 notification_num, notification_time, notification_rate,
                                                                 mtbf, mttr, time_type)
select date_format(tt.hour_start_time, '%Y-%m-%d')                                                                    as time_value,
       date(tt.hour_start_time)                                                                                       as date_value,
       null                                                                                                           as hour_value,
       tt.robot_num + tt.device_num + tt.server_num +
       coalesce(t1.order_num, 0)                                                                                      as object_num,
       tt.robot_num,
       tt.device_num,
       tt.server_num,
       coalesce(t1.order_num, 0)                                                                                      as order_num,
       coalesce(t1.the_hour_order_seconds_sum, 0)                                                                     as the_hour_order_seconds_sum,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d') =
                                        date_format(tt.hour_start_time, '%Y-%m-%d')
                                       then t.notification_id end),
                0)                                                                                                    as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                                 as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                        as notification_time,
       cast(coalesce(sum(the_hour_cost_seconds), 0) / (3600 * 24 * (tt.robot_num + tt.device_num + tt.server_num) +
                                                       coalesce(t1.the_hour_order_seconds_sum, 0)) as decimal(10, 4)) as notification_rate,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(
                            ((3600 * 24 * (tt.robot_num + tt.device_num + tt.server_num) +
                              coalesce(t1.the_hour_order_seconds_sum, 0)) - coalesce(sum(the_hour_cost_seconds), 0)) /
                            count(distinct t.notification_id) as decimal(10, 2))
                else 3600 * 24 * (tt.robot_num + tt.device_num + tt.server_num) +
                     coalesce(t1.the_hour_order_seconds_sum, 0) end as decimal(10, 2))                                as mtbf,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(coalesce(sum(the_hour_cost_seconds), 0) /
                                                                      count(distinct t.notification_id) as decimal(10, 2))
                else 0 end as decimal(10, 2))                                                                         as mttr,
       '天'                                                                                                            as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             (select count(distinct robot_code) from phoenix_basic.basic_robot)                as robot_num,
             (select count(distinct equipment_code) from phoenix_basic.basic_equipment)        as device_num,
             (select value from phoenix_basic.basic_system_config where name = 'SERVER_COUNT') as server_num
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
                       (SELECT @u := -1) AS i) th) t1) tt
         left join qt_smartreport.qt_notification_system_module_p1_time_hour_detail t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      date_format(t.hour_start_time, '%Y-%m-%d') =
                      date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
         left join (select date_format(tt.hour_start_time, '%Y-%m-%d') as time_value,
                           count(distinct order_id)                    as order_num,
                           coalesce(sum(the_hour_order_seconds), 0)    as the_hour_order_seconds_sum
                    from (select t1.hour_start_time,
                                 t1.next_hour_start_time,
                                 t2.order_id,
                                 t2.order_type,
                                 t2.robot_code,
                                 t2.start_time,
                                 t2.end_time,
                                 t2.order_state,
                                 case
                                     when t2.start_time >= t1.hour_start_time and
                                          t2.start_time < t1.next_hour_start_time and
                                          coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time then timestampdiff(
                                             second,
                                             t2.start_time,
                                             coalesce(t2.end_time, sysdate()))
                                     when t2.start_time >= t1.hour_start_time and
                                          t2.start_time < t1.next_hour_start_time and
                                          coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time
                                         then timestampdiff(second, t2.start_time, t1.next_hour_start_time)
                                     when t2.start_time < t1.hour_start_time and
                                          coalesce(t2.end_time, sysdate()) >= t1.hour_start_time and
                                          coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time then timestampdiff(
                                             second,
                                             t1.hour_start_time,
                                             coalesce(t2.end_time, sysdate()))
                                     when t2.start_time < t1.hour_start_time and
                                          coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time
                                         then timestampdiff(second, t1.hour_start_time, t1.next_hour_start_time)
                                     end the_hour_order_seconds

                          from (select th.day_hours                               as hour_start_time,
                                       DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
                                from (SELECT DATE_FORMAT(
                                                     DATE_SUB(DATE_FORMAT(date_add(sysdate(), interval -1 day),
                                                                          '%Y-%m-%d 00:00:00'),
                                                              INTERVAL
                                                              (-(@ho := @ho + 1)) HOUR),
                                                     '%Y-%m-%d %H:00:00') as day_hours
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
                                           (SELECT @ho := -1) AS i) th) t1
                                   inner join
                               (select order_id,
                                       order_type,
                                       robot_code,
                                       create_time as start_time,
                                       update_time as end_time,
                                       state       as order_state
                                from phoenix_rms.transport_order
                                where 1 = 1
                                  and ((create_time >=
                                        date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                                        create_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
                                        date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
                                        date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                                    or
                                       (create_time >=
                                        date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                                        create_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
                                        date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                                        date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                                    or
                                       (create_time <
                                        date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                                        date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                                        date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                                        date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
                                        date_format(sysdate(), '%Y-%m-%d 00:00:00'))
                                    or
                                       (create_time <
                                        date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
                                        date_format(coalesce(update_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
                                        date_format(sysdate(), '%Y-%m-%d 00:00:00')))
                               ) t2 on 1
                          having the_hour_order_seconds is not null) tt
                    group by time_value) t1 on t1.time_value = date_format(tt.hour_start_time, '%Y-%m-%d')
group by date_format(tt.hour_start_time, '%Y-%m-%d'), date_value, hour_value, object_num, robot_num, device_num,
         server_num, t1.order_num,
         t1.the_hour_order_seconds_sum
;

------------------------------------------------------------------------------------------

##step10:建表(qt_notification_system_module_p1_index_stat)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_system_module_p1_index_stat
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                datetime  NOT NULL COMMENT '统计时间',
    `date_value`                date               DEFAULT NULL COMMENT '日期',
    `hour_value`                varchar(100)       DEFAULT NULL COMMENT '小时',
    `time_type`                 varchar(100)       DEFAULT NULL COMMENT '统计维度',
    `index_value`               decimal(65, 4)     DEFAULT NULL COMMENT '指标值',
    `value_type`                varchar(100)       DEFAULT NULL COMMENT '指标类型',
    `created_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='全场系统类P1级告警通知在时间段内各指标值';	
	
	
--------------------------------
##step11:删除数据(qt_notification_system_module_p1_index_stat)
DELETE
FROM qt_smartreport.qt_notification_system_module_p1_index_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');   
	

--------------------------------
##step12:插入当天相关数据(qt_notification_system_module_p1_index_stat)
insert into qt_smartreport.qt_notification_system_module_p1_index_stat(time_value, date_value, hour_value, time_type,index_value, value_type)
select time_value,
       DATE(date_value)                         as date_value,
       hour_value,
       time_type,
       cast(add_notification_num as decimal(65, 4)) as index_value,
       '新增故障次数'                                   as value_type
from qt_smartreport.qt_notification_system_module_p1_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                          as date_value,
       hour_value,
       time_type,
       cast(notification_time as decimal(65, 4)) as index_value,
       '故障时长'                                    as value_type
from qt_smartreport.qt_notification_system_module_p1_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                          as date_value,
       hour_value,
       time_type,
       cast(notification_rate as decimal(65, 4)) as index_value,
       '故障率'                                     as value_type
from qt_smartreport.qt_notification_system_module_p1_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)             as date_value,
       hour_value,
       time_type,
       cast(mtbf as decimal(65, 4)) as index_value,
       'MTBF'                       as value_type
from qt_smartreport.qt_notification_system_module_p1_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)             as date_value,
       hour_value,
       time_type,
       cast(mttr as decimal(65, 4)) as index_value,
       'MTTR'                       as value_type
from qt_smartreport.qt_notification_system_module_p1_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
;