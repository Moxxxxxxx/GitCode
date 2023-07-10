--------------------------------
##step1:建表（临时表 qt_notification_error_time_hour_detail_temp）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_error_time_hour_detail_temp
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `hour_start_time`           datetime  NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time`      datetime  NOT NULL COMMENT '下一个小时开始时间',
    `robot_code`                varchar(100)       DEFAULT NULL COMMENT '机器人编码',
    `first_classification_name` varchar(100)       DEFAULT NULL COMMENT '机器人类型',
    `notification_id`           varchar(100)       DEFAULT NULL COMMENT '通知ID',
    `error_code`                varchar(100)       DEFAULT NULL COMMENT '错误码',
    `alarm_module`              varchar(100)       DEFAULT NULL COMMENT '告警模块',
    `alarm_service`             varchar(100)       DEFAULT NULL COMMENT '告警服务',
    `alarm_type`                varchar(100)       DEFAULT NULL COMMENT '告警分类',
    `alarm_level`               varchar(100)       DEFAULT NULL COMMENT '告警级别',
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
    ROW_FORMAT = DYNAMIC COMMENT ='通知在小时内耗时明细临时表';	




--------------------------------
##step2:删除当天相关数据（临时表 qt_notification_error_time_hour_detail_temp）
DELETE
FROM qt_smartreport.qt_notification_error_time_hour_detail_temp
WHERE date_format(hour_start_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  


--------------------------------
##step3:插入当天相关数据(临时表 qt_notification_error_time_hour_detail_temp)
insert into qt_smartreport.qt_notification_error_time_hour_detail_temp(hour_start_time, next_hour_start_time,
                                                                       robot_code, first_classification_name,
                                                                       notification_id, error_code, alarm_module,
                                                                       alarm_service, alarm_type, alarm_level,
                                                                       start_time, end_time, the_hour_cost_seconds)
select t1.hour_start_time,
       t1.next_hour_start_time,
       t2.robot_code,
       t2.first_classification_name,
       t2.notification_id,
       t2.error_code,
       t2.alarm_module,
       t2.alarm_service,
       t2.alarm_type,
       t2.alarm_level,
       t2.start_time,
       t2.end_time,
       case
           when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                t2.end_time < t1.next_hour_start_time then timestampdiff(second, t2.start_time, t2.end_time)
           when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                t2.end_time >= t1.next_hour_start_time
               then timestampdiff(second, t2.start_time, t1.next_hour_start_time)
           when t2.start_time < t1.hour_start_time and t2.end_time >= t1.hour_start_time and
                t2.end_time < t1.next_hour_start_time then timestampdiff(second, t1.hour_start_time, t2.end_time)
           when t2.start_time < t1.hour_start_time and t2.end_time >= t1.next_hour_start_time
               then timestampdiff(second, t1.hour_start_time, t1.next_hour_start_time)
           end the_hour_cost_seconds

from (select th.day_hours                               as hour_start_time,
             DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
      from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00'), INTERVAL
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
     (select bn.robot_code,
             case
                 when brt.first_classification = 'WORKBIN' then '料箱车'
                 when brt.first_classification = 'STOREFORKBIN' then '存储一体式'
                 when brt.first_classification = 'CARRIER' then '潜伏式'
                 when brt.first_classification = 'ROLLER' then '辊筒'
                 when brt.first_classification = 'FORKLIFT' then '堆高全向车'
                 when brt.first_classification = 'DELIVER' then '投递车'
                 when brt.first_classification = 'SC' then '四向穿梭车'
                 else brt.first_classification end as first_classification_name,
             bn.id                                 as notification_id,
             bn.error_code,
             bn.alarm_module,
             bn.alarm_service,
             bn.alarm_type,
             bn.alarm_level,
             bn.start_time,
             bn.end_time
      from phoenix_basic.basic_notification bn
               left join phoenix_basic.basic_robot br on br.robot_code = bn.robot_code
               left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
      where 1 = 1
        and ((bn.start_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
              bn.start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
              bn.end_time < date_format(sysdate(), '%Y-%m-%d 00:00:00'))
          or
             (bn.start_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
              bn.start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
              bn.end_time >= date_format(sysdate(), '%Y-%m-%d 00:00:00'))
          or
             (bn.start_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
              bn.end_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
              bn.end_time < date_format(sysdate(), '%Y-%m-%d 00:00:00'))
          or
             (bn.start_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
              bn.end_time >= date_format(sysdate(), '%Y-%m-%d 00:00:00')))
     ) t2 on 1
;



--------------------------------
##step4:建表(qt_notification_error_time_stat)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_error_time_stat
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                datetime  NOT NULL COMMENT '统计时间',
    `date_value`                date               DEFAULT NULL COMMENT '日期',
    `hour_value`                varchar(100)       DEFAULT NULL COMMENT '小时',
    `robot_code`                varchar(100)       DEFAULT NULL COMMENT '机器人编码',
    `first_classification_name` varchar(100)       DEFAULT NULL COMMENT '机器人类型',
    `alarm_module`              varchar(100)       DEFAULT NULL COMMENT '告警模块',
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
    ROW_FORMAT = DYNAMIC COMMENT ='告警通知在统计时间段内明细';	
	
	
	
--------------------------------
##step5:删除当天相关数据(qt_notification_error_time_stat)
DELETE
FROM qt_smartreport.qt_notification_error_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  





--------------------------------
##step6:插入当天相关数据(qt_notification_error_time_stat)
#time_type='小时'

insert into qt_smartreport.qt_notification_error_time_stat(time_value, date_value, hour_value, robot_code,
                                                           first_classification_name, alarm_module,add_notification_num, notification_num,
                                                           notification_time, notification_rate, mtbf, mttr, time_type)
select tt.hour_start_time                                                                                                                         as time_value,
       date(tt.hour_start_time)                                                                                                                   as date_value,
       HOUR(tt.hour_start_time)                                                                                                                   as hour_value,
       tt.robot_code,
       tt.first_classification_name,
       t.alarm_module,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d %H:00:00') = tt.hour_start_time
                                       then t.notification_id end),
                0)                                                                                                                                as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                                                             as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                                                    as notification_time,
       cast(coalesce(sum(the_hour_cost_seconds), 0) / 3600 as decimal(10, 4))                                                                     as notification_rate,
       cast(case
                when count(distinct t.notification_id) != 0 then cast((3600 - coalesce(sum(the_hour_cost_seconds), 0)) /
                                                                      count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2)) as mtbf,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(coalesce(sum(the_hour_cost_seconds), 0) /
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
         left join qt_smartreport.qt_notification_error_time_hour_detail_temp t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.robot_code = tt.robot_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                       date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, robot_code, first_classification_name, alarm_module
;


--------------------------------
##step7:插入当天相关数据(qt_notification_error_time_stat)
#time_type='天'

insert into qt_smartreport.qt_notification_error_time_stat(time_value, date_value, hour_value, robot_code,
                                                           first_classification_name, alarm_module,add_notification_num, notification_num,
                                                           notification_time, notification_rate, mtbf, mttr, time_type)

select date_format(tt.hour_start_time, '%Y-%m-%d')                                                                                                as time_value,
       date(tt.hour_start_time)                                                                                                                   as date_value,
       null                                                                                                                                       as hour_value,
       tt.robot_code,
       tt.first_classification_name,
       t.alarm_module,
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
         left join qt_smartreport.qt_notification_error_time_hour_detail_temp t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.robot_code = tt.robot_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                       date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, robot_code, first_classification_name, alarm_module
;



--------------------------------
##step8:建表(qt_notification_error_rate_stat)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_error_rate_stat
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                datetime  NOT NULL COMMENT '统计时间',
    `date_value`                date               DEFAULT NULL COMMENT '日期',
    `hour_value`                varchar(100)       DEFAULT NULL COMMENT '小时',
    `robot_code`                varchar(100)       DEFAULT NULL COMMENT '机器人编码',
    `first_classification_name` varchar(100)       DEFAULT NULL COMMENT '机器人类型',
    `time_type`                 varchar(100)       DEFAULT NULL COMMENT '统计维度',
    `alarm_module`              varchar(100)       DEFAULT NULL COMMENT '告警模块',
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
    ROW_FORMAT = DYNAMIC COMMENT ='告警通知在统计时间段各指标值';	
	
	
	
--------------------------------
##step9:删除数据(qt_notification_error_rate_stat)
DELETE
FROM qt_smartreport.qt_notification_error_rate_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');   
	

--------------------------------
##step10:插入当天相关数据(qt_notification_error_rate_stat)
insert into qt_smartreport.qt_notification_error_rate_stat(time_value, date_value, hour_value, robot_code,
                                                           first_classification_name, time_type, alarm_module,
                                                           index_value, value_type)
select time_value,
       DATE(date_value)                         as date_value,
       hour_value,
       robot_code,
       first_classification_name,
       time_type,
       alarm_module,
       cast(add_notification_num as decimal(65, 4)) as index_value,
       '新增故障次数'                                   as value_type
from qt_smartreport.qt_notification_error_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                          as date_value,
       hour_value,
       robot_code,
       first_classification_name,
       time_type,
       alarm_module,
       cast(notification_time as decimal(65, 4)) as index_value,
       '故障时长'                                    as value_type
from qt_smartreport.qt_notification_error_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                          as date_value,
       hour_value,
       robot_code,
       first_classification_name,
       time_type,
       alarm_module,
       cast(notification_rate as decimal(65, 4)) as index_value,
       '故障率'                                     as value_type
from qt_smartreport.qt_notification_error_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)             as date_value,
       hour_value,
       robot_code,
       first_classification_name,
       time_type,
       alarm_module,
       cast(mtbf as decimal(65, 4)) as index_value,
       'MTBF'                       as value_type
from qt_smartreport.qt_notification_error_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)             as date_value,
       hour_value,
       robot_code,
       first_classification_name,
       time_type,
       alarm_module,
       cast(mttr as decimal(65, 4)) as index_value,
       'MTTR'                       as value_type
from qt_smartreport.qt_notification_error_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
;




-------------------------------------------------------------------------------------------------------------------------------
##step11:建表(qt_robot_notification_error_time_stat)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_notification_error_time_stat
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                datetime  NOT NULL COMMENT '统计时间',
    `date_value`                date               DEFAULT NULL COMMENT '日期',
    `hour_value`                varchar(100)       DEFAULT NULL COMMENT '小时',
    `robot_code`                varchar(100)       DEFAULT NULL COMMENT '机器人编码',
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
    ROW_FORMAT = DYNAMIC COMMENT ='告警通知在统计时间段内明细(不考虑告警模块)';	
	
	
	
--------------------------------
##step12:删除当天相关数据(qt_robot_notification_error_time_stat)
DELETE
FROM qt_smartreport.qt_robot_notification_error_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  





--------------------------------
##step13:插入当天相关数据(qt_robot_notification_error_time_stat)
--time_type='小时'
insert into qt_smartreport.qt_robot_notification_error_time_stat(time_value, date_value, hour_value, robot_code,
                                                           first_classification_name,add_notification_num, notification_num,
                                                           notification_time, notification_rate, mtbf, mttr, time_type)
select tt.hour_start_time                                                                                                                         as time_value,
       date(tt.hour_start_time)                                                                                                                   as date_value,
       HOUR(tt.hour_start_time)                                                                                                                   as hour_value,
       tt.robot_code,
       tt.first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d %H:00:00') = tt.hour_start_time
                                       then t.notification_id end),
                0)                                                                                                                                as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                                                             as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                                                    as notification_time,
       cast(coalesce(sum(the_hour_cost_seconds), 0) / 3600 as decimal(10, 4))                                                                     as notification_rate,
       cast(case
                when count(distinct t.notification_id) != 0 then cast((3600 - coalesce(sum(the_hour_cost_seconds), 0)) /
                                                                      count(distinct t.notification_id) as decimal(10, 2)) end as decimal(10, 2)) as mtbf,
       cast(case
                when count(distinct t.notification_id) != 0 then cast(coalesce(sum(the_hour_cost_seconds), 0) /
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
         left join qt_smartreport.qt_notification_error_time_hour_detail_temp t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.robot_code = tt.robot_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                       date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, robot_code, first_classification_name
;

##step14:插入当天相关数据(qt_robot_notification_error_time_stat)
--time_type='天'
insert into qt_smartreport.qt_robot_notification_error_time_stat(time_value, date_value, hour_value, robot_code,
                                                           first_classification_name,add_notification_num, notification_num,
                                                           notification_time, notification_rate, mtbf, mttr, time_type)
select date_format(tt.hour_start_time, '%Y-%m-%d')                                                                                                as time_value,
       date(tt.hour_start_time)                                                                                                                   as date_value,
       null                                                                                                                                       as hour_value,
       tt.robot_code,
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
         left join qt_smartreport.qt_notification_error_time_hour_detail_temp t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.robot_code = tt.robot_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                       date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, robot_code, first_classification_name
;


-----------------------------------------------------------------------------------------------------------------------
##step15:建表(qt_robot_notification_error_rate_stat)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_notification_error_rate_stat
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                datetime  NOT NULL COMMENT '统计时间',
    `date_value`                date               DEFAULT NULL COMMENT '日期',
    `hour_value`                varchar(100)       DEFAULT NULL COMMENT '小时',
    `robot_code`                varchar(100)       DEFAULT NULL COMMENT '机器人编码',
    `first_classification_name` varchar(100)       DEFAULT NULL COMMENT '机器人类型',
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
    ROW_FORMAT = DYNAMIC COMMENT ='告警通知在统计时间段各指标值(不考虑告警模块)';	
	
	
	
--------------------------------
##step16:删除数据(qt_robot_notification_error_rate_stat)
DELETE
FROM qt_smartreport.qt_robot_notification_error_rate_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');   
	

--------------------------------
##step17:插入当天相关数据(qt_robot_notification_error_rate_stat)
insert into qt_smartreport.qt_robot_notification_error_rate_stat(time_value, date_value, hour_value, robot_code,
                                                           first_classification_name, time_type,
                                                           index_value, value_type)
select time_value,
       DATE(date_value)                         as date_value,
       hour_value,
       robot_code,
       first_classification_name,
       time_type,
       cast(add_notification_num as decimal(65, 4)) as index_value,
       '新增故障次数'                                   as value_type
from qt_smartreport.qt_robot_notification_error_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                          as date_value,
       hour_value,
       robot_code,
       first_classification_name,
       time_type,
       cast(notification_time as decimal(65, 4)) as index_value,
       '故障时长'                                    as value_type
from qt_smartreport.qt_robot_notification_error_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                          as date_value,
       hour_value,
       robot_code,
       first_classification_name,
       time_type,
       cast(notification_rate as decimal(65, 4)) as index_value,
       '故障率'                                     as value_type
from qt_smartreport.qt_robot_notification_error_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)             as date_value,
       hour_value,
       robot_code,
       first_classification_name,
       time_type,
       cast(mtbf as decimal(65, 4)) as index_value,
       'MTBF'                       as value_type
from qt_smartreport.qt_robot_notification_error_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)             as date_value,
       hour_value,
       robot_code,
       first_classification_name,
       time_type,
       cast(mttr as decimal(65, 4)) as index_value,
       'MTTR'                       as value_type
from qt_smartreport.qt_robot_notification_error_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
;

