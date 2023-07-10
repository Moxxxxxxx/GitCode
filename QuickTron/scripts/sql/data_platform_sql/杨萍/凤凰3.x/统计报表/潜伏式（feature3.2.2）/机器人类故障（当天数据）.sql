#step1:建表（qt_basic_notification_clear1_realtime）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_basic_notification_clear1_realtime
(
    `id`           int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`   date         NOT NULL COMMENT '日期',
    `robot_code`   varchar(100) NOT NULL COMMENT '机器人编码',
    `error_id`     varchar(100) NOT NULL COMMENT '故障通知ID',
    `error_code`   varchar(100)          DEFAULT NULL COMMENT '错误码',
    `start_time`   datetime(6)           DEFAULT NULL COMMENT '开始时间-告警触发时间',
    `end_time`     datetime(6)           DEFAULT NULL COMMENT '结束时间-告警结束时间',
    `created_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_robot_code (`robot_code`),
    key idx_error_id (`error_id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类故障收敛清洗step1（当天数据）';



#step2:删除相关数据（qt_basic_notification_clear1_realtime）
DELETE FROM qt_smartreport.qt_basic_notification_clear1_realtime;



#step3:插入相关数据（qt_basic_notification_clear1_realtime）
insert into qt_smartreport.qt_basic_notification_clear1_realtime(date_value, robot_code, error_id, error_code, start_time, end_time)
select CURRENT_DATE() as date_value,
       robot_code,
       id             as error_id,
       error_code,
       start_time,
       end_time
from phoenix_basic.basic_notification
where alarm_module = 'robot'
  and alarm_level >= 3
  and (
        (start_time >= date_format(sysdate(), '%Y-%m-%d 00:00:00') and
         start_time < sysdate() and coalesce(end_time, sysdate()) < sysdate()) or
        (start_time >= date_format(sysdate(), '%Y-%m-%d 00:00:00') and
         start_time < sysdate() and coalesce(end_time, sysdate()) >= sysdate()) or
        (start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
         coalesce(end_time, sysdate()) >= date_format(sysdate(), '%Y-%m-%d 00:00:00') and
         coalesce(end_time, sysdate()) < sysdate()) or
        (start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
         coalesce(end_time, sysdate()) >= sysdate())
    )
;



#step4:建表（qt_basic_notification_clear2_realtime）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_basic_notification_clear2_realtime
(
    `id`             int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`     date         NOT NULL COMMENT '日期',
    `robot_code`     varchar(100) NOT NULL COMMENT '机器人编码',
    `error_id`       varchar(100) NOT NULL COMMENT '故障通知ID',
    `error_code`     varchar(100)          DEFAULT NULL COMMENT '错误码',
    `start_time`     datetime(6)           DEFAULT NULL COMMENT '开始时间-告警触发时间',
    `end_time`       datetime(6)           DEFAULT NULL COMMENT '结束时间-告警结束时间',
    `pre_error_id`   varchar(100)          DEFAULT NULL COMMENT '前一个故障通知ID',
    `pre_start_time` datetime(6)           DEFAULT NULL COMMENT '前一个开始时间-告警触发时间',
    `pre_end_time`   datetime(6)           DEFAULT NULL COMMENT '前一个结束时间-告警结束时间',
    `diff_seconds`   decimal(65, 10)       DEFAULT NULL COMMENT '间隔时长（秒）',
    `created_time`   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_robot_code (`robot_code`),
    key idx_error_id (`error_id`),
    key idx_pre_error_id (`pre_error_id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类故障收敛清洗step2（当天数据）';





#step5:删除相关数据（qt_basic_notification_clear2_realtime）
DELETE FROM qt_smartreport.qt_basic_notification_clear2_realtime;



#step6:插入相关数据（qt_basic_notification_clear2_realtime）
insert into qt_smartreport.qt_basic_notification_clear2_realtime(date_value, robot_code, error_id, error_code, start_time,
                                                        end_time, pre_error_id, pre_start_time, pre_end_time,
                                                        diff_seconds)
select CURRENT_DATE() as date_value,
       t5.robot_code,
       t5.error_id,
       t5.error_code,
       t5.start_time,
       t5.end_time,
       t5.pre_error_id,
       t5.pre_start_time,
       t5.pre_end_time,
       t5.diff_seconds
from (select t3.*,
             t4.start_time                                               as pre_start_time,
             t4.end_time                                                 as pre_end_time,
             UNIX_TIMESTAMP(t3.start_time) - UNIX_TIMESTAMP(t4.end_time) as diff_seconds,
             case
                 when t3.pre_error_id is null then 1
                 when UNIX_TIMESTAMP(t3.start_time) - UNIX_TIMESTAMP(t4.end_time) < 3 then 0
                 else 1 end                                                 is_effective
      from (select t1.error_id,
                   t1.robot_code,
                   t1.error_code,
                   t1.start_time,
                   t1.end_time,
                   max(t2.error_id) as pre_error_id
            from qt_smartreport.qt_basic_notification_clear1_realtime t1
                     left join qt_smartreport.qt_basic_notification_clear1_realtime t2
                               on t2.robot_code = t1.robot_code and t2.error_code = t1.error_code and
                                  t2.date_value = t1.date_value and t2.start_time < t1.start_time
            where t1.date_value = CURRENT_DATE()
            group by t1.error_id, t1.robot_code, t1.error_code, t1.start_time, t1.end_time) t3
               left join qt_smartreport.qt_basic_notification_clear1 t4
                         on t4.robot_code = t3.robot_code and t4.error_id = t3.pre_error_id) t5
where t5.is_effective = 1
;



#step7:建表（qt_basic_notification_clear3_realtime）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_basic_notification_clear3_realtime
(
    `id`             int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`     date         NOT NULL COMMENT '日期',
    `robot_code`     varchar(100) NOT NULL COMMENT '机器人编码',
    `error_id`       varchar(100) NOT NULL COMMENT '故障通知ID',
    `error_code`     varchar(100)          DEFAULT NULL COMMENT '错误码',
    `start_time`     datetime(6)           DEFAULT NULL COMMENT '开始时间-告警触发时间',
    `end_time`       datetime(6)           DEFAULT NULL COMMENT '结束时间-告警结束时间',
    `created_time`   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_robot_code (`robot_code`),
    key idx_error_id (`error_id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类故障收敛清洗step3（当天数据）';



#step8:删除相关数据（qt_basic_notification_clear3_realtime）
DELETE FROM qt_smartreport.qt_basic_notification_clear3_realtime;




#step9:插入相关数据（qt_basic_notification_clear3_realtime）
insert into qt_smartreport.qt_basic_notification_clear3_realtime(date_value, robot_code, error_id, error_code, start_time, end_time)
select CURRENT_DATE() as date_value,
       t1.robot_code,
       t1.error_id,
       t1.error_code,
       t1.start_time,
       t1.end_time
from qt_smartreport.qt_basic_notification_clear2_realtime t1
         inner join (select robot_code,
                            end_time,
                            min(error_id) as first_error_id
                     from qt_smartreport.qt_basic_notification_clear2_realtime
                     where date_value = CURRENT_DATE()
                     group by robot_code, end_time) t on t.robot_code = t1.robot_code and t.first_error_id = t1.error_id
where t1.date_value = CURRENT_DATE()
;



#step10:建表（qt_basic_notification_clear4_realtime）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_basic_notification_clear4_realtime
(
    `id`             int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`     date         NOT NULL COMMENT '日期',
    `robot_code`     varchar(100) NOT NULL COMMENT '机器人编码',
    `error_id`       varchar(100) NOT NULL COMMENT '故障通知ID',
    `error_code`     varchar(100)          DEFAULT NULL COMMENT '错误码',
    `start_time`     datetime(6)           DEFAULT NULL COMMENT '开始时间-告警触发时间',
    `end_time`       datetime(6)           DEFAULT NULL COMMENT '结束时间-告警结束时间',
    `created_time`   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_robot_code (`robot_code`),
    key idx_error_id (`error_id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类故障收敛清洗step4（当天数据）';



#step11:删除相关数据（qt_basic_notification_clear4_realtime）
DELETE FROM qt_smartreport.qt_basic_notification_clear4_realtime;



#step12:插入相关数据（qt_basic_notification_clear4_realtime）
insert into qt_smartreport.qt_basic_notification_clear4_realtime(date_value, robot_code, error_id, error_code, start_time, end_time)
select CURRENT_DATE() as date_value,
       t5.robot_code,
       t5.error_id,
       t5.error_code,
       t5.start_time,
       t5.end_time
from (select t3.*,
             t4.start_time                                             as pre_start_time,
             t4.end_time                                               as pre_end_time,
             UNIX_TIMESTAMP(t3.end_time) - UNIX_TIMESTAMP(t4.end_time) as diff_seconds,
             case
                 when t3.pre_error_id is null then 1
                 when UNIX_TIMESTAMP(t3.end_time) - UNIX_TIMESTAMP(t4.end_time) < 3 then 0
                 else 1 end                                               is_effective
      from (select t1.error_id,
                   t1.robot_code,
                   t1.error_code,
                   t1.start_time,
                   t1.end_time,
                   max(t2.error_id) as pre_error_id
            from qt_smartreport.qt_basic_notification_clear3_realtime t1
                     left join qt_smartreport.qt_basic_notification_clear3_realtime t2
                               on t2.robot_code = t1.robot_code and t1.date_value = t2.date_value and
                                  t2.start_time < t1.start_time
            where t1.date_value = CURRENT_DATE()
            group by t1.error_id, t1.robot_code, t1.error_code, t1.start_time, t1.end_time) t3
               left join qt_smartreport.qt_basic_notification_clear3_realtime t4
                         on t4.robot_code = t3.robot_code and t4.error_id = t3.pre_error_id and
                            t4.date_value = CURRENT_DATE()) t5
where t5.is_effective = 1
;









#step13:建表（qt_notification_robot_module_time_hour_detail_realtime）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_robot_module_time_hour_detail_realtime
(
    `id`                        int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`                date         NOT NULL COMMENT '日期',
    `hour_start_time`           datetime     NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time`      datetime     NOT NULL COMMENT '下一个小时开始时间',
    `robot_code`                varchar(100) NOT NULL COMMENT '机器人编码',
    `first_classification_name` varchar(100)          DEFAULT NULL COMMENT '机器人类型',
    `notification_id`           varchar(100) NOT NULL COMMENT '通知ID',
    `error_code`                varchar(100)          DEFAULT NULL COMMENT '错误码',
    `alarm_module`              varchar(100)          DEFAULT NULL COMMENT '告警模块',
    `alarm_service`             varchar(100)          DEFAULT NULL COMMENT '告警服务',
    `alarm_type`                varchar(100)          DEFAULT NULL COMMENT '告警分类',
    `alarm_level`               varchar(100)          DEFAULT NULL COMMENT '告警级别',
    `start_time`                datetime(6)           DEFAULT NULL COMMENT '开始时间-告警触发时间',
    `end_time`                  datetime(6)           DEFAULT NULL COMMENT '结束时间-告警结束时间',
    `the_hour_cost_seconds`     decimal(30, 6)        DEFAULT NULL COMMENT '在该小时内时长（秒）',
    `created_time`              timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`              timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_hour_start_time (`hour_start_time`),
    key idx_next_hour_start_time (`next_hour_start_time`),
    key idx_robot_code (`robot_code`),
    key idx_notification_id (notification_id)

)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类故障通知在小时内耗时明细（当天数据）';	





#step14:删除相关数据（qt_notification_robot_module_time_hour_detail_realtime）
DELETE FROM qt_smartreport.qt_notification_robot_module_time_hour_detail_realtime;





#step15:插入相关数据(qt_notification_robot_module_time_hour_detail_realtime)
insert into qt_smartreport.qt_notification_robot_module_time_hour_detail_realtime(date_value,hour_start_time, next_hour_start_time,
                                                                         robot_code, first_classification_name,
                                                                         notification_id, error_code, alarm_module,
                                                                         alarm_service, alarm_type, alarm_level,
                                                                         start_time, end_time, the_hour_cost_seconds)
select CURRENT_DATE() as date_value,
       t1.hour_start_time,
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
                coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time then
                   UNIX_TIMESTAMP(coalesce(t2.end_time, sysdate())) - UNIX_TIMESTAMP(t2.start_time)
           when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time then
                   UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t2.start_time)
           when t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.hour_start_time and
                coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time then
                   UNIX_TIMESTAMP(coalesce(t2.end_time, sysdate())) - UNIX_TIMESTAMP(t1.hour_start_time)
           when t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time then
                   UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t1.hour_start_time)
           end           the_hour_cost_seconds
from (select h.hour_start_time,
             h.next_hour_start_time
      from (select th.day_hours                               as hour_start_time,
                   DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
            from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'), INTERVAL
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
         inner join
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
               inner join phoenix_basic.basic_robot br on br.robot_code = bn.robot_code and br.usage_state = 'using'
               left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
               inner join qt_smartreport.qt_basic_notification_clear4_realtime tbn on tbn.error_id = bn.id
      where 1 = 1
        and bn.alarm_module = 'robot'
        and (((bn.start_time >= date_format(sysdate(), '%Y-%m-%d 00:00:00') and
               bn.start_time < sysdate() and
               coalesce(bn.end_time, sysdate()) <
               sysdate())
          or
              (bn.start_time >= date_format(sysdate(), '%Y-%m-%d 00:00:00') and
               bn.start_time < sysdate() and
               coalesce(bn.end_time, sysdate()) >=
               sysdate())
          or
              (bn.start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
               coalesce(bn.end_time, sysdate()) >=
               date_format(sysdate(), '%Y-%m-%d 00:00:00') and
               coalesce(bn.end_time, sysdate()) <
               sysdate())
          or
              (bn.start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
               coalesce(bn.end_time, sysdate()) >=
               sysdate())) or bn.end_time is null)) t2 on
         (
                 (t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                  coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time)
                 or (t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                     coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time)
                 or (t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.hour_start_time and
                     coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time)
                 or (t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time)
             );





##step16:建表(qt_notification_robot_module_object_stat_realtime)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_robot_module_object_stat_realtime
(
    `id`                           int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                   datetime     NOT NULL COMMENT '统计时间',
    `date_value`                   date         NOT NULL COMMENT '日期',
    `hour_value`                   varchar(100)          DEFAULT NULL COMMENT '小时',
    `robot_code`                   varchar(100) NOT NULL COMMENT '机器人编码',
    `first_classification_name`    varchar(100)          DEFAULT NULL COMMENT '机器人类型',
    `add_notification_num`         decimal(65, 10)       DEFAULT NULL COMMENT '新增告警次数',
    `notification_num`             decimal(65, 10)       DEFAULT NULL COMMENT '告警次数',
    `notification_time`            decimal(65, 10)       DEFAULT NULL COMMENT '告警时长（秒）',
    `notification_rate`            decimal(65, 10)       DEFAULT NULL COMMENT '告警率',
    `notification_rate_fenzi`      decimal(65, 10)       DEFAULT NULL COMMENT '告警率分子',
    `notification_rate_fenmu`      decimal(65, 10)       DEFAULT NULL COMMENT '告警率分母',
    `mtbf`                         decimal(65, 10)       DEFAULT NULL COMMENT 'mtbf',
    `mtbf_fenzi`                   decimal(65, 10)       DEFAULT NULL COMMENT 'mtbf分子',
    `mtbf_fenmu`                   decimal(65, 10)       DEFAULT NULL COMMENT 'mtbf分母',
    `mttr`                         decimal(65, 10)       DEFAULT NULL COMMENT 'mttr',
    `mttr_fenzi`                   decimal(65, 10)       DEFAULT NULL COMMENT 'mttr分子',
    `mttr_fenmu`                   decimal(65, 10)       DEFAULT NULL COMMENT 'mttr分母',
    `notification_per_order`       decimal(65, 10)       DEFAULT NULL COMMENT '平均每作业单故障数',
    `notification_per_order_fenzi` decimal(65, 10)       DEFAULT NULL COMMENT '平均每作业单故障数分子',
    `notification_per_order_fenmu` decimal(65, 10)       DEFAULT NULL COMMENT '平均每作业单故障数分母',
    `notification_per_job`         decimal(65, 10)       DEFAULT NULL COMMENT '平均每任务故障数',
    `notification_per_job_fenzi`   decimal(65, 10)       DEFAULT NULL COMMENT '平均每任务故障数分子',
    `notification_per_job_fenmu`   decimal(65, 10)       DEFAULT NULL COMMENT '平均每任务故障数分母',
    `time_type`                    varchar(100)          DEFAULT NULL COMMENT '统计维度',
    `created_time`                 timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`                 timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_time_value (`time_value`),
    key idx_date_value (`date_value`),
    key idx_robot_code (`robot_code`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类告警通知在时间段内指标统计（当天数据）';	
	
	
	

##step17:删除当天相关数据(qt_notification_robot_module_object_stat_realtime)
DELETE FROM qt_smartreport.qt_notification_robot_module_object_stat_realtime;








##step18-1:插入当天相关数据(qt_notification_robot_module_object_stat_realtime)
#time_type='小时' 

insert into qt_smartreport.qt_notification_robot_module_object_stat_realtime(time_value, date_value, hour_value, robot_code,
                                                                        first_classification_name, add_notification_num,
                                                                        notification_num, notification_time,
                                                                        notification_rate, notification_rate_fenzi,
                                                                        notification_rate_fenmu, mtbf, mtbf_fenzi,
                                                                        mtbf_fenmu, mttr, mttr_fenzi, mttr_fenmu,
                                                                        notification_per_order,
                                                                        notification_per_order_fenzi,
                                                                        notification_per_order_fenmu,
                                                                        notification_per_job,
                                                                        notification_per_job_fenzi,
                                                                        notification_per_job_fenmu, time_type)

select tt.hour_start_time                                                                                           as time_value,
       date(tt.hour_start_time)                                                                                     as date_value,
       HOUR(tt.hour_start_time)                                                                                     as hour_value,
       tt.robot_code,
       tt.first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d %H:00:00') = tt.hour_start_time
                                       then t.notification_id end),
                0)                                                                                                  as add_notification_num,
       count(distinct t.notification_id)                                                                            as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                      as notification_time,
       cast(coalesce(sum(the_hour_cost_seconds), 0) / (case
                                                           when HOUR(tt.hour_start_time) = HOUR(sysdate())
                                                               then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(tt.hour_start_time)
                                                           else 3600 end) as decimal(65, 10))                       as notification_rate,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                      as notification_rate_fenzi,
       (case
            when HOUR(tt.hour_start_time) = HOUR(sysdate())
                then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(tt.hour_start_time)
            else 3600 end)                                                                                          as notification_rate_fenmu,
       cast(case
                when count(distinct t.notification_id) != 0 then ((case
                                                                       when HOUR(tt.hour_start_time) = HOUR(sysdate())
                                                                           then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(tt.hour_start_time)
                                                                       else 3600 end) -
                                                                  coalesce(sum(the_hour_cost_seconds), 0)) /
                                                                 count(distinct t.notification_id)
                else (case
                          when HOUR(tt.hour_start_time) = HOUR(sysdate())
                              then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(tt.hour_start_time)
                          else 3600 end) end as decimal(65, 10))                                                    as mtbf,
       ((case
             when HOUR(tt.hour_start_time) = HOUR(sysdate())
                 then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(tt.hour_start_time)
             else 3600 end) -
        coalesce(sum(the_hour_cost_seconds), 0))                                                                    as mtbf_fenzi,
       count(distinct t.notification_id)                                                                            as mtbf_fenmu,
       cast(case
                when count(distinct t.notification_id) != 0 then coalesce(sum(the_hour_cost_seconds), 0) /
                                                                 count(distinct t.notification_id)
                else 0 end as decimal(65, 10))                                                                      as mttr,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                      as mttr_fenzi,
       count(distinct t.notification_id)                                                                            as mttr_fenmu,
       cast(case
                when tto.order_num != 0
                    then count(distinct t.notification_id) / tto.order_num end as decimal(65, 10))                  as notification_per_order,
       count(distinct t.notification_id)                                                                            as notification_per_order_fenzi,
       coalesce(tto.order_num, 0)                                                                                   as notification_per_order_fenmu,
       cast(case
                when jh.job_num != 0
                    then count(distinct t.notification_id) / jh.job_num end as decimal(65, 10))                    as notification_per_job,
       count(distinct t.notification_id)                                                                            as notification_per_job_fenzi,
       coalesce(jh.job_num, 0)                                                                                     as notification_per_job_fenmu,
       '小时'                                                                                                         as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             t2.robot_code,
             t2.first_classification_name
      from (select h.hour_start_time,
                   h.next_hour_start_time
            from (select th.day_hours                               as hour_start_time,
                         DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
                  from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'), INTERVAL
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
                     left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
            where br.usage_state = 'using') t2 on 1) tt
         left join qt_smartreport.qt_notification_robot_module_time_hour_detail_realtime t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.robot_code = tt.robot_code and t.date_value = CURRENT_DATE() and
                      t.the_hour_cost_seconds is not null
					  
		         left join (select dispatch_robot_code                           as robot_code,
                           date_format(update_time, '%Y-%m-%d %H:00:00') as hour_start_time,
                           count(distinct order_no)                      as order_num
                    from phoenix_rss.transport_order
                    where order_state in ('COMPLETED', 'CANCELED', 'ABNORMAL_COMPLETED', 'ABNORMAL_CANCELED')
                      and date_format(update_time, '%Y-%m-%d') =
                          date_format(sysdate(), '%Y-%m-%d')
                    group by dispatch_robot_code, hour_start_time) tto
                   on tto.robot_code = tt.robot_code and
                      tto.hour_start_time = tt.hour_start_time
         left join (select robot_code,
                           date_format(finish_time, '%Y-%m-%d %H:00:00') as hour_start_time,
                           count(distinct job_sn)                        as job_num
                    from phoenix_rms.job_history
                    where date_format(finish_time, '%Y-%m-%d') =
                          date_format(sysdate(), '%Y-%m-%d')
                    group by robot_code, hour_start_time) jh
                   on jh.robot_code = tt.robot_code and jh.hour_start_time = tt.hour_start_time						   
group by tt.hour_start_time, date(tt.hour_start_time), HOUR(tt.hour_start_time), tt.robot_code,
         tt.first_classification_name
;





##step18-2:插入当天相关数据(qt_notification_robot_module_object_stat_realtime)
#time_type='天'

insert into qt_smartreport.qt_notification_robot_module_object_stat_realtime(time_value, date_value, hour_value, robot_code,
                                                                        first_classification_name, add_notification_num,
                                                                        notification_num, notification_time,
                                                                        notification_rate, notification_rate_fenzi,
                                                                        notification_rate_fenmu, mtbf, mtbf_fenzi,
                                                                        mtbf_fenmu, mttr, mttr_fenzi, mttr_fenmu,
                                                                        notification_per_order,
                                                                        notification_per_order_fenzi,
                                                                        notification_per_order_fenmu,
                                                                        notification_per_job,
                                                                        notification_per_job_fenzi,
                                                                        notification_per_job_fenmu, time_type)
select date(tt.hour_start_time)                                                                                                         as time_value,
       date(tt.hour_start_time)                                                                                                         as date_value,
       null                                                                                                                             as hour_value,
       tt.robot_code,
       tt.first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d') =
                                        date_format(tt.hour_start_time, '%Y-%m-%d')
                                       then t.notification_id end),
                0)                                                                                                                      as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                                                   as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                                          as notification_time,
       cast(coalesce(sum(the_hour_cost_seconds), 0) / (UNIX_TIMESTAMP(sysdate()) -
                                                       UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) as decimal(65, 10)) as notification_rate,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                                          as notification_rate_fenzi,
       (UNIX_TIMESTAMP(sysdate()) -
        UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00')))                                                                    as notification_rate_fenmu,


       cast(case
                when count(distinct t.notification_id) != 0 then
                        ((UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) -
                         coalesce(sum(the_hour_cost_seconds), 0)) /
                        count(distinct t.notification_id)
                else (UNIX_TIMESTAMP(sysdate()) -
                      UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end as decimal(65, 10))                              as mtbf,
       ((UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) -
        coalesce(sum(the_hour_cost_seconds), 0))                                                                                        as mtbf_fenzi,
       coalesce(count(distinct t.notification_id), 0)                                                                                   as mtbf_fenmu,
       cast(case
                when count(distinct t.notification_id) != 0 then coalesce(sum(the_hour_cost_seconds), 0) /
                                                                 count(distinct t.notification_id)
                else 0 end as decimal(65, 10))                                                                                          as mttr,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                                          as mttr_fenzi,
       coalesce(count(distinct t.notification_id), 0)                                                                                   as mttr_fenmu,
       cast(case
                when tto.order_num != 0
                    then coalesce(count(distinct t.notification_id), 0) / tto.order_num end as decimal(65, 10))                         as notification_per_order,
       coalesce(count(distinct t.notification_id), 0)                                                                                   as notification_per_order_fenzi,
       coalesce(tto.order_num, 0)                                                                                                       as notification_per_order_fenmu,
       cast(case
                when jh.job_num != 0
                    then coalesce(count(distinct t.notification_id), 0) / jh.job_num end as decimal(65, 10))                           as notification_per_job,
       coalesce(count(distinct t.notification_id), 0)                                                                                   as notification_per_job_fenzi,
       coalesce(jh.job_num, 0)                                                                                                         as notification_per_job_fenmu,
       '天'                                                                                                                              as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             t2.robot_code,
             t2.first_classification_name
      from (select h.hour_start_time,
                   h.next_hour_start_time
            from (select th.day_hours                               as hour_start_time,
                         DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
                  from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'), INTERVAL
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
                     left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
            where br.usage_state = 'using') t2 on 1) tt
         left join qt_smartreport.qt_notification_robot_module_time_hour_detail_realtime t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.robot_code = tt.robot_code and t.date_value = CURRENT_DATE() and
                      t.the_hour_cost_seconds is not null			  
		         left join (select dispatch_robot_code                  as robot_code,
                           date_format(update_time, '%Y-%m-%d') as date_value,
                           count(distinct order_no)             as order_num
                    from phoenix_rss.transport_order
                    where order_state in ('COMPLETED', 'CANCELED', 'ABNORMAL_COMPLETED', 'ABNORMAL_CANCELED')
                      and date_format(update_time, '%Y-%m-%d') =
                          date_format(sysdate(), '%Y-%m-%d')
                    group by dispatch_robot_code, date_value) tto
                   on tto.robot_code = tt.robot_code and
                      tto.date_value = date(tt.hour_start_time)
         left join (select robot_code,
                           date_format(finish_time, '%Y-%m-%d') as date_value,
                           count(distinct job_sn)               as job_num
                    from phoenix_rms.job_history
                    where date_format(finish_time, '%Y-%m-%d') =
                          date_format(sysdate(), '%Y-%m-%d')
                    group by robot_code, date_value) jh
                   on jh.robot_code = tt.robot_code and
                      jh.date_value = date(tt.hour_start_time)
group by date(tt.hour_start_time), date(tt.hour_start_time), hour_value, tt.robot_code,
         tt.first_classification_name
;	