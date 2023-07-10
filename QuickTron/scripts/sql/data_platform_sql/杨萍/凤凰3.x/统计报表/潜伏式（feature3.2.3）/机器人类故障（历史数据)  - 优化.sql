机器人类故障收敛规则：
1、故障等级>=3（现场需要人工介入的机器人故障）
2、按故障开始时间排序，连续相同故障码error_code符合上一条的结束时间与下一条的开始时间间隔<60s的故障归位一组，则整组故障开始时间取第一条的start_time，整组故障的结束时间取最后一条的end_time
3、机器人多条故障均没有结束时间or结束时间相同，取第一条
4、按故障开始时间排序，机器人多条故障结束时间间隔<3s，取第一条


规则2mysql5.7实现算法：
step1：将每一条故障与其上一条进行比较，如果error_code不相同 or error_code相同但是start_time与上一条end_time间隔时间>=60s,则标记为1，否则标记为0。（1表示是每组的开始故障）
step2:将step1中所有标记为1的故障用start_time升序排序，标记排序序号，此序号即为组故障的排序位置。
step3:将每一条故障与其下一条进行比较，如果error_code不相同 or error_code相同但是end_time与下一条start_time间隔>=60s，则标记为1，否则标记为0。（1表示是每组的结束故障）
step4:将step3中所有标记为1的故障用start_time升序排序，标记排序序号，此序号即为组故障的排序位置。
step5:用step2与step4中的组排序位置相关联，找到每组故障的开始故障和结束故障，再用结束故障的end_time替换掉开始故障的end_time。收敛的故障即为每组故障的开始故障。







#step1:建表（qt_basic_notification_clear1）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_basic_notification_clear1
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
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类故障收敛清洗step1';



#step2:删除相关数据（qt_basic_notification_clear1）
DELETE
FROM qt_smartreport.qt_basic_notification_clear1
where date_value=date_add(CURRENT_DATE(), interval -1 day);



#step3:插入相关数据（qt_basic_notification_clear1）
insert into qt_smartreport.qt_basic_notification_clear1(date_value, robot_code, error_id, error_code, start_time, end_time)
select date_add(CURRENT_DATE(), interval -1 day) as date_value,
       robot_code,
       id                                        as error_id,
       error_code,
       start_time,
       end_time
from phoenix_basic.basic_notification
where alarm_module = 'robot'
  and alarm_level >= 3
  and (
        (start_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
         start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
         coalesce(end_time, sysdate()) < date_format(sysdate(), '%Y-%m-%d 00:00:00')) or
        (start_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
         start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
         coalesce(end_time, sysdate()) >= date_format(sysdate(), '%Y-%m-%d 00:00:00')) or
        (start_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
         coalesce(end_time, sysdate()) >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
         coalesce(end_time, sysdate()) < date_format(sysdate(), '%Y-%m-%d 00:00:00')) or
        (start_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
         coalesce(end_time, sysdate()) >= date_format(sysdate(), '%Y-%m-%d 00:00:00'))
    )
;



#step4:建表（qt_basic_notification_clear2）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_basic_notification_clear2
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
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类故障收敛清洗step2';


#step5:删除相关数据（qt_basic_notification_clear2）
DELETE
FROM qt_smartreport.qt_basic_notification_clear2
where date_value=date_add(CURRENT_DATE(), interval -1 day);



#step6:插入相关数据（qt_basic_notification_clear2）
insert into qt_smartreport.qt_basic_notification_clear2(date_value, robot_code, error_id, error_code, start_time,
                                                        end_time, pre_error_id, pre_start_time, pre_end_time,
                                                        diff_seconds)
select date_add(CURRENT_DATE(), interval -1 day) as date_value,
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
                 when UNIX_TIMESTAMP(t3.start_time) - UNIX_TIMESTAMP(t4.end_time) < 60 then 0
                 else 1 end                                                 is_effective
      from (select t1.error_id,
                   t1.robot_code,
                   t1.error_code,
                   t1.start_time,
                   t1.end_time,
                   max(t2.error_id) as pre_error_id
            from qt_smartreport.qt_basic_notification_clear1 t1
                     left join qt_smartreport.qt_basic_notification_clear1 t2
                               on t2.robot_code = t1.robot_code and t2.error_code = t1.error_code and
                                  t2.date_value = t1.date_value and t2.start_time < t1.start_time
            where t1.date_value = date_add(CURRENT_DATE(), interval -1 day)
            group by t1.error_id, t1.robot_code, t1.error_code, t1.start_time, t1.end_time) t3
               left join qt_smartreport.qt_basic_notification_clear1 t4
                         on t4.robot_code = t3.robot_code and t4.error_id = t3.pre_error_id) t5
where t5.is_effective = 1
;





#step7:建表（qt_basic_notification_clear3）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_basic_notification_clear3
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
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类故障收敛清洗step3';



#step8:删除相关数据（qt_basic_notification_clear3）
DELETE
FROM qt_smartreport.qt_basic_notification_clear3
where date_value=date_add(CURRENT_DATE(), interval -1 day);




#step9:插入相关数据（qt_basic_notification_clear3）
insert into qt_smartreport.qt_basic_notification_clear3(date_value, robot_code, error_id, error_code, start_time, end_time)
select date_add(CURRENT_DATE(), interval -1 day) as date_value,
       t1.robot_code,
       t1.error_id,
       t1.error_code,
       t1.start_time,
       t1.end_time
from qt_smartreport.qt_basic_notification_clear2 t1
         inner join (select robot_code,
                            end_time,
                            min(error_id) as first_error_id
                     from qt_smartreport.qt_basic_notification_clear2
                     where date_value = date_add(CURRENT_DATE(), interval -1 day)
                     group by robot_code, end_time) t on t.robot_code = t1.robot_code and t.first_error_id = t1.error_id
where t1.date_value = date_add(CURRENT_DATE(), interval -1 day)
;




#step10:建表（qt_basic_notification_clear4）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_basic_notification_clear4
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
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类故障收敛清洗step4';



#step11:删除相关数据（qt_basic_notification_clear4）
DELETE
FROM qt_smartreport.qt_basic_notification_clear4
where date_value=date_add(CURRENT_DATE(), interval -1 day);



#step12:插入相关数据（qt_basic_notification_clear4）
insert into qt_smartreport.qt_basic_notification_clear4(date_value, robot_code, error_id, error_code, start_time, end_time)
select date_add(CURRENT_DATE(), interval -1 day) as date_value,
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
            from qt_smartreport.qt_basic_notification_clear3 t1
                     left join qt_smartreport.qt_basic_notification_clear3 t2
                               on t2.robot_code = t1.robot_code and t1.date_value = t2.date_value and
                                  t2.start_time < t1.start_time
            where t1.date_value = date_add(CURRENT_DATE(), interval -1 day)
            group by t1.error_id, t1.robot_code, t1.error_code, t1.start_time, t1.end_time) t3
               left join qt_smartreport.qt_basic_notification_clear3 t4
                         on t4.robot_code = t3.robot_code and t4.error_id = t3.pre_error_id and
                            t4.date_value = date_add(CURRENT_DATE(), interval -1 day)) t5
where t5.is_effective = 1
;



#step13:建表（qt_notification_robot_module_time_hour_detail）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_robot_module_time_hour_detail
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
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类故障通知在小时内耗时明细';	




#step14:删除相关数据（qt_notification_robot_module_time_hour_detail）
DELETE
FROM qt_smartreport.qt_notification_robot_module_time_hour_detail
where date_value=date_add(CURRENT_DATE(), interval -1 day);




#step15:插入相关数据(qt_notification_robot_module_time_hour_detail)
insert into qt_smartreport.qt_notification_robot_module_time_hour_detail(date_value,hour_start_time, next_hour_start_time,
                                                                         robot_code, first_classification_name,
                                                                         notification_id, error_code, alarm_module,
                                                                         alarm_service, alarm_type, alarm_level,
                                                                         start_time, end_time, the_hour_cost_seconds)
select date_add(CURRENT_DATE(), interval -1 day) as date_value,
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
           end                                      the_hour_cost_seconds

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
      from qt_smartreport.qt_basic_notification_clear4 tbn
               inner join phoenix_basic.basic_notification bn
                          on tbn.error_id = bn.id and tbn.date_value = date_add(CURRENT_DATE(), interval -1 day)
               inner join phoenix_basic.basic_robot br on br.robot_code = bn.robot_code and br.usage_state = 'using'
               left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
      where tbn.date_value = date_add(CURRENT_DATE(), interval -1 day)) t2 on
         (
                 (t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                  coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time)
                 or (t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                     coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time)
                 or (t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.hour_start_time and
                     coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time)
                 or (t2.start_time < t1.hour_start_time and coalesce(t2.end_time, sysdate()) >= t1.next_hour_start_time)
             );
			 
		
		
		
##step16:建表(qt_notification_robot_module_object_stat)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_robot_module_object_stat
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
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类告警通知在时间段内指标统计';	
	
	
	
--------------------------------
##step17:删除当天相关数据(qt_notification_robot_module_object_stat)
DELETE
FROM qt_smartreport.qt_notification_robot_module_object_stat
where date_value=date_add(CURRENT_DATE(), interval -1 day);






##step18-1:插入当天相关数据(qt_notification_robot_module_object_stat)
#time_type='小时'
 
insert into qt_smartreport.qt_notification_robot_module_object_stat(time_value, date_value, hour_value, robot_code,
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
       cast(coalesce(sum(the_hour_cost_seconds), 0) / 3600 as decimal(65, 10))                                      as notification_rate,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                      as notification_rate_fenzi,
       3600                                                                                                         as notification_rate_fenmu,
       cast(case
                when count(distinct t.notification_id) != 0 then (3600 - coalesce(sum(the_hour_cost_seconds), 0)) /
                                                                 count(distinct t.notification_id)
                else 3600 end as decimal(65, 10))                                                                   as mtbf,
       (3600 - coalesce(sum(the_hour_cost_seconds), 0))                                                             as mtbf_fenzi,
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
                     left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
            where br.usage_state = 'using') t2 on 1) tt
         left join qt_smartreport.qt_notification_robot_module_time_hour_detail t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.robot_code = tt.robot_code and t.date_value = date_add(CURRENT_DATE(), interval -1 day) and
                      t.the_hour_cost_seconds is not null
					  
		         left join (select dispatch_robot_code                           as robot_code,
                           date_format(update_time, '%Y-%m-%d %H:00:00') as hour_start_time,
                           count(distinct order_no)                      as order_num
                    from phoenix_rss.transport_order
                    where order_state in ('COMPLETED', 'CANCELED', 'ABNORMAL_COMPLETED', 'ABNORMAL_CANCELED')
                      and date_format(update_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                    group by dispatch_robot_code, hour_start_time) tto
                   on tto.robot_code = tt.robot_code and
                      tto.hour_start_time = tt.hour_start_time
         left join (select robot_code,
                           date_format(finish_time, '%Y-%m-%d %H:00:00') as hour_start_time,
                           count(distinct job_sn)                        as job_num
                    from phoenix_rms.job_history
                    where date_format(finish_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                    group by robot_code, hour_start_time) jh
                   on jh.robot_code = tt.robot_code and jh.hour_start_time = tt.hour_start_time			   
group by tt.hour_start_time, date(tt.hour_start_time), HOUR(tt.hour_start_time), tt.robot_code,
         tt.first_classification_name
;


##step18-2:插入当天相关数据(qt_notification_robot_module_object_stat)
#time_type='天'

insert into qt_smartreport.qt_notification_robot_module_object_stat(time_value, date_value, hour_value, robot_code,
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

select date(tt.hour_start_time)                                                                                 as time_value,
       date(tt.hour_start_time)                                                                                 as date_value,
       null                                                                                                     as hour_value,
       tt.robot_code,
       tt.first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d') =
                                        date_format(tt.hour_start_time, '%Y-%m-%d')
                                       then t.notification_id end),
                0)                                                                                              as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                           as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                  as notification_time,
       cast(coalesce(sum(the_hour_cost_seconds), 0) / (3600 * 24) as decimal(65, 10))                           as notification_rate,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                  as notification_rate_fenzi,
       3600 * 24                                                                                                as notification_rate_fenmu,
       cast(case
                when count(distinct t.notification_id) != 0 then (3600 * 24 - coalesce(sum(the_hour_cost_seconds), 0)) /
                                                                 count(distinct t.notification_id)
                else 3600 * 24 end as decimal(65, 10))                                                          as mtbf,
       (3600 * 24 - coalesce(sum(the_hour_cost_seconds), 0))                                                    as mtbf_fenzi,
       coalesce(count(distinct t.notification_id), 0)                                                           as mtbf_fenmu,
       cast(case
                when count(distinct t.notification_id) != 0 then coalesce(sum(the_hour_cost_seconds), 0) /
                                                                 count(distinct t.notification_id)
                else 0 end as decimal(65, 10))                                                                  as mttr,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                  as mttr_fenzi,
       coalesce(count(distinct t.notification_id), 0)                                                           as mttr_fenmu,
       cast(case
                when tto.order_num != 0
                    then coalesce(count(distinct t.notification_id), 0) / tto.order_num end as decimal(65, 10)) as notification_per_order,
       coalesce(count(distinct t.notification_id), 0)                                                           as notification_per_order_fenzi,
       coalesce(tto.order_num, 0)                                                                               as notification_per_order_fenmu,
       cast(case
                when jh.job_num != 0
                    then coalesce(count(distinct t.notification_id), 0) / jh.job_num end as decimal(65, 10))   as notification_per_job,
       coalesce(count(distinct t.notification_id), 0)                                                           as notification_per_job_fenzi,
       coalesce(jh.job_num, 0)                                                                                 as notification_per_job_fenmu,
       '天'                                                                                                      as time_type
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
                     left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
            where br.usage_state = 'using') t2 on 1) tt
         left join qt_smartreport.qt_notification_robot_module_time_hour_detail t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.robot_code = tt.robot_code and t.date_value = date_add(CURRENT_DATE(), interval -1 day) and
                      t.the_hour_cost_seconds is not null
		
         left join (select dispatch_robot_code                  as robot_code,
                           date_format(update_time, '%Y-%m-%d') as date_value,
                           count(distinct order_no)             as order_num
                    from phoenix_rss.transport_order
                    where order_state in ('COMPLETED', 'CANCELED', 'ABNORMAL_COMPLETED', 'ABNORMAL_CANCELED')
                      and date_format(update_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                    group by dispatch_robot_code, date_value) tto
                   on tto.robot_code = tt.robot_code and
                      tto.date_value = date(tt.hour_start_time)

         left join (select robot_code,
                           date_format(finish_time, '%Y-%m-%d') as date_value,
                           count(distinct job_sn)               as job_num
                    from phoenix_rms.job_history
                    where date_format(finish_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                    group by robot_code, date_value) jh
                   on jh.robot_code = tt.robot_code and
                      jh.date_value = date(tt.hour_start_time)
group by date(tt.hour_start_time), date(tt.hour_start_time), hour_value, tt.robot_code,
         tt.first_classification_name
;
