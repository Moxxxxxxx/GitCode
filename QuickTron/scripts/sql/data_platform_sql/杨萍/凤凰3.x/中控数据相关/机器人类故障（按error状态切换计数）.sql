------------------------------------------------------------------------------------------------
--step1:建表（qt_robot_error_state_time_hour_detail）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_error_state_time_hour_detail
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `hour_start_time`           datetime  NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time`      datetime  NOT NULL COMMENT '下一个小时开始时间',
    `robot_code`                varchar(100)       DEFAULT NULL COMMENT '机器人编码',
    `error_id`           varchar(100)       DEFAULT NULL COMMENT '机器人故障ID',
    `start_time`                datetime           DEFAULT NULL COMMENT '开始时间',
    `end_time`                  datetime           DEFAULT NULL COMMENT '结束时间',
    `the_hour_cost_seconds`     decimal(10, 3)     DEFAULT NULL COMMENT '在该小时内时长（秒）',
    `created_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类故障状态在小时内耗时明细';	


------------------------------------------------------------------------------------------------
--step2:删除当天相关数据（qt_robot_error_state_time_hour_detail）
DELETE
FROM qt_smartreport.qt_robot_error_state_time_hour_detail
WHERE date_format(hour_start_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  


------------------------------------------------------------------------------------------------
--step3:插入当天相关数据(qt_robot_error_state_time_hour_detail)
insert into qt_smartreport.qt_robot_error_state_time_hour_detail(hour_start_time, next_hour_start_time,robot_code, error_id,start_time, end_time, the_hour_cost_seconds)
select t1.hour_start_time,
       t1.next_hour_start_time,
       t2.robot_code,
       t2.error_id,
       t2.start_time,
       t2.end_time,
       case
           when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                coalesce(t2.end_time, sysdate()) < t1.next_hour_start_time then timestampdiff(second, t2.start_time,
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
         inner join
     (select t.error_id,
             t.robot_code,
             t.error_start_time as start_time,
             t.next_state_time  as end_time
      from (select t1.error_id,
                   t1.robot_code,
                   t1.error_start_time,
                   min(t2.state_id)    as next_state_id,
                   min(t2.create_time) as next_state_time
            from (select id          as error_id,
                         robot_code,
                         create_time as error_start_time,
                         job_sn
                  from phoenix_rms.robot_state_history
                  where work_state = 'ERROR'
                    and create_time >= date_format(date_add(sysdate(), interval -3 day), '%Y-%m-%d 00:00:00')) t1
                     left join
                 (select id as state_id,
                         robot_code,
                         create_time
                  from phoenix_rms.robot_state_history
                  where create_time >= date_format(date_add(sysdate(), interval -3 day), '%Y-%m-%d 00:00:00')) t2
                 on t2.robot_code = t1.robot_code and t2.create_time > t1.error_start_time
            group by t1.error_id, t1.robot_code, t1.error_start_time) t
      where 1 = 1
        and ((t.error_start_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
              t.error_start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
              date_format(coalesce(t.next_state_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
              date_format(sysdate(), '%Y-%m-%d 00:00:00'))
          or
             (t.error_start_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
              t.error_start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
              date_format(coalesce(t.next_state_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
              date_format(sysdate(), '%Y-%m-%d 00:00:00'))
          or
             (t.error_start_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
              date_format(coalesce(t.next_state_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
              date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
              date_format(coalesce(t.next_state_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
              date_format(sysdate(), '%Y-%m-%d 00:00:00'))
          or
             (t.error_start_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
              date_format(coalesce(t.next_state_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
              date_format(sysdate(), '%Y-%m-%d 00:00:00')))
     ) t2 on 1
;




--------------------------------
##step4:建表(qt_robot_error_state_object_stat)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_error_state_object_stat
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                datetime  NOT NULL COMMENT '统计时间',
    `date_value`                date               DEFAULT NULL COMMENT '日期',
    `hour_value`                varchar(100)       DEFAULT NULL COMMENT '小时',
    `robot_code`                varchar(100)       DEFAULT NULL COMMENT '机器人编码',
    `first_classification_name` varchar(100)       DEFAULT NULL COMMENT '机器人类型',
    `add_error_num`      int(100)           DEFAULT NULL COMMENT '新增故障次数',
    `error_num`          int(100)           DEFAULT NULL COMMENT '故障次数',
    `error_time`         int(100)           DEFAULT NULL COMMENT '故障时长（秒）',
    `error_rate`         decimal(10, 4)     DEFAULT NULL COMMENT '故障率',
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
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类故障状态在时间段内指标统计';	
	

	
--------------------------------
##step5:删除当天相关数据(qt_robot_error_state_object_stat)
DELETE
FROM qt_smartreport.qt_robot_error_state_object_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  



--------------------------------
##step6-1:插入当天相关数据(qt_robot_error_state_object_stat)
#time_type='小时' 
insert into qt_smartreport.qt_robot_error_state_object_stat(time_value, date_value, hour_value, robot_code,first_classification_name,add_error_num, error_num,
error_time, error_rate, mtbf, mttr, time_type)
select tt.hour_start_time                                                                                                           as time_value,
       date(tt.hour_start_time)                                                                                                     as date_value,
       HOUR(tt.hour_start_time)                                                                                                     as hour_value,
       tt.robot_code,
       tt.first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d %H:00:00') = tt.hour_start_time
                                       then t.error_id end),
                0)                                                                                                                  as add_notification_num,
       coalesce(count(distinct t.error_id), 0)                                                                                      as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                                      as notification_time,
       cast(coalesce(sum(the_hour_cost_seconds), 0) / 3600 as decimal(10, 4))                                                       as notification_rate,
       cast(case
                when count(distinct t.error_id) != 0 then cast((3600 - coalesce(sum(the_hour_cost_seconds), 0)) /
                                                               count(distinct t.error_id) as decimal(10, 2)) end as decimal(10, 2)) as mtbf,
       cast(case
                when count(distinct t.error_id) != 0 then cast(coalesce(sum(the_hour_cost_seconds), 0) /
                                                               count(distinct t.error_id) as decimal(10, 2)) end as decimal(10, 2)) as mttr,
       '小时'                                                                                                                         as time_type
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
         left join qt_smartreport.qt_robot_error_state_time_hour_detail t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.robot_code = tt.robot_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                       date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, robot_code, first_classification_name
;



--------------------------------
##step6-2:插入当天相关数据(qt_robot_error_state_object_stat)
#time_type='天'

insert into qt_smartreport.qt_robot_error_state_object_stat(time_value, date_value, hour_value, robot_code,first_classification_name,add_error_num, error_num,
error_time, error_rate, mtbf, mttr, time_type)
select date_format(tt.hour_start_time, '%Y-%m-%d')                                                                                  as time_value,
       date(tt.hour_start_time)                                                                                                     as date_value,
       null                                                                                                                         as hour_value,
       tt.robot_code,
       tt.first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d') =
                                        date_format(tt.hour_start_time, '%Y-%m-%d')
                                       then t.error_id end),
                0)                                                                                                                  as add_notification_num,
       coalesce(count(distinct t.error_id), 0)                                                                                      as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                                      as notification_time,
       cast(coalesce(sum(the_hour_cost_seconds), 0) / 3600 / 24 as decimal(10, 4))                                                  as notification_rate,
       cast(case
                when count(distinct t.error_id) != 0 then cast(
                            (3600 * 24 - coalesce(sum(the_hour_cost_seconds), 0)) /
                            count(distinct t.error_id) as decimal(10, 2)) end as decimal(10, 2))                                    as mtbf,
       cast(case
                when count(distinct t.error_id) != 0 then cast(coalesce(sum(the_hour_cost_seconds), 0) /
                                                               count(distinct t.error_id) as decimal(10, 2)) end as decimal(10, 2)) as mttr,
       '天'                                                                                                                          as time_type
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
         left join qt_smartreport.qt_robot_error_state_time_hour_detail t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      t.robot_code = tt.robot_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                       date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, robot_code, first_classification_name
;





--------------------------------
##step7:建表(qt_robot_error_state_stat)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_error_state_stat
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                datetime  NOT NULL COMMENT '统计时间',
    `date_value`                date               DEFAULT NULL COMMENT '日期',
    `hour_value`                varchar(100)       DEFAULT NULL COMMENT '小时',
    `robot_num`                int(100)       DEFAULT NULL COMMENT '机器人数量',
    `add_error_num`      int(100)           DEFAULT NULL COMMENT '新增故障次数',
    `error_num`          int(100)           DEFAULT NULL COMMENT '故障次数',
    `error_time`         int(100)           DEFAULT NULL COMMENT '故障时长（秒）',
    `error_rate`         decimal(10, 4)     DEFAULT NULL COMMENT '故障率',
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
    ROW_FORMAT = DYNAMIC COMMENT ='全场机器人类故障状态在时间段内指标统计';	
	
	
--------------------------------
##step8:删除当天相关数据(qt_robot_error_state_stat)
DELETE
FROM qt_smartreport.qt_robot_error_state_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  



--------------------------------
##step9-1:插入当天相关数据(qt_robot_error_state_stat)
#time_type='小时' 
insert into qt_smartreport.qt_robot_error_state_stat(time_value, date_value, hour_value, robot_num,add_error_num, error_num,error_time, error_rate, mtbf, mttr,time_type)
select tt.hour_start_time                                                                      as time_value,
       date(tt.hour_start_time)                                                                as date_value,
       HOUR(tt.hour_start_time)                                                                as hour_value,
       tt.robot_num,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d %H:00:00') = tt.hour_start_time
                                       then t.error_id end),
                0)                                                                             as add_error_num,
       coalesce(count(distinct t.error_id), 0)                                                 as error_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                 as error_time,
       cast(coalesce(sum(the_hour_cost_seconds), 0) / (3600 * tt.robot_num) as decimal(10, 4)) as error_rate,
       cast(case
                when count(distinct t.error_id) != 0 then cast(
                            (3600 * tt.robot_num - coalesce(sum(the_hour_cost_seconds), 0)) /
                            count(distinct t.error_id) as decimal(10, 2))
                else 3600 * tt.robot_num end as decimal(10, 2))                                as mtbf,
       cast(case
                when count(distinct t.error_id) != 0 then cast(coalesce(sum(the_hour_cost_seconds), 0) /
                                                               count(distinct t.error_id) as decimal(10, 2))
                else 0 end as decimal(10, 2))                                                  as mttr,
       '小时'                                                                                    as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             count(distinct t2.robot_code) as robot_num
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
                     left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id) t2 on 1
      group by t1.hour_start_time, t1.next_hour_start_time) tt
         left join qt_smartreport.qt_robot_error_state_time_hour_detail t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      date_format(t.hour_start_time, '%Y-%m-%d') =
                      date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, robot_num
;



--------------------------------
##step9-2:插入当天相关数据(qt_robot_error_state_stat)
#time_type='天'

insert into qt_smartreport.qt_robot_error_state_stat(time_value, date_value, hour_value, robot_num,add_error_num, error_num,error_time, error_rate, mtbf, mttr,time_type)
select date_format(tt.hour_start_time, '%Y-%m-%d')                                                  as time_value,
       date(tt.hour_start_time)                                                                     as date_value,
       null                                                                                         as hour_value,
       tt.robot_num,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d') =
                                        date_format(tt.hour_start_time, '%Y-%m-%d')
                                       then t.error_id end),
                0)                                                                                  as add_error_num,
       coalesce(count(distinct t.error_id), 0)                                                      as error_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                      as error_time,
       cast(coalesce(sum(the_hour_cost_seconds), 0) / (3600 * 24 * tt.robot_num) as decimal(10, 4)) as error_rate,
       cast(case
                when count(distinct t.error_id) != 0 then cast(
                            (3600 * 24 * tt.robot_num - coalesce(sum(the_hour_cost_seconds), 0)) /
                            count(distinct t.error_id) as decimal(10, 2))
                else 3600 * 24 * tt.robot_num end as decimal(10, 2))                                as mtbf,
       cast(case
                when count(distinct t.error_id) != 0 then cast(coalesce(sum(the_hour_cost_seconds), 0) /
                                                               count(distinct t.error_id) as decimal(10, 2))
                else 0 end as decimal(10, 2))                                                       as mttr,
       '天'                                                                                          as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             count(distinct t2.robot_code) as robot_num
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
                     left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id) t2 on 1
      group by t1.hour_start_time, t1.next_hour_start_time) tt
         left join qt_smartreport.qt_robot_error_state_time_hour_detail t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      date_format(t.hour_start_time, '%Y-%m-%d') =
                      date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
group by time_value, date_value, hour_value, robot_num
;



--------------------------------
##step10:建表(qt_robot_error_state_index_stat)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_error_state_index_stat
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
    ROW_FORMAT = DYNAMIC COMMENT ='全场机器人类故障状态在时间段内各指标值';	
	
	
--------------------------------
##step11:删除数据(qt_robot_error_state_index_stat)
DELETE
FROM qt_smartreport.qt_robot_error_state_index_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');   
	


--------------------------------
##step12:插入当天相关数据(qt_robot_error_state_index_stat)
insert into qt_smartreport.qt_robot_error_state_index_stat(time_value, date_value, hour_value, time_type,index_value, value_type)
select time_value,
       DATE(date_value)                         as date_value,
       hour_value,
       time_type,
       cast(add_error_num as decimal(65, 4)) as index_value,
       '新增故障次数'                                   as value_type
from qt_smartreport.qt_robot_error_state_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                          as date_value,
       hour_value,
       time_type,
       cast(error_time as decimal(65, 4)) as index_value,
       '故障时长'                                    as value_type
from qt_smartreport.qt_robot_error_state_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                          as date_value,
       hour_value,
       time_type,
       cast(error_rate as decimal(65, 4)) as index_value,
       '故障率'                                     as value_type
from qt_smartreport.qt_robot_error_state_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)             as date_value,
       hour_value,
       time_type,
       cast(mtbf as decimal(65, 4)) as index_value,
       'MTBF'                       as value_type
from qt_smartreport.qt_robot_error_state_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)             as date_value,
       hour_value,
       time_type,
       cast(mttr as decimal(65, 4)) as index_value,
       'MTTR'                       as value_type
from qt_smartreport.qt_robot_error_state_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
;
