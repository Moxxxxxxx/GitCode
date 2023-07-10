
##step1:建表（临时表qt_robot_state_time_hour_detail_temp）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_state_time_hour_detail_temp
(
    `id`                    int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `hour_start_time`       datetime     NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time`  datetime     NOT NULL COMMENT '下一个小时开始时间',
    `state_id`              varchar(100) NOT NULL COMMENT '机器人状态变化id',
    `robot_code`            varchar(100)          DEFAULT NULL COMMENT '机器人编码',
    `create_time`           datetime(6)              DEFAULT NULL COMMENT '状态开始时间',
    `network_state`         varchar(100)          DEFAULT NULL COMMENT '网络状态',
    `online_state`          varchar(100)          DEFAULT NULL COMMENT '在线状态',
    `work_state`            varchar(100)          DEFAULT NULL COMMENT '工作状态',
    `job_sn`                varchar(100)          DEFAULT NULL COMMENT '任务ID',
    `cause`                 varchar(100)          DEFAULT NULL COMMENT '原因',
    `next_id`               varchar(100)          DEFAULT NULL COMMENT '下一个机器人状态变化id',
    `next_time`             datetime(6)              DEFAULT NULL COMMENT '下一个状态开始时间',
    `start_time`            datetime(6)              DEFAULT NULL COMMENT '该状态当天计算开始时间',
    `end_time`              datetime(6)              DEFAULT NULL COMMENT '该状态当天计算结束时间',
    `the_hour_cost_seconds` decimal(30, 6)        DEFAULT NULL COMMENT '状态在该小时内耗时（秒）',
    `created_time`          timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`          timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人状态在小时内耗时明细临时表';	





##step2:删除当天相关数据（临时表）
DELETE
FROM qt_smartreport.qt_robot_state_time_hour_detail_temp
WHERE date_format(hour_start_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  






##step3:插入当天相关数据（临时表）
insert into qt_smartreport.qt_robot_state_time_hour_detail_temp(hour_start_time, next_hour_start_time, state_id,
                                                                robot_code, create_time, network_state, online_state,
                                                                work_state, job_sn, cause, next_id, next_time,
                                                                start_time, end_time, the_hour_cost_seconds)
select t1.hour_start_time,
       t1.next_hour_start_time,
       t2.id as state_id,
       t2.robot_code,
       t2.create_time,
       t2.network_state,
       t2.online_state,
       t2.work_state,
       t2.job_sn,
       t2.cause,
       t2.next_id,
       t2.next_time,
       t2.start_time,
       t2.end_time,
       case
           when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                t2.end_time < t1.next_hour_start_time then UNIX_TIMESTAMP(t2.end_time) - UNIX_TIMESTAMP(t2.start_time)
           when t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                t2.end_time >= t1.next_hour_start_time
               then UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t2.start_time)
           when t2.start_time < t1.hour_start_time and t2.end_time >= t1.hour_start_time and
                t2.end_time < t1.next_hour_start_time
               then UNIX_TIMESTAMP(t2.end_time)-UNIX_TIMESTAMP(t1.hour_start_time)
           when t2.start_time < t1.hour_start_time and t2.end_time >= t1.next_hour_start_time
               then UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t1.hour_start_time)
           end  the_hour_cost_seconds

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
     (select t.*,
             case
                 when t.create_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00')
                     then date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00')
                 else t.create_time end as start_time,
             case
                 when t.next_time >= date_format(sysdate(), '%Y-%m-%d 00:00:00')
                     then date_format(sysdate(), '%Y-%m-%d 00:00:00')
                 else t.next_time end   as end_time
      from (select t.*,
                   coalesce(t.next_create_time,
                            date_format(date_add(t.create_time, interval 1 day), '%Y-%m-%d 00:00:00')) as next_time
            from (select a.*,
                         min(b.id)          as next_id,
                         min(b.create_time) as next_create_time
                  from (select id,
                               robot_code,
                               create_time,
                               network_state,
                               online_state,
                               work_state,
                               job_sn,
                               cause
                        from phoenix_rms.robot_state_history
                        where date_format(create_time, '%Y-%m-%d') >=
                              date_format(date_add(sysdate(), interval -10 day), '%Y-%m-%d')) a
                           left join
                       (select id, robot_code, create_time
                        from phoenix_rms.robot_state_history
                        where date_format(create_time, '%Y-%m-%d') >=
                              date_format(date_add(sysdate(), interval -10 day), '%Y-%m-%d')) b
                       on b.robot_code = a.robot_code and b.create_time > a.create_time
                  group by a.id, a.robot_code, a.create_time, a.network_state, a.online_state, a.work_state, a.job_sn,
                           a.cause) t) t
      where 1 = 1
        and ((create_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
              create_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
              next_time < date_format(sysdate(), '%Y-%m-%d 00:00:00'))
          or
             (create_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
              create_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
              next_time >= date_format(sysdate(), '%Y-%m-%d 00:00:00'))
          or
             (create_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
              next_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
              next_time < date_format(sysdate(), '%Y-%m-%d 00:00:00'))
          or
             (create_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and
              next_time >= date_format(sysdate(), '%Y-%m-%d 00:00:00')))) t2 on 1
;	 



-----------------------------------------------------------------------
##step4:建表(qt_robot_state_time_stat)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_state_time_stat
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                datetime  NOT NULL COMMENT '统计时间',
    `date_value`                date               DEFAULT NULL COMMENT '日期',
    `hour_value`                varchar(100)       DEFAULT NULL COMMENT '小时',
    `first_classification_name` varchar(100)       DEFAULT NULL COMMENT '机器人类型',
    `robot_code`                varchar(100)       DEFAULT NULL COMMENT '机器人编码',
    `loading_busy_time`         decimal(30, 6)            DEFAULT NULL COMMENT '带载作业时长（秒）',
    `empty_busy_time`           decimal(30, 6)            DEFAULT NULL COMMENT '空载作业时长（秒）',
    `idle_time`                 decimal(30, 6)            DEFAULT NULL COMMENT '空闲时长（秒）',
    `charging_time`             decimal(30, 6)            DEFAULT NULL COMMENT '充电时长（秒）',
    `lock_time`                 decimal(30, 6)           DEFAULT NULL COMMENT '锁定时长（秒）',
    `error_time`                decimal(30, 6)            DEFAULT NULL COMMENT '异常时长（秒）',
    `offline_time`              decimal(30, 6)            DEFAULT NULL COMMENT '离线时长（秒）',
    `loading_busy_rate`         decimal(30, 10)     DEFAULT NULL COMMENT '带载作业率',
    `time_type`                 varchar(100)       DEFAULT NULL COMMENT '统计维度',
    `created_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人运行状态';	
	

	
	
	
##step5:删除当天相关数据(qt_robot_state_time_stat)
DELETE
FROM qt_smartreport.qt_robot_state_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  





##step6:插入当天相关数据(qt_robot_state_time_stat)
insert into qt_smartreport.qt_robot_state_time_stat(time_value, date_value, hour_value, first_classification_name,
                                                    robot_code,
                                                    loading_busy_time, empty_busy_time, idle_time, charging_time,
                                                    lock_time, error_time, offline_time, loading_busy_rate, time_type)
select t.hour_start_time                                                                                  as time_value,
       date(t.hour_start_time)                                                                            as date_value,
       HOUR(t.hour_start_time)                                                                            as hour_value,
       t.first_classification_name,
       t.robot_code,
       coalesce(sum(t.loading_busy_time), 0)                                                              as loading_busy_time,
       coalesce(sum(t.empty_busy_time), 0)                                                                as empty_busy_time,
       coalesce(sum(t.idle_time), 0)                                                                      as idle_time,
       coalesce(sum(t.charging_time), 0)                                                                  as charging_time,
       coalesce(sum(t.lock_time), 0)                                                                      as lock_time,
       coalesce(sum(t.error_time), 0)                                                                     as error_time,
       3600 - coalesce(sum(t.loading_busy_time), 0) - coalesce(sum(t.empty_busy_time), 0) -
       coalesce(sum(t.idle_time), 0) -
       coalesce(sum(t.charging_time), 0) - coalesce(sum(t.lock_time), 0) -
       coalesce(sum(t.error_time), 0)                                                                     as offline_time,
       coalesce(sum(t.loading_busy_time), 0) / 3600                                            as loading_busy_rate,
       '小时'                                                                                               as time_type
from (select tt.hour_start_time,
             tt.next_hour_start_time,
             tt.robot_code,
             tt.first_classification_name,
             case
                 when t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and rjsc.job_sn is not null
                     then t.the_hour_cost_seconds end                          as loading_busy_time,
             case
                 when t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and rjsc.job_sn is null
                     then t.the_hour_cost_seconds end                          as empty_busy_time,
             case
                 when t.online_state = 'REGISTERED' and t.work_state = 'IDLE'
                     then t.the_hour_cost_seconds end                          as idle_time,
             case
                 when t.online_state = 'REGISTERED' and t.work_state = 'CHARGING'
                     then t.the_hour_cost_seconds end                          as charging_time,
             case
                 when t.online_state = 'REGISTERED' and t.work_state = 'LOCKED'
                     then t.the_hour_cost_seconds end                          as lock_time,
             case when t.work_state = 'ERROR' then t.the_hour_cost_seconds end as error_time,
             t.state_id,
             t.the_hour_cost_seconds
      from (select t1.hour_start_time,
                   t1.next_hour_start_time,
                   t2.robot_code,
                   t2.first_classification_name
            from (select th.day_hours                               as hour_start_time,
                         DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
                  from (SELECT DATE_FORMAT(
                                       DATE_SUB(DATE_FORMAT(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00'),
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
						   where br.usage_state='using') t2 on 1) tt
               left join qt_smartreport.qt_robot_state_time_hour_detail_temp t
                         on t.hour_start_time = tt.hour_start_time and
                            t.next_hour_start_time = tt.next_hour_start_time and
                            t.robot_code = tt.robot_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                             date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d') and
                            t.the_hour_cost_seconds is not null
               left join (select DISTINCT job_sn
                          from phoenix_rss.transport_order_carrier_job
                          where date_format(create_time, '%Y-%m-%d') >=
                                date_format(date_add(sysdate(), interval -10 day), '%Y-%m-%d')) rjsc
                         on rjsc.job_sn = t.job_sn) t
group by time_value, date_value, hour_value, first_classification_name, robot_code

union all

select date_format(t.hour_start_time, '%Y-%m-%d')                                                         as time_value,
       date(t.hour_start_time)                                                                            as date_value,
       null                                                                                               as hour_value,
       t.first_classification_name,
       t.robot_code,
       coalesce(sum(t.loading_busy_time), 0)                                                              as loading_busy_time,
       coalesce(sum(t.empty_busy_time), 0)                                                                as empty_busy_time,
       coalesce(sum(t.idle_time), 0)                                                                      as idle_time,
       coalesce(sum(t.charging_time), 0)                                                                  as charging_time,
       coalesce(sum(t.lock_time), 0)                                                                      as lock_time,
       coalesce(sum(t.error_time), 0)                                                                     as error_time,
       3600 * 24 - coalesce(sum(t.loading_busy_time), 0) - coalesce(sum(t.empty_busy_time), 0) -
       coalesce(sum(t.idle_time), 0) -
       coalesce(sum(t.charging_time), 0) - coalesce(sum(t.lock_time), 0) -
       coalesce(sum(t.error_time), 0)                                                                     as offline_time,
       coalesce(sum(t.loading_busy_time), 0) / (3600 * 24)                                                as loading_busy_rate,
       '天'                                                                                                as time_type
from (select tt.hour_start_time,
             tt.next_hour_start_time,
             tt.robot_code,
             tt.first_classification_name,
             case
                 when t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and rjsc.job_sn is not null
                     then t.the_hour_cost_seconds end                          as loading_busy_time,
             case
                 when t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and rjsc.job_sn is null
                     then t.the_hour_cost_seconds end                          as empty_busy_time,
             case
                 when t.online_state = 'REGISTERED' and t.work_state = 'IDLE'
                     then t.the_hour_cost_seconds end                          as idle_time,
             case
                 when t.online_state = 'REGISTERED' and t.work_state = 'CHARGING'
                     then t.the_hour_cost_seconds end                          as charging_time,
             case
                 when t.online_state = 'REGISTERED' and t.work_state = 'LOCKED'
                     then t.the_hour_cost_seconds end                          as lock_time,
             case when t.work_state = 'ERROR' then t.the_hour_cost_seconds end as error_time,
             t.state_id,
             t.the_hour_cost_seconds
      from (select t1.hour_start_time,
                   t1.next_hour_start_time,
                   t2.robot_code,
                   t2.first_classification_name
            from (select th.day_hours                               as hour_start_time,
                         DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
                  from (SELECT DATE_FORMAT(
                                       DATE_SUB(DATE_FORMAT(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00'),
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
						   where br.usage_state='using') t2 on 1) tt
               left join qt_smartreport.qt_robot_state_time_hour_detail_temp t
                         on t.hour_start_time = tt.hour_start_time and
                            t.next_hour_start_time = tt.next_hour_start_time and
                            t.robot_code = tt.robot_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                             date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d') and
                            t.the_hour_cost_seconds is not null
               left join (select DISTINCT job_sn
                          from phoenix_rss.transport_order_carrier_job
                          where date_format(create_time, '%Y-%m-%d') >=
                                date_format(date_add(sysdate(), interval -10 day), '%Y-%m-%d')) rjsc
                         on rjsc.job_sn = t.job_sn) t
group by time_value, date_value, hour_value, first_classification_name, robot_code
;



----------------------------------
##step7:建表(qt_robot_state_time_rate_stat)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_state_time_rate_stat
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                datetime  NOT NULL COMMENT '统计时间',
    `date_value`                date               DEFAULT NULL COMMENT '日期',
    `hour_value`                varchar(100)       DEFAULT NULL COMMENT '小时',
    `first_classification_name` varchar(100)       DEFAULT NULL COMMENT '机器人类型',
    `robot_code`                varchar(100)       DEFAULT NULL COMMENT '机器人编码',
    `time_type`                 varchar(100)       DEFAULT NULL COMMENT '统计维度',
    `total_time`                decimal(30, 6)            DEFAULT NULL COMMENT '统计维度总时长（秒）',
    `state_durations`           decimal(30, 6)            DEFAULT NULL COMMENT '状态时长（秒）',
    `state_durations_rate`      decimal(30, 10)     DEFAULT NULL COMMENT '状态时长占比',
    `state_type`                varchar(100)       DEFAULT NULL COMMENT '状态分类',
    `created_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人各类运行状态时长占比';	
	

##step8:删除数据(qt_robot_state_time_rate_stat)
DELETE
FROM qt_smartreport.qt_robot_state_time_rate_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');   



##step9:插入当天相关数据(qt_robot_state_time_rate_stat)
insert into qt_smartreport.qt_robot_state_time_rate_stat(time_value, date_value, hour_value, first_classification_name,
                                                         robot_code, time_type, total_time, state_durations,
                                                         state_durations_rate, state_type)
select time_value,
       DATE(date_value)                                                                                 as date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       case when time_type = '小时' then 3600 when time_type = '天' then 3600 * 24 end                     as total_time,
       loading_busy_time                                                                                as state_durations,
       loading_busy_time / case
                               when time_type = '小时' then 3600
                               when time_type = '天'
                                   then 3600 * 24 end                                                   as state_durations_rate,
       '带载作业'                                                                                           as state_type
from qt_smartreport.qt_robot_state_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                                                                               as date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       case when time_type = '小时' then 3600 when time_type = '天' then 3600 * 24 end                   as total_time,
       empty_busy_time                                                                                as state_durations,
       empty_busy_time / case
                             when time_type = '小时' then 3600
                             when time_type = '天'
                                 then 3600 * 24 end                                                   as state_durations_rate,
       '空载作业'                                                                                         as state_type
from qt_smartreport.qt_robot_state_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                                                                         as date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       case when time_type = '小时' then 3600 when time_type = '天' then 3600 * 24 end             as total_time,
       idle_time                                                                                as state_durations,
       idle_time / case when time_type = '小时' then 3600 when time_type = '天' then 3600 * 24 end as state_durations_rate,
       '空闲'                                                                                     as state_type
from qt_smartreport.qt_robot_state_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                                                                             as date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       case when time_type = '小时' then 3600 when time_type = '天' then 3600 * 24 end                 as total_time,
       charging_time                                                                                as state_durations,
       charging_time / case
                           when time_type = '小时' then 3600
                           when time_type = '天'
                               then 3600 * 24 end                                                   as state_durations_rate,
       '充电'                                                                                         as state_type
from qt_smartreport.qt_robot_state_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                                                                         as date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       case when time_type = '小时' then 3600 when time_type = '天' then 3600 * 24 end             as total_time,
       lock_time                                                                                as state_durations,
       lock_time / case when time_type = '小时' then 3600 when time_type = '天' then 3600 * 24 end as state_durations_rate,
       '锁定'                                                                                     as state_type
from qt_smartreport.qt_robot_state_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                                                                          as date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       case when time_type = '小时' then 3600 when time_type = '天' then 3600 * 24 end              as total_time,
       error_time                                                                                as state_durations,
       error_time / case
                        when time_type = '小时' then 3600
                        when time_type = '天'
                            then 3600 * 24 end                                                   as state_durations_rate,
       '异常'                                                                                      as state_type
from qt_smartreport.qt_robot_state_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                                                                            as date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       case when time_type = '小时' then 3600 when time_type = '天' then 3600 * 24 end                as total_time,
       offline_time                                                                                as state_durations,
       offline_time / case
                          when time_type = '小时' then 3600
                          when time_type = '天'
                              then 3600 * 24 end                                                   as state_durations_rate,
       '离线'                                                                                        as state_type
from qt_smartreport.qt_robot_state_time_stat
WHERE date_format(time_value, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')


