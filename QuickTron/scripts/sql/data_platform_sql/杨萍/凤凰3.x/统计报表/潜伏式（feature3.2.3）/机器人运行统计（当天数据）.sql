##step1:建表（qt_robot_state_history_next_step1_realtime）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_state_history_next_step1_realtime
(
    id         bigint(20),
    robot_code varchar(32),
    current_id varchar(32),
    next_id    varchar(32),
    PRIMARY KEY (`id`),
    key idx_current_id (current_id),
    key idx_next_id (next_id)
);



##step2:删除相关数据（qt_robot_state_history_next_step1_realtime）
DELETE FROM qt_smartreport.qt_robot_state_history_next_step1_realtime;



##step3:插入相关数据（qt_robot_state_history_next_step1_realtime）
insert into qt_smartreport.qt_robot_state_history_next_step1_realtime(id,robot_code,current_id,next_id)
SELECT id,
       robot_code,
       CONCAT(robot_code, '-', @rn := @rn + 1) current_id,
       CONCAT(robot_code, '-', @rn + 1)        next_id
from phoenix_rms.robot_state_history,
     (SELECT @rn := 0) tmp
WHERE id >= (SELECT id
                 from phoenix_rms.robot_state_history
                 WHERE create_time >= date_add(date(SYSDATE()) , interval -11 day)
                 LIMIT 1)
ORDER BY robot_code, id;



##step4:建表（qt_robot_state_history_next_step2_realtime）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_state_history_next_step2_realtime
(
    id         bigint(20),
    robot_code varchar(32),
    next_id    varchar(32),
    PRIMARY KEY (`id`),
    key idx_robot_code (robot_code),
    key idx_next_id (next_id)
);


##step5:删除相关数据（qt_robot_state_history_next_step2_realtime）
DELETE FROM qt_smartreport.qt_robot_state_history_next_step2_realtime;


##step6:插入相关数据（qt_robot_state_history_next_step2_realtime）
insert into qt_smartreport.qt_robot_state_history_next_step2_realtime(id,robot_code,next_id)
SELECT t1.id, t1.robot_code, t2.id as next_id
from qt_smartreport.qt_robot_state_history_next_step1_realtime t1
         LEFT JOIN qt_smartreport.qt_robot_state_history_next_step1_realtime t2
                   ON t1.next_id = t2.current_id;



##step7:建表（qt_robot_state_time_hour_detail_realtime）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_state_time_hour_detail_realtime
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
    PRIMARY KEY (`id`),
	key idx_robot_code (robot_code),
	key idx_hour_start_time (hour_start_time)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人状态在小时内耗时明细（当天数据）';	


##step8:删除当天相关数据（qt_robot_state_time_hour_detail_realtime）
DELETE
FROM qt_smartreport.qt_robot_state_time_hour_detail_realtime
WHERE date(hour_start_time) = date(sysdate());  



##step9:插入当天相关数据（qt_robot_state_time_hour_detail_realtime）
insert into qt_smartreport.qt_robot_state_time_hour_detail_realtime(hour_start_time, next_hour_start_time, state_id,
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
               then UNIX_TIMESTAMP(t2.end_time) - UNIX_TIMESTAMP(t1.hour_start_time)
           when t2.start_time < t1.hour_start_time and t2.end_time >= t1.next_hour_start_time
               then UNIX_TIMESTAMP(t1.next_hour_start_time) - UNIX_TIMESTAMP(t1.hour_start_time)
           end  the_hour_cost_seconds
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
     (select t.*,

             case
                 when t.create_time < date_format(sysdate(), '%Y-%m-%d 00:00:00')
                     then date_format(sysdate(), '%Y-%m-%d 00:00:00')
                 else t.create_time end       as start_time,
             coalesce(t.next_time, sysdate()) as end_time
      from (select t1.id,
                   t1.robot_code,
                   t1.create_time,
                   t1.network_state,
                   t1.online_state,
                   t1.work_state,
                   t1.job_sn,
                   t1.cause,
                   t2.id                               as next_id,
                   coalesce(t2.create_time, sysdate()) as next_time,
                   t2.create_time                      as next_create_time,
                   t1.network_state                    as next_network_state,
                   t1.online_state                     as next_online_state,
                   t1.work_state                       as next_work_state,
                   t1.job_sn                           as next_job_sn,
                   t1.cause                            as next_cause
            from phoenix_rms.robot_state_history t1
                     left join qt_smartreport.qt_robot_state_history_next_step2_realtime tm
                               on tm.robot_code = t1.robot_code and tm.id = t1.id
                     left join phoenix_rms.robot_state_history t2
                               on t2.robot_code = tm.robot_code and t2.id = tm.next_id
            where date(t1.create_time) >= date(date_add(date(sysdate()), interval -10 day))) t
      where 1 = 1
        and ((t.create_time >= date_format(date(sysdate()), '%Y-%m-%d 00:00:00') and
              t.create_time < date_format(date_add(date(sysdate()), interval 1 day), '%Y-%m-%d 00:00:00') and
              t.next_time < date_format(date_add(date(sysdate()), interval 1 day), '%Y-%m-%d 00:00:00'))
          or
             (t.create_time >= date_format(date(sysdate()), '%Y-%m-%d 00:00:00') and
              t.create_time < date_format(date_add(date(sysdate()), interval 1 day), '%Y-%m-%d 00:00:00') and
              t.next_time >= date_format(date_add(date(sysdate()), interval 1 day), '%Y-%m-%d 00:00:00'))
          or
             (t.create_time < date_format(date(sysdate()), '%Y-%m-%d 00:00:00') and
              t.next_time >= date_format(date(sysdate()), '%Y-%m-%d 00:00:00') and
              t.next_time < date_format(date_add(date(sysdate()), interval 1 day), '%Y-%m-%d 00:00:00'))
          or
             (t.create_time < date_format(date(sysdate()), '%Y-%m-%d 00:00:00') and
              t.next_time >= date_format(date_add(date(sysdate()), interval 1 day), '%Y-%m-%d 00:00:00')))) t2 on
         ((t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
           t2.end_time < t1.next_hour_start_time)
             or (t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                 t2.end_time >= t1.next_hour_start_time)
             or (t2.start_time < t1.hour_start_time and t2.end_time >= t1.hour_start_time and
                 t2.end_time < t1.next_hour_start_time)
             or (t2.start_time < t1.hour_start_time and t2.end_time >= t1.next_hour_start_time));
	 


-----------------------------------------------------------------------
##step10:建表(qt_robot_state_time_stat_realtime)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_state_time_stat_realtime
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
    PRIMARY KEY (`id`),
	key idx_robot_code (robot_code),
	key idx_date_value (date_value)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人运行状态（当天数据）';	
	

	
	
	
##step11:删除当天相关数据(qt_robot_state_time_stat_realtime)
DELETE
FROM qt_smartreport.qt_robot_state_time_stat_realtime;  




##step12-1:插入当天相关数据(qt_robot_state_time_stat_realtime)
##time_type='小时'
insert into qt_smartreport.qt_robot_state_time_stat_realtime(time_value, date_value, hour_value, first_classification_name,
                                                    robot_code,
                                                    loading_busy_time, empty_busy_time, idle_time, charging_time,
                                                    lock_time, error_time, offline_time, loading_busy_rate, time_type)
select t.hour_start_time                                           as time_value,
       date(t.hour_start_time)                                     as date_value,
       HOUR(t.hour_start_time)                                     as hour_value,
       t.first_classification_name,
       t.robot_code,
       coalesce(sum(t.loading_busy_time), 0)                       as loading_busy_time,
       coalesce(sum(t.empty_busy_time), 0)                         as empty_busy_time,
       coalesce(sum(t.idle_time), 0)                               as idle_time,
       coalesce(sum(t.charging_time), 0)                           as charging_time,
       coalesce(sum(t.lock_time), 0)                               as lock_time,
       coalesce(sum(t.error_time), 0)                              as error_time,		   
       (case
            when HOUR(t.hour_start_time) = HOUR(sysdate())
                then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t.hour_start_time)
            else 3600 end) - coalesce(sum(t.loading_busy_time), 0) - coalesce(sum(t.empty_busy_time), 0) -
       coalesce(sum(t.idle_time), 0) -
       coalesce(sum(t.charging_time), 0) - coalesce(sum(t.lock_time), 0) -
       coalesce(sum(t.error_time), 0)                              as offline_time,
       coalesce(sum(t.loading_busy_time), 0) / (case
                                                    when HOUR(t.hour_start_time) = HOUR(sysdate())
                                                        then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t.hour_start_time)
                                                    else 3600 end) as loading_busy_rate,
       '小时'                                                        as time_type
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
						   where br.usage_state='using') t2 on 1) tt
               left join qt_smartreport.qt_robot_state_time_hour_detail_realtime t
                         on t.hour_start_time = tt.hour_start_time and
                            t.next_hour_start_time = tt.next_hour_start_time and
                            t.robot_code = tt.robot_code and
                            date_format(t.hour_start_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d') and
                            t.the_hour_cost_seconds is not null
               left join (select DISTINCT job_sn
                          from phoenix_rss.transport_order_carrier_job
                          where date_format(create_time, '%Y-%m-%d') >=
                                date_format(date_add(sysdate(), interval -10 day), '%Y-%m-%d')) rjsc
                         on rjsc.job_sn = t.job_sn) t
group by time_value, date_value, hour_value, first_classification_name, robot_code
;


##step12-2:插入当天相关数据(qt_robot_state_time_stat_realtime)
##time_type='天'
insert into qt_smartreport.qt_robot_state_time_stat_realtime(time_value, date_value, hour_value, first_classification_name,
                                                    robot_code,
                                                    loading_busy_time, empty_busy_time, idle_time, charging_time,
                                                    lock_time, error_time, offline_time, loading_busy_rate, time_type)

select date_format(t.hour_start_time, '%Y-%m-%d')                                                as time_value,
       date(t.hour_start_time)                                                                   as date_value,
       null                                                                                      as hour_value,
       t.first_classification_name,
       t.robot_code,
       coalesce(sum(t.loading_busy_time), 0)                                                     as loading_busy_time,
       coalesce(sum(t.empty_busy_time), 0)                                                       as empty_busy_time,
       coalesce(sum(t.idle_time), 0)                                                             as idle_time,
       coalesce(sum(t.charging_time), 0)                                                         as charging_time,
       coalesce(sum(t.lock_time), 0)                                                             as lock_time,
       coalesce(sum(t.error_time), 0)                                                            as error_time,
       (UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) -
       coalesce(sum(t.loading_busy_time), 0) - coalesce(sum(t.empty_busy_time), 0) -
       coalesce(sum(t.idle_time), 0) -
       coalesce(sum(t.charging_time), 0) - coalesce(sum(t.lock_time), 0) -
       coalesce(sum(t.error_time), 0)                                                            as offline_time,
       coalesce(sum(t.loading_busy_time), 0) /
       (UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) as loading_busy_rate,
       '天'                                                                                       as time_type
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
						   where br.usage_state='using') t2 on 1) tt
               left join qt_smartreport.qt_robot_state_time_hour_detail_realtime t
                         on t.hour_start_time = tt.hour_start_time and
                            t.next_hour_start_time = tt.next_hour_start_time and
                            t.robot_code = tt.robot_code and
                            date_format(t.hour_start_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d') and
                            t.the_hour_cost_seconds is not null
               left join (select DISTINCT job_sn
                          from phoenix_rss.transport_order_carrier_job
                          where date_format(create_time, '%Y-%m-%d') >=
                                date_format(date_add(sysdate(), interval -10 day), '%Y-%m-%d')) rjsc
                         on rjsc.job_sn = t.job_sn) t
group by time_value, date_value, hour_value, first_classification_name, robot_code
;




----------------------------------
##step13:建表(qt_robot_state_time_rate_stat_realtime)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_robot_state_time_rate_stat_realtime
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
    PRIMARY KEY (`id`),
	key idx_date_value (date_value)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人各类运行状态时长占比（当天数据）';	
	

##step14:删除数据(qt_robot_state_time_rate_stat_realtime)
DELETE
FROM qt_smartreport.qt_robot_state_time_rate_stat_realtime;   




##step15:插入当天相关数据(qt_robot_state_time_rate_stat_realtime)
insert into qt_smartreport.qt_robot_state_time_rate_stat_realtime(time_value, date_value, hour_value,
                                                                  first_classification_name,
                                                                  robot_code, time_type, total_time, state_durations,
                                                                  state_durations_rate, state_type)
select time_value,
       DATE(date_value)                                                                                   as date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       case
           when time_type = '小时' then (case
                                           when hour(sysdate()) = hour(time_value)
                                               then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(time_value)
                                           else 3600 end)
           when time_type = '天' then (UNIX_TIMESTAMP(sysdate()) -
                                      UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end    as total_time,
       loading_busy_time                                                                                  as state_durations,
       loading_busy_time / case
                               when time_type = '小时' then (case
                                                               when hour(sysdate()) = hour(time_value)
                                                                   then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(time_value)
                                                               else 3600 end)
                               when time_type = '天'
                                   then (UNIX_TIMESTAMP(sysdate()) -
                                         UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end as state_durations_rate,
       '带载作业'                                                                                             as state_type
from qt_smartreport.qt_robot_state_time_stat_realtime
WHERE date(time_value) = date(sysdate())

union all
select time_value,
       DATE(date_value)                                                                                 as date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       case
           when time_type = '小时' then (case
                                           when hour(sysdate()) = hour(time_value)
                                               then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(time_value)
                                           else 3600 end)
           when time_type = '天' then (UNIX_TIMESTAMP(sysdate()) -
                                      UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end  as total_time,
       empty_busy_time                                                                                  as state_durations,
       empty_busy_time / case
                             when time_type = '小时' then (case
                                                             when hour(sysdate()) = hour(time_value)
                                                                 then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(time_value)
                                                             else 3600 end)
                             when time_type = '天'
                                 then (UNIX_TIMESTAMP(sysdate()) -
                                       UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end as state_durations_rate,
       '空载作业'                                                                                           as state_type
from qt_smartreport.qt_robot_state_time_stat_realtime
WHERE date(time_value) = date(sysdate())
union all
select time_value,
       DATE(date_value)                                                                                            as date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       case
           when time_type = '小时' then (case
                                           when hour(sysdate()) = hour(time_value)
                                               then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(time_value)
                                           else 3600 end)
           when time_type = '天' then (UNIX_TIMESTAMP(sysdate()) -
                                      UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end             as total_time,
       idle_time                                                                                                   as state_durations,
       idle_time / case
                       when time_type = '小时' then (case
                                                       when hour(sysdate()) = hour(time_value)
                                                           then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(time_value)
                                                       else 3600 end)
                       when time_type = '天' then (UNIX_TIMESTAMP(sysdate()) -
                                                  UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end as state_durations_rate,
       '空闲'                                                                                                        as state_type
from qt_smartreport.qt_robot_state_time_stat_realtime
WHERE date(time_value) = date(sysdate())
union all
select time_value,
       DATE(date_value)                                                                                as date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       case
           when time_type = '小时' then (case
                                           when hour(sysdate()) = hour(time_value)
                                               then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(time_value)
                                           else 3600 end)
           when time_type = '天' then (UNIX_TIMESTAMP(sysdate()) -
                                      UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end as total_time,
       charging_time                                                                                   as state_durations,
       charging_time / case
                           when time_type = '小时' then (case
                                                           when hour(sysdate()) = hour(time_value)
                                                               then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(time_value)
                                                           else 3600 end)
                           when time_type = '天'
                               then (UNIX_TIMESTAMP(sysdate()) -
                                     UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end  as state_durations_rate,
       '充电'                                                                                            as state_type
from qt_smartreport.qt_robot_state_time_stat_realtime
WHERE date(time_value) = date(sysdate())
union all
select time_value,
       DATE(date_value)                                                                                            as date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       case
           when time_type = '小时' then (case
                                           when hour(sysdate()) = hour(time_value)
                                               then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(time_value)
                                           else 3600 end)
           when time_type = '天' then (UNIX_TIMESTAMP(sysdate()) -
                                      UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end             as total_time,
       lock_time                                                                                                   as state_durations,
       lock_time / case
                       when time_type = '小时' then (case
                                                       when hour(sysdate()) = hour(time_value)
                                                           then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(time_value)
                                                       else 3600 end)
                       when time_type = '天' then (UNIX_TIMESTAMP(sysdate()) -
                                                  UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end as state_durations_rate,
       '锁定'                                                                                                        as state_type
from qt_smartreport.qt_robot_state_time_stat_realtime
WHERE date(time_value) = date(sysdate())
union all
select time_value,
       DATE(date_value)                                                                                as date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       case
           when time_type = '小时' then (case
                                           when hour(sysdate()) = hour(time_value)
                                               then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(time_value)
                                           else 3600 end)
           when time_type = '天' then (UNIX_TIMESTAMP(sysdate()) -
                                      UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end as total_time,
       error_time                                                                                      as state_durations,
       error_time / case
                        when time_type = '小时' then (case
                                                        when hour(sysdate()) = hour(time_value)
                                                            then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(time_value)
                                                        else 3600 end)
                        when time_type = '天'
                            then (UNIX_TIMESTAMP(sysdate()) -
                                  UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end     as state_durations_rate,
       '异常'                                                                                            as state_type
from qt_smartreport.qt_robot_state_time_stat_realtime
WHERE date(time_value) = date(sysdate())
union all
select time_value,
       DATE(date_value)                                                                                as date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       case
           when time_type = '小时' then (case
                                           when hour(sysdate()) = hour(time_value)
                                               then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(time_value)
                                           else 3600 end)
           when time_type = '天' then (UNIX_TIMESTAMP(sysdate()) -
                                      UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end as total_time,
       offline_time                                                                                    as state_durations,
       offline_time / case
                          when time_type = '小时' then (case
                                                          when hour(sysdate()) = hour(time_value)
                                                              then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(time_value)
                                                          else 3600 end)
                          when time_type = '天'
                              then (UNIX_TIMESTAMP(sysdate()) -
                                    UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end   as state_durations_rate,
       '离线'                                                                                            as state_type
from qt_smartreport.qt_robot_state_time_stat_realtime
WHERE date(time_value) = date(sysdate())
;