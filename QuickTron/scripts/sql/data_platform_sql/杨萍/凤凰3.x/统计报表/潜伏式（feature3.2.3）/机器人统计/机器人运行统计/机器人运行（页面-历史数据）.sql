#####################################天维度进入计算的数据#########################################
set @now_start_time = '2022-08-31 00:00:00.000000000';
set @now_end_time = '2022-08-31 23:59:59.999999999';
set @next_start_time = '2022-09-01 00:00:00.000000000';


select null               as                        date_value,
       t2.robot_code,
       t2.id              as                        state_id,
       t2.create_time     as                        state_create_time,

       t2.network_state,
       t2.online_state,
       t2.work_state,
       t2.job_sn,
       t2.cause,
       t2.duration / 1000 as                        duration,
       case
           when sysdate() < @next_start_time then UNIX_TIMESTAMP(coalesce(t3.the_day_first_create_time, sysdate())) -
                                                  UNIX_TIMESTAMP(@now_start_time)
           else UNIX_TIMESTAMP(coalesce(t3.the_day_first_create_time, @next_start_time)) -
                UNIX_TIMESTAMP(@now_start_time) end stat_duration
from (select robot_code, max(id) as before_day_last_id
      from phoenix_rms.robot_state_history
      where create_time < @now_start_time
      group by robot_code) t1
         left join phoenix_rms.robot_state_history t2 on t2.robot_code = t1.robot_code and t2.id = t1.before_day_last_id
         left join (select robot_code, min(create_time) as the_day_first_create_time
                    from phoenix_rms.robot_state_history
                    where create_time >= @now_start_time
                      and create_time < @next_start_time
                    group by robot_code) t3 on t3.robot_code = t1.robot_code
union all
select null               as           date_value,
       t4.robot_code,
       t4.id              as           state_id,
       t4.create_time     as           state_create_time,
       t4.network_state,
       t4.online_state,
       t4.work_state,
       t4.job_sn,
       t4.cause,
       t4.duration / 1000 as           duration,
       case
           when t5.the_day_last_id is not null and sysdate() >= @next_start_time
               then UNIX_TIMESTAMP(@next_start_time) - UNIX_TIMESTAMP(t4.create_time)
           when t5.the_day_last_id is not null and sysdate() < @next_start_time
               then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t4.create_time)
           else t4.duration / 1000 end stat_duration
from phoenix_rms.robot_state_history t4
         left join (select robot_code,
                           max(id)          as the_day_last_id,
                           max(create_time) as the_day_last_create_time
                    from phoenix_rms.robot_state_history
                    where create_time >= @now_start_time
                      and create_time < @next_start_time
                    group by robot_code) t5 on t5.robot_code = t4.robot_code and t5.the_day_last_id = t4.id
where t4.create_time >= @now_start_time
  and t4.create_time < @next_start_time			
	
	
	
	
	

#####################################小时维度进入计算的数据######################################### 

set @now_start_time = '2022-09-01 00:00:00.000000000';
set @now_end_time = '2022-09-01 23:59:59.999999999';
set @next_start_time = '2022-09-02 00:00:00.000000000';


select null               as                           date_value,
       t1.robot_code,
       t1.hour_start_time,
       t1.next_hour_start_time,
       t2.id              as                           state_id,
       t2.create_time     as                           state_create_time,
       t2.network_state,
       t2.online_state,
       t2.work_state,
       t2.job_sn,
       t2.cause,
       t2.duration / 1000 as                           duration,
       case
           when sysdate() < t1.next_hour_start_time then
                   UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, sysdate())) -
                   UNIX_TIMESTAMP(t1.hour_start_time)
           else UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, t1.next_hour_start_time)) -
                UNIX_TIMESTAMP(t1.hour_start_time) end stat_duration
from (select t.robot_code, t1.hour_start_time, t1.next_hour_start_time, max(t.id) as before_day_last_id
      from phoenix_rms.robot_state_history t
               inner join (select br.robot_code,
                                  t.hour_start_time,
                                  t.next_hour_start_time
                           from (select th.day_hours                               as hour_start_time,
                                        DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
                                 from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(@now_start_time, '%Y-%m-%d 00:00:00'),
                                                                   INTERVAL
                                                                   (-(@a := @a + 1)) HOUR),
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
                                            (SELECT @a := -1) AS i) th
                                 where th.day_hours <= sysdate()) t
                                    left join phoenix_basic.basic_robot br on 1) t1
                          on t1.robot_code = t.robot_code and t.create_time < t1.hour_start_time
      group by t.robot_code, t1.hour_start_time, t1.next_hour_start_time) t1
         left join phoenix_rms.robot_state_history t2 on t2.robot_code = t1.robot_code and t2.id = t1.before_day_last_id
         left join (select t.robot_code,
                           t1.hour_start_time,
                           t1.next_hour_start_time,
                           min(create_time) as the_hour_first_create_time
                    from phoenix_rms.robot_state_history t
                             inner join (select br.robot_code,
                                                t.hour_start_time,
                                                t.next_hour_start_time
                                         from (select th.day_hours                               as hour_start_time,
                                                      DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
                                               from (SELECT DATE_FORMAT(DATE_SUB(
                                                                                DATE_FORMAT(@now_start_time, '%Y-%m-%d 00:00:00'),
                                                                                INTERVAL
                                                                                (-(@b := @b + 1)) HOUR),
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
                                                          (SELECT @b := -1) AS i) th
                                               where th.day_hours <= sysdate()) t
                                                  left join phoenix_basic.basic_robot br on 1) t1
                                        on t1.robot_code = t.robot_code and t.create_time >= t1.hour_start_time and
                                           t.create_time < t1.next_hour_start_time
                    group by t.robot_code, t1.hour_start_time, t1.next_hour_start_time) t3
                   on t3.robot_code = t1.robot_code and t3.hour_start_time = t1.hour_start_time and
                      t3.next_hour_start_time = t1.next_hour_start_time
union all
select null               as           date_value,
       t.robot_code,
       t.hour_start_time,
       t.next_hour_start_time,
       t4.id              as           state_id,
       t4.create_time     as           state_create_time,
       t4.network_state,
       t4.online_state,
       t4.work_state,
       t4.job_sn,
       t4.cause,
       t4.duration / 1000 as           duration,
       case
           when t5.the_hour_last_id is not null and sysdate() >= t.hour_start_time
               then UNIX_TIMESTAMP(t.next_hour_start_time) - UNIX_TIMESTAMP(t4.create_time)
           when t5.the_hour_last_id is not null and sysdate() < t.next_hour_start_time
               then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t4.create_time)
           else t4.duration / 1000 end stat_duration
from phoenix_rms.robot_state_history t4
         inner join
     (select br.robot_code,
             t.hour_start_time,
             t.next_hour_start_time
      from (select th.day_hours                               as hour_start_time,
                   DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
            from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(@now_start_time, '%Y-%m-%d 00:00:00'),
                                              INTERVAL
                                              (-(@c := @c + 1)) HOUR),
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
                       (SELECT @c := -1) AS i) th
            where th.day_hours <= sysdate()) t
               left join phoenix_basic.basic_robot br on 1) t
     on t4.robot_code = t.robot_code and t4.create_time >= t.hour_start_time and
        t4.create_time < t.next_hour_start_time
         left join (select t.robot_code,
                           t.hour_start_time,
                           t.next_hour_start_time,
                           max(t1.id)          as the_hour_last_id,
                           max(t1.create_time) as the_hour_last_create_time
                    from (select br.robot_code,
                                 t.hour_start_time,
                                 t.next_hour_start_time
                          from (select th.day_hours                               as hour_start_time,
                                       DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
                                from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(@now_start_time, '%Y-%m-%d 00:00:00'),
                                                                  INTERVAL
                                                                  (-(@d := @d + 1)) HOUR),
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
                                           (SELECT @d := -1) AS i) th
                                where th.day_hours <= sysdate()) t
                                   left join phoenix_basic.basic_robot br on 1) t
                             inner join phoenix_rms.robot_state_history t1
                                        on t1.robot_code = t.robot_code and t1.create_time >= t.hour_start_time and
                                           t1.create_time < t.next_hour_start_time
                    group by t.robot_code, t.hour_start_time, t.next_hour_start_time) t5
                   on t5.robot_code = t4.robot_code and t5.the_hour_last_id = t4.id
				   
				   
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################



#step1:建表（qt_day_robot_state_detail_duration_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_day_robot_state_detail_duration_his
(
    `id`                bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`        date       NOT NULL COMMENT '日期',
    `robot_code`        varchar(255)        DEFAULT NULL COMMENT '机器人编号',
    `state_id`          bigint(20) NOT NULL COMMENT '状态ID',
    `state_create_time` datetime(6)         DEFAULT NULL COMMENT '状态时间',
    `network_state`     varchar(255)        DEFAULT NULL COMMENT '网络连接状态',
    `online_state`      varchar(255)        DEFAULT NULL COMMENT '在线状态',
    `work_state`        varchar(255)        DEFAULT NULL COMMENT '作业状态',
    `job_sn`            varchar(255)        DEFAULT NULL COMMENT '任务编码',
    `cause`             varchar(255)        DEFAULT NULL COMMENT '变更原因',
    `duration`          decimal(65, 10)     DEFAULT NULL COMMENT '到下一条状态生成的间隔时长（秒）',
    `stat_duration`     decimal(65, 10)     DEFAULT NULL COMMENT '该状态在统计时间段内持续时长（秒）',
    `created_time`      timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`      timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_robot_code (`robot_code`),
    key idx_state_id (state_id),
    key idx_state_create_time (`state_create_time`),
    key idx_network_state (`network_state`),
    key idx_online_state (`online_state`),
    key idx_work_state (`work_state`),
    key idx_job_sn (`job_sn`),
    key idx_cause (`cause`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人天维度状态时长明细（T+1）';
	




#step2:删除相关数据（qt_day_robot_state_detail_duration_his）
DELETE
FROM qt_smartreport.qt_day_robot_state_detail_duration_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);
	
	

	
#step3:插入相关数据（qt_day_robot_state_detail_duration_his）
insert into qt_smartreport.qt_day_robot_state_detail_duration_his(date_value,robot_code,state_id,state_create_time,network_state,online_state,work_state,job_sn,cause,duration,stat_duration)	
select date_add(current_date(), interval -1 day) as                            date_value,
       t2.robot_code,
       t2.id                                     as                            state_id,
       t2.create_time                            as                            state_create_time,
       t2.network_state,
       t2.online_state,
       t2.work_state,
       t2.job_sn,
       t2.cause,
       t2.duration / 1000                        as                            duration,
       case
           when sysdate() < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000') then
                   UNIX_TIMESTAMP(coalesce(t3.the_day_first_create_time, sysdate())) -
                   UNIX_TIMESTAMP(date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000'))
           else UNIX_TIMESTAMP(coalesce(t3.the_day_first_create_time,
                                        date_format(current_date(), '%Y-%m-%d 00:00:00.000000000'))) -
                UNIX_TIMESTAMP(date_format(date_add(current_date(), interval -1 day),
                                           '%Y-%m-%d 00:00:00.000000000')) end stat_duration
from (select robot_code, max(id) as before_day_last_id
      from phoenix_rms.robot_state_history
      where create_time < date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')
      group by robot_code) t1
         left join phoenix_rms.robot_state_history t2 on t2.robot_code = t1.robot_code and t2.id = t1.before_day_last_id
         left join (select robot_code, min(create_time) as the_day_first_create_time
                    from phoenix_rms.robot_state_history
                    where create_time >=
                          date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')
                      and create_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')
                    group by robot_code) t3 on t3.robot_code = t1.robot_code
union all
select date_add(current_date(), interval -1 day) as date_value,
       t4.robot_code,
       t4.id                                     as state_id,
       t4.create_time                            as state_create_time,
       t4.network_state,
       t4.online_state,
       t4.work_state,
       t4.job_sn,
       t4.cause,
       t4.duration / 1000                        as duration,
       case
           when t5.the_day_last_id is not null and
                sysdate() >= date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')
               then UNIX_TIMESTAMP(date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')) -
                    UNIX_TIMESTAMP(t4.create_time)
           when t5.the_day_last_id is not null and
                sysdate() < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')
               then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t4.create_time)
           else t4.duration / 1000 end              stat_duration
from phoenix_rms.robot_state_history t4
         left join (select robot_code,
                           max(id)          as the_day_last_id,
                           max(create_time) as the_day_last_create_time
                    from phoenix_rms.robot_state_history
                    where create_time >=
                          date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')
                      and create_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')
                    group by robot_code) t5 on t5.robot_code = t4.robot_code and t5.the_day_last_id = t4.id
where t4.create_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')
  and t4.create_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')			





	
#step4:建表（qt_hour_robot_state_detail_duration_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_hour_robot_state_detail_duration_his
(
    `id`                bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`        date       NOT NULL COMMENT '日期',
    `robot_code`        varchar(255)        DEFAULT NULL COMMENT '机器人编号',
    `hour_start_time`       datetime     NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time`  datetime     NOT NULL COMMENT '下一个小时开始时间',	
    `state_id`          bigint(20) NOT NULL COMMENT '状态ID',
    `state_create_time` datetime(6)         DEFAULT NULL COMMENT '状态时间',
    `network_state`     varchar(255)        DEFAULT NULL COMMENT '网络连接状态',
    `online_state`      varchar(255)        DEFAULT NULL COMMENT '在线状态',
    `work_state`        varchar(255)        DEFAULT NULL COMMENT '作业状态',
    `job_sn`            varchar(255)        DEFAULT NULL COMMENT '任务编码',
    `cause`             varchar(255)        DEFAULT NULL COMMENT '变更原因',
    `duration`          decimal(65, 10)     DEFAULT NULL COMMENT '到下一条状态生成的间隔时长（秒）',
    `stat_duration`     decimal(65, 10)     DEFAULT NULL COMMENT '该状态在统计时间段内持续时长（秒）',
    `created_time`      timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`      timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_robot_code (`robot_code`),
    key idx_hour_start_time (`hour_start_time`),	
    key idx_next_hour_start_time (`next_hour_start_time`),	
    key idx_state_id (state_id),
    key idx_state_create_time (`state_create_time`),
    key idx_network_state (`network_state`),
    key idx_online_state (`online_state`),
    key idx_work_state (`work_state`),
    key idx_job_sn (`job_sn`),
    key idx_cause (`cause`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人小时维度状态时长明细（T+1）';	
	
	
	

#step5:删除相关数据（qt_hour_robot_state_detail_duration_his）
DELETE
FROM qt_smartreport.qt_hour_robot_state_detail_duration_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);



#step6:插入相关数据（qt_hour_robot_state_detail_duration_his）
insert into qt_smartreport.qt_hour_robot_state_detail_duration_his(date_value, robot_code, hour_start_time,
                                                                   next_hour_start_time, state_id, state_create_time,
                                                                   network_state, online_state, work_state, job_sn,
                                                                   cause, duration, stat_duration)
select date_add(current_date(), interval -1 day) as    date_value,
       t1.robot_code,
       t1.hour_start_time,
       t1.next_hour_start_time,
       t2.id                                     as    state_id,
       t2.create_time                            as    state_create_time,
       t2.network_state,
       t2.online_state,
       t2.work_state,
       t2.job_sn,
       t2.cause,
       t2.duration / 1000                        as    duration,
       case
           when sysdate() < t1.next_hour_start_time then
                   UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, sysdate())) -
                   UNIX_TIMESTAMP(t1.hour_start_time)
           else UNIX_TIMESTAMP(coalesce(t3.the_hour_first_create_time, t1.next_hour_start_time)) -
                UNIX_TIMESTAMP(t1.hour_start_time) end stat_duration
from (select t.robot_code, t1.hour_start_time, t1.next_hour_start_time, max(t.id) as before_day_last_id
      from (phoenix_rms.robot_state_history t
               inner join (select br.robot_code,
                                  t.hour_start_time,
                                  t.next_hour_start_time
                           from (select th.day_hours                               as hour_start_time,
                                        DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
                                 from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(date_format(
                                                                                       date_add(current_date(), interval -1 day),
                                                                                       '%Y-%m-%d 00:00:00.000000000'),
                                                                               '%Y-%m-%d 00:00:00'),
                                                                   INTERVAL
                                                                   (-(@a := @a + 1)) HOUR),
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
                                            (SELECT @a := -1) AS i) th
                                 where th.day_hours <= sysdate()) t
                                    left join phoenix_basic.basic_robot br on 1) t1
                          on t1.robot_code = t.robot_code and t.create_time < t1.hour_start_time
      group by t.robot_code, t1.hour_start_time, t1.next_hour_start_time) t1
         left join phoenix_rms.robot_state_history t2 on t2.robot_code = t1.robot_code and t2.id = t1.before_day_last_id
         left join (select t.robot_code,
                           t1.hour_start_time,
                           t1.next_hour_start_time,
                           min(create_time) as the_hour_first_create_time
                    from phoenix_rms.robot_state_history t
                             inner join (select br.robot_code,
                                                t.hour_start_time,
                                                t.next_hour_start_time
                                         from (select th.day_hours                               as hour_start_time,
                                                      DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
                                               from (SELECT DATE_FORMAT(DATE_SUB(
                                                                                DATE_FORMAT(date_format(
                                                                                                    date_add(current_date(), interval -1 day),
                                                                                                    '%Y-%m-%d 00:00:00.000000000'),
                                                                                            '%Y-%m-%d 00:00:00'),
                                                                                INTERVAL
                                                                                (-(@b := @b + 1)) HOUR),
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
                                                          (SELECT @b := -1) AS i) th
                                               where th.day_hours <= sysdate()) t
                                                  left join phoenix_basic.basic_robot br on 1) t1
                                        on t1.robot_code = t.robot_code and t.create_time >= t1.hour_start_time and
                                           t.create_time < t1.next_hour_start_time
                    group by t.robot_code, t1.hour_start_time, t1.next_hour_start_time) t3
                   on t3.robot_code = t1.robot_code and t3.hour_start_time = t1.hour_start_time and
                      t3.next_hour_start_time = t1.next_hour_start_time
union all
select date_add(current_date(), interval -1 day) as date_value,
       t.robot_code,
       t.hour_start_time,
       t.next_hour_start_time,
       t4.id                                     as state_id,
       t4.create_time                            as state_create_time,
       t4.network_state,
       t4.online_state,
       t4.work_state,
       t4.job_sn,
       t4.cause,
       t4.duration / 1000                        as duration,
       case
           when t5.the_hour_last_id is not null and sysdate() >= t.hour_start_time
               then UNIX_TIMESTAMP(t.next_hour_start_time) - UNIX_TIMESTAMP(t4.create_time)
           when t5.the_hour_last_id is not null and sysdate() < t.next_hour_start_time
               then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(t4.create_time)
           else t4.duration / 1000 end              stat_duration
from phoenix_rms.robot_state_history t4
         inner join
     (select br.robot_code,
             t.hour_start_time,
             t.next_hour_start_time
      from (select th.day_hours                               as hour_start_time,
                   DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
            from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(date_format(date_add(current_date(), interval -1 day),
                                                                      '%Y-%m-%d 00:00:00.000000000'),
                                                          '%Y-%m-%d 00:00:00'),
                                              INTERVAL
                                              (-(@c := @c + 1)) HOUR),
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
                       (SELECT @c := -1) AS i) th
            where th.day_hours <= sysdate()) t
               left join phoenix_basic.basic_robot br on 1) t
     on t4.robot_code = t.robot_code and t4.create_time >= t.hour_start_time and
        t4.create_time < t.next_hour_start_time
         left join (select t.robot_code,
                           t.hour_start_time,
                           t.next_hour_start_time,
                           max(t1.id)          as the_hour_last_id,
                           max(t1.create_time) as the_hour_last_create_time
                    from (select br.robot_code,
                                 t.hour_start_time,
                                 t.next_hour_start_time
                          from (select th.day_hours                               as hour_start_time,
                                       DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
                                from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(date_format(
                                                                                      date_add(current_date(), interval -1 day),
                                                                                      '%Y-%m-%d 00:00:00.000000000'),
                                                                              '%Y-%m-%d 00:00:00'),
                                                                  INTERVAL
                                                                  (-(@d := @d + 1)) HOUR),
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
                                           (SELECT @d := -1) AS i) th
                                where th.day_hours <= sysdate()) t
                                   left join phoenix_basic.basic_robot br on 1) t
                             inner join phoenix_rms.robot_state_history t1
                                        on t1.robot_code = t.robot_code and t1.create_time >= t.hour_start_time and
                                           t1.create_time < t.next_hour_start_time
                    group by t.robot_code, t.hour_start_time, t.next_hour_start_time) t5
                   on t5.robot_code = t4.robot_code and t5.the_hour_last_id = t4.id
				   



#step7:建表（qt_hour_robot_state_duration_stat_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_hour_robot_state_duration_stat_his
(
    `id`                          bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`                  date       NOT NULL COMMENT '日期',
    `hour_start_time`             datetime   NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time`        datetime   NOT NULL COMMENT '下一个小时开始时间',
    `robot_code`                  varchar(255)        DEFAULT NULL COMMENT '机器人编号',
    `robot_type_code`             varchar(255)        DEFAULT NULL COMMENT '机器人类型编码',
    `robot_type_name`             varchar(255)        DEFAULT NULL COMMENT '机器人类型',
    `uptime_state_rate`           decimal(65, 20)     DEFAULT NULL COMMENT '开动率',
    `uptime_state_duration`       decimal(65, 20)     DEFAULT NULL COMMENT '开动时长（秒）',
    `uptime_state_rate_fenmu`     decimal(65, 20)     DEFAULT NULL COMMENT '开动率分母（秒）',
    `utilization_rate`            decimal(65, 20)     DEFAULT NULL COMMENT '利用率',
    `utilization_duration`        decimal(65, 20)     DEFAULT NULL COMMENT '利用时长（秒）',
    `utilization_rate_fenmu`      decimal(65, 20)     DEFAULT NULL COMMENT '利用率分母（秒）',
    `loading_busy_state_duration` decimal(65, 20)     DEFAULT NULL COMMENT '搬运作业时长（秒）',
    `empty_busy_state_duration`   decimal(65, 20)     DEFAULT NULL COMMENT '空闲作业时长（秒）',
    `charging_state_duration`     decimal(65, 20)     DEFAULT NULL COMMENT '充电时长（秒）',
    `busy_state_duration`         decimal(65, 20)     DEFAULT NULL COMMENT '作业时长（秒）',
    `idle_state_duration`         decimal(65, 20)     DEFAULT NULL COMMENT '空闲时长（秒）',
    `locked_state_duration`       decimal(65, 20)     DEFAULT NULL COMMENT '锁定时长（秒）',
    `error_state_duration`        decimal(65, 20)     DEFAULT NULL COMMENT '异常时长（秒）',
    `offline_duration`            decimal(65, 20)     DEFAULT NULL COMMENT '离线时长（秒）',
    `created_time`                timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`                timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_hour_start_time (`hour_start_time`),
    key idx_next_hour_start_time (`next_hour_start_time`),
    key idx_robot_code (`robot_code`),
    key idx_robot_type_code (`robot_type_code`),
    key idx_robot_type_name (`robot_type_name`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人小时维度状态持续时长（T+1）';		
	
	
	
#step8:删除相关数据（qt_hour_robot_state_duration_stat_his）
DELETE
FROM qt_smartreport.qt_hour_robot_state_duration_stat_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);



#step9:插入相关数据（qt_hour_robot_state_duration_stat_his）
insert into qt_smartreport.qt_hour_robot_state_duration_stat_his(date_value, hour_start_time, next_hour_start_time,
                                                                 robot_code, robot_type_code, robot_type_name,
                                                                 uptime_state_rate, uptime_state_duration,
                                                                 uptime_state_rate_fenmu, utilization_rate,
                                                                 utilization_duration, utilization_rate_fenmu,
                                                                 loading_busy_state_duration, empty_busy_state_duration,
                                                                 charging_state_duration, busy_state_duration,
                                                                 idle_state_duration, locked_state_duration,
                                                                 error_state_duration, offline_duration)
select date_add(CURRENT_DATE(), interval -1 day)                                    as date_value,
       tbr.hour_start_time,
       tbr.next_hour_start_time,
       tbr.robot_code,
       tbr.robot_type_code,
       tbr.robot_type_name,
       COALESCE(t1.uptime_state_duration, 0) / 3600                                 as uptime_state_rate,
       COALESCE(t1.uptime_state_duration, 0)                                        as uptime_state_duration,
       3600                                                                         as uptime_state_rate_fenmu,
       COALESCE(t1.loading_busy_state_duration, 0) / 3600                           as utilization_rate,
       COALESCE(t1.loading_busy_state_duration, 0)                                  as utilization_duration,
       3600                                                                         as utilization_rate_fenmu,
       COALESCE(t1.loading_busy_state_duration, 0)                                  as loading_busy_state_duration,
       COALESCE(t1.empty_busy_state_duration, 0)                                    as empty_busy_state_duration,
       COALESCE(t1.charging_state_duration, 0)                                      as charging_state_duration,
       COALESCE(t1.busy_state_duration, 0)                                          as busy_state_duration,
       COALESCE(t1.idle_state_duration, 0)                                          as idle_state_duration,
       COALESCE(t1.locked_state_duration, 0)                                        as locked_state_duration,
       COALESCE(t1.error_state_duration, 0)                                         as error_state_duration,
       3600 - COALESCE(t1.loading_busy_state_duration, 0) - COALESCE(t1.empty_busy_state_duration, 0) -
       COALESCE(t1.charging_state_duration, 0) - COALESCE(t1.idle_state_duration, 0) -
       COALESCE(t1.locked_state_duration, 0) - COALESCE(t1.error_state_duration, 0) as offline_duration
from (select br.robot_code,
             br.robot_type_code,
             br.robot_type_name,
             t.hour_start_time,
             t.next_hour_start_time
      from (select th.day_hours                               as hour_start_time,
                   DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
            from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(date_format(
                                                                  date_add(current_date(), interval -1 day),
                                                                  '%Y-%m-%d 00:00:00.000000000'),
                                                          '%Y-%m-%d 00:00:00'),
                                              INTERVAL
                                              (-(@d := @d + 1)) HOUR),
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
                       (SELECT @d := -1) AS i) th
            where th.day_hours <= sysdate()) t
               left join (select br.robot_code, brt.robot_type_code, brt.robot_type_name
                          from phoenix_basic.basic_robot br
                                   left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id) br
                         on 1) tbr
         left join
     (select ts.hour_start_time,
             ts.next_hour_start_time,
             ts.robot_code,
             sum(case when ts.is_uptime_state = 1 then ts.stat_duration end)       as uptime_state_duration,
             sum(case when ts.is_loading_busy_state = 1 then ts.stat_duration end) as loading_busy_state_duration,
             sum(case when ts.is_empty_busy_state = 1 then ts.stat_duration end)   as empty_busy_state_duration,
             sum(case when ts.is_busy_state = 1 then ts.stat_duration end)         as busy_state_duration,
             sum(case when ts.is_charging_state = 1 then ts.stat_duration end)     as charging_state_duration,
             sum(case when ts.is_idle_state = 1 then ts.stat_duration end)         as idle_state_duration,
             sum(case when ts.is_locked_state = 1 then ts.stat_duration end)       as locked_state_duration,
             sum(case when ts.is_error_state = 1 then ts.stat_duration end)        as error_state_duration
      from (select t.hour_start_time,
                   t.next_hour_start_time,
                   t.robot_code,
                   t.state_id,
                   t.online_state,
                   t.work_state,
                   t.job_sn,
                   case
                       when (t.work_state in ('BUSY', 'CHARGING')) or (t.work_state = 'ERROR' and t.job_sn is not null)
                           then 1
                       else 0 end                                                                        is_uptime_state,
                   case
                       when t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and
                            ((tjh.job_sn is not null and tjh.job_type = 'CUSTOMIZE') or
                             (tj.job_sn is not null and tj.job_type = 'CUSTOMIZE')) then 1
                       else 0 end                                                                        is_loading_busy_state,
                   case
                       when t.online_state = 'REGISTERED' and t.work_state = 'BUSY' and
                            ((tjh.job_sn is not null and tjh.job_type != 'CUSTOMIZE') or
                             (tj.job_sn is not null and tj.job_type != 'CUSTOMIZE')) then 1
                       else 0 end                                                                        is_empty_busy_state,
                   case when t.online_state = 'REGISTERED' and t.work_state = 'BUSY' then 1 else 0 end   is_busy_state,
                   case when t.online_state = 'REGISTERED' and t.work_state = 'IDLE' then 1 else 0 end   is_idle_state,
                   case
                       when t.online_state = 'REGISTERED' and t.work_state = 'CHARGING' then 1
                       else 0 end                                                                        is_charging_state,
                   case
                       when t.online_state = 'REGISTERED' and t.work_state = 'LOCKED' then 1
                       else 0 end                                                                        is_locked_state,
                   case when t.work_state = 'ERROR' then 1 else 0 end                                    is_error_state,
                   t.duration,
                   t.stat_duration
            from qt_smartreport.qt_hour_robot_state_detail_duration_his t
                     left join (select job_sn, job_type from phoenix_rms.job_history) tjh on tjh.job_sn = t.job_sn
                     left join (select job_sn, job_type from phoenix_rms.job) tj on tj.job_sn = t.job_sn
            where t.date_value = date_add(CURRENT_DATE(), interval -1 day)) ts
      group by ts.hour_start_time, ts.next_hour_start_time, ts.robot_code) t1
     on t1.robot_code = tbr.robot_code and t1.hour_start_time = tbr.hour_start_time and
        t1.next_hour_start_time = tbr.next_hour_start_time