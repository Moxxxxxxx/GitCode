###基于mysql5.7
set @do_date = date(date_add(sysdate(), interval -1 day));



DROP TABLE if EXISTS qt_smartreport.qt_tmp_step1;
create table qt_smartreport.qt_tmp_step1
(
    id         bigint(20),
    robot_code varchar(32),
    current_id varchar(32),
    next_id    varchar(32),
    PRIMARY KEY (`id`),
    key idx_current_id (current_id),
    key idx_next_id (next_id)
);


set @begin_id = (SELECT id
                 from phoenix_rms.robot_state_history
                 WHERE create_time >= date_add(@do_date, interval -11 day)
                 LIMIT 1);


insert into qt_smartreport.qt_tmp_step1
SELECT id,
       robot_code,
       CONCAT(robot_code, '-', @rn := @rn + 1) current_id,
       CONCAT(robot_code, '-', @rn + 1)        next_id
from phoenix_rms.robot_state_history,
     (SELECT @rn := 0) tmp
WHERE id >= @begin_id
ORDER BY robot_code, id;



DROP TABLE if EXISTS qt_smartreport.qt_tmp_step2;
create table qt_smartreport.qt_tmp_step2
(
    id         bigint(20),
    robot_code varchar(32),
    next_id    varchar(32),
    PRIMARY KEY (`id`),
    key idx_robot_code (robot_code),
    key idx_next_id (next_id)
);

insert into qt_smartreport.qt_tmp_step2
SELECT t1.id, t1.robot_code, t2.id as next_id
from qt_smartreport.qt_tmp_step1 t1
         LEFT JOIN qt_smartreport.qt_tmp_step1 t2
                   ON t1.next_id = t2.current_id;


#机器人状态在小时内耗时明细

DROP TABLE if EXISTS qt_smartreport.qt_tmp_step3;
create table qt_smartreport.qt_tmp_step3
as
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
from (select th.day_hours                               as hour_start_time,
             DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
      from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(@do_date, '%Y-%m-%d 00:00:00'), INTERVAL
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
         inner join(select t.*,
                           case
                               when t.create_time < date_format(@do_date, '%Y-%m-%d 00:00:00')
                                   then date_format(@do_date, '%Y-%m-%d 00:00:00')
                               else t.create_time end as start_time,
                           case
                               when t.next_time >= date_format(date_add(@do_date, interval 1 day), '%Y-%m-%d 00:00:00')
                                   then date_format(date_add(@do_date, interval 1 day), '%Y-%m-%d 00:00:00')
                               else t.next_time end   as end_time
                    from (select t1.id,
                                 t1.robot_code,
                                 t1.create_time,
                                 t1.network_state,
                                 t1.online_state,
                                 t1.work_state,
                                 t1.job_sn,
                                 t1.cause,
                                 t2.id                                                      as next_id,
                                 coalesce(t2.create_time, date_format(date_add(t1.create_time, interval 1 day),
                                                                      '%Y-%m-%d 00:00:00')) as next_time,
                                 t2.create_time                                             as next_create_time,
                                 t1.network_state                                           as next_network_state,
                                 t1.online_state                                            as next_online_state,
                                 t1.work_state                                              as next_work_state,
                                 t1.job_sn                                                  as next_job_sn,
                                 t1.cause                                                   as next_cause
                          from phoenix_rms.robot_state_history t1
                                   left join qt_smartreport.qt_tmp_step2 tm
                                             on tm.robot_code = t1.robot_code and tm.id = t1.id
                                   left join phoenix_rms.robot_state_history t2
                                             on t2.robot_code = tm.robot_code and t2.id = tm.next_id
                          where date(t1.create_time) >= date(date_add(@do_date, interval -10 day))) t
                    where 1 = 1
                      and ((t.create_time >= date_format(@do_date, '%Y-%m-%d 00:00:00') and
                            t.create_time < date_format(date_add(@do_date, interval 1 day), '%Y-%m-%d 00:00:00') and
                            t.next_time < date_format(date_add(@do_date, interval 1 day), '%Y-%m-%d 00:00:00'))
                        or
                           (t.create_time >= date_format(@do_date, '%Y-%m-%d 00:00:00') and
                            t.create_time < date_format(date_add(@do_date, interval 1 day), '%Y-%m-%d 00:00:00') and
                            t.next_time >= date_format(date_add(@do_date, interval 1 day), '%Y-%m-%d 00:00:00'))
                        or
                           (t.create_time < date_format(@do_date, '%Y-%m-%d 00:00:00') and
                            t.next_time >= date_format(@do_date, '%Y-%m-%d 00:00:00') and
                            t.next_time < date_format(date_add(@do_date, interval 1 day), '%Y-%m-%d 00:00:00'))
                        or
                           (t.create_time < date_format(@do_date, '%Y-%m-%d 00:00:00') and
                            t.next_time >= date_format(date_add(@do_date, interval 1 day), '%Y-%m-%d 00:00:00')))) t2
                   on (
                           (t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                            t2.end_time < t1.next_hour_start_time)
                           or (t2.start_time >= t1.hour_start_time and t2.start_time < t1.next_hour_start_time and
                               t2.end_time >= t1.next_hour_start_time)
                           or (t2.start_time < t1.hour_start_time and t2.end_time >= t1.hour_start_time and
                               t2.end_time < t1.next_hour_start_time)
                           or (t2.start_time < t1.hour_start_time and t2.end_time >= t1.next_hour_start_time)
                       );


#机器人运行状态

#time_type='小时'

DROP TABLE if EXISTS qt_smartreport.qt_tmp_step4;
create table qt_smartreport.qt_tmp_step4
as
select t.hour_start_time                            as time_value,
       date(t.hour_start_time)                      as date_value,
       HOUR(t.hour_start_time)                      as hour_value,
       t.first_classification_name,
       t.robot_code,
       coalesce(sum(t.loading_busy_time), 0)        as loading_busy_time,
       coalesce(sum(t.empty_busy_time), 0)          as empty_busy_time,
       coalesce(sum(t.idle_time), 0)                as idle_time,
       coalesce(sum(t.charging_time), 0)            as charging_time,
       coalesce(sum(t.lock_time), 0)                as lock_time,
       coalesce(sum(t.error_time), 0)               as error_time,
       3600 - coalesce(sum(t.loading_busy_time), 0) - coalesce(sum(t.empty_busy_time), 0) -
       coalesce(sum(t.idle_time), 0) -
       coalesce(sum(t.charging_time), 0) - coalesce(sum(t.lock_time), 0) -
       coalesce(sum(t.error_time), 0)               as offline_time,
       coalesce(sum(t.loading_busy_time), 0) / 3600 as loading_busy_rate,
       '小时'                                         as time_type
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
                  from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(@do_date, '%Y-%m-%d 00:00:00'), INTERVAL
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
               left join qt_smartreport.qt_tmp_step3 t
                         on t.hour_start_time = tt.hour_start_time and
                            t.next_hour_start_time = tt.next_hour_start_time and
                            t.robot_code = tt.robot_code and date(t.hour_start_time) = @do_date and
                            t.the_hour_cost_seconds is not null
               left join (select DISTINCT job_sn
                          from phoenix_rss.transport_order_carrier_job
                          where date(create_time) >= date(date_add(@do_date, interval -11 day))) rjsc
                         on rjsc.job_sn = t.job_sn) t
group by time_value, date_value, hour_value, first_classification_name, robot_code
;

#time_type='天'

insert into qt_smartreport.qt_tmp_step4
select date_format(t.hour_start_time, '%Y-%m-%d')          as time_value,
       date(t.hour_start_time)                             as date_value,
       null                                                as hour_value,
       t.first_classification_name,
       t.robot_code,
       coalesce(sum(t.loading_busy_time), 0)               as loading_busy_time,
       coalesce(sum(t.empty_busy_time), 0)                 as empty_busy_time,
       coalesce(sum(t.idle_time), 0)                       as idle_time,
       coalesce(sum(t.charging_time), 0)                   as charging_time,
       coalesce(sum(t.lock_time), 0)                       as lock_time,
       coalesce(sum(t.error_time), 0)                      as error_time,
       3600 * 24 - coalesce(sum(t.loading_busy_time), 0) - coalesce(sum(t.empty_busy_time), 0) -
       coalesce(sum(t.idle_time), 0) -
       coalesce(sum(t.charging_time), 0) - coalesce(sum(t.lock_time), 0) -
       coalesce(sum(t.error_time), 0)                      as offline_time,
       coalesce(sum(t.loading_busy_time), 0) / (3600 * 24) as loading_busy_rate,
       '天'                                                 as time_type
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
                  from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT(@do_date, '%Y-%m-%d 00:00:00'), INTERVAL
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
               left join qt_smartreport.qt_tmp_step3 t
                         on t.hour_start_time = tt.hour_start_time and
                            t.next_hour_start_time = tt.next_hour_start_time and
                            t.robot_code = tt.robot_code and date(t.hour_start_time) = @do_date and
                            t.the_hour_cost_seconds is not null
               left join (select DISTINCT job_sn
                          from phoenix_rss.transport_order_carrier_job
                          where date(create_time) >= date(date_add(@do_date, interval -11 day))) rjsc
                         on rjsc.job_sn = t.job_sn) t
group by time_value, date_value, hour_value, first_classification_name, robot_code
;



#机器人各类运行状态时长占比

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
from qt_smartreport.qt_tmp_step4
WHERE date(time_value) = @do_date
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
from qt_smartreport.qt_tmp_step4
WHERE date(time_value) = @do_date
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
from qt_smartreport.qt_tmp_step4
WHERE date(time_value) = @do_date
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
from qt_smartreport.qt_tmp_step4
WHERE date(time_value) = @do_date
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
from qt_smartreport.qt_tmp_step4
WHERE date(time_value) = @do_date
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
from qt_smartreport.qt_tmp_step4
WHERE date(time_value) = @do_date
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
from qt_smartreport.qt_tmp_step4
WHERE date(time_value) = @do_date
;
