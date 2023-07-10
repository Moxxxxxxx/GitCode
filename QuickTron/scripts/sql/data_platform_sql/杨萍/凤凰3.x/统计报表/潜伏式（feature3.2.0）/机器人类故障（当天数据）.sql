------------------------------------------------------------------------------------------------
--step1:建表（qt_notification_robot_module_time_hour_detail_realtime）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_robot_module_time_hour_detail_realtime
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
    `start_time`                datetime(6)           DEFAULT NULL COMMENT '开始时间-告警触发时间',
    `end_time`                  datetime(6)           DEFAULT NULL COMMENT '结束时间-告警结束时间',
    `the_hour_cost_seconds`     decimal(30, 6)     DEFAULT NULL COMMENT '在该小时内时长（秒）',
    `created_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类故障通知在小时内耗时明细（当天数据）';	




------------------------------------------------------------------------------------------------
--step2:删除当天相关数据（qt_notification_robot_module_time_hour_detail_realtime）
DELETE
FROM qt_smartreport.qt_notification_robot_module_time_hour_detail_realtime;  




------------------------------------------------------------------------------------------------
--step3:插入当天相关数据(qt_notification_robot_module_time_hour_detail_realtime)
insert into qt_smartreport.qt_notification_robot_module_time_hour_detail_realtime(hour_start_time, next_hour_start_time,
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
           end the_hour_cost_seconds
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
               inner join phoenix_basic.basic_robot br on br.robot_code = bn.robot_code and br.usage_state='using'
               left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
               inner join (select t.*
                           from (select tn.*,
                                        b.end_time                                                 as prev_end_time,

                                        UNIX_TIMESTAMP(tn.start_time) - UNIX_TIMESTAMP(b.end_time) as diff_seconds,
                                        case
                                            when tn.prev_error_id is null then 1
                                            when UNIX_TIMESTAMP(tn.start_time) - UNIX_TIMESTAMP(b.end_time) < 60 then 0
                                            else 1 end                                                is_effective
                                 from (select t.error_id,
                                              t.robot_code,
                                              t.start_time,
                                              t.end_time,
                                              t.error_code,
                                              max(t.before_error_id)   as prev_error_id,
                                              max(t.before_start_time) as prev_start_time
                                       from (select t1.id         as error_id,
                                                    t1.robot_code,
                                                    t1.start_time,
                                                    t1.end_time,
                                                    t1.error_code,
                                                    t2.id         as before_error_id,
                                                    t2.start_time as before_start_time,
                                                    t2.end_time   as before_end_time
                                             from (select b1.*
                                                   from phoenix_basic.basic_notification b1
                                                            inner join (SELECT robot_code,
                                                                               end_time,
                                                                               min(id)         as first_error_id,
                                                                               min(start_time) as first_start_time
                                                                        from phoenix_basic.basic_notification bn
                                                                        where bn.alarm_module = 'robot'
                                                                          and bn.alarm_level >= 3
                                                                          and start_time >= date_format(
                                                                                date_add(sysdate(), interval -10 day),
                                                                                '%Y-%m-%d 00:00:00')
                                                                        group by robot_code, end_time) b
                                                                       on b.first_error_id = b1.id
                                                   where 1 = 1
                                                     and b1.alarm_module = 'robot'
                                                     and b1.alarm_level >= 3
                                                     and (b1.start_time >=
                                                          date_format(date_add(sysdate(), interval -10 day),
                                                                      '%Y-%m-%d 00:00:00') or b1.end_time is null or
                                                          b1.end_time >=
                                                          date_format(date_add(sysdate(), interval -10 day),
                                                                      '%Y-%m-%d 00:00:00'))) t1
                                                      left join
                                                  (select b2.*
                                                   from phoenix_basic.basic_notification b2
                                                            inner join (SELECT robot_code,
                                                                               end_time,
                                                                               min(id)         as first_error_id,
                                                                               min(start_time) as first_start_time
                                                                        from phoenix_basic.basic_notification bn
                                                                        where bn.alarm_module = 'robot'
                                                                          and bn.alarm_level >= 3
                                                                          and (start_time >= date_format(
                                                                                date_add(sysdate(), interval -10 day),
                                                                                '%Y-%m-%d 00:00:00') or
                                                                               end_time is null or end_time >=
                                                                                                   date_format(
                                                                                                           date_add(sysdate(), interval -10 day),
                                                                                                           '%Y-%m-%d 00:00:00'))
                                                                        group by robot_code, end_time) b
                                                                       on b.first_error_id = b2.id
                                                   where 1 = 1
                                                     and b2.alarm_module = 'robot'
                                                     and b2.alarm_level >= 3
                                                     and (b2.start_time >=
                                                          date_format(date_add(sysdate(), interval -10 day),
                                                                      '%Y-%m-%d 00:00:00') or b2.end_time is null or
                                                          b2.end_time >=
                                                          date_format(date_add(sysdate(), interval -10 day),
                                                                      '%Y-%m-%d 00:00:00'))) t2
                                                  on t2.robot_code = t1.robot_code and t2.error_code = t1.error_code and
                                                     t2.start_time < t1.start_time) t
                                       group by t.error_id, t.robot_code, t.start_time, t.end_time, t.error_code) tn
                                          left join (SELECT robot_code,
                                                            end_time,
                                                            min(id)         as first_error_id,
                                                            min(start_time) as first_start_time
                                                     from phoenix_basic.basic_notification bn
                                                     where bn.alarm_module = 'robot'
                                                       and bn.alarm_level >= 3
                                                       and (start_time >=
                                                            date_format(date_add(sysdate(), interval -10 day),
                                                                        '%Y-%m-%d 00:00:00') or end_time is null or
                                                            end_time >=
                                                            date_format(date_add(sysdate(), interval -10 day),
                                                                        '%Y-%m-%d 00:00:00'))
                                                     group by robot_code, end_time) b
                                                    on b.first_error_id = tn.prev_error_id) t
                           where t.is_effective = 1) tbn on tbn.error_id = bn.id
      where 1 = 1
        and bn.alarm_module = 'robot'
        and (((bn.start_time >= date_format(sysdate(), '%Y-%m-%d 00:00:00') and
               bn.start_time < sysdate() and
               date_format(coalesce(bn.end_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
               sysdate())
          or
              (bn.start_time >= date_format(sysdate(), '%Y-%m-%d 00:00:00') and
               bn.start_time < sysdate() and
               date_format(coalesce(bn.end_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
               sysdate())
          or
              (bn.start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
               date_format(coalesce(bn.end_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
               date_format(sysdate(), '%Y-%m-%d 00:00:00') and
               date_format(coalesce(bn.end_time, sysdate()), '%Y-%m-%d %H:%i:%s') <
               sysdate())
          or
              (bn.start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and
               date_format(coalesce(bn.end_time, sysdate()), '%Y-%m-%d %H:%i:%s') >=
               sysdate())) or bn.end_time is null)) t2 on 1
;






--------------------------------
##step4:建表(qt_notification_robot_module_object_stat_realtime)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_robot_module_object_stat_realtime
(
    `id`                           int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                   datetime  NOT NULL COMMENT '统计时间',
    `date_value`                   date               DEFAULT NULL COMMENT '日期',
    `hour_value`                   varchar(100)       DEFAULT NULL COMMENT '小时',
    `robot_code`                   varchar(100)       DEFAULT NULL COMMENT '机器人编码',
    `first_classification_name`    varchar(100)       DEFAULT NULL COMMENT '机器人类型',
    `add_notification_num`         decimal(30, 6)           DEFAULT NULL COMMENT '新增告警次数',
    `notification_num`             decimal(30, 6)          DEFAULT NULL COMMENT '告警次数',
    `notification_time`            decimal(30, 6)     DEFAULT NULL COMMENT '告警时长（秒）',
    `notification_rate`            decimal(30, 6)     DEFAULT NULL COMMENT '告警率',
    `notification_rate_fenzi`      decimal(30, 6)     DEFAULT NULL COMMENT '告警率分子',
    `notification_rate_fenmu`      decimal(30, 6)     DEFAULT NULL COMMENT '告警率分母',
    `mtbf`                         decimal(30, 6)     DEFAULT NULL COMMENT 'mtbf',
    `mtbf_fenzi`                   decimal(30, 6)     DEFAULT NULL COMMENT 'mtbf分子',
    `mtbf_fenmu`                   decimal(30, 6)     DEFAULT NULL COMMENT 'mtbf分母',
    `mttr`                         decimal(30, 6)     DEFAULT NULL COMMENT 'mttr',
    `mttr_fenzi`                   decimal(30, 6)     DEFAULT NULL COMMENT 'mttr分子',
    `mttr_fenmu`                   decimal(30, 6)     DEFAULT NULL COMMENT 'mttr分母',
    `notification_per_order`       decimal(30, 6)     DEFAULT NULL COMMENT '平均每作业单故障数',
    `notification_per_order_fenzi` decimal(30, 6)     DEFAULT NULL COMMENT '平均每作业单故障数分子',
    `notification_per_order_fenmu` decimal(30, 6)     DEFAULT NULL COMMENT '平均每作业单故障数分母',
    `notification_per_job`       decimal(30, 6)     DEFAULT NULL COMMENT '平均每任务故障数',
    `notification_per_job_fenzi` decimal(30, 6)     DEFAULT NULL COMMENT '平均每任务故障数分子',
    `notification_per_job_fenmu` decimal(30, 6)     DEFAULT NULL COMMENT '平均每任务故障数分母',	
    `time_type`                    varchar(100)       DEFAULT NULL COMMENT '统计维度',
    `created_time`                 timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`                 timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='机器人类告警通知在时间段内指标统计（当天数据）';	
	
	
--------------------------------
##step5:删除当天相关数据(qt_notification_robot_module_object_stat_realtime)
DELETE
FROM qt_smartreport.qt_notification_robot_module_object_stat_realtime;  





--------------------------------
##step6-1:插入当天相关数据(qt_notification_robot_module_object_stat_realtime)
#time_type='小时' 

insert into qt_smartreport.qt_notification_robot_module_object_stat_realtime(time_value, date_value, hour_value, robot_code,first_classification_name,add_notification_num, notification_num,notification_time, notification_rate,notification_rate_fenzi,notification_rate_fenmu, mtbf,mtbf_fenzi,mtbf_fenmu,mttr,mttr_fenzi,mttr_fenmu,notification_per_order,notification_per_order_fenzi,notification_per_order_fenmu,notification_per_job,notification_per_job_fenzi,notification_per_job_fenmu, time_type)

select tt.hour_start_time                                                                                             as time_value,
       date(tt.hour_start_time)                                                                                       as date_value,
       HOUR(tt.hour_start_time)                                                                                       as hour_value,
       tt.robot_code,
       tt.first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d %H:00:00') = tt.hour_start_time
                                       then t.notification_id end),
                0)                                                                                                    as add_notification_num,
       count(distinct t.notification_id)                                                                              as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                        as notification_time,
       coalesce(sum(the_hour_cost_seconds), 0) / (case
                                                      when HOUR(tt.hour_start_time) = HOUR(sysdate())
                                                          then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(tt.hour_start_time)
                                                      else 3600 end)                                                  as notification_rate,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                        as notification_rate_fenzi,
       (case
            when HOUR(tt.hour_start_time) = HOUR(sysdate())
                then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(tt.hour_start_time)
            else 3600 end)                                                                                            as notification_rate_fenmu,
       case
           when count(distinct t.notification_id) != 0 then ((case
                                                                  when HOUR(tt.hour_start_time) = HOUR(sysdate())
                                                                      then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(tt.hour_start_time)
                                                                  else 3600 end) -
                                                             coalesce(sum(the_hour_cost_seconds), 0)) /
                                                            count(distinct t.notification_id)
           else (case
                     when HOUR(tt.hour_start_time) = HOUR(sysdate())
                         then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(tt.hour_start_time)
                     else 3600 end) end                                                                               as mtbf,
       ((case
             when HOUR(tt.hour_start_time) = HOUR(sysdate())
                 then UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(tt.hour_start_time)
             else 3600 end) -
        coalesce(sum(the_hour_cost_seconds), 0))                                                                      as mtbf_fenzi,
       count(distinct t.notification_id)                                                                              as mtbf_fenmu,
       case
           when count(distinct t.notification_id) != 0 then coalesce(sum(the_hour_cost_seconds), 0) /
                                                            count(distinct t.notification_id)
           else 0 end                                                                                                 as mttr,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                        as mttr_fenzi,
       count(distinct t.notification_id)                                                                              as mttr_fenmu,
       case
           when tto.order_num != 0
               then count(distinct t.notification_id) / tto.order_num end                      as notification_per_order,
       count(distinct t.notification_id)                                                       as notification_per_order_fenzi,
       coalesce(tto.order_num, 0)                                                              as notification_per_order_fenmu,
       case when jh.job_num != 0 then count(distinct t.notification_id) / jh.job_num end       as notification_per_job,
       count(distinct t.notification_id)                                                       as notification_per_job_fenzi,
       coalesce(jh.job_num, 0)                                                                 as notification_per_job_fenmu,
       '小时'                                                                                                           as time_type
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
                      t.robot_code = tt.robot_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                       date_format(sysdate(), '%Y-%m-%d') and
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
group by time_value, date_value, hour_value, robot_code, first_classification_name
;




--------------------------------
##step6-2:插入当天相关数据(qt_notification_robot_module_object_stat_realtime)
#time_type='天'

insert into qt_smartreport.qt_notification_robot_module_object_stat_realtime(time_value, date_value, hour_value, robot_code,first_classification_name,add_notification_num, notification_num,notification_time, notification_rate,notification_rate_fenzi,notification_rate_fenmu, mtbf,mtbf_fenzi,mtbf_fenmu,mttr,mttr_fenzi,mttr_fenmu,notification_per_order,notification_per_order_fenzi,notification_per_order_fenmu,notification_per_job,notification_per_job_fenzi,notification_per_job_fenmu, time_type)


select date_format(tt.hour_start_time, '%Y-%m-%d')                                                             as time_value,
       date(tt.hour_start_time)                                                                                as date_value,
       null                                                                                                    as hour_value,
       tt.robot_code,
       tt.first_classification_name,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d') =
                                        date_format(tt.hour_start_time, '%Y-%m-%d')
                                       then t.notification_id end),
                0)                                                                                             as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                                          as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                 as notification_time,
       coalesce(sum(the_hour_cost_seconds), 0) / (UNIX_TIMESTAMP(sysdate()) -
                                                  UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) as notification_rate,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                 as notification_rate_fenzi,
       (UNIX_TIMESTAMP(sysdate()) -
        UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00')))                                           as notification_rate_fenmu,


       case
           when count(distinct t.notification_id) != 0 then
                   ((UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) -
                    coalesce(sum(the_hour_cost_seconds), 0)) /
                   count(distinct t.notification_id)
           else (UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) end  as mtbf,
       ((UNIX_TIMESTAMP(sysdate()) - UNIX_TIMESTAMP(DATE_FORMAT(sysdate(), '%Y-%m-%d 00:00:00'))) -
        coalesce(sum(the_hour_cost_seconds), 0))                                                               as mtbf_fenzi,
       coalesce(count(distinct t.notification_id), 0)                                                          as mtbf_fenmu,
       case
           when count(distinct t.notification_id) != 0 then coalesce(sum(the_hour_cost_seconds), 0) /
                                                            count(distinct t.notification_id)
           else 0 end                                                                                          as mttr,
       coalesce(sum(the_hour_cost_seconds), 0)                                                                 as mttr_fenzi,
       coalesce(count(distinct t.notification_id), 0)                                                          as mttr_fenmu,
       case
           when tto.order_num != 0
               then coalesce(count(distinct t.notification_id), 0) / tto.order_num end                     as notification_per_order,
       coalesce(count(distinct t.notification_id), 0)                                                      as notification_per_order_fenzi,
       coalesce(tto.order_num, 0)                                                                          as notification_per_order_fenmu,
       case
           when jh.job_num != 0
               then coalesce(count(distinct t.notification_id), 0) / jh.job_num end                        as notification_per_job,
       coalesce(count(distinct t.notification_id), 0)                                                      as notification_per_job_fenzi,
       coalesce(jh.job_num, 0)                                                                             as notification_per_job_fenmu,
       '天'                                                                                                     as time_type
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
                      t.robot_code = tt.robot_code and date_format(t.hour_start_time, '%Y-%m-%d') =
                                                       date_format(sysdate(), '%Y-%m-%d') and
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
                      tto.date_value = date_format(tt.hour_start_time, '%Y-%m-%d')

         left join (select robot_code,
                           date_format(finish_time, '%Y-%m-%d') as date_value,
                           count(distinct job_sn)               as job_num
                    from phoenix_rms.job_history
                    where date_format(finish_time, '%Y-%m-%d') =
                          date_format(sysdate(), '%Y-%m-%d')
                    group by robot_code, date_value) jh
                   on jh.robot_code = tt.robot_code and
                      jh.date_value = date_format(tt.hour_start_time, '%Y-%m-%d')
group by time_value, date_value, hour_value, robot_code, first_classification_name
;













































----------------------------------------------------------------------------------!!!!!!!以下数据集后期慢慢不用了！！！！！！！！------------------------------------------------------------------------

--------------------------------
##step7:建表(qt_notification_robot_module_stat_realtime)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_robot_module_stat_realtime
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                datetime  NOT NULL COMMENT '统计时间',
    `date_value`                date               DEFAULT NULL COMMENT '日期',
    `hour_value`                varchar(100)       DEFAULT NULL COMMENT '小时',
    `robot_num`                int(100)       DEFAULT NULL COMMENT '机器人数量',
    `add_notification_num`      int(100)           DEFAULT NULL COMMENT '新增告警次数',
    `notification_num`          int(100)           DEFAULT NULL COMMENT '告警次数',
    `notification_time`         decimal(10, 3)           DEFAULT NULL COMMENT '告警时长（秒）',
    `notification_rate`         decimal(10, 4)     DEFAULT NULL COMMENT '告警率',
    `notification_rate_fenzi`   decimal(10, 3)          DEFAULT NULL COMMENT '告警率分子',
    `notification_rate_fenmu`   decimal(10, 3)           DEFAULT NULL COMMENT '告警率分母',
    `mtbf`                      decimal(10, 2)     DEFAULT NULL COMMENT 'mtbf',
    `mtbf_fenzi`                decimal(10, 3)           DEFAULT NULL COMMENT 'mtbf分子',
    `mtbf_fenmu`                decimal(10, 3)           DEFAULT NULL COMMENT 'mtbf分母',
    `mttr`                      decimal(10, 2)     DEFAULT NULL COMMENT 'mttr',
    `mttr_fenzi`                decimal(10, 3)           DEFAULT NULL COMMENT 'mttr分子',
    `mttr_fenmu`                decimal(10, 3)          DEFAULT NULL COMMENT 'mttr分母',
    `time_type`                 varchar(100)       DEFAULT NULL COMMENT '统计维度',
    `created_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='全场机器人类告警通知在时间段内指标统计（当天数据）';	
	
	
--------------------------------
##step8:删除当天相关数据(qt_notification_robot_module_stat_realtime)
DELETE
FROM qt_smartreport.qt_notification_robot_module_stat_realtime;  


--------------------------------
##step9-1:插入当天相关数据(qt_notification_robot_module_stat_realtime)
#time_type='小时' 
insert into qt_smartreport.qt_notification_robot_module_stat_realtime(time_value, date_value, hour_value, robot_num,add_notification_num, notification_num,notification_time, notification_rate,notification_rate_fenzi,notification_rate_fenmu, mtbf,mtbf_fenzi,mtbf_fenmu,mttr,mttr_fenzi,mttr_fenmu,time_type)

select tt.hour_start_time                                                                      as time_value,
       date(tt.hour_start_time)                                                                as date_value,
       HOUR(tt.hour_start_time)                                                                as hour_value,
       tt.robot_num,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d %H:00:00') = tt.hour_start_time
                                       then t.notification_id end),
                0)                                                                             as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                          as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                 as notification_time,
       case when tt.robot_num!=0 then cast(coalesce(sum(the_hour_cost_seconds), 0) / (3600 * tt.robot_num) as decimal(10, 4)) end as notification_rate,
	   coalesce(sum(the_hour_cost_seconds), 0) as  notification_rate_fenzi,
	   (3600 * coalesce(tt.robot_num,0)) as  notification_rate_fenmu,
       cast(case when count(distinct t.notification_id) != 0 then cast((3600 * coalesce(tt.robot_num,0) - coalesce(sum(the_hour_cost_seconds), 0)) / count(distinct t.notification_id) as decimal(10, 2)) else 3600 * coalesce(tt.robot_num,0) end as decimal(10, 2)) as mtbf,
	   case when count(distinct t.notification_id) != 0 then (3600 * tt.robot_num - coalesce(sum(the_hour_cost_seconds), 0)) else 3600 * coalesce(tt.robot_num,0) end as mtbf_fenzi,
	   coalesce(count(distinct t.notification_id),0) mtbf_fenmu,
       cast(case when count(distinct t.notification_id) != 0 then cast(coalesce(sum(the_hour_cost_seconds), 0) /  count(distinct t.notification_id) as decimal(10, 2)) else 0 end as decimal(10, 2)) as mttr,
	   coalesce(sum(the_hour_cost_seconds), 0) as  mttr_fenzi,
	   coalesce(count(distinct t.notification_id),0) as mttr_fenmu,
       '小时'                                                                                    as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             count(distinct t2.robot_code) as robot_num
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
                     left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id) t2 on 1
      group by t1.hour_start_time, t1.next_hour_start_time) tt
         left join qt_smartreport.qt_notification_robot_module_time_hour_detail_realtime t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      date_format(t.hour_start_time, '%Y-%m-%d') =
                      date_format(sysdate(), '%Y-%m-%d') and t.the_hour_cost_seconds is not null 
group by time_value, date_value, hour_value, robot_num
;





--------------------------------
##step9-2:插入当天相关数据(qt_notification_robot_module_stat_realtime)
#time_type='天'

insert into qt_smartreport.qt_notification_robot_module_stat_realtime(time_value, date_value, hour_value, robot_num,add_notification_num, notification_num,notification_time, notification_rate,notification_rate_fenzi,notification_rate_fenmu, mtbf,mtbf_fenzi,mtbf_fenmu,mttr,mttr_fenzi,mttr_fenmu,time_type)

select date_format(tt.hour_start_time, '%Y-%m-%d')                                                  as time_value,
       date(tt.hour_start_time)                                                                     as date_value,
       null                                                                                         as hour_value,
       tt.robot_num,
       coalesce(count(distinct case
                                   when date_format(t.start_time, '%Y-%m-%d') =
                                        date_format(tt.hour_start_time, '%Y-%m-%d')
                                       then t.notification_id end),
                0)                                                                                  as add_notification_num,
       coalesce(count(distinct t.notification_id), 0)                                               as notification_num,
       coalesce(sum(the_hour_cost_seconds), 0)                                                      as notification_time,
       case when tt.robot_num !=0 then cast(coalesce(sum(the_hour_cost_seconds), 0) / (3600 * 24 * tt.robot_num) as decimal(10, 4)) end as notification_rate,
	   coalesce(sum(the_hour_cost_seconds), 0) as notification_rate_fenzi,
	   (3600 * 24 * coalesce(tt.robot_num,0)) as notification_rate_fenmu,
       cast(case when count(distinct t.notification_id) != 0 then cast((3600 * 24 * tt.robot_num - coalesce(sum(the_hour_cost_seconds), 0)) / count(distinct t.notification_id) as decimal(10, 2)) else 3600 * 24 * tt.robot_num end as decimal(10, 2)) as mtbf,
	   case when count(distinct t.notification_id) != 0 then (3600 * 24 * coalesce(tt.robot_num,0) - coalesce(sum(the_hour_cost_seconds), 0)) else 3600 * 24 * coalesce(tt.robot_num,0) end mtbf_fenzi, 
	   coalesce(count(distinct t.notification_id),0) mtbf_fenmu,
       cast(case when count(distinct t.notification_id) != 0 then cast(coalesce(sum(the_hour_cost_seconds), 0) / count(distinct t.notification_id) as decimal(10, 2)) else 0 end as decimal(10, 2)) as mttr,
	   coalesce(sum(the_hour_cost_seconds), 0) as  mttr_fenzi,
	   coalesce(count(distinct t.notification_id),0) mttr_fenmu,
       '天'                                                                                          as time_type
from (select t1.hour_start_time,
             t1.next_hour_start_time,
             count(distinct t2.robot_code) as robot_num
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
                     left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id) t2 on 1
      group by t1.hour_start_time, t1.next_hour_start_time) tt
         left join qt_smartreport.qt_notification_robot_module_time_hour_detail_realtime t
                   on t.hour_start_time = tt.hour_start_time and t.next_hour_start_time = tt.next_hour_start_time and
                      date_format(t.hour_start_time, '%Y-%m-%d') =
                      date_format(sysdate(), '%Y-%m-%d') and t.the_hour_cost_seconds is not null 
group by time_value, date_value, hour_value, robot_num
;






--------------------------------
##step10:建表(qt_notification_robot_module_index_stat_realtime)
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_notification_robot_module_index_stat_realtime
(
    `id`                        int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `time_value`                datetime  NOT NULL COMMENT '统计时间',
    `date_value`                date               DEFAULT NULL COMMENT '日期',
    `hour_value`                varchar(100)       DEFAULT NULL COMMENT '小时',
    `time_type`                 varchar(100)       DEFAULT NULL COMMENT '统计维度',
    `index_value`               decimal(65, 4)     DEFAULT NULL COMMENT '指标值',
    `index_value_fenzi`               decimal(65, 4)     DEFAULT NULL COMMENT '指标值分子',
    `index_value_fenmu`               decimal(65, 4)     DEFAULT NULL COMMENT '指标值分母',		
    `value_type`                varchar(100)       DEFAULT NULL COMMENT '指标类型',
    `created_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`              timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='全场机器人类告警通知在时间段内各指标值（当天数据）';	
	
	
--------------------------------
##step11:删除数据(qt_notification_robot_module_index_stat_realtime)
DELETE
FROM qt_smartreport.qt_notification_robot_module_index_stat_realtime;   
	


--------------------------------
##step12:插入当天相关数据(qt_notification_robot_module_index_stat_realtime)
insert into qt_smartreport.qt_notification_robot_module_index_stat_realtime(time_value, date_value, hour_value, time_type,index_value,index_value_fenzi,index_value_fenmu, value_type)
select time_value,
       DATE(date_value)                         as date_value,
       hour_value,
       time_type,
       cast(add_notification_num as decimal(65, 4)) as index_value,
	   null as index_value_fenzi,
	   null as index_value_fenmu,	   
       '新增故障次数'                                   as value_type
from qt_smartreport.qt_notification_robot_module_stat_realtime
WHERE date_format(time_value, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                          as date_value,
       hour_value,
       time_type,
       cast(notification_time as decimal(65, 4)) as index_value,
	   null as index_value_fenzi,
	   null as index_value_fenmu,	   
       '故障时长'                                    as value_type
from qt_smartreport.qt_notification_robot_module_stat_realtime
WHERE date_format(time_value, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)                          as date_value,
       hour_value,
       time_type,
       cast(notification_rate as decimal(65, 4)) as index_value,
	   cast(notification_rate_fenzi as decimal(65, 4)) as index_value_fenzi,
	   cast(notification_rate_fenmu as decimal(65, 4)) as index_value_fenmu,	   
       '故障率'                                     as value_type
from qt_smartreport.qt_notification_robot_module_stat_realtime
WHERE date_format(time_value, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)             as date_value,
       hour_value,
       time_type,
       cast(mtbf as decimal(65, 4)) as index_value,
	   cast(mtbf_fenzi as decimal(65, 4)) as index_value_fenzi,
	   cast(mtbf_fenmu as decimal(65, 4)) as index_value_fenmu,		   
       'MTBF'                       as value_type
from qt_smartreport.qt_notification_robot_module_stat_realtime
WHERE date_format(time_value, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
union all
select time_value,
       DATE(date_value)             as date_value,
       hour_value,
       time_type,
       cast(mttr as decimal(65, 4)) as index_value,
	   cast(mttr_fenzi as decimal(65, 4)) as index_value_fenzi,
	   cast(mttr_fenmu as decimal(65, 4)) as index_value_fenmu,		   
       'MTTR'                       as value_type
from qt_smartreport.qt_notification_robot_module_stat_realtime
WHERE date_format(time_value, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
;

