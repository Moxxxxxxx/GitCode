##step1:建表（qt_transport_order_efficiency_stat_detail）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_transport_order_efficiency_stat_detail
(
    `id`                           int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `order_id`                     varchar(100) NOT NULL COMMENT '作业单ID',
    `order_create_time`            datetime(6)           DEFAULT NULL COMMENT '作业单创建时间',
    `order_done_time`              datetime(6)           DEFAULT NULL COMMENT '作业单完成时间',
    `scene_type`                   varchar(100)          DEFAULT NULL COMMENT '场景类型',
    `stat_time`                    datetime     NOT NULL COMMENT '统计时间',
    `order_type`                   varchar(100)          DEFAULT NULL COMMENT '作业单类型',
    `robot_code`                   varchar(100)          DEFAULT NULL COMMENT '机器人编码',
    `start_point`                  varchar(100)          DEFAULT NULL COMMENT '起始点',
    `target_point`                 varchar(100)          DEFAULT NULL COMMENT '目标点',
    `total_time_consuming`         decimal(10, 3)        DEFAULT NULL COMMENT '总耗时(秒)',
    `init_job_time_consuming`      decimal(10, 3)        DEFAULT NULL COMMENT '分车耗时(秒)',
    `move_time_consuming`          decimal(10, 3)        DEFAULT NULL COMMENT '空车移动耗时(秒)',
    `lift_up_time_consuming`       decimal(10, 3)        DEFAULT NULL COMMENT '顶升耗时(秒)',
    `rack_move_time_consuming`     decimal(10, 3)        DEFAULT NULL COMMENT '带载移动耗时(秒)',
    `put_down_time_consuming`      decimal(10, 3)        DEFAULT NULL COMMENT '放下耗时(秒)',
    `guide_time_consuming`         decimal(10, 3)        DEFAULT NULL COMMENT '末端引导耗时(秒)',
    `move_abnormal_time_consuming` decimal(10, 3)        DEFAULT NULL COMMENT '移动异常时长(秒)',
    `move_distance`                decimal(10, 2)              DEFAULT NULL COMMENT '实际移动距离(米)',
    `move_speed`                   decimal(10, 2)        DEFAULT NULL COMMENT '实际行驶速度(米/秒)',
    `rotation_num`                 int(100)              DEFAULT NULL COMMENT '机器人旋转次数',
    `rotation_time_consuming`      decimal(10, 3)        DEFAULT NULL COMMENT '旋转耗时(秒)',
    `loading_move_distance`               decimal(10, 2)              DEFAULT NULL COMMENT '带载移动距离(米)',
    `loading_move_speed`                   decimal(10, 2)        DEFAULT NULL COMMENT '带载移动速度(米/秒)',
    `empty_move_distance`                decimal(10, 2)              DEFAULT NULL COMMENT '空车移动距离(米)',
    `empty_move_speed`                   decimal(10, 2)        DEFAULT NULL COMMENT '空车移动速度(米/秒)',	
    `created_time`                 timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`                 timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='作业单效率统计信息明细';	
	
	
--------------------------------------------------------------------------------------------------------------
	
##step2:删除当天相关数据（qt_transport_order_efficiency_stat_detail）
DELETE
FROM qt_smartreport.qt_transport_order_efficiency_stat_detail
WHERE date_format(stat_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  	

--------------------------------------------------------------------------------------------------------------
##step3:插入当天相关数据（qt_transport_order_efficiency_stat_detail）
insert into qt_smartreport.qt_transport_order_efficiency_stat_detail(order_id, order_create_time, order_done_time,
                                                                     scene_type,
                                                                     stat_time, order_type, robot_code, start_point,
                                                                     target_point,
                                                                     total_time_consuming, init_job_time_consuming,
                                                                     move_time_consuming, lift_up_time_consuming,
                                                                     rack_move_time_consuming, put_down_time_consuming,
                                                                     guide_time_consuming, move_abnormal_time_consuming,
                                                                     move_distance, move_speed, rotation_num,
                                                                     rotation_time_consuming, loading_move_distance,
                                                                     loading_move_speed, empty_move_distance,
                                                                     empty_move_speed)
select t.order_no                                                                    as order_id,
       t.create_time                                                                 as order_create_time,
       t.update_time                                                                 as order_done_time,
       substring(t.order_type, 1, instr(t.order_type, '_') - 1)                      as scene_type,
       date_format(t.update_time, '%Y-%m-%d %H:00:00')                               as stat_time,
       t.order_type,
       t.dispatch_robot_code                                                         as robot_code,
       coalesce(t.start_point_code, 'unknow')                                        as start_point,
       coalesce(t.target_point_code, 'unknow')                                       as target_point,
       unix_timestamp(t.update_time) - unix_timestamp(t.create_time)                 as total_time_consuming,
       coalesce(t2.init_job_time_consuming, 0)                                       as init_job_time_consuming,
       coalesce(t3.move_time_consuming, 0)                                           as move_time_consuming,
       coalesce(t4.lift_up_time_consuming, 0)                                        as lift_up_time_consuming,
       coalesce(t5.rack_move_time_consuming, 0)                                      as rack_move_time_consuming,
       coalesce(t6.put_down_time_consuming, 0)                                       as put_down_time_consuming,
       coalesce(t7.guide_time_consuming, 0)                                          as guide_time_consuming,
       null                                                                          as move_abnormal_time_consuming,
       round(coalesce(t7.order_actual_move_distance, 0), 2)                          as move_distance,
       round(coalesce(t7.order_actual_move_distance, 0) /
             (unix_timestamp(t.update_time) - unix_timestamp(t.create_time)), 2)     as move_speed,
       coalesce(t7.order_rotate_count, 0)                                            as rotation_num,
       null                                                                          as rotation_time_consuming,
       round(coalesce(t7.loading_move_distance, 0), 2)                               as loading_move_distance,
       round(coalesce(t7.loading_move_distance, 0) / t5.rack_move_time_consuming, 2) as loading_move_speed,
       round(coalesce(t7.empty_move_distance, 0), 2)                                 as empty_move_distance,
       round(coalesce(t7.empty_move_distance, 0) / t3.move_time_consuming, 2)        as empty_move_speed
from phoenix_rss.transport_order t
         left join phoenix_rss.transport_order_carrier t1 on t1.id = t.id
         left join (select t.order_no,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as init_job_time_consuming
                    from (select t1.order_no,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_no,
                                       t.id,
                                       t.create_time
                                from phoenix_rss.transport_order_link t
                                         inner join phoenix_rss.transport_order t1
                                                    on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.execute_state = 'INIT_JOB') t1
                                   left join
                               (select t.order_no,
                                       t.id,
                                       t.create_time
                                from phoenix_rss.transport_order_link t
                                         inner join phoenix_rss.transport_order t1
                                                    on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.execute_state = 'WAITING_ROBOT') t2
                               on t2.order_no = t1.order_no and t2.create_time < t1.create_time
                          group by t1.order_no, t1.id, t1.create_time) t
                    group by t.order_no) t2 on t2.order_no = t.order_no
         left join (select t.order_no,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as move_time_consuming
                    from (select t1.order_no,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_no,
                                       t.id,
                                       t.create_time
                                from phoenix_rss.transport_order_link t
                                         inner join phoenix_rss.transport_order t1
                                                    on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.execute_state = 'MOVE_DONE') t1
                                   left join
                               (select t.order_no,
                                       t.id,
                                       t.create_time
                                from phoenix_rss.transport_order_link t
                                         inner join phoenix_rss.transport_order t1
                                                    on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.execute_state = 'MOVE_START') t2
                               on t2.order_no = t1.order_no and t2.create_time < t1.create_time
                          group by t1.order_no, t1.id, t1.create_time) t
                    group by t.order_no) t3 on t3.order_no = t.order_no
         left join (select t.order_no,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as lift_up_time_consuming
                    from (select t1.order_no,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_no,
                                       t.id,
                                       t.create_time
                                from phoenix_rss.transport_order_link t
                                         inner join phoenix_rss.transport_order t1
                                                    on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.execute_state = 'LIFT_UP_DONE') t1
                                   left join
                               (select t.order_no,
                                       t.id,
                                       t.create_time
                                from phoenix_rss.transport_order_link t
                                         inner join phoenix_rss.transport_order t1
                                                    on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.execute_state = 'LIFT_UP_START') t2
                               on t2.order_no = t1.order_no and t2.create_time < t1.create_time
                          group by t1.order_no, t1.id, t1.create_time) t
                    group by t.order_no) t4 on t4.order_no = t.order_no
         left join (select t.order_no,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as rack_move_time_consuming
                    from (select t1.order_no,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_no,
                                       t.id,
                                       t.create_time
                                from phoenix_rss.transport_order_link t
                                         inner join phoenix_rss.transport_order t1
                                                    on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.execute_state = 'RACK_MOVE_DONE') t1
                                   left join
                               (select t.order_no,
                                       t.id,
                                       t.create_time
                                from phoenix_rss.transport_order_link t
                                         inner join phoenix_rss.transport_order t1
                                                    on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.execute_state = 'RACK_MOVE_START') t2
                               on t2.order_no = t1.order_no and t2.create_time < t1.create_time
                          group by t1.order_no, t1.id, t1.create_time) t
                    group by t.order_no) t5 on t5.order_no = t.order_no
         left join (select t.order_no,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as put_down_time_consuming
                    from (select t1.order_no,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_no,
                                       t.id,
                                       t.create_time
                                from phoenix_rss.transport_order_link t
                                         inner join phoenix_rss.transport_order t1
                                                    on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.execute_state = 'PUT_DOWN_DONE') t1
                                   left join
                               (select t.order_no,
                                       t.id,
                                       t.create_time
                                from phoenix_rss.transport_order_link t
                                         inner join phoenix_rss.transport_order t1
                                                    on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.execute_state = 'PUT_DOWN_START') t2
                               on t2.order_no = t1.order_no and t2.create_time < t1.create_time
                          group by t1.order_no, t1.id, t1.create_time) t
                    group by t.order_no) t6 on t6.order_no = t.order_no
         left join (select tj.order_no,
                           sum(rasd.rotate_count)                                 as order_rotate_count,
                           sum(rasd.actual_move_distance * 1000)                  as order_actual_move_distance,
                           sum(case
                                   when rasd.action_code = 'MOVE_LIFT_UP' or
                                        (rasd.action_code = 'MOVE' and rasd.is_loading = 0)
                                       then rasd.actual_move_distance * 1000 end) as empty_move_distance,
                           sum(case
                                   when rasd.action_code = 'MOVE_PUT_DOWN' or
                                        (rasd.action_code = 'MOVE' and rasd.is_loading = 1)
                                       then rasd.actual_move_distance * 1000 end) as loading_move_distance,
                           sum(unix_timestamp(terminal_guide_end_time) -
                               unix_timestamp(terminal_guide_start_time))         as guide_time_consuming
                    from phoenix_rss.transport_order_carrier_job tj
                             left join phoenix_rms.rms_action_statistics_data rasd on rasd.job_sn = tj.job_sn
                    where date_format(tj.update_time, '%Y-%m-%d') =
                          date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                    group by tj.order_no) t7 on t7.order_no = t.order_no
where t.order_state = 'COMPLETED'
  and date_format(t.update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
;