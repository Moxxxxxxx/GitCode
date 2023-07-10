搬运作业单分时统计（开发页面）
涉及字段命名：
order_id                 --作业单ID        #示例：SIRack_165053598867200001
order_create_time        --作业单创建时间  #示例：2022-04-21 18:13:09
order_done_time          --作业单完成时间  #示例：2022-04-21 18:13:56
scene_type               --场景类型        #示例：CARRIER
stat_time                --统计时间        #示例：2022-05-11 01:00:00
order_type               --作业单类型      #示例：CARRIER_SI_RACK_MOVE
robot_code               --机器人编码      #示例：CARRIER_001
start_point              --起始点          #示例：irCa67
target_point             --目标点          #示例：8iHRHi
total_time_consuming     --总耗时（秒）    #示例：47
init_job_time_consuming  --分车耗时（秒）   #示例：10
move_time_consuming      --空车移动耗时（秒）#示例： 15
lift_up_time_consuming   --顶升耗时（秒）    #示例：2
rack_move_time_consuming --带载移动耗时（秒） #示例：8
put_down_time_consuming  --放下耗时（秒）   #示例：5
guide_time_consuming     --末端引导耗时（秒）   #示例：5   备注：本期不做
move_abnormal_time_consuming     --移动异常时长（秒）   #示例：5   备注：本期不做
move_distance              --实际行驶距离（米） #示例：100   备注：本期不做
move_speed       --实际行驶速度（米/分钟）  #示例：16.55  备注：本期不做
rotation_num --机器人旋转次数    #示例：5     备注：本期不做
rotation_time_consuming --旋转耗时（秒） #示例：5   备注：本期不做

--------------------------------------------------------------------------------------------------------------

##step1:建表（qt_transport_order_stat_detail）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_transport_order_stat_detail
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
    `move_distance`                int(100)              DEFAULT NULL COMMENT '移动距离(米)',
    `move_speed`                   decimal(10, 2)        DEFAULT NULL COMMENT '实际行驶速度(米/分钟)',
    `rotation_num`                 int(100)              DEFAULT NULL COMMENT '机器人旋转次数',
    `rotation_time_consuming`      decimal(10, 3)        DEFAULT NULL COMMENT '旋转耗时(秒)',
    `created_time`                 timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`                 timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='作业单统计信息明细';	
	
	
--------------------------------------------------------------------------------------------------------------
	
##step2:删除当天相关数据（qt_transport_order_stat_detail）
DELETE
FROM qt_smartreport.qt_transport_order_stat_detail
WHERE date_format(stat_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  	

--------------------------------------------------------------------------------------------------------------
##step3:插入当天相关数据（qt_transport_order_stat_detail）
insert into qt_smartreport.qt_transport_order_stat_detail(order_id, order_create_time, order_done_time, scene_type,
                                                          stat_time, order_type, robot_code, start_point, target_point,
                                                          total_time_consuming, init_job_time_consuming,
                                                          move_time_consuming, lift_up_time_consuming,
                                                          rack_move_time_consuming, put_down_time_consuming,
                                                          guide_time_consuming, move_abnormal_time_consuming,
                                                          move_distance, move_speed, rotation_num,
                                                          rotation_time_consuming)
select t.order_id,
       t.create_time                                                 as order_create_time,
       t.update_time                                                 as order_done_time,
       substring(t.order_type, 1, instr(t.order_type, '_') - 1)      as scene_type,
       date_format(t.update_time, '%Y-%m-%d %H:00:00')               as stat_time,
       t.order_type,
       t.dispatch_robot_code                                         as robot_code,
       coalesce(t1.start_point, 'unknow')                            as start_point,
       coalesce(t1.target_point, 'unknow')                           as target_point,
       unix_timestamp(t.update_time) - unix_timestamp(t.create_time) as total_time_consuming,
       coalesce(t2.init_job_time_consuming, 0)                       as init_job_time_consuming,
       coalesce(t3.move_time_consuming, 0)                           as move_time_consuming,
       coalesce(t4.lift_up_time_consuming, 0)                        as lift_up_time_consuming,
       coalesce(t5.rack_move_time_consuming, 0)                      as rack_move_time_consuming,
       coalesce(t6.put_down_time_consuming, 0)                       as put_down_time_consuming,
       null                                                          as guide_time_consuming,
       null                                                          as move_abnormal_time_consuming,
       null                                                          as move_distance,
       null                                                          as move_speed,
       null                                                          as rotation_num,
       null                                                          as rotation_time_consuming
from phoenix_rms.transport_order t
         left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id
         left join (select t.order_id,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as init_job_time_consuming
                    from (select t1.order_id,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.state = 'INIT_JOB') t1
                                   left join
                               (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.state = 'WAITING_ROBOT') t2
                               on t2.order_id = t1.order_id and t2.create_time < t1.create_time
                          group by t1.order_id, t1.id, t1.create_time) t
                    group by t.order_id) t2 on t2.order_id = t.order_id
         left join (select t.order_id,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as move_time_consuming
                    from (select t1.order_id,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.state = 'MOVE_DONE') t1
                                   left join
                               (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.state = 'MOVE_START') t2
                               on t2.order_id = t1.order_id and t2.create_time < t1.create_time
                          group by t1.order_id, t1.id, t1.create_time) t
                    group by t.order_id) t3 on t3.order_id = t.order_id
         left join (select t.order_id,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as lift_up_time_consuming
                    from (select t1.order_id,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.state = 'LIFT_UP_DONE') t1
                                   left join
                               (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.state = 'LIFT_UP_START') t2
                               on t2.order_id = t1.order_id and t2.create_time < t1.create_time
                          group by t1.order_id, t1.id, t1.create_time) t
                    group by t.order_id) t4 on t4.order_id = t.order_id
         left join (select t.order_id,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as rack_move_time_consuming
                    from (select t1.order_id,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.state = 'RACK_MOVE_DONE') t1
                                   left join
                               (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.state = 'RACK_MOVE_START') t2
                               on t2.order_id = t1.order_id and t2.create_time < t1.create_time
                          group by t1.order_id, t1.id, t1.create_time) t
                    group by t.order_id) t5 on t5.order_id = t.order_id
         left join (select t.order_id,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as put_down_time_consuming
                    from (select t1.order_id,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.state = 'PUT_DOWN_DONE') t1
                                   left join
                               (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
                                where t.state = 'PUT_DOWN_START') t2
                               on t2.order_id = t1.order_id and t2.create_time < t1.create_time
                          group by t1.order_id, t1.id, t1.create_time) t
                    group by t.order_id) t6 on t6.order_id = t.order_id
where t.state = 'DONE'
  and date_format(t.update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
;


------------------------------------------------------------------------------------------------------------
####qt_smartreport.qt_transport_order_stat_detail 当天逻辑

select t.order_id,
       t.qt_smartreport.create_time                                                 as order_create_time,
       t.update_time                                                 as order_done_time,
       substring(t.order_type, 1, instr(t.order_type, '_') - 1)      as scene_type,
       date_format(t.update_time, '%Y-%m-%d %H:00:00')               as stat_time,
       t.order_type,
       t.dispatch_robot_code                                         as robot_code,
       coalesce(t1.start_point, 'unknow')                            as start_point,
       coalesce(t1.target_point, 'unknow')                           as target_point,
       unix_timestamp(t.update_time) - unix_timestamp(t.create_time) as total_time_consuming,
       coalesce(t2.init_job_time_consuming, 0)                       as init_job_time_consuming,
       coalesce(t3.move_time_consuming, 0)                           as move_time_consuming,
       coalesce(t4.lift_up_time_consuming, 0)                        as lift_up_time_consuming,
       coalesce(t5.rack_move_time_consuming, 0)                      as rack_move_time_consuming,
       coalesce(t6.put_down_time_consuming, 0)                       as put_down_time_consuming,
       null                                                          as guide_time_consuming,
       null                                                          as move_abnormal_time_consuming,
       null                                                          as move_distance,
       null                                                          as move_speed,
       null                                                          as rotation_num,
       null                                                          as rotation_time_consuming
from phoenix_rms.transport_order t
         left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id
         left join (select t.order_id,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as init_job_time_consuming
                    from (select t1.order_id,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'INIT_JOB') t1
                                   left join
                               (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'WAITING_ROBOT') t2
                               on t2.order_id = t1.order_id and t2.create_time < t1.create_time
                          group by t1.order_id, t1.id, t1.create_time) t
                    group by t.order_id) t2 on t2.order_id = t.order_id
         left join (select t.order_id,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as move_time_consuming
                    from (select t1.order_id,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'MOVE_DONE') t1
                                   left join
                               (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'MOVE_START') t2
                               on t2.order_id = t1.order_id and t2.create_time < t1.create_time
                          group by t1.order_id, t1.id, t1.create_time) t
                    group by t.order_id) t3 on t3.order_id = t.order_id
         left join (select t.order_id,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as lift_up_time_consuming
                    from (select t1.order_id,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'LIFT_UP_DONE') t1
                                   left join
                               (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'LIFT_UP_START') t2
                               on t2.order_id = t1.order_id and t2.create_time < t1.create_time
                          group by t1.order_id, t1.id, t1.create_time) t
                    group by t.order_id) t4 on t4.order_id = t.order_id
         left join (select t.order_id,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as rack_move_time_consuming
                    from (select t1.order_id,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'RACK_MOVE_DONE') t1
                                   left join
                               (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'RACK_MOVE_START') t2
                               on t2.order_id = t1.order_id and t2.create_time < t1.create_time
                          group by t1.order_id, t1.id, t1.create_time) t
                    group by t.order_id) t5 on t5.order_id = t.order_id
         left join (select t.order_id,
                           sum(unix_timestamp(end_time) - unix_timestamp(start_time)) as put_down_time_consuming
                    from (select t1.order_id,
                                 t1.id               as init_job_id,
                                 t1.create_time      as end_time,
                                 max(t2.create_time) as start_time
                          from (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'PUT_DOWN_DONE') t1
                                   left join
                               (select t.order_id,
                                       t.id,
                                       t.create_time
                                from phoenix_rms.transport_order_link t
                                         inner join phoenix_rms.transport_order t1
                                                    on t1.order_id = t.order_id and t1.state = 'DONE'
                                                        and date_format(t1.update_time, '%Y-%m-%d') =
                                                            date_format(sysdate(), '%Y-%m-%d')
                                where t.state = 'PUT_DOWN_START') t2
                               on t2.order_id = t1.order_id and t2.create_time < t1.create_time
                          group by t1.order_id, t1.id, t1.create_time) t
                    group by t.order_id) t6 on t6.order_id = t.order_id
where t.state = 'DONE'
  and date_format(t.update_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
;


---------------------------------------------------------------------------------------------
##step4:建表（qt_transport_order_create_stat_detail）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_transport_order_create_stat_detail
(
    `id`                           int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `order_id`                     varchar(100) NOT NULL COMMENT '作业单ID',
    `order_create_time`            datetime(6)           DEFAULT NULL COMMENT '作业单创建时间',
    `scene_type`                   varchar(100)          DEFAULT NULL COMMENT '场景类型',
    `stat_time`                    datetime     NOT NULL COMMENT '统计时间',
    `order_type`                   varchar(100)          DEFAULT NULL COMMENT '作业单类型',
    `robot_code`                   varchar(100)          DEFAULT NULL COMMENT '机器人编码',
    `start_point`                  varchar(100)          DEFAULT NULL COMMENT '起始点',
    `target_point`                 varchar(100)          DEFAULT NULL COMMENT '目标点',
    `created_time`                 timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`                 timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='作业单下发创建统计信息明细';	
	
	
--------------------------------------------------------------------------------------------------------------
	
##step5:删除当天相关数据（qt_transport_order_create_stat_detail）
DELETE
FROM qt_smartreport.qt_transport_order_create_stat_detail
WHERE date_format(stat_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d');  	


----------------------------------------------------------------------------------
##step6:插入当天相关数据（qt_transport_order_create_stat_detail）
insert into qt_smartreport.qt_transport_order_create_stat_detail(order_id, order_create_time,scene_type,stat_time, order_type, robot_code, start_point, target_point)													 													  
select t.order_id,
       t.create_time                                                 as order_create_time,
       substring(t.order_type, 1, instr(t.order_type, '_') - 1)      as scene_type,
       date_format(t.create_time, '%Y-%m-%d %H:00:00')               as stat_time,
       t.order_type,
       t.dispatch_robot_code                                         as robot_code,
       coalesce(t1.start_point, 'unknow')                            as start_point,
       coalesce(t1.target_point, 'unknow')                           as target_point	   
from phoenix_rms.transport_order t
         left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id		 
where  date_format(t.create_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
;

---------------------------------------------------------------------------------------------------------------------------
####qt_smartreport.qt_transport_order_create_stat_detail 当天逻辑
select t.order_id,
       t.create_time                                                 as order_create_time,
       substring(t.order_type, 1, instr(t.order_type, '_') - 1)      as scene_type,
       date_format(t.create_time, '%Y-%m-%d %H:00:00')               as stat_time,
       t.order_type,
       t.dispatch_robot_code                                         as robot_code,
       coalesce(t1.start_point, 'unknow')                            as start_point,
       coalesce(t1.target_point, 'unknow')                           as target_point	   
from phoenix_rms.transport_order t
         left join phoenix_rss.rss_carrier_order t1 on t1.order_id = t.order_id		 
where  date_format(t.create_time, '%Y-%m-%d') = date_format(sysdate(), '%Y-%m-%d')
;

-------------------------------------------------------------------------------------------------------------
