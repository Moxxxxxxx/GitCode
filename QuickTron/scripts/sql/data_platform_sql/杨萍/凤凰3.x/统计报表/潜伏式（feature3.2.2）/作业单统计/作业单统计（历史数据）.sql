##step1:建表（qt_transport_order_next_step1）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_transport_order_next_step1
(
    id         bigint(20),
    order_no   varchar(32),
    current_id varchar(32),
    next_id    varchar(32),
    PRIMARY KEY (`id`),
    key idx_current_id (current_id),
    key idx_next_id (next_id)
);




##step2:删除相关数据（qt_transport_order_next_step1）
DELETE FROM qt_smartreport.qt_transport_order_next_step1;



##step3:插入相关数据（qt_transport_order_next_step1）
insert into qt_smartreport.qt_transport_order_next_step1(id,order_no,current_id,next_id)
SELECT id,
       order_no,
       CONCAT(order_no, '-', @rn := @rn + 1) current_id,
       CONCAT(order_no, '-', @rn + 1)        next_id
from phoenix_rss.transport_order_link,
     (SELECT @rn := 0) tmp
WHERE id >= (SELECT id
                 from phoenix_rss.transport_order_link
                 WHERE create_time >= date_add(date(SYSDATE()) , interval -10 day)
                 LIMIT 1)
ORDER BY order_no, id;




##step4:建表（qt_transport_order_next_step2）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_transport_order_next_step2
(
    id         bigint(20),
    order_no varchar(32),
    next_id    varchar(32),
    PRIMARY KEY (`id`),
    key idx_robot_code (order_no),
    key idx_next_id (next_id)
);



##step5:删除相关数据（qt_transport_order_next_step2）
DELETE FROM qt_smartreport.qt_transport_order_next_step2;



##step6:插入相关数据（qt_transport_order_next_step2）
insert into qt_smartreport.qt_transport_order_next_step2(id,order_no,next_id)
SELECT t1.id, t1.order_no, t2.id as next_id
from qt_smartreport.qt_transport_order_next_step1 t1
         LEFT JOIN qt_smartreport.qt_transport_order_next_step1 t2
                   ON t1.next_id = t2.current_id;




##step7:建表（qt_transport_order_link_detail_stat）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_transport_order_link_detail_stat
(
    `id`                          int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`                  date         NOT NULL COMMENT '日期',
    `upstream_order_no`           varchar(100) NOT NULL COMMENT '上游作业单ID',
    `order_no`                    varchar(100) NOT NULL COMMENT '搬运作业单ID',
    `link_create_time`            datetime(6)           DEFAULT NULL COMMENT '节点创建时间',
    `execute_state`               varchar(100)          DEFAULT NULL COMMENT '节点的执行状态',
    `order_state`                 varchar(100)          DEFAULT NULL COMMENT '搬运作业单状态',
    `robot_code`                  varchar(100)          DEFAULT NULL COMMENT '机器人编码',
    `first_classification` varchar(1000)         DEFAULT NULL COMMENT '机器人一级分类',	
    `robot_type_name` varchar(1000)         DEFAULT NULL COMMENT '机器人类型名称',		
    `next_link_create_time`       datetime(6)           DEFAULT NULL COMMENT '下一个节点的创建时间',
    `next_execute_state`          varchar(100)          DEFAULT NULL COMMENT '下一个节点的执行状态',
    `next_order_state`            varchar(100)          DEFAULT NULL COMMENT '下一个节点搬运作业单的状态',
    `to_next_link_time_consuming` decimal(10, 3)        DEFAULT NULL COMMENT '此节点到下一个节点的耗时（秒）',
    `created_time`                timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`                timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_upstream_order_no (`upstream_order_no`),
    key idx_order_no (`order_no`),
    key idx_execute_state (`execute_state`),
    key idx_create_time (`link_create_time`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='搬运作业单链路表';	
	
	
	
--------------------------------------------------------------------------------------------------------------
	
##step8:删除当天相关数据（qt_transport_order_link_detail_stat）
DELETE
FROM qt_smartreport.qt_transport_order_link_detail_stat
WHERE date_value = date_add(current_date(), interval -1 day);  	



--------------------------------------------------------------------------------------------------------------
##step9:插入当天相关数据（qt_transport_order_link_detail_stat）
insert into qt_smartreport.qt_transport_order_link_detail_stat(date_value, upstream_order_no, order_no,
                                                               link_create_time, execute_state, order_state,
                                                               robot_code, first_classification, robot_type_name,next_link_create_time,
                                                               next_execute_state,
                                                               next_order_state, to_next_link_time_consuming)
select date_add(current_date(), interval -1 day)                       as date_value,
       t.upstream_order_no,
       t.order_no,
       t1.create_time                                                  as link_create_time,
       t1.execute_state,
       t1.order_state,
       t1.robot_code,
       brt.first_classification,
	   brt.robot_type_name,
       t2.create_time                                                  as next_link_create_time,
       t2.execute_state                                                as next_execute_state,
       t2.order_state                                                  as next_order_state,
       unix_timestamp(t2.create_time) - unix_timestamp(t1.create_time) as to_next_link_time_consuming
from (select distinct upstream_order_no, order_no
      from phoenix_rss.transport_order
      where update_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')
        and update_time <= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 23:59:59.999999999')) t
         left join phoenix_rss.transport_order_link t1 on t1.order_no = t.order_no
         left join qt_smartreport.qt_transport_order_next_step2 tm on tm.order_no = t1.order_no and tm.id = t1.id
         left join phoenix_rss.transport_order_link t2 on t2.order_no = tm.order_no and t2.id = tm.next_id
         left join phoenix_basic.basic_robot br on br.robot_code = t1.robot_code
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
;




	
##step10:建表（qt_transport_order_detail_stat）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_transport_order_detail_stat
(
    `id`                                int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`                        date         NOT NULL COMMENT '日期',
    `upstream_order_no`                 varchar(100) NOT NULL COMMENT '上游作业单ID',
    `order_no`                          varchar(100) NOT NULL COMMENT '搬运作业单ID',
    `scene_type`                        varchar(100)          DEFAULT NULL COMMENT '场景类型',	
    `start_point`                       varchar(100)          DEFAULT NULL COMMENT '起始点',
    `start_area`                        varchar(100)          DEFAULT NULL COMMENT '起始区域',
    `target_point`                      varchar(100)          DEFAULT NULL COMMENT '目标点',
    `target_area`                       varchar(100)          DEFAULT NULL COMMENT '目标区域',
    `order_state`                       varchar(100)          DEFAULT NULL COMMENT '搬运作业单状态',
    `dispatch_robot_code_num`           int(100)              DEFAULT NULL COMMENT '分配机器人数量',
    `dispatch_robot_code_str`           varchar(1000)         DEFAULT NULL COMMENT '分配机器人',
    `dispatch_robot_classification_str` varchar(1000)         DEFAULT NULL COMMENT '机器人类型',
    `total_time_consuming`              decimal(20, 6)        DEFAULT NULL COMMENT '总耗时(秒)',
    `empty_move_distance`               decimal(65, 20)        DEFAULT NULL COMMENT '空车移动距离(米)',
    `empty_move_speed`                  decimal(65, 20)       DEFAULT NULL COMMENT '空车移动速度(米/秒)',
    `loading_move_distance`             decimal(65, 20)        DEFAULT NULL COMMENT '带载移动距离(米)',
    `loading_move_speed`                decimal(65, 20)       DEFAULT NULL COMMENT '带载移动速度(米/秒)',
    `waiting_robot_time_consuming`      decimal(20, 6)        DEFAULT NULL COMMENT '分车耗时(秒)',
    `move_time_consuming`               decimal(20, 6)        DEFAULT NULL COMMENT '空车移动耗时(秒)',
    `lift_up_time_consuming`            decimal(20, 6)        DEFAULT NULL COMMENT '顶升耗时(秒)',
    `rack_move_time_consuming`          decimal(20, 6)        DEFAULT NULL COMMENT '带载移动耗时(秒)',
    `put_down_time_consuming`           decimal(20, 6)        DEFAULT NULL COMMENT '放下耗时(秒)',
    `guide_time_consuming`              decimal(20, 6)        DEFAULT NULL COMMENT '末端引导耗时(秒)',
    `robot_rotate_num`                  int(100)              DEFAULT NULL COMMENT '机器人旋转次数',
    `order_create_time`                 datetime(6)           DEFAULT NULL COMMENT '作业单创建时间',
    `order_completed_time`              datetime(6)           DEFAULT NULL COMMENT '作业单完成时间',
    `created_time`                      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`                      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_upstream_order_no (`upstream_order_no`),
    key idx_order_no (`order_no`),
	key idx_scene_type (`scene_type`),
    key idx_start_point (`start_point`),
    key idx_start_area (`start_area`),
    key idx_target_point (`target_point`),
    key idx_target_area (`target_area`),
    key idx_order_create_time (`order_create_time`)

)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='搬运作业单统计明细';	



	
--------------------------------------------------------------------------------------------------------------
	
##step11:删除当天相关数据（qt_transport_order_detail_stat）
DELETE
FROM qt_smartreport.qt_transport_order_detail_stat
WHERE date_value = date_add(current_date(), interval -1 day);  	



	
--------------------------------------------------------------------------------------------------------------
##step12:插入当天相关数据（qt_transport_order_detail_stat）
insert into qt_smartreport.qt_transport_order_detail_stat(date_value, upstream_order_no, order_no,scene_type, start_point,
                                                          start_area,
                                                          target_point, target_area, order_state,
                                                          dispatch_robot_code_num, dispatch_robot_code_str,
                                                          dispatch_robot_classification_str, total_time_consuming,
                                                          empty_move_distance, empty_move_speed, loading_move_distance,
                                                          loading_move_speed, waiting_robot_time_consuming,
                                                          move_time_consuming, lift_up_time_consuming,
                                                          rack_move_time_consuming, put_down_time_consuming,
                                                          guide_time_consuming, robot_rotate_num, order_create_time,
                                                          order_completed_time)
select date_add(current_date(), interval -1 day)                                            as date_value,
       t.upstream_order_no,
       t.order_no,
	   t.scenario                                                    as scene_type,
       case
           when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code
           else 'unknow' end                                                                   start_point,
       case
           when t.start_area_code <> '' and t.start_area_code is not null then t.start_area_code
           else 'unknow' end                                                                   start_area,
       case
           when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code
           else 'unknow' end                                                                   target_point,
       case
           when t.target_area_code <> '' and t.target_area_code is not null then t.target_area_code
           else 'unknow' end                                                                   target_area,
       case
           when t.order_state = 'WAITING_ROBOT_ASSIGN' then '待分车'
           when t.order_state = 'EXECUTING' then '正在执行'
           when t.order_state = 'COMPLETED' then '已完成'
           when t.order_state = 'CANCELED' then '取消'
           when t.order_state = 'PENDING' then '挂起'
           when t.order_state = 'ABNORMAL_COMPLETED' then '异常完成'
           when t.order_state = 'ABNORMAL_CANCELED' then '异常取消'
           end                                                                              as order_state,
       t1.dispatch_robot_code_num,
       t1.dispatch_robot_code_str,
       t1.dispatch_robot_classification_str,
       unix_timestamp(t.update_time) - unix_timestamp(t.create_time)                        as total_time_consuming,
       t2.empty_move_distance,
       case
           when t1.move_time_consuming is not null
               then coalesce(t2.empty_move_distance, 0) / t1.move_time_consuming end        as empty_move_speed,
       t2.loading_move_distance,
       case
           when t1.rack_move_time_consuming is not null
               then coalesce(t2.loading_move_distance, 0) / t1.rack_move_time_consuming end as loading_move_speed,
       t1.waiting_robot_time_consuming,
       t1.move_time_consuming,
       t1.lift_up_time_consuming,
       t1.rack_move_time_consuming,
       t1.put_down_time_consuming,
       t2.guide_time_consuming,
       t2.robot_rotate_num,
       t.create_time                                                                        as order_create_time,
       case when t.order_state = 'COMPLETED' then t.update_time end                         as order_completed_time
from phoenix_rss.transport_order t
         left join (select tk.order_no,
                           count(distinct tk.robot_code)                             as dispatch_robot_code_num,
                           group_concat(distinct tk.robot_code)                      as dispatch_robot_code_str,
                           group_concat(distinct brt.first_classification)           as dispatch_robot_classification_str,
                           sum(case
                                   when tk.execute_state = 'WAITING_ROBOT'
                                       then unix_timestamp(tk.next_link_create_time) -
                                            unix_timestamp(tk.link_create_time) end) as waiting_robot_time_consuming,
                           sum(case
                                   when tk.execute_state = 'MOVE_START'
                                       then unix_timestamp(tk.next_link_create_time) -
                                            unix_timestamp(tk.link_create_time) end) as move_time_consuming,
                           sum(case
                                   when tk.execute_state = 'LIFT_UP_START'
                                       then unix_timestamp(tk.next_link_create_time) -
                                            unix_timestamp(tk.link_create_time) end) as lift_up_time_consuming,
                           sum(case
                                   when tk.execute_state = 'RACK_MOVE_START'
                                       then unix_timestamp(tk.next_link_create_time) -
                                            unix_timestamp(tk.link_create_time) end) as rack_move_time_consuming,
                           sum(case
                                   when tk.execute_state = 'PUT_DOWN_START'
                                       then unix_timestamp(tk.next_link_create_time) -
                                            unix_timestamp(tk.link_create_time) end) as put_down_time_consuming
                    from qt_smartreport.qt_transport_order_link_detail_stat tk
                             left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
                             left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
                    where date_value = date_add(current_date(), interval -1 day)
                    group by tk.order_no) t1 on t1.order_no = t.order_no
         left join (select t.order_no,
                           sum(rasd.rotate_count)                                 as robot_rotate_num,
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
                    from phoenix_rss.transport_order t
                             left join phoenix_rss.transport_order_carrier_job tj on tj.order_no = t.order_no
                             left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
                    where t.update_time >=
                          date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')
                      and t.update_time <=
                          date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 23:59:59.999999999')

                    group by t.order_no) t2 on t2.order_no = t.order_no
where t.update_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')
  and t.update_time <= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 23:59:59.999999999')
;		  	 	  	  	 	  	  





##step13:建表（qt_transport_upstream_order_detail_stat）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_transport_upstream_order_detail_stat
(
    `id`                                int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`                        date         NOT NULL COMMENT '日期',
    `upstream_order_no`                 varchar(100) NOT NULL COMMENT '上游作业单ID',
    `scene_type`                        varchar(100)          DEFAULT NULL COMMENT '场景类型',
    `stat_time`                         datetime     NOT NULL COMMENT '统计时间',
    `start_point`                       varchar(100)          DEFAULT NULL COMMENT '起始点',
    `start_area`                        varchar(100)          DEFAULT NULL COMMENT '起始区域',
    `target_point`                      varchar(100)          DEFAULT NULL COMMENT '目标点',
    `target_area`                       varchar(100)          DEFAULT NULL COMMENT '目标区域',
    `upstream_order_state`              varchar(100)          DEFAULT NULL COMMENT '上游作业单状态',
    `dispatch_robot_code_num`           int(100)              DEFAULT NULL COMMENT '分配机器人数量',
    `dispatch_robot_code_str`           varchar(1000)         DEFAULT NULL COMMENT '分配机器人',
    `dispatch_robot_classification_str` varchar(1000)         DEFAULT NULL COMMENT '机器人类型',
    `total_time_consuming`              decimal(20, 6)        DEFAULT NULL COMMENT '总耗时(秒)',
    `dispatch_order_no`                 varchar(1000)         DEFAULT NULL COMMENT '搬运作业单',
    `order_no_num`                 int(100)         DEFAULT NULL COMMENT '搬运作业单数',	
    `upstream_order_create_time`        datetime(6)           DEFAULT NULL COMMENT '上游作业单创建时间',
    `upstream_order_completed_time`     datetime(6)           DEFAULT NULL COMMENT '上游作业单完成时间',
    `created_time`                      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`                      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_upstream_order_no (`upstream_order_no`),
    key idx_scene_type (`scene_type`),
    key idx_start_point (`start_point`),
    key idx_start_area (`start_area`),
    key idx_target_point (`target_point`),
    key idx_target_area (`target_area`),
    key idx_upstream_stat_time (`stat_time`),
    key idx_upstream_order_create_time (`upstream_order_create_time`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='上游作业单统计明细';	
	
	
--------------------------------------------------------------------------------------------------------------
	
##step14:删除当天相关数据（qt_transport_upstream_order_detail_stat）
DELETE
FROM qt_smartreport.qt_transport_upstream_order_detail_stat
WHERE date_value = date_add(current_date(), interval -1 day);  		



--------------------------------------------------------------------------------------------------------------
##step15:插入当天相关数据（qt_transport_upstream_order_detail_stat）
insert into qt_smartreport.qt_transport_upstream_order_detail_stat(date_value, upstream_order_no, scene_type, stat_time,
                                                                   start_point, start_area, target_point, target_area,
                                                                   upstream_order_state, dispatch_robot_code_num,
                                                                   dispatch_robot_code_str,
                                                                   dispatch_robot_classification_str,
                                                                   total_time_consuming, dispatch_order_no,
                                                                   order_no_num,
                                                                   upstream_order_create_time,
                                                                   upstream_order_completed_time)
select date_add(current_date(), interval -1 day)                      as date_value,
       t.upstream_order_no,
       t1.scenario                                                    as scene_type,
       date_format(t.upstream_order_create_time, '%Y-%m-%d %H:00:00') as stat_time,
       case
           when t1.start_point_code <> '' and t1.start_point_code is not null then t1.start_point_code
           else 'unknow' end                                             start_point,
       case
           when t1.start_area_code <> '' and t1.start_area_code is not null then t1.start_area_code
           else 'unknow' end                                             start_area,
       case
           when t1.target_point_code <> '' and t1.target_point_code is not null then t1.target_point_code
           else 'unknow' end                                             target_point,
       case
           when t1.target_area_code <> '' and t1.target_area_code is not null then t1.target_area_code
           else 'unknow' end                                             target_area,
       case
           when t1.order_state = 'WAITING_ROBOT_ASSIGN' then '待分车'
           when t1.order_state = 'EXECUTING' then '正在执行'
           when t1.order_state = 'COMPLETED' then '已完成'
           when t1.order_state = 'CANCELED' then '取消'
           when t1.order_state = 'PENDING' then '挂起'
           when t1.order_state = 'ABNORMAL_COMPLETED' then '异常完成'
           when t1.order_state = 'ABNORMAL_CANCELED' then '异常取消'
           end                                                        as upstream_order_state,
       t.dispatch_robot_code_num,
       t.dispatch_robot_code_str,
       t.dispatch_robot_classification_str,
       unix_timestamp(t.upstream_order_completed_time) -
       unix_timestamp(t.upstream_order_create_time)                   as total_time_consuming,
       t.dispatch_order_no,
       t.order_no_num,
       t.upstream_order_create_time,
       t.upstream_order_completed_time
from (select tu.upstream_order_no,
             min(t.create_time)                                                as upstream_order_create_time,
             max(case when t.order_state = 'COMPLETED' then t.update_time end) as upstream_order_completed_time,
             count(distinct tk.robot_code)                                     as dispatch_robot_code_num,
             group_concat(distinct tk.robot_code)                              as dispatch_robot_code_str,
             group_concat(distinct brt.first_classification)                   as dispatch_robot_classification_str,
             count(distinct t.order_no)                                        as order_no_num,
             group_concat(distinct t.order_no)                                 as dispatch_order_no,
             max(t.id)                                                         as latest_id
      from (select distinct upstream_order_no
            from phoenix_rss.transport_order
            where update_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')
              and update_time <=
                  date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 23:59:59.999999999')) tu
               left join phoenix_rss.transport_order t on tu.upstream_order_no = t.upstream_order_no
               left join phoenix_rss.transport_order_link tk on tu.upstream_order_no = tk.upstream_order_no
               left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
               left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
      group by tu.upstream_order_no) t
         left join phoenix_rss.transport_order t1 on t1.upstream_order_no = t.upstream_order_no and t.latest_id = t1.id
;		 