##########################################################################################
-- 搬运作业单链路
set @now_start_time = date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');
set @next_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
select @now_start_time, @next_start_time;


select 
date(tc.update_time)                    as date_value,
tol.id as link_id,
tol.upstream_order_no,
tol.order_no,
tol.create_time as link_create_time,
tol.event_time,
tol.execute_state,
tol.order_state,
tol.robot_code,
brt.first_classification,
brt.robot_type_name,
tol.cost_time/1000 as cost_time 
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_link tol on tol.order_no = tc.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tol.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tc.update_time >=@now_start_time and tc.update_time < @next_start_time
order by tol.order_no,tol.id asc




##########################################################################################
-- 搬运作业单
set @now_start_time = date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');
set @next_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
select @now_start_time, @next_start_time;


select 
date(@now_start_time) as date_value,
@now_start_time as hour_start_time,
@next_start_time as next_hour_start_time,
t.upstream_order_no,
t.order_no,
t.scenario  as scene_type,
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
		          tr.dispatch_robot_code_num,
       tr.dispatch_robot_code_str,
       tr.dispatch_robot_classification_str,
       COALESCE(tc.total_cost,0)/1000 as total_time_consuming,
       coalesce(tj.empty_move_distance, 0) as empty_move_distance,
       case when COALESCE(tc.move_cost,0)!=0 then coalesce(tj.empty_move_distance, 0)/COALESCE(tc.move_cost,0) else null end as empty_move_speed,
       COALESCE(tj.loading_move_distance,0) as loading_move_distance,
       case when COALESCE(tc.rack_move_cost,0)!=0 then COALESCE(tj.loading_move_distance,0)/COALESCE(tc.rack_move_cost,0) else null end as loading_move_speed,
       COALESCE(tc.assign_cost,0)/1000 as waiting_robot_time_consuming,
       COALESCE(tc.move_cost,0)/1000 as move_time_consuming,
       COALESCE(tc.lift_cost,0)/1000 as lift_up_time_consuming,
       COALESCE(tc.rack_move_cost,0)/1000 as rack_move_time_consuming,
       COALESCE(tc.put_cost,0)/1000 as put_down_time_consuming,
       COALESCE(tj.guide_time_consuming,0)  as guide_time_consuming,
       COALESCE(tj.robot_rotate_num,0)  as robot_rotate_num,
       t.create_time as order_create_time,
       tc.order_update_time as order_completed_time
           
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no 
left join 
(select tc.order_no,
                           count(distinct tk.robot_code)                   as dispatch_robot_code_num,
                           group_concat(distinct tk.robot_code)            as dispatch_robot_code_str,
                           group_concat(distinct brt.first_classification) as dispatch_robot_classification_str
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_link tk on tk.order_no = tc.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tc.update_time >=@now_start_time and tc.update_time < @next_start_time
group by tc.order_no)tr on tr.order_no = t.order_no
left join 
(select t.order_no,
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
                           COALESCE (sum(unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time)),0)         as guide_time_consuming 
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no 
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
where tc.update_time >=@now_start_time and tc.update_time < @next_start_time
group by t.order_no)tj on tj.order_no = t.order_no 
where tc.update_time >=@now_start_time and tc.update_time < @next_start_time

##########################################################################################

-- 上游作业单
-- 上游作业单
set @now_start_time = date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');
set @next_start_time = date_format(sysdate(), '%Y-%m-%d %H:00:00');
select @now_start_time, @next_start_time;

select 
date(@now_start_time) as date_value,
@now_start_time as hour_start_time,
@next_start_time as next_hour_start_time,
t.upstream_order_no,
tr.scenario  as scene_type,
case
    when tr.start_point_code <> '' and tr.start_point_code is not null then tr.start_point_code
    else 'unknow' end                                                                   start_point,
case
    when tr.start_area_code <> '' and tr.start_area_code is not null then tr.start_area_code
    else 'unknow' end                                                                   start_area,
case
    when tr.target_point_code <> '' and tr.target_point_code is not null then tr.target_point_code
    else 'unknow' end                                                                   target_point,
case
    when tr.target_area_code <> '' and tr.target_area_code is not null then tr.target_area_code
    else 'unknow' end                                                                   target_area,
case
    when tr.order_state = 'WAITING_ROBOT_ASSIGN' then '待分车'
    when tr.order_state = 'EXECUTING' then '正在执行'
    when tr.order_state = 'COMPLETED' then '已完成'
    when tr.order_state = 'CANCELED' then '取消'
    when tr.order_state = 'PENDING' then '挂起'
    when tr.order_state = 'ABNORMAL_COMPLETED' then '异常完成'
    when tr.order_state = 'ABNORMAL_CANCELED' then '异常取消'
    end                                                                              as upstream_order_state,
t.dispatch_robot_code_num,
t.dispatch_robot_code_str,
t.dispatch_robot_classification_str,
tsc.total_time_consuming,
coalesce(tj.empty_move_distance, 0) as empty_move_distance,
case when tsc.move_time_consuming !=0 then coalesce(tj.empty_move_distance, 0)/tsc.move_time_consuming else null end as empty_move_speed,	   
coalesce(tj.empty_move_distance, 0) as empty_move_distance,
case when tsc.rack_move_time_consuming !=0 then COALESCE(tj.loading_move_distance,0)/tsc.rack_move_time_consuming else null end as loading_move_speed,
tsc.waiting_robot_time_consuming,
tsc.move_time_consuming,
tsc.lift_up_time_consuming,
tsc.rack_move_time_consuming,
tsc.put_down_time_consuming,
COALESCE(tj.guide_time_consuming,0)  as guide_time_consuming,
COALESCE(tj.robot_rotate_num,0)  as robot_rotate_num,
t.dispatch_order_num,
t.order_no_num,
t.upstream_order_create_time,
t.upstream_order_completed_time
from 
(select 
tc.upstream_order_no,
min(t.create_time)                                                as upstream_order_create_time,
max(case when t.order_state = 'COMPLETED' then tc.order_update_time end) as upstream_order_completed_time,
max(tc.order_update_time)                                                as upstream_order_update_time,
count(distinct tk.robot_code)                                     as dispatch_robot_code_num,
group_concat(distinct tk.robot_code)                              as dispatch_robot_code_str,
group_concat(distinct brt.first_classification)                   as dispatch_robot_classification_str,
count(distinct t.order_no)                                        as order_no_num,
group_concat(distinct t.order_no)                                 as dispatch_order_num,
max(t.id)      as latest_id
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no =tc.order_no 
left join phoenix_rss.transport_order_link tk on t.order_no = tk.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tc.update_time >=@now_start_time and tc.update_time < @next_start_time
group by tc.upstream_order_no)t 
left join 
(select 
upstream_order_no,
COALESCE(sum(total_cost),0)/1000 as total_time_consuming,
COALESCE(sum(assign_cost),0)/1000 as waiting_robot_time_consuming,
COALESCE(sum(move_cost),0)/1000 as move_time_consuming,
COALESCE(sum(lift_cost),0)/1000 as lift_up_time_consuming,
COALESCE(sum(rack_move_cost),0)/1000 as rack_move_time_consuming,
COALESCE(sum(put_cost),0)/1000 as put_down_time_consuming
from phoenix_rss.transport_order_carrier_cost
where update_time >=@now_start_time and update_time < @next_start_time
group by upstream_order_no)tsc  on tsc.upstream_order_no = t.upstream_order_no
 left join phoenix_rss.transport_order tr on tr.upstream_order_no = t.upstream_order_no and t.latest_id = tr.id
left join 
(select tc.upstream_order_no,
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
                           COALESCE (sum(unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time)),0)         as guide_time_consuming 
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no 
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
where tc.update_time >=@now_start_time and tc.update_time < @next_start_time
group by tc.upstream_order_no)tj on tj.upstream_order_no = t.upstream_order_no

##########################################################################################
##########################################################################################
##########################################################################################
##########################################################################################

# step1:建表（qt_hour_transport_order_link_detail_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_hour_transport_order_link_detail_his
(
    `id`                          int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`                  date      NOT NULL COMMENT '日期',
    `hour_start_time`             datetime  NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time`        datetime  NOT NULL COMMENT '下一个小时开始时间',
    `link_id`                      bigint(20)        DEFAULT NULL COMMENT '链路表ID',	
    `upstream_order_no`           varchar(255)       DEFAULT NULL COMMENT '上游作业单ID',
    `order_no`                    varchar(255)       DEFAULT NULL COMMENT '搬运作业单ID',
    `link_create_time`            datetime(6)        DEFAULT NULL COMMENT '节点创建时间',
    `event_time`                  datetime(6)        DEFAULT NULL COMMENT '节点事件时间',	
    `execute_state`               varchar(255)       DEFAULT NULL COMMENT '节点的执行状态',
    `order_state`                 varchar(255)       DEFAULT NULL COMMENT '搬运作业单状态',
    `robot_code`                  varchar(255)       DEFAULT NULL COMMENT '机器人编码',
    `first_classification`        varchar(255)       DEFAULT NULL COMMENT '机器人一级分类',
    `robot_type_name`             varchar(255)       DEFAULT NULL COMMENT '机器人类型名称',
    `cost_time`                   decimal(65, 10)    DEFAULT NULL COMMENT '此节点到下一个节点的耗时（秒）',
    `created_time`                timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`                timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_hour_start_time (`hour_start_time`),
    key idx_next_hour_start_time (`next_hour_start_time`),
    key idx_link_id (`link_id`),	
    key idx_upstream_order_no (`upstream_order_no`),
    key idx_order_no (`order_no`),
    key idx_link_create_time (`link_create_time`),	
    key idx_event_time (`event_time`),	
    key idx_execute_state (`execute_state`),
    key idx_robot_code (`robot_code`)	
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='小时内完成的搬运作业单链路表（H+1）';
	
	
	
# step2:删除相关数据（qt_hour_transport_order_link_detail_his）
DELETE
FROM qt_smartreport.qt_hour_transport_order_link_detail_his
where hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');	


# step3:插入相关数据（qt_hour_transport_order_link_detail_his）
insert into qt_smartreport.qt_hour_transport_order_link_detail_his(date_value,hour_start_time,next_hour_start_time,link_id, upstream_order_no, order_no,link_create_time,event_time, execute_state, order_state,robot_code, first_classification,robot_type_name,cost_time)
select 
date(DATE_ADD(sysdate(), INTERVAL -1 HOUR)) as date_value,
date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') as hour_start_time,
date_format(sysdate(), '%Y-%m-%d %H:00:00') as next_hour_start_time,
tol.id as link_id,
tol.upstream_order_no,
tol.order_no,
tol.create_time as link_create_time,
tol.event_time,
tol.execute_state,
tol.order_state,
tol.robot_code,
brt.first_classification,
brt.robot_type_name,
tol.cost_time/1000 as cost_time 
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_link tol on tol.order_no = tc.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tol.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tc.update_time >=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and tc.update_time < date_format(sysdate(), '%Y-%m-%d %H:00:00')
order by tol.order_no,tol.id asc




# step4:建表（qt_hour_transport_order_detail_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_hour_transport_order_detail_his
(
    `id`                                int(20)      NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`                        date         NOT NULL COMMENT '日期',
    `hour_start_time`                   datetime     NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time`              datetime     NOT NULL COMMENT '下一个小时开始时间',
    `upstream_order_no`                 varchar(255) NOT NULL COMMENT '上游作业单ID',
    `order_no`                          varchar(255) NOT NULL COMMENT '搬运作业单ID',
    `scene_type`                        varchar(255)          DEFAULT NULL COMMENT '场景类型',
    `start_point`                       varchar(255)          DEFAULT NULL COMMENT '起始点',
    `start_area`                        varchar(255)          DEFAULT NULL COMMENT '起始区域',
    `target_point`                      varchar(255)          DEFAULT NULL COMMENT '目标点',
    `target_area`                       varchar(255)          DEFAULT NULL COMMENT '目标区域',
    `order_state`                       varchar(255)          DEFAULT NULL COMMENT '搬运作业单状态',
    `dispatch_robot_code_num`           int(100)              DEFAULT NULL COMMENT '分配机器人数量',
    `dispatch_robot_code_str`           varchar(255)          DEFAULT NULL COMMENT '分配机器人',
    `dispatch_robot_classification_str` varchar(255)          DEFAULT NULL COMMENT '机器人类型',
    `total_time_consuming`              decimal(65, 20)       DEFAULT NULL COMMENT '总耗时(秒)',
    `empty_move_distance`               decimal(65, 20)       DEFAULT NULL COMMENT '空车移动距离(米)',
    `empty_move_speed`                  decimal(65, 20)       DEFAULT NULL COMMENT '空车移动速度(米/秒)',
    `loading_move_distance`             decimal(65, 20)       DEFAULT NULL COMMENT '带载移动距离(米)',
    `loading_move_speed`                decimal(65, 20)       DEFAULT NULL COMMENT '带载移动速度(米/秒)',
    `waiting_robot_time_consuming`      decimal(65, 10)       DEFAULT NULL COMMENT '分车耗时(秒)',
    `move_time_consuming`               decimal(65, 20)       DEFAULT NULL COMMENT '空车移动耗时(秒)',
    `lift_up_time_consuming`            decimal(65, 20)       DEFAULT NULL COMMENT '顶升耗时(秒)',
    `rack_move_time_consuming`          decimal(65, 20)       DEFAULT NULL COMMENT '带载移动耗时(秒)',
    `put_down_time_consuming`           decimal(65, 20)       DEFAULT NULL COMMENT '放下耗时(秒)',
    `guide_time_consuming`              decimal(65, 20)       DEFAULT NULL COMMENT '末端引导耗时(秒)',
    `robot_rotate_num`                  int(100)              DEFAULT NULL COMMENT '机器人旋转次数',
    `order_create_time`                 datetime(6)           DEFAULT NULL COMMENT '作业单创建时间',
    `order_completed_time`              datetime(6)           DEFAULT NULL COMMENT '作业单完成时间',
    `created_time`                      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`                      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_hour_start_time (`hour_start_time`),
    key idx_next_hour_start_time (`next_hour_start_time`),
    key idx_upstream_order_no (`upstream_order_no`),
    key idx_order_no (`order_no`),
    key idx_scene_type (`scene_type`),
    key idx_start_point (`start_point`),
    key idx_start_area (`start_area`),
    key idx_target_point (`target_point`),
    key idx_target_area (`target_area`),
    key idx_order_state (`order_state`),
    key idx_order_create_time (`order_create_time`),
    key idx_order_completed_time (`order_completed_time`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='小时内完成的搬运作业单明细（H+1）';	



# step5:删除相关数据（qt_hour_transport_order_detail_his）
DELETE
FROM qt_smartreport.qt_hour_transport_order_detail_his
where hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');	




# step6:插入相关数据（qt_hour_transport_order_detail_his）
insert into qt_smartreport.qt_hour_transport_order_detail_his(date_value,hour_start_time,next_hour_start_time, upstream_order_no, order_no, scene_type,start_point,start_area,target_point, target_area, order_state,dispatch_robot_code_num, dispatch_robot_code_str,dispatch_robot_classification_str, total_time_consuming,empty_move_distance, empty_move_speed,loading_move_distance,loading_move_speed, waiting_robot_time_consuming,move_time_consuming, lift_up_time_consuming,rack_move_time_consuming, put_down_time_consuming,guide_time_consuming, robot_rotate_num, order_create_time,order_completed_time)
select 
date(DATE_ADD(sysdate(), INTERVAL -1 HOUR)) as date_value,
date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') as hour_start_time,
date_format(sysdate(), '%Y-%m-%d %H:00:00') as next_hour_start_time,
t.upstream_order_no,
t.order_no,
t.scenario  as scene_type,
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
		          tr.dispatch_robot_code_num,
       tr.dispatch_robot_code_str,
       tr.dispatch_robot_classification_str,
       nullif(tc.total_cost,0)/1000 as total_time_consuming,
       nullif(tj.empty_move_distance,0) as empty_move_distance,
       case when COALESCE(tc.move_cost,0)!=0 then nullif(tj.empty_move_distance,0)/tc.move_cost else null end as empty_move_speed,
       nullif(tj.loading_move_distance,0) as loading_move_distance,
       case when COALESCE(tc.rack_move_cost,0)!=0 then nullif(tj.loading_move_distance,0)/tc.rack_move_cost else null end as loading_move_speed,
       nullif(tc.assign_cost,0)/1000 as waiting_robot_time_consuming,
       nullif(tc.move_cost,0)/1000 as move_time_consuming,
       nullif(tc.lift_cost,0)/1000 as lift_up_time_consuming,
       nullif(tc.rack_move_cost,0)/1000 as rack_move_time_consuming,
       nullif(tc.put_cost,0)/1000 as put_down_time_consuming,
       nullif(tj.guide_time_consuming,0)  as guide_time_consuming,
       nullif(tj.robot_rotate_num,0)  as robot_rotate_num,
       t.create_time as order_create_time,
       tc.order_update_time as order_completed_time
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no 
left join 
(select tc.order_no,
                           count(distinct tk.robot_code)                   as dispatch_robot_code_num,
                           group_concat(distinct tk.robot_code)            as dispatch_robot_code_str,
                           group_concat(distinct brt.first_classification) as dispatch_robot_classification_str
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_link tk on tk.order_no = tc.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tc.update_time >=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and tc.update_time < date_format(sysdate(), '%Y-%m-%d %H:00:00')
group by tc.order_no)tr on tr.order_no = t.order_no
left join 
(select t.order_no,
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
                           sum(unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time))         as guide_time_consuming 
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no 
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
where tc.update_time >=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and tc.update_time < date_format(sysdate(), '%Y-%m-%d %H:00:00')
group by t.order_no)tj on tj.order_no = t.order_no 
where tc.update_time >=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and tc.update_time < date_format(sysdate(), '%Y-%m-%d %H:00:00')



# step7:建表（qt_hour_transport_upstream_order_detail_his）
CREATE TABLE IF NOT EXISTS qt_smartreport.qt_hour_transport_upstream_order_detail_his
(
    `id`                                int(20)   NOT NULL AUTO_INCREMENT COMMENT '主键',
    `date_value`                        date      NOT NULL COMMENT '日期',
    `hour_start_time`                   datetime  NOT NULL COMMENT '小时开始时间',
    `next_hour_start_time`              datetime  NOT NULL COMMENT '下一个小时开始时间',
    `upstream_order_no`                 varchar(100)       DEFAULT NULL COMMENT '上游作业单ID',
    `scene_type`                        varchar(100)       DEFAULT NULL COMMENT '场景类型',
    `start_point`                       varchar(100)       DEFAULT NULL COMMENT '起始点',
    `start_area`                        varchar(100)       DEFAULT NULL COMMENT '起始区域',
    `target_point`                      varchar(100)       DEFAULT NULL COMMENT '目标点',
    `target_area`                       varchar(100)       DEFAULT NULL COMMENT '目标区域',
    `upstream_order_state`              varchar(100)       DEFAULT NULL COMMENT '上游作业单状态',
    `dispatch_robot_code_num`           int(100)           DEFAULT NULL COMMENT '分配机器人数量',
    `dispatch_robot_code_str`           varchar(1000)      DEFAULT NULL COMMENT '分配机器人',
    `dispatch_robot_classification_str` varchar(1000)      DEFAULT NULL COMMENT '机器人类型',
    `total_time_consuming`              decimal(20, 6)     DEFAULT NULL COMMENT '总耗时(秒)',
    `empty_move_distance`               decimal(65, 20)    DEFAULT NULL COMMENT '空车移动距离(米)',
    `empty_move_speed`                  decimal(65, 20)    DEFAULT NULL COMMENT '空车移动速度(米/秒)',
    `loading_move_distance`             decimal(65, 20)    DEFAULT NULL COMMENT '带载移动距离(米)',
    `loading_move_speed`                decimal(65, 20)    DEFAULT NULL COMMENT '带载移动速度(米/秒)',
    `waiting_robot_time_consuming`      decimal(65, 10)    DEFAULT NULL COMMENT '分车耗时(秒)',
    `move_time_consuming`               decimal(65, 20)    DEFAULT NULL COMMENT '空车移动耗时(秒)',
    `lift_up_time_consuming`            decimal(65, 20)    DEFAULT NULL COMMENT '顶升耗时(秒)',
    `rack_move_time_consuming`          decimal(65, 20)    DEFAULT NULL COMMENT '带载移动耗时(秒)',
    `put_down_time_consuming`           decimal(65, 20)    DEFAULT NULL COMMENT '放下耗时(秒)',
    `guide_time_consuming`              decimal(65, 20)    DEFAULT NULL COMMENT '末端引导耗时(秒)',
    `robot_rotate_num`                  int(100)           DEFAULT NULL COMMENT '机器人旋转次数',
    `dispatch_order_no`                 varchar(1000)      DEFAULT NULL COMMENT '搬运作业单',
    `dispatch_order_num`                int(100)           DEFAULT NULL COMMENT '搬运作业单数',
    `upstream_order_create_time`        datetime(6)        DEFAULT NULL COMMENT '上游作业单创建时间',
    `upstream_order_completed_time`     datetime(6)        DEFAULT NULL COMMENT '上游作业单完成时间',
    `created_time`                      timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`                      timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    key idx_date_value (`date_value`),
    key idx_hour_start_time (`hour_start_time`),
    key idx_next_hour_start_time (`next_hour_start_time`),
    key idx_upstream_order_no (`upstream_order_no`),
    key idx_scene_type (`scene_type`),
    key idx_start_point (`start_point`),
    key idx_start_area (`start_area`),
    key idx_target_point (`target_point`),
    key idx_target_area (`target_area`),
    key idx_upstream_order_state (`upstream_order_state`),
    key idx_upstream_order_create_time (`upstream_order_create_time`),
    key idx_upstream_order_completed_time (`upstream_order_completed_time`)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8
    COLLATE = utf8_general_ci
    AUTO_INCREMENT = 1
    ROW_FORMAT = DYNAMIC COMMENT ='小时内完成的上游作业单明细（H+1）';	
	
	
# step8:删除相关数据（qt_hour_transport_upstream_order_detail_his）
DELETE
FROM qt_smartreport.qt_hour_transport_upstream_order_detail_his
where hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00');	



# step9:插入相关数据（qt_hour_transport_upstream_order_detail_his）
insert into qt_smartreport.qt_hour_transport_upstream_order_detail_his(date_value,hour_start_time,next_hour_start_time, upstream_order_no, scene_type,start_point, start_area, target_point,target_area,upstream_order_state, dispatch_robot_code_num,dispatch_robot_code_str,dispatch_robot_classification_str,total_time_consuming, empty_move_distance,empty_move_speed,loading_move_distance,loading_move_speed, waiting_robot_time_consuming,move_time_consuming, lift_up_time_consuming,rack_move_time_consuming,put_down_time_consuming,guide_time_consuming, robot_rotate_num,dispatch_order_no,dispatch_order_num,upstream_order_create_time,upstream_order_completed_time)
select 
date(DATE_ADD(sysdate(), INTERVAL -1 HOUR)) as date_value,
date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') as hour_start_time,
date_format(sysdate(), '%Y-%m-%d %H:00:00') as next_hour_start_time,
t.upstream_order_no,
tr.scenario  as scene_type,
case
    when tr.start_point_code <> '' and tr.start_point_code is not null then tr.start_point_code
    else 'unknow' end                                                                   start_point,
case
    when tr.start_area_code <> '' and tr.start_area_code is not null then tr.start_area_code
    else 'unknow' end                                                                   start_area,
case
    when tr.target_point_code <> '' and tr.target_point_code is not null then tr.target_point_code
    else 'unknow' end                                                                   target_point,
case
    when tr.target_area_code <> '' and tr.target_area_code is not null then tr.target_area_code
    else 'unknow' end                                                                   target_area,
case
    when tr.order_state = 'WAITING_ROBOT_ASSIGN' then '待分车'
    when tr.order_state = 'EXECUTING' then '正在执行'
    when tr.order_state = 'COMPLETED' then '已完成'
    when tr.order_state = 'CANCELED' then '取消'
    when tr.order_state = 'PENDING' then '挂起'
    when tr.order_state = 'ABNORMAL_COMPLETED' then '异常完成'
    when tr.order_state = 'ABNORMAL_CANCELED' then '异常取消'
    end                                                                              as upstream_order_state,
t.dispatch_robot_code_num,
t.dispatch_robot_code_str,
t.dispatch_robot_classification_str,
nullif(tsc.total_time_consuming,0) as total_time_consuming,
nullif(tj.empty_move_distance, 0) as empty_move_distance,
case when coalesce(tsc.move_time_consuming,0) !=0 then nullif(tj.empty_move_distance, 0)/tsc.move_time_consuming else null end as empty_move_speed,	   
nullif(tj.loading_move_distance, 0) as loading_move_distance,
case when coalesce(tsc.rack_move_time_consuming,0) !=0 then nullif(tj.loading_move_distance,0)/tsc.rack_move_time_consuming else null end as loading_move_speed,
nullif(tsc.waiting_robot_time_consuming,0) as waiting_robot_time_consuming,
nullif(tsc.move_time_consuming,0) as move_time_consuming,
nullif(tsc.lift_up_time_consuming,0) as lift_up_time_consuming,
nullif(tsc.rack_move_time_consuming,0) as rack_move_time_consuming,
nullif(tsc.put_down_time_consuming,0) as put_down_time_consuming,
nullif(tj.guide_time_consuming,0)  as guide_time_consuming,
nullif(tj.robot_rotate_num,0)  as robot_rotate_num,
t.dispatch_order_num,
t.order_no_num,
t.upstream_order_create_time,
t.upstream_order_completed_time
from 
(select 
tc.upstream_order_no,
min(t.create_time)                                                as upstream_order_create_time,
max(case when t.order_state = 'COMPLETED' then tc.order_update_time end) as upstream_order_completed_time,
max(tc.order_update_time)                                                as upstream_order_update_time,
count(distinct tk.robot_code)                                     as dispatch_robot_code_num,
group_concat(distinct tk.robot_code)                              as dispatch_robot_code_str,
group_concat(distinct brt.first_classification)                   as dispatch_robot_classification_str,
count(distinct t.order_no)                                        as order_no_num,
group_concat(distinct t.order_no)                                 as dispatch_order_num,
max(t.id)      as latest_id
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no =tc.order_no 
left join phoenix_rss.transport_order_link tk on t.order_no = tk.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tc.update_time >=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and tc.update_time < date_format(sysdate(), '%Y-%m-%d %H:00:00')
group by tc.upstream_order_no)t 
left join 
(select 
upstream_order_no,
nullif(sum(total_cost),0)/1000 as total_time_consuming,
nullif(sum(assign_cost),0)/1000 as waiting_robot_time_consuming,
nullif(sum(move_cost),0)/1000 as move_time_consuming,
nullif(sum(lift_cost),0)/1000 as lift_up_time_consuming,
nullif(sum(rack_move_cost),0)/1000 as rack_move_time_consuming,
nullif(sum(put_cost),0)/1000 as put_down_time_consuming
from phoenix_rss.transport_order_carrier_cost
where update_time >=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and update_time < date_format(sysdate(), '%Y-%m-%d %H:00:00')
group by upstream_order_no)tsc on tsc.upstream_order_no = t.upstream_order_no
 left join phoenix_rss.transport_order tr on tr.upstream_order_no = t.upstream_order_no and t.latest_id = tr.id
left join 
(select tc.upstream_order_no,
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
                           sum(unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time))         as guide_time_consuming 
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no 
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
where tc.update_time >=date_format(DATE_ADD(sysdate(), INTERVAL -1 HOUR), '%Y-%m-%d %H:00:00') and tc.update_time < date_format(sysdate(), '%Y-%m-%d %H:00:00')
group by tc.upstream_order_no)tj on tj.upstream_order_no = t.upstream_order_no

