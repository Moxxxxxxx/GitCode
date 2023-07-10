###基于mysql5.7
set @do_start_time = '2022-07-12 00:00:00';
set @do_end_time = '2022-07-12 23:59:59';


DROP TABLE if EXISTS qt_smartreport.qt_tmp_step1;
create table qt_smartreport.qt_tmp_step1
(
    id         bigint(20),
    order_no   varchar(32),
    current_id varchar(32),
    next_id    varchar(32),
    PRIMARY KEY (`id`),
    key idx_current_id (current_id),
    key idx_next_id (next_id)
);



set @begin_id = (SELECT t.id
                 from phoenix_rss.transport_order_link t
                 WHERE t.create_time >= date_add(@do_start_time, interval -10 day)
                 LIMIT 1);



insert into qt_smartreport.qt_tmp_step1
SELECT t.id,
       t.order_no,
       CONCAT(t.order_no, '-', @rn := @rn + 1) current_id,
       CONCAT(t.order_no, '-', @rn + 1)        next_id
from phoenix_rss.transport_order_link t,
     (SELECT @rn := 0) tmp
WHERE t.id >= @begin_id
ORDER BY t.order_no, t.id;



DROP TABLE if EXISTS qt_smartreport.qt_tmp_step2;
create table qt_smartreport.qt_tmp_step2
(
    id       bigint(20),
    order_no varchar(32),
    next_id  varchar(32),
    PRIMARY KEY (`id`),
    key idx_order_no (order_no),
    key idx_next_id (next_id)
);


insert into qt_smartreport.qt_tmp_step2
SELECT t1.id, t1.order_no, t2.id as next_id
from qt_smartreport.qt_tmp_step1 t1
         LEFT JOIN qt_smartreport.qt_tmp_step1 t2
                   ON t1.next_id = t2.current_id;


#上游作业单明细

select t1.upstream_order_no,                                                                                      #上游作业单ID
       coalesce(t.start_point_code, 'unknow')                                    as start_point,                  #起始点
       COALESCE(t.start_area_code, 'unknow')                                     as start_area,                   #起始区域
       coalesce(t.target_point_code, 'unknow')                                   as target_point,                 #目标点
       COALESCE(t.target_area_code, 'unknow')                                    as start_area,                   #目标区域
       case
           when t.order_state = 'WAITING_ROBOT_ASSIGN' then '待分车'
           when t.order_state = 'EXECUTING' then '正在执行'
           when t.order_state = 'COMPLETED' then '已完成'
           when t.order_state = 'CANCELED' then '取消'
           when t.order_state = 'PENDING' then '挂起'
           when t.order_state = 'ABNORMAL_COMPLETED' then '异常完成'
           when t.order_state = 'ABNORMAL_CANCELED' then '异常取消'
           end                                                                      current_upstream_order_state, #作业单状态
       t1.current_total_dispatch_robot_code,
       unix_timestamp(t1.max_update_time) - unix_timestamp(t2.start_create_time) as current_total_time_consuming, #总耗时(秒)
       t1.current_total_order_no as dispatch_order_no,                                                                                 #搬运作业单
       t4.empty_move_distance,                                                                                    #空车移动距离(米)
       t4.empty_move_distance / t3.move_time_consuming                           as empty_move_speed,             #空车移动速度(米/秒)
       t4.loading_move_distance,                                                                                  #带载移动距离(米)
       t4.loading_move_distance / t3.rack_move_time_consuming                    as loading_move_speed,           #带载移动速度(米/秒)
       t3.init_job_time_consuming as waiting_robot_time_consuming,                                                                                #分车耗时(秒)
       t3.move_time_consuming,                                                                                    #空车移动耗时(秒)
       t3.lift_up_time_consuming,                                                                                 #顶升耗时(秒)
       t3.rack_move_time_consuming,                                                                               #带载移动耗时(秒)
       t3.put_down_time_consuming,                                                                                #降下耗时(秒)
       t4.guide_time_consuming,                                                                                   #末端引导耗时(秒)
       t4.robot_rotate_count as robot_rotate_num,                                                                                     #机器人旋转次数
       t2.completed_time,                                                                                         #作业单完成时间
       t2.start_create_time                                                                                       #作业单创建时间


from (select upstream_order_no,
             max(update_time)                    as max_update_time,
             count(distinct dispatch_robot_code) as current_total_dispatch_robot_code,
             count(distinct order_no)            as order_no_num,
             group_concat(order_no)              as current_total_order_no
      from phoenix_rss.transport_order
      where update_time between @do_start_time and @do_end_time
      group by upstream_order_no) t1
         left join
     (select upstream_order_no,
             min(create_time)                                              as start_create_time,
             max(case when order_state = 'COMPLETED' then update_time end) as completed_time
      from phoenix_rss.transport_order
      group by upstream_order_no) t2 on t2.upstream_order_no = t1.upstream_order_no
         left join phoenix_rss.transport_order t
                   on t.upstream_order_no = t1.upstream_order_no and t.update_time = t1.max_update_time
         left join (select t1.upstream_order_no,
                           sum(case
                                   when t1.execute_state = 'WAITING_ROBOT'
                                       then unix_timestamp(next_create_time) - unix_timestamp(create_time) end) as init_job_time_consuming,
                           sum(case
                                   when t1.execute_state = 'MOVE_START'
                                       then unix_timestamp(next_create_time) - unix_timestamp(create_time) end) as move_time_consuming,
                           sum(case
                                   when t1.execute_state = 'LIFT_UP_START'
                                       then unix_timestamp(next_create_time) - unix_timestamp(create_time) end) as lift_up_time_consuming,
                           sum(case
                                   when t1.execute_state = 'RACK_MOVE_START'
                                       then unix_timestamp(next_create_time) - unix_timestamp(create_time) end) as rack_move_time_consuming,
                           sum(case
                                   when t1.execute_state = 'PUT_DOWN_START'
                                       then unix_timestamp(next_create_time) - unix_timestamp(create_time) end) as put_down_time_consuming
                    from (select tol.id,
                                 tol.create_time,
                                 tol.upstream_order_no,
                                 tol.order_no,
                                 tol.execute_state,
                                 tol.order_state,
                                 tol2.id            as next_id,
                                 tol2.create_time   as next_create_time,
                                 tol2.execute_state as next_execute_state,
                                 tol2.order_state   as next_order_state
                          from phoenix_rss.transport_order t
                                   inner join phoenix_rss.transport_order_link tol on t.order_no = tol.order_no
                                   left join qt_smartreport.qt_tmp_step2 tm
                                             on tm.order_no = tol.order_no and tm.id = tol.id
                                   left join phoenix_rss.transport_order_link tol2
                                             on tol2.order_no = tm.order_no and tol2.id = tm.next_id
                          where t.update_time between @do_start_time and @do_end_time) t1
                    group by t1.upstream_order_no) t3 on t3.upstream_order_no = t.upstream_order_no
         left join (select t.upstream_order_no,
                           sum(rasd.rotate_count)                                 as robot_rotate_count,
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
                    where t.update_time between @do_start_time and @do_end_time
                    group by t.upstream_order_no) t4 on t4.upstream_order_no = t.upstream_order_no
;




#作业单时间链路

select tol.id,                                                                                           #主键ID
       tol.create_time,                                                                                  #创建时间
       tol.upstream_order_no,                                                                            #上游作业单ID
       tol.order_no,                                                                                     #搬运作业单ID
       tol.execute_state,                                                                                #搬运作业单节点的执行状态
       tol.order_state,                                                                                  #搬运作业单状态
       tol2.create_time                                                   as next_create_time,           #下一个节点的创建时间
       tol2.execute_state                                                 as next_execute_state,         #下一个节点的执行状态
       tol2.order_state                                                   as next_order_state,           #下一个节点搬运作业单的状态
       unix_timestamp(tol2.create_time) - unix_timestamp(tol.create_time) as to_next_link_time_consuming #此节点到下一个节点的耗时（秒）
from phoenix_rss.transport_order t
         inner join phoenix_rss.transport_order_link tol on t.order_no = tol.order_no
         left join qt_smartreport.qt_tmp_step2 tm
                   on tm.order_no = tol.order_no and tm.id = tol.id
         left join phoenix_rss.transport_order_link tol2
                   on tol2.order_no = tm.order_no and tol2.id = tm.next_id
where t.update_time between @do_start_time and @do_end_time
order by tol.upstream_order_no,tol.create_time
;













/*

-----------------------------------------------------------
phoenix_rss.transport_order.order_state

/**
 * 待分车：任务下发之后的状态
 */
WAITING_ROBOT_ASSIGN(10, "WAITING_ROBOT_ASSIGN", "待分车"),
/**
 * 正在执行：分车后状态
 */
EXECUTING(20, "EXECUTING", "正在执行"),
/**
 * 已完成：作业单搬运任务完成
 */
COMPLETED(30, "COMPLETED", "已完成"),
/**
 * 取消：上游调用接口或中控取消
 */
CANCELED(40, "CANCELED", "取消"),
/**
 * 挂起：车辆故障离场后状态
 */
PENDING(50, "PENDING", "挂起"),
/**
 * 异常完成：挂起作业单点击异常完成
 */
ABNORMAL_COMPLETED(60, "ABNORMAL_COMPLETED", "异常完成"),
/**
 * 异常取消：挂起作业单点击异常取消
 */
ABNORMAL_CANCELED(70, "ABNORMAL_CANCELED", "异常取消");

---------------------------------------------------------------------------

phoenix_rss.transport_order_link.execute_state



/**
 * 待执行
 */
WAITING_NEXTSTOP(10, "WAITING_NEXTSTOP", "待执行"),
/**
 * 待分配ROBOT
 */
WAITING_ROBOT(20, "WAITING_ROBOT", "待分配ROBOT"),
/**
 * 待下发
 */
WAITING_RESOURCE(22, "WAITING_RESOURCE", "待下发"),
/**
 * 待下发(多楼层)
 */
PENDING_INIT_JOB(23, "PENDING_INIT_JOB", "待下发(多楼层)"),
/**
 * 等待任务下发
 */
WAITING_DISPATCHER(25, "WAITING_DISPACHER", "等待任务下发"),
/**
 * 已分配
 */
INIT_JOB(30, "INIT_JOB", "已分配"),
/**
 * 执行中
 */
EXECUTING(35, "EXECUTING", "执行中"),
/**
 * 开始空车移动（潜伏式）
 */
MOVE_START(36, "MOVE_START", "开始空车移动"),
/**
 * 空车移动结束（潜伏式）
 */
MOVE_DONE(37, "MOVE_DONE", "空车移动结束"),
/**
 * 开始顶升
 */
LIFT_UP_START(40, "LIFT_UP_START", "开始顶升"),
/**
 * 顶升完成
 */
LIFT_UP_DONE(45, "LIFT_UP_DONE", "顶升完成"),
/**
 * 等待区域一段任务完成
 */
AWAIT_DONE(50, "AWAIT_DONE", "等待区域一段任务完成"),
/**
 * 带载开始移动(潜伏式)
 */
RACK_MOVE_START(55, "RACK_MOVE_START", "带载开始移动"),
/**
 * 带载开始结束(潜伏式)
 */
RACK_MOVE_DONE(56, "RACK_MOVE_DONE", "带载开始结束"),
/**
 * 急停
 */
SUSPEND(60, "SUSPEND", "急停"),
/**
 * 开始放下
 */
PUT_DOWN_START(70, "PUT_DOWN_START", "开始放下货架"),
/**
 * 放下货架完成
 */
PUT_DOWN_DONE(75, "PUT_DOWN_DONE", "放下货架完成"),
/**
 * 到站
 */
ENTER_STATION(80, "ENTER_STATION", "到站"),
/**
 * 开始二次移动
 */
AGAIN_MOVE_START(84, "AGAIN_MOVE_START", "开始二次移动"),
/**
 * 二次移动完成
 */
AGAIN_MOVE_DONE(85, "AGAIN_MOVE_DONE", "二次移动完成"),
/**
 * 完成
 */
DONE(90, "DONE", "完成"),
/**
 * 挂起
 */
PENDING(100, "PENDING", "挂起"),
/**
 * 取消
 */
CANCEL(110, "CANCEL", "取消"),
/**
 * 失败
 */
FAILED(120, "FAILED", "失败"),
/**
 * 异常完成
 */
ABNORMAL_COMPLETED(130, "ABNORMAL_COMPLETED", "异常完成"),
/**
 * 等待分配电梯
 */
WAITING_LIFT(140, "WAITING_LIFT", "等待分配电梯"),
/**
 * 异常取消
 */
ABNORMAL_CANCEL(150, "ABNORMAL_CANCEL", "异常取消");

-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
