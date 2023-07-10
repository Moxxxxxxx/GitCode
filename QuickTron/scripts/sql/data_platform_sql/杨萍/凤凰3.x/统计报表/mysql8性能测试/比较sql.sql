涉及表：

phoenix_rss.transport_order （共 85834 条数据，其中2022-08-01 这天4455条）
phoenix_rss.transport_order_link （共 876860 条数据 ，其中2022-08-01 这天 44162条）
phoenix_rms.robot_state_history（共 1070608 条数据，其中2022-08-01 这天 27966 条）
phoenix_rms.rms_action_statistics_data （共 237154 条数据，其中2022-08-01 这天 12923 条）


code1:

select count(*)
from phoenix_rss.transport_order;

code2:

select date(create_time) as date_value,
       count(*)
from phoenix_rss.transport_order
group by date(create_time)
order by date_value desc;




1、计算搬运作业单的各类节点数据（耗时、距离、速度等）


set @do_date = '2022-08-01';
select t.order_no                                                                    as order_id,                 #作业单ID
       t.create_time                                                                 as order_create_time,        #作业单创建时间
       t.update_time                                                                 as order_done_time,          #作业单完成时间
       substring(t.order_type, 1, instr(t.order_type, '_') - 1)                      as scene_type,               #场景类型
       date_format(t.update_time, '%Y-%m-%d %H:00:00')                               as stat_time,                #统计时间
       t.order_type,                                                                                              #作业单类型
       t.dispatch_robot_code                                                         as robot_code,               #机器人编码
       coalesce(t.start_point_code, 'unknow')                                        as start_point,              #起始点
       coalesce(t.target_point_code, 'unknow')                                       as target_point,             #目标点
       unix_timestamp(t.update_time) - unix_timestamp(t.create_time)                 as total_time_consuming,     #总耗时(秒)
       coalesce(t2.init_job_time_consuming, 0)                                       as init_job_time_consuming,  #分车耗时(秒)
       coalesce(t3.move_time_consuming, 0)                                           as move_time_consuming,      #空车移动耗时(秒)
       coalesce(t4.lift_up_time_consuming, 0)                                        as lift_up_time_consuming,   #顶升耗时(秒)
       coalesce(t5.rack_move_time_consuming, 0)                                      as rack_move_time_consuming, #带载移动耗时(秒)
       coalesce(t6.put_down_time_consuming, 0)                                       as put_down_time_consuming,  #放下耗时(秒)
       coalesce(t7.guide_time_consuming, 0)                                          as guide_time_consuming,     #末端引导耗时(秒)
       round(coalesce(t7.order_actual_move_distance, 0), 2)                          as move_distance,            #实际移动距离(米)
       round(coalesce(t7.order_actual_move_distance, 0) /
             (unix_timestamp(t.update_time) - unix_timestamp(t.create_time)), 2)     as move_speed,               #实际行驶速度(米/秒)
       coalesce(t7.order_rotate_count, 0)                                            as rotation_num,             #机器人旋转次数
       round(coalesce(t7.loading_move_distance, 0), 2)                               as loading_move_distance,    #带载移动距离(米)
       round(coalesce(t7.loading_move_distance, 0) / t5.rack_move_time_consuming, 2) as loading_move_speed,       #带载移动速度(米/秒)
       round(coalesce(t7.empty_move_distance, 0), 2)                                 as empty_move_distance,      #空车移动距离(米)
       round(coalesce(t7.empty_move_distance, 0) / t3.move_time_consuming, 2)        as empty_move_speed          #空车移动速度(米/秒)
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
                                                        and date(t1.update_time) = @do_date
                                where t.execute_state = 'INIT_JOB') t1
                                   left join
                               (select t.order_no,
                                       t.id,
                                       t.create_time
                                from phoenix_rss.transport_order_link t
                                         inner join phoenix_rss.transport_order t1
                                                    on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                        and date(t1.update_time) = @do_date
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
                                                        and date(t1.update_time) = @do_date
                                where t.execute_state = 'MOVE_DONE') t1
                                   left join
                               (select t.order_no,
                                       t.id,
                                       t.create_time
                                from phoenix_rss.transport_order_link t
                                         inner join phoenix_rss.transport_order t1
                                                    on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                        and date(t1.update_time) = @do_date
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
                                                        and date(t1.update_time) = @do_date
                                where t.execute_state = 'LIFT_UP_DONE') t1
                                   left join
                               (select t.order_no,
                                       t.id,
                                       t.create_time
                                from phoenix_rss.transport_order_link t
                                         inner join phoenix_rss.transport_order t1
                                                    on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                        and date(t1.update_time) = @do_date
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
                                                        and date(t1.update_time) = @do_date
                                where t.execute_state = 'RACK_MOVE_DONE') t1
                                   left join
                               (select t.order_no,
                                       t.id,
                                       t.create_time
                                from phoenix_rss.transport_order_link t
                                         inner join phoenix_rss.transport_order t1
                                                    on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                        and date(t1.update_time) = @do_date
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
                                                        and date(t1.update_time) = @do_date
                                where t.execute_state = 'PUT_DOWN_DONE') t1
                                   left join
                               (select t.order_no,
                                       t.id,
                                       t.create_time
                                from phoenix_rss.transport_order_link t
                                         inner join phoenix_rss.transport_order t1
                                                    on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                        and date(t1.update_time) = @do_date
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
                    where date(tj.update_time) = @do_date
                    group by tj.order_no) t7 on t7.order_no = t.order_no
where t.order_state = 'COMPLETED'
  and date_format(t.update_time, '%Y-%m-%d') = date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d')
;