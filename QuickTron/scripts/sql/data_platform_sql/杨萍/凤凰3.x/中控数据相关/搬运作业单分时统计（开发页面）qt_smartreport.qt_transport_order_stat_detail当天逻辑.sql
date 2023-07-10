搬运作业单分时统计（开发页面）qt_smartreport.qt_transport_order_stat_detail当天逻辑


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