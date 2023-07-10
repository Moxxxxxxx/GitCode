select tt.date_value,
       tt.upstream_order_no,
       tt.scene_type,
       tt.stat_time,
       tt.start_point,
       tt.start_area,
       tt.target_point,
       tt.target_area,
       tt.upstream_order_state,
       tt.dispatch_robot_code_num,
       tt.dispatch_robot_code_str,
       tt.dispatch_robot_classification_str,
       l.line_id,
       COALESCE(l.line_name, '未配置')                               AS                          line_name,
       CONCAT(tt.start_point, ' - ', tt.target_point)                AS                          path_name,
       l.estimate_move_time_consuming,
       case when tt.total_time_consuming > l.estimate_move_time_consuming * 60 then 1 else 0 end overtime,
       tt.total_time_consuming - l.estimate_move_time_consuming * 60 as                          timeout_duration,
       tt.total_time_consuming,
       COALESCE(bre.robot_error_num, 0)                              as                          robot_error_num,
       COALESCE(bre.robot_error_time, 0)                             as                          robot_error_time,
       COALESCE(brs.sys_error_num, 0)                                as                          sys_error_num,
       tt.empty_move_distance,
       tt.empty_move_speed,
       tt.loading_move_distance,
       tt.loading_move_speed,
       tt.waiting_robot_time_consuming,
       tt.move_time_consuming,
       tt.lift_up_time_consuming,
       tt.rack_move_time_consuming,
       tt.put_down_time_consuming,
       tt.guide_time_consuming,
       tt.robot_rotate_num,
       tt.dispatch_order_no,
       tt.dispatch_order_num,
       tt.upstream_order_create_time,
       tt.upstream_order_completed_time
from (select tt.date_value,
             tt.upstream_order_no,
             tt.scene_type,
             tt.stat_time,
             tt.start_point,
             tt.start_area,
             tt.target_point,
             tt.target_area,
             tt.upstream_order_state,
             tt.dispatch_robot_code_num,
             tt.dispatch_robot_code_str,
             tt.dispatch_robot_classification_str,
             tt.total_time_consuming,
             tt.empty_move_distance,
             tt.empty_move_speed,
             tt.loading_move_distance,
             tt.loading_move_speed,
             tt.waiting_robot_time_consuming,
             tt.move_time_consuming,
             tt.lift_up_time_consuming,
             tt.rack_move_time_consuming,
             tt.put_down_time_consuming,
             tt.guide_time_consuming,
             tt.robot_rotate_num,
             tt.dispatch_order_no,
             tt.dispatch_order_num,
             tt.upstream_order_create_time,
             tt.upstream_order_completed_time
      from (select date_value,
                   upstream_order_no,
                   scene_type,
                   stat_time,
                   start_point,
                   start_area,
                   target_point,
                   target_area,
                   upstream_order_state,
                   dispatch_robot_code_num,
                   dispatch_robot_code_str,
                   dispatch_robot_classification_str,
                   total_time_consuming,
                   empty_move_distance,
                   empty_move_speed,
                   loading_move_distance,
                   loading_move_speed,
                   waiting_robot_time_consuming,
                   move_time_consuming,
                   lift_up_time_consuming,
                   rack_move_time_consuming,
                   put_down_time_consuming,
                   guide_time_consuming,
                   robot_rotate_num,
                   dispatch_order_no,
                   dispatch_order_num,
                   upstream_order_create_time,
                   upstream_order_completed_time
            from qt_smartreport.qt_transport_upstream_order_detail_stat_his
            union all
            select current_date()                                                                       as date_value,
                   t.upstream_order_no,
                   tr.scenario                                                                          as scene_type,
                   date_format(t.upstream_order_create_time, '%Y-%m-%d %H:00:00')                       as stat_time,
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
                   unix_timestamp(t.upstream_order_update_time) -
                   unix_timestamp(t.upstream_order_create_time)                                         as total_time_consuming,

                   tj.empty_move_distance,
                   case
                       when t2.move_time_consuming is not null
                           then coalesce(tj.empty_move_distance, 0) / t2.move_time_consuming end        as empty_move_speed,
                   tj.loading_move_distance,
                   case
                       when t4.rack_move_time_consuming is not null
                           then coalesce(tj.loading_move_distance, 0) / t4.rack_move_time_consuming end as loading_move_speed,
                   t1.waiting_robot_time_consuming,
                   t2.move_time_consuming,
                   t3.lift_up_time_consuming,
                   t4.rack_move_time_consuming,
                   t5.put_down_time_consuming,
                   tj.guide_time_consuming,
                   tj.robot_rotate_num,

                   t.dispatch_order_num,
                   t.order_no_num,
                   t.upstream_order_create_time,
                   t.upstream_order_completed_time
            from (select tu.upstream_order_no,
                         min(t.create_time)                                                as upstream_order_create_time,
                         max(case when t.order_state = 'COMPLETED' then t.update_time end) as upstream_order_completed_time,
                         max(t.update_time)                                                as upstream_order_update_time,
                         count(distinct tk.robot_code)                                     as dispatch_robot_code_num,
                         group_concat(distinct tk.robot_code)                              as dispatch_robot_code_str,
                         group_concat(distinct brt.first_classification)                   as dispatch_robot_classification_str,
                         count(distinct t.order_no)                                        as order_no_num,
                         group_concat(distinct t.order_no)                                 as dispatch_order_num,
                         max(t.id)                                                         as latest_id
                  from (select distinct upstream_order_no
                        from phoenix_rss.transport_order
                        where update_time >= {now_start_time}
                          and update_time <= {now_end_time}) tu
                           left join phoenix_rss.transport_order t on tu.upstream_order_no = t.upstream_order_no
                           left join phoenix_rss.transport_order_link tk on t.order_no = tk.order_no
                           left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
                           left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
                  group by tu.upstream_order_no) t
                     left join phoenix_rss.transport_order tr
                               on tr.upstream_order_no = t.upstream_order_no and t.latest_id = tr.id
                     left join
                 (select t.upstream_order_no,
                         sum(t.order_waiting_robot_time_consuming) as waiting_robot_time_consuming
                  from (select t.upstream_order_no,
                               t.order_no,
                               sum(unix_timestamp(tk.create_time) - unix_timestamp(t.create_time)) as order_waiting_robot_time_consuming
                        from (select distinct upstream_order_no
                              from phoenix_rss.transport_order
                              where update_time >= {now_start_time}
                                and update_time <= {now_end_time}) tu
                                 left join phoenix_rss.transport_order t on tu.upstream_order_no = t.upstream_order_no
                                 inner join phoenix_rss.transport_order_link tk
                                            on t.order_no = tk.order_no and tk.execute_state = 'INIT_JOB'
                        group by t.upstream_order_no, t.order_no) t
                  group by t.upstream_order_no) t1 on t1.upstream_order_no = t.upstream_order_no
                     left join
                 (select t.upstream_order_no,
                         sum(unix_timestamp(t.end_time) - unix_timestamp(t.start_time)) as move_time_consuming
                  from (select t1.upstream_order_no,
                               t1.order_no,
                               t1.id               as link_id,
                               t1.create_time      as end_time,
                               max(t2.create_time) as start_time
                        from (select t.upstream_order_no, t.order_no, tk.id, tk.create_time
                              from (select distinct upstream_order_no
                                    from phoenix_rss.transport_order
                                    where update_time >= {now_start_time}
                                      and update_time <= {now_end_time}) tu
                                       left join phoenix_rss.transport_order t
                                                 on tu.upstream_order_no = t.upstream_order_no
                                       inner join phoenix_rss.transport_order_link tk
                                                  on t.order_no = tk.order_no and tk.execute_state = 'MOVE_DONE') t1
                                 left join
                             (select t.upstream_order_no, t.order_no, tk.id, tk.create_time
                              from (select distinct upstream_order_no
                                    from phoenix_rss.transport_order
                                    where update_time >= {now_start_time}
                                      and update_time <= {now_end_time}) tu
                                       left join phoenix_rss.transport_order t
                                                 on tu.upstream_order_no = t.upstream_order_no
                                       inner join phoenix_rss.transport_order_link tk
                                                  on t.order_no = tk.order_no and tk.execute_state = 'MOVE_START') t2
                             on t2.upstream_order_no = t1.upstream_order_no and t2.order_no = t1.order_no and
                                t2.create_time < t1.create_time
                        group by t1.upstream_order_no, t1.order_no, t1.id, t1.create_time) t
                  group by t.upstream_order_no) t2 on t2.upstream_order_no = t.upstream_order_no
                     left join
                 (select t.upstream_order_no,
                         sum(unix_timestamp(t.end_time) - unix_timestamp(t.start_time)) as lift_up_time_consuming
                  from (select t1.upstream_order_no,
                               t1.order_no,
                               t1.id               as link_id,
                               t1.create_time      as end_time,
                               max(t2.create_time) as start_time
                        from (select t.upstream_order_no, t.order_no, tk.id, tk.create_time
                              from (select distinct upstream_order_no
                                    from phoenix_rss.transport_order
                                    where update_time >= {now_start_time}
                                      and update_time <= {now_end_time}) tu
                                       left join phoenix_rss.transport_order t
                                                 on tu.upstream_order_no = t.upstream_order_no
                                       inner join phoenix_rss.transport_order_link tk
                                                  on t.order_no = tk.order_no and tk.execute_state = 'LIFT_UP_DONE') t1
                                 left join
                             (select t.upstream_order_no, t.order_no, tk.id, tk.create_time
                              from (select distinct upstream_order_no
                                    from phoenix_rss.transport_order
                                    where update_time >= {now_start_time}
                                      and update_time <= {now_end_time}) tu
                                       left join phoenix_rss.transport_order t
                                                 on tu.upstream_order_no = t.upstream_order_no
                                       inner join phoenix_rss.transport_order_link tk
                                                  on t.order_no = tk.order_no and tk.execute_state = 'LIFT_UP_START') t2
                             on t2.upstream_order_no = t1.upstream_order_no and t2.order_no = t1.order_no and
                                t2.create_time < t1.create_time
                        group by t1.upstream_order_no, t1.order_no, t1.id, t1.create_time) t
                  group by t.upstream_order_no) t3 on t3.upstream_order_no = t.upstream_order_no
                     left join
                 (select t.upstream_order_no,
                         sum(unix_timestamp(t.end_time) - unix_timestamp(t.start_time)) as rack_move_time_consuming
                  from (select t1.upstream_order_no,
                               t1.order_no,
                               t1.id               as link_id,
                               t1.create_time      as end_time,
                               max(t2.create_time) as start_time
                        from (select t.upstream_order_no, t.order_no, tk.id, tk.create_time
                              from (select distinct upstream_order_no
                                    from phoenix_rss.transport_order
                                    where update_time >= {now_start_time}
                                      and update_time <= {now_end_time}) tu
                                       left join phoenix_rss.transport_order t
                                                 on tu.upstream_order_no = t.upstream_order_no
                                       inner join phoenix_rss.transport_order_link tk
                                                  on t.order_no = tk.order_no and tk.execute_state = 'RACK_MOVE_DONE') t1
                                 left join
                             (select t.upstream_order_no, t.order_no, tk.id, tk.create_time
                              from (select distinct upstream_order_no
                                    from phoenix_rss.transport_order
                                    where update_time >= {now_start_time}
                                      and update_time <= {now_end_time}) tu
                                       left join phoenix_rss.transport_order t
                                                 on tu.upstream_order_no = t.upstream_order_no
                                       inner join phoenix_rss.transport_order_link tk
                                                  on t.order_no = tk.order_no and tk.execute_state = 'RACK_MOVE_START') t2
                             on t2.upstream_order_no = t1.upstream_order_no and t2.order_no = t1.order_no and
                                t2.create_time < t1.create_time
                        group by t1.upstream_order_no, t1.order_no, t1.id, t1.create_time) t
                  group by t.upstream_order_no) t4 on t4.upstream_order_no = t.upstream_order_no
                     left join
                 (select t.upstream_order_no,
                         sum(unix_timestamp(t.end_time) - unix_timestamp(t.start_time)) as put_down_time_consuming
                  from (select t1.upstream_order_no,
                               t1.order_no,
                               t1.id               as link_id,
                               t1.create_time      as end_time,
                               max(t2.create_time) as start_time
                        from (select t.upstream_order_no, t.order_no, tk.id, tk.create_time
                              from (select distinct upstream_order_no
                                    from phoenix_rss.transport_order
                                    where update_time >= {now_start_time}
                                      and update_time <= {now_end_time}) tu
                                       left join phoenix_rss.transport_order t
                                                 on tu.upstream_order_no = t.upstream_order_no
                                       inner join phoenix_rss.transport_order_link tk
                                                  on t.order_no = tk.order_no and tk.execute_state = 'PUT_DOWN_DONE') t1
                                 left join
                             (select t.upstream_order_no, t.order_no, tk.id, tk.create_time
                              from (select distinct upstream_order_no
                                    from phoenix_rss.transport_order
                                    where update_time >= {now_start_time}
                                      and update_time <= {now_end_time}) tu
                                       left join phoenix_rss.transport_order t
                                                 on tu.upstream_order_no = t.upstream_order_no
                                       inner join phoenix_rss.transport_order_link tk
                                                  on t.order_no = tk.order_no and tk.execute_state = 'PUT_DOWN_START') t2
                             on t2.upstream_order_no = t1.upstream_order_no and t2.order_no = t1.order_no and
                                t2.create_time < t1.create_time
                        group by t1.upstream_order_no, t1.order_no, t1.id, t1.create_time) t
                  group by t.upstream_order_no) t5 on t5.upstream_order_no = t.upstream_order_no
                     left join (select t.upstream_order_no,
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
                                from (select distinct upstream_order_no
                                      from phoenix_rss.transport_order
                                      where update_time >= {now_start_time}
                                        and update_time <= {now_end_time}) tu
                                         left join phoenix_rss.transport_order t
                                                   on tu.upstream_order_no = t.upstream_order_no
                                         left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
                                         left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
                                group by t.upstream_order_no) tj on tj.upstream_order_no = t.upstream_order_no) tt
               inner join (select td.upstream_order_no,
                                  date(td.update_time) as date_value
                           from (select upstream_order_no, max(update_time) as update_time
                                 from phoenix_rss.transport_order
                                 group by upstream_order_no) td) ttd
                          on ttd.upstream_order_no = tt.upstream_order_no and ttd.date_value = tt.date_value) tt
         left join (select tob.upstream_order_no,
                           count(distinct bn.id)              as robot_error_num,
                           sum(unix_timestamp(coalesce(bn.end_time, sysdate())) -
                               unix_timestamp(bn.start_time)) as robot_error_time
                    from (select error_id
                          from qt_smartreport.qt_day_robot_error_detail_his
                          union all
                          select id as error_id
                          from ({tb_day_robot_error_detail}) tb) t -- day_robot_error_detail.sql
                             inner join phoenix_basic.basic_notification bn on bn.id = t.error_id
                             inner join phoenix_rss.transport_order_carrier_job toj on toj.job_sn = bn.robot_job
                             left join phoenix_rss.transport_order tob on tob.id = toj.order_id
                    group by tob.upstream_order_no) bre on bre.upstream_order_no = tt.upstream_order_no
         left join (select tob.upstream_order_no,
                           count(distinct bn.id)              as sys_error_num,
                           sum(unix_timestamp(coalesce(bn.end_time, sysdate())) -
                               unix_timestamp(bn.start_time)) as sys_error_time
                    from (select error_id
                          from qt_smartreport.qt_day_sys_error_detail_his
                          union all
                          select id as error_id
                          from phoenix_basic.basic_notification
                          where alarm_module in ('system', 'server')
                            and alarm_level >= 3
                            and (
                                  (start_time >= {now_start_time} and
                                   start_time <= {now_end_time} and
                                   coalesce(end_time, sysdate()) <= {now_end_time}) or
                                  (start_time >= {now_start_time} and
                                   start_time <= {now_end_time} and
                                   coalesce(end_time, sysdate()) >= {now_start_time}) or
                                  (start_time < {now_start_time} and
                                   coalesce(end_time, sysdate()) >=
                                   {now_start_time} and
                                   coalesce(end_time, sysdate()) <= {now_start_time}) or
                                  (start_time < {now_start_time} and
                                   coalesce(end_time, sysdate()) > {now_start_time})
                              )) t
                             inner join phoenix_basic.basic_notification bn on bn.id = t.error_id
                             inner join phoenix_rss.transport_order_carrier_job toj
                                        on toj.job_sn = bn.robot_job or bn.job_order = toj.order_no
                             left join phoenix_rss.transport_order tob on tob.id = toj.order_id
                    group by tob.upstream_order_no) brs on brs.upstream_order_no = tt.upstream_order_no

         LEFT JOIN
     (SELECT DISTINCT tmp1.id AS line_id
                    , tmp1.line_name
                    , tmp1.estimate_move_time_consuming
                    , tmp2.start_point_code
                    , tmp3.target_point_code
      FROM qt_smartreport.carry_job_line_info_v4 tmp1
               LEFT JOIN
           qt_smartreport.carry_job_start_point_code_v4 tmp2
           ON
               tmp1.id = tmp2.line_id
               LEFT JOIN
           qt_smartreport.carry_job_target_point_code_v4 tmp3
           ON
               tmp1.id = tmp3.line_id) l
     ON
                 tt.start_point = l.start_point_code
             AND
                 tt.target_point = l.target_point_code