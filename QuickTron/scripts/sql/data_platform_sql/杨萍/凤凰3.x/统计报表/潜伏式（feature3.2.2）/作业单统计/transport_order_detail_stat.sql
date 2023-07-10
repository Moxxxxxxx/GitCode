select tt.upstream_order_no,
       tt.order_no,
	   tt.scene_type,
       tt.start_point,
       tt.start_area,
       tt.target_point,
       tt.target_area,
       tt.order_state,
       tl.id  as line_id,
       COALESCE(tl.line_name, '未配置')                                     as                           line_name,
       COALESCE(tp.name, '未配置')                                        as                           path_name,
       tp.estimate_move_time_consuming,
       case when tt.total_time_consuming > tp.estimate_move_time_consuming * 60 then 1 else 0 end overtime,
	   tt.total_time_consuming-tp.estimate_move_time_consuming * 60 as timeout_duration, 
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
       tt.order_create_time,
       tt.order_completed_time

from (select t.date_value,
             t.upstream_order_no,
             t.order_no,
			 t.scene_type,
             t.start_point,
             t.start_area,
             t.target_point,
             t.target_area,
             t.order_state,
             t.dispatch_robot_code_num,
             t.dispatch_robot_code_str,
             t.dispatch_robot_classification_str,
             t.total_time_consuming,
             t.empty_move_distance,
             t.empty_move_speed,
             t.loading_move_distance,
             t.loading_move_speed,
             t.waiting_robot_time_consuming,
             t.move_time_consuming,
             t.lift_up_time_consuming,
             t.rack_move_time_consuming,
             t.put_down_time_consuming,
             t.guide_time_consuming,
             t.robot_rotate_num,
             t.order_create_time,
             t.order_completed_time

      from (select date_value,
                   upstream_order_no,
                   order_no,
				   scene_type,
                   start_point,
                   start_area,
                   target_point,
                   target_area,
                   order_state,
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
                   order_create_time,
                   order_completed_time

            from qt_smartreport.qt_transport_order_detail_stat
            union all
            select current_date()                                                                       as date_value,
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
                                       count(distinct tk.robot_code)                                as dispatch_robot_code_num,
                                       group_concat(distinct tk.robot_code)                         as dispatch_robot_code_str,
                                       group_concat(distinct brt.first_classification) as dispatch_robot_classification_str,
                                       sum(case
                                               when tk.execute_state = 'WAITING_ROBOT'
                                                   then unix_timestamp(tk.next_link_create_time) -
                                                        unix_timestamp(tk.link_create_time) end)    as waiting_robot_time_consuming,
                                       sum(case
                                               when tk.execute_state = 'MOVE_START'
                                                   then unix_timestamp(tk.next_link_create_time) -
                                                        unix_timestamp(tk.link_create_time) end)    as move_time_consuming,
                                       sum(case
                                               when tk.execute_state = 'LIFT_UP_START'
                                                   then unix_timestamp(tk.next_link_create_time) -
                                                        unix_timestamp(tk.link_create_time) end)    as lift_up_time_consuming,
                                       sum(case
                                               when tk.execute_state = 'RACK_MOVE_START'
                                                   then unix_timestamp(tk.next_link_create_time) -
                                                        unix_timestamp(tk.link_create_time) end)    as rack_move_time_consuming,
                                       sum(case
                                               when tk.execute_state = 'PUT_DOWN_START'
                                                   then unix_timestamp(tk.next_link_create_time) -
                                                        unix_timestamp(tk.link_create_time) end)    as put_down_time_consuming
                                from (select current_date()                                                      as date_value,
                                             t.upstream_order_no,
                                             t.order_no,
                                             t.link_create_time,
                                             t.execute_state,
                                             t.order_state,
                                             t.robot_code,
                                             t3.create_time                                                      as next_link_create_time,
                                             t3.execute_state                                                    as next_execute_state,
                                             t3.order_state                                                      as next_order_state,
                                             unix_timestamp(t3.create_time) - unix_timestamp(t.link_create_time) as to_next_link_time_consuming
                                      from (select t.upstream_order_no,
                                                   t.order_no,
                                                   t1.id          as current_id,
                                                   t1.create_time as link_create_time,
                                                   t1.execute_state,
                                                   t1.order_state,
                                                   t1.robot_code,
                                                   min(t2.id)     as next_id
                                            from (select distinct upstream_order_no, order_no
                                                  from phoenix_rss.transport_order
                                                  where update_time BETWEEN {now_start_time} and {now_end_time}) t
                                                     left join phoenix_rss.transport_order_link t1
                                                               on t1.upstream_order_no = t.upstream_order_no and t1.order_no = t.order_no
                                                     left join phoenix_rss.transport_order_link t2
                                                               on t2.upstream_order_no = t1.upstream_order_no and
                                                                  t2.order_no = t1.order_no and
                                                                  t2.create_time > t1.create_time
                                            group by t.upstream_order_no, t.order_no, t1.id, t1.create_time,
                                                     t1.execute_state,
                                                     t1.order_state,
                                                     t1.robot_code) t
                                               left join phoenix_rss.transport_order_link t3
                                                         on t3.order_no = t.order_no and t3.id = t.next_id) tk
                                         left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
                                         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
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
                                where t.update_time BETWEEN {now_start_time} and {now_end_time}
                                group by t.order_no) t2 on t2.order_no = t.order_no
            where t.update_time BETWEEN {now_start_time} and {now_end_time}) t
               inner join (select td.order_no,
                                  date(td.update_time) as date_value
                           from (select order_no, max(update_time) as update_time
                                 from phoenix_rss.transport_order
                                 group by order_no) td) ttd
                          on ttd.order_no = t.order_no and ttd.date_value = t.date_value) tt
LEFT JOIN
    qt_smartreport.carry_job_path_info_v3 tp
ON
     tt.start_point = tp.start_point_code and tt.target_point = tp.target_point_code
LEFT JOIN
    qt_smartreport.carry_job_line_info_v3 tl
ON
     tp.line_id = tl.id