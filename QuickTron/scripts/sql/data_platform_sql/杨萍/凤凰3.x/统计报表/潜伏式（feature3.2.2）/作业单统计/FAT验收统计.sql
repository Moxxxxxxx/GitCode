####:路线路径配置
select tl.id                         as line_id,
       COALESCE(tl.line_name, '未配置') as line_name,
       COALESCE(tp.name, '未配置')      as path_name,
       tp.start_point_code,
       tp.start_area_code,
       tp.target_point_code,
       tp.target_area_code,
       tp.estimate_move_time_consuming
from qt_smartreport.carry_job_path_info_v3 tp
         left join qt_smartreport.carry_job_line_info_v3 tl on tl.id = tp.line_id
		 
		 
		 


####:搬运作业单效率明细
select tt.order_create_time                                           as                          time_value,
       tt.upstream_order_no,
       tt.order_no,
       tt.start_point,
       tt.start_area,
       tt.target_point,
       tt.target_area,
       tt.order_state,
       tl.id                                                          as                          line_id,
       COALESCE(tl.line_name, '未配置')                                  as                          line_name,
       COALESCE(tp.name, '未配置')                                       as                          path_name,
       tp.estimate_move_time_consuming,
       case when tt.total_time_consuming > tp.estimate_move_time_consuming * 60 then 1 else 0 end overtime,
       tt.total_time_consuming - tp.estimate_move_time_consuming * 60 as                          timeout_duration,
       tt.dispatch_robot_code_num,
       tt.dispatch_robot_code_str,
       tt.dispatch_robot_classification_str,
       te.error_num,
       te.error_time,
       se.sys_error_num,
       se.sys_error_time,
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
            select date_value,
                   upstream_order_no,
                   order_no,
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
            from qt_smartreport.qt_transport_order_detail_stat_realtime) t
               inner join (select td.order_no,
                                  date(td.update_time) as date_value
                           from (select order_no, max(update_time) as update_time
                                 from phoenix_rss.transport_order
                                 group by order_no) td) ttd
                          on ttd.order_no = t.order_no and ttd.date_value = t.date_value) tt
         left join (select t.date_value,
                           toj.order_no,
                           count(distinct error_id)          as error_num,
                           sum(unix_timestamp(coalesce(t.end_time, sysdate())) -
                               unix_timestamp(t.start_time)) as error_time
                    from (select date_value,
                                 robot_code,
                                 error_id,
                                 error_code,
                                 start_time,
                                 end_time
                          from qt_smartreport.qt_basic_notification_clear4
                          union all
                          select date_value,
                                 robot_code,
                                 error_id,
                                 error_code,
                                 start_time,
                                 end_time
                          from qt_smartreport.qt_basic_notification_clear4_realtime) t
                             inner join phoenix_basic.basic_notification bn on bn.id = t.error_id
                             inner join phoenix_rss.transport_order_carrier_job toj on toj.job_sn = bn.robot_job
                    group by t.date_value, toj.order_no) te
                   on te.date_value = tt.date_value and te.order_no = tt.order_no
         left join (select COALESCE(bn.job_order, toj.order_no)                                                  as order_no,
                           count(distinct bn.id)                                                                 as sys_error_num,
                           sum(unix_timestamp(coalesce(bn.end_time, sysdate())) -
                               unix_timestamp(bn.start_time))                                                    as sys_error_time
                    from phoenix_basic.basic_notification bn
                             left join phoenix_rss.transport_order_carrier_job toj on toj.job_sn = bn.robot_job
                    where bn.alarm_module in ('system', 'server', 'device')
                      and bn.alarm_level >= 3
                      and (bn.job_order is not null or bn.robot_job is not null)
                    group by COALESCE(bn.job_order, toj.order_no)) se on se.order_no = tt.order_no

         LEFT JOIN
     qt_smartreport.carry_job_path_info_v3 tp
     ON
                 tt.start_point = tp.start_point_code and tt.target_point = tp.target_point_code
         LEFT JOIN
     qt_smartreport.carry_job_line_info_v3 tl
     ON
         tp.line_id = tl.id