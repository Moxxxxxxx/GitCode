select tt.upstream_order_no,
       tt.order_no,
       tt.scene_type,
       tt.start_point,
       tt.start_area,
       tt.target_point,
       tt.target_area,
       tt.order_state,
       l.line_id,
       COALESCE(l.line_name, 'unknow')                               AS                          line_name,
       CONCAT(tt.start_point, ' - ', tt.target_point)                AS                          path_name,
       l.estimate_move_time_consuming,
       case when tt.total_time_consuming > l.estimate_move_time_consuming * 60 then 1 else 0 end overtime,
       tt.total_time_consuming - l.estimate_move_time_consuming * 60 as                          timeout_duration,
       nullif(bre.robot_error_num, 0)                              as                          robot_error_num,
       nullif(bre.robot_error_time, 0)                             as                          robot_error_time,
       null                                                          as                          sys_error_num,
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
      from qt_smartreport.qt_transport_order_detail_stat_his
      where order_create_time BETWEEN {start_time} and {end_time}
      union
      select current_date()                        as date_value,
             t.upstream_order_no,
             t.order_no,
             t.scenario                            as scene_type,
             case
                 when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code
                 else 'unknow' end                    start_point,
             case
                 when t.start_area_code <> '' and t.start_area_code is not null then t.start_area_code
                 else 'unknow' end                    start_area,
             case
                 when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code
                 else 'unknow' end                    target_point,
             case
                 when t.target_area_code <> '' and t.target_area_code is not null then t.target_area_code
                 else 'unknow' end                    target_area,
             t.order_state,
             tr.dispatch_robot_code_num,
             tr.dispatch_robot_code_str,
             tr.dispatch_robot_classification_str,
             nullif(tc.total_cost, 0) / 1000     as total_time_consuming,
             nullif(tj.empty_move_distance, 0)   as empty_move_distance,
             case when COALESCE(tc.move_cost, 0) != 0 then nullif(tj.empty_move_distance, 0) / tc.move_cost else null end as empty_move_speed,
             nullif(tj.loading_move_distance, 0) as loading_move_distance,
             case when COALESCE(tc.rack_move_cost, 0) != 0 then nullif(tj.loading_move_distance, 0)/tc.rack_move_cost else null end as loading_move_speed,
             nullif(tc.assign_cost, 0) / 1000    as waiting_robot_time_consuming,
             nullif(tc.move_cost, 0) / 1000      as move_time_consuming,
             nullif(tc.lift_cost, 0) / 1000      as lift_up_time_consuming,
             nullif(tc.rack_move_cost, 0) / 1000 as rack_move_time_consuming,
             nullif(tc.put_cost, 0) / 1000       as put_down_time_consuming,
             nullif(tj.guide_time_consuming, 0)  as guide_time_consuming,
             nullif(tj.robot_rotate_num, 0)      as robot_rotate_num,
             t.create_time                         as order_create_time,
             tc.order_update_time                  as order_completed_time
      from phoenix_rss.transport_order_carrier_cost tc
               inner join phoenix_rss.transport_order t
                          on t.order_no = tc.order_no and t.create_time BETWEEN {start_time} and {end_time}
               left join
           (select tc.order_no,
                   count(distinct tk.robot_code)                   as dispatch_robot_code_num,
                   group_concat(distinct tk.robot_code)            as dispatch_robot_code_str,
                   group_concat(distinct brt.first_classification) as dispatch_robot_classification_str
            from phoenix_rss.transport_order_carrier_cost tc
                     inner join phoenix_rss.transport_order_link tk on tk.order_no = tc.order_no
                     left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
                     left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
            where tc.update_time >= {now_start_time}
              and tc.update_time < {next_start_time}
            group by tc.order_no) tr on tr.order_no = t.order_no
               left join
           (select t.order_no,
                   sum(rasd.rotate_count)                                 as robot_rotate_num,
                   sum(rasd.actual_move_distance * 1000)                  as order_actual_move_distance,
                   sum(case when rasd.action_code = 'MOVE_LIFT_UP' or (rasd.action_code = 'MOVE' and rasd.is_loading = 0)then rasd.actual_move_distance * 1000 end) as empty_move_distance,
                   sum(case when rasd.action_code = 'MOVE_PUT_DOWN' or (rasd.action_code = 'MOVE' and rasd.is_loading = 1)then rasd.actual_move_distance * 1000 end) as loading_move_distance,
                   sum(unix_timestamp(terminal_guide_end_time) - unix_timestamp(terminal_guide_start_time))                                            as guide_time_consuming
            from phoenix_rss.transport_order_carrier_cost tc
                     inner join phoenix_rss.transport_order t
                                on t.order_no = tc.order_no and t.create_time BETWEEN {start_time} and {end_time}
                     left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
                     left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
            where tc.update_time >= {now_start_time}
              and tc.update_time < {next_start_time}
            group by t.order_no) tj on tj.order_no = t.order_no
      where tc.update_time >= {now_start_time}
        and tc.update_time < {next_start_time}) tt
         left join (select t.order_no,
                           count(distinct t.id)                                                                as robot_error_num,
                           sum(unix_timestamp(coalesce(t.end_time, sysdate())) - unix_timestamp(t.start_time))  as robot_error_time
                    from (select t.order_no, bn.id, bn.start_time, bn.end_time
                          from phoenix_rss.transport_order t
                                   inner join phoenix_rss.transport_order_carrier_job toj
                                              on toj.order_id = t.id and t.create_time BETWEEN {start_time} and {end_time}
                                   inner join phoenix_basic.basic_notification bn on toj.job_sn = bn.robot_job
                                   inner join qt_smartreport.qt_day_robot_error_detail_his teh on teh.error_id = bn.id
                          union
                          select t.order_no, bn.id, bn.start_time, bn.end_time
                          from phoenix_rss.transport_order t
                                   inner join phoenix_rss.transport_order_carrier_job toj
                                              on toj.order_id = t.id and t.create_time BETWEEN {start_time} and {end_time}
                                   inner join phoenix_basic.basic_notification bn on toj.job_sn = bn.robot_job
                                   inner join ({tb_day_robot_error_detail}) tb on tb.id = bn.id -- day_robot_error_detail.sql
                         ) t
                    group by t.order_no) bre on bre.order_no = tt.order_no
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