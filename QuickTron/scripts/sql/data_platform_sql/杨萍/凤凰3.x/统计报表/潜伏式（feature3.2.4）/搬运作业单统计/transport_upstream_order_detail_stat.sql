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
       COALESCE(l.line_name, 'unknow')                               AS                          line_name,
       CONCAT(tt.start_point, ' - ', tt.target_point)                AS                          path_name,
       l.estimate_move_time_consuming,
       case when tt.total_time_consuming > l.estimate_move_time_consuming * 60 then 1 else 0 end overtime,
       tt.total_time_consuming - l.estimate_move_time_consuming * 60 as                          timeout_duration,
       tt.total_time_consuming,
       nullif(bre.robot_error_num, 0)     as                          robot_error_num,
       nullif(bre.robot_error_time, 0)    as                          robot_error_time,
       null                               as                          sys_error_num,
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
			where upstream_order_create_time BETWEEN {start_time} and {end_time}		
			union 			
select date_add(current_date(), interval -1 day)                       as date_value,
t.upstream_order_no,
tr.scenario  as scene_type,
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
tr.order_state as upstream_order_state,
t.dispatch_robot_code_num,
t.dispatch_robot_code_str,
t.dispatch_robot_classification_str,
nullif(tsc.total_time_consuming,0) as total_time_consuming,
nullif(tj.empty_move_distance, 0) as empty_move_distance,
case when COALESCE(tsc.move_time_consuming,0) !=0 then nullif(tj.empty_move_distance, 0)/tsc.move_time_consuming else null end as empty_move_speed,	   
nullif(tj.loading_move_distance, 0) as loading_move_distance,
case when COALESCE(tsc.rack_move_time_consuming,0) !=0 then nullif(tj.loading_move_distance,0)/tsc.rack_move_time_consuming else null end as loading_move_speed,
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
where tc.update_time >={now_start_time} and tc.update_time < {next_start_time}
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
where update_time >={now_start_time} and update_time < {next_start_time}
group by upstream_order_no)tsc on tsc.upstream_order_no = t.upstream_order_no
left join phoenix_rss.transport_order tr on tr.upstream_order_no = t.upstream_order_no and t.latest_id = tr.id and t.upstream_order_create_time BETWEEN {start_time} and {end_time}
left join 
(select tc.upstream_order_no,
                           sum(rasd.rotate_count)                                 as robot_rotate_num,
                           sum(rasd.actual_move_distance * 1000)                  as order_actual_move_distance,
                           sum(case when rasd.action_code = 'MOVE_LIFT_UP' or(rasd.action_code = 'MOVE' and rasd.is_loading = 0) then rasd.actual_move_distance * 1000 end) as empty_move_distance,
                           sum(case when rasd.action_code = 'MOVE_PUT_DOWN' or (rasd.action_code = 'MOVE' and rasd.is_loading = 1)then rasd.actual_move_distance * 1000 end) as loading_move_distance,
                           sum(unix_timestamp(terminal_guide_end_time) -unix_timestamp(terminal_guide_start_time))  as guide_time_consuming 
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order t on t.order_no = tc.order_no 
left join phoenix_rss.transport_order_carrier_job tj on tj.order_id = t.id
left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
where tc.update_time >={now_start_time} and tc.update_time < {next_start_time}
group by tc.upstream_order_no)tj on tj.upstream_order_no = t.upstream_order_no) tt					  
         left join (select t.upstream_order_no,
                           count(distinct t.id)                                                                as robot_error_num,
                           sum(unix_timestamp(coalesce(t.end_time, sysdate())) - unix_timestamp(t.start_time))  as robot_error_time
                    from (select t.upstream_order_no,t.order_no, bn.id, bn.start_time, bn.end_time
                          from phoenix_rss.transport_order t
                                   inner join phoenix_rss.transport_order_carrier_job toj
                                              on toj.order_id = t.id and t.create_time BETWEEN {start_time} and {end_time}
                                   inner join phoenix_basic.basic_notification bn on toj.job_sn = bn.robot_job
                                   inner join qt_smartreport.qt_day_robot_error_detail_his teh on teh.error_id = bn.id
                          union
                          select t.upstream_order_no,t.order_no, bn.id, bn.start_time, bn.end_time
                          from phoenix_rss.transport_order t
                                   inner join phoenix_rss.transport_order_carrier_job toj
                                              on toj.order_id = t.id and t.create_time BETWEEN {start_time} and {end_time}
                                   inner join phoenix_basic.basic_notification bn on toj.job_sn = bn.robot_job
                                   inner join ({tb_day_robot_error_detail}) tb on tb.id = bn.id -- day_robot_error_detail.sql
                         ) t
                    group by t.upstream_order_no) bre on bre.upstream_order_no = tt.upstream_order_no							
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