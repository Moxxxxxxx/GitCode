SELECT coalesce(tl.line_name,'未配置路线') as line_name
     , t.order_id
     , t.order_done_time
     , t.move_distance
     , t.scene_type
     , t.stat_time
     , t.order_type
     , t.robot_code
     , t.start_point
     , t.target_point
     , t.total_time_consuming
     , t.init_job_time_consuming
     , t.move_time_consuming
     , t.lift_up_time_consuming
     , t.rack_move_time_consuming
     , t.put_down_time_consuming
     , t.guide_time_consuming
     , t.move_speed
     , t.move_abnormal_time_consuming
     , t.rotation_num
     , t.rotation_time_consuming
     , t.loading_move_distance
     , t.loading_move_speed
     , t.empty_move_distance
     , t.empty_move_speed

FROM qt_smartreport.qt_transport_order_efficiency_stat_detail t
         left join(
    SELECT l.line_name
         , l.start_region_id
         , r1.spots AS start_point
         , l.target_region_id
         , r2.spots AS target_point
    FROM qt_smartreport.carry_job_line_info l
             LEFT JOIN qt_smartreport.carry_job_region_info r1 ON l.start_region_id = r1.region_name
             LEFT JOIN qt_smartreport.carry_job_region_info r2 ON l.target_region_id = r2.region_name
) tl on tl.start_point like CONCAT('%',t.start_point,'%') and tl.target_point like CONCAT('%',t.target_point,'%')
  

UNION ALL

SELECT coalesce(tl.line_name,'未配置路线') as line_name
     , t.order_id
     , t.order_done_time
     , t.move_distance
     , t.scene_type
     , t.stat_time
     , t.order_type
     , t.robot_code
     , t.start_point
     , t.target_point
     , t.total_time_consuming
     , t.init_job_time_consuming
     , t.move_time_consuming
     , t.lift_up_time_consuming
     , t.rack_move_time_consuming
     , t.put_down_time_consuming
     , t.guide_time_consuming
     , t.move_speed
     , t.move_abnormal_time_consuming
     , t.rotation_num
     , t.rotation_time_consuming
     , t.loading_move_distance
     , t.loading_move_speed
     , t.empty_move_distance
     , t.empty_move_speed

FROM (select t.order_no                                                                    as order_id
           , t.update_time                                                                 AS order_done_time
           , round(coalesce(t7.order_actual_move_distance, 0),2)                                    AS move_distance
           , substring(t.order_type, 1, instr(t.order_type, '_') - 1)                      as scene_type
           , date_format(t.update_time, '%Y-%m-%d %H:00:00')                               as stat_time
           , t.order_type
           , t.dispatch_robot_code                                                         as robot_code
           , coalesce(t.start_point_code, 'unknow')                                        as start_point
           , coalesce(t.target_point_code, 'unknow')                                       as target_point
           , unix_timestamp(t.update_time) - unix_timestamp(t.create_time)                 as total_time_consuming
           , coalesce(t2.init_job_time_consuming, 0)                                       as init_job_time_consuming
           , coalesce(t3.move_time_consuming, 0)                                           as move_time_consuming
           , coalesce(t4.lift_up_time_consuming, 0)                                        as lift_up_time_consuming
           , coalesce(t5.rack_move_time_consuming, 0)                                      as rack_move_time_consuming
           , coalesce(t6.put_down_time_consuming, 0)                                       as put_down_time_consuming
           , coalesce(t7.guide_time_consuming, 0)                                                                             AS guide_time_consuming
           , round(coalesce(t7.order_actual_move_distance, 0) /
                   (unix_timestamp(t.update_time) - unix_timestamp(t.create_time)), 2)     AS move_speed
           , 0                                                                             AS move_abnormal_time_consuming
           , coalesce(t7.order_rotate_count, 0)                                            as rotation_num
           , null                                                                          as rotation_time_consuming
           , round(coalesce(t7.loading_move_distance, 0),2)                                         as loading_move_distance
           , round(coalesce(t7.loading_move_distance, 0) / t5.rack_move_time_consuming, 2) as loading_move_speed
           , round(coalesce(t7.empty_move_distance, 0),2)                                           as empty_move_distance
           , round(coalesce(t7.empty_move_distance, 0) / t3.move_time_consuming, 2)        as empty_move_speed
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
														  and t1.update_time BETWEEN {now_start_time} and {now_end_time}                                                        
                                      where t.execute_state = 'INIT_JOB') t1
                                         left join
                                     (select t.order_no,
                                             t.id,
                                             t.create_time
                                      from phoenix_rss.transport_order_link t
                                               inner join phoenix_rss.transport_order t1
                                                          on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                              and t1.update_time BETWEEN {now_start_time} and {now_end_time} 
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
                                                              and t1.update_time BETWEEN {now_start_time} and {now_end_time} 
                                      where t.execute_state = 'MOVE_DONE') t1
                                         left join
                                     (select t.order_no,
                                             t.id,
                                             t.create_time
                                      from phoenix_rss.transport_order_link t
                                               inner join phoenix_rss.transport_order t1
                                                          on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                              and t1.update_time BETWEEN {now_start_time} and {now_end_time} 
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
                                                              and t1.update_time BETWEEN {now_start_time} and {now_end_time} 
                                      where t.execute_state = 'LIFT_UP_DONE') t1
                                         left join
                                     (select t.order_no,
                                             t.id,
                                             t.create_time
                                      from phoenix_rss.transport_order_link t
                                               inner join phoenix_rss.transport_order t1
                                                          on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                              and t1.update_time BETWEEN {now_start_time} and {now_end_time} 
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
                                                              and t1.update_time BETWEEN {now_start_time} and {now_end_time} 
                                      where t.execute_state = 'RACK_MOVE_DONE') t1
                                         left join
                                     (select t.order_no,
                                             t.id,
                                             t.create_time
                                      from phoenix_rss.transport_order_link t
                                               inner join phoenix_rss.transport_order t1
                                                          on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                              and t1.update_time BETWEEN {now_start_time} and {now_end_time} 
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
                                                              and t1.update_time BETWEEN {now_start_time} and {now_end_time} 
                                      where t.execute_state = 'PUT_DOWN_DONE') t1
                                         left join
                                     (select t.order_no,
                                             t.id,
                                             t.create_time
                                      from phoenix_rss.transport_order_link t
                                               inner join phoenix_rss.transport_order t1
                                                          on t1.order_no = t.order_no and t1.order_state = 'COMPLETED'
                                                              and t1.update_time BETWEEN {now_start_time} and {now_end_time} 
                                      where t.execute_state = 'PUT_DOWN_START') t2
                                     on t2.order_no = t1.order_no and t2.create_time < t1.create_time
                                group by t1.order_no, t1.id, t1.create_time) t
                          group by t.order_no) t6 on t6.order_no = t.order_no
               left join (select tj.order_no,
                                 sum(rasd.rotate_count)                          as order_rotate_count,
                                 sum(rasd.actual_move_distance*1000)                  as order_actual_move_distance,
                                 sum(case
                                         when rasd.action_code = 'MOVE_LIFT_UP' or (rasd.action_code = 'MOVE' and rasd.is_loading=0)
                                             then rasd.actual_move_distance*1000 end) as empty_move_distance,
                                 sum(case
                                         when rasd.action_code = 'MOVE_PUT_DOWN' or (rasd.action_code = 'MOVE' and rasd.is_loading=1)
                                             then rasd.actual_move_distance*1000 end) as loading_move_distance,
											sum(unix_timestamp(terminal_guide_end_time) -
                                     unix_timestamp(terminal_guide_start_time))         as guide_time_consuming		 
                          from phoenix_rss.transport_order_carrier_job tj
                                   left join phoenix_rms.job_action_statistics_data rasd on rasd.job_sn = tj.job_sn
						  where tj.update_time BETWEEN {now_start_time} and {now_end_time}
                          group by tj.order_no) t7 on t7.order_no = t.order_no
      where t.order_state = 'COMPLETED'
	  and t.update_time BETWEEN {now_start_time} and {now_end_time}) t
         left join(
    SELECT l.line_name
         , l.start_region_id
         , r1.spots AS start_point
         , l.target_region_id
         , r2.spots AS target_point
    FROM qt_smartreport.carry_job_line_info l
             LEFT JOIN qt_smartreport.carry_job_region_info r1 ON l.start_region_id = r1.region_name
             LEFT JOIN qt_smartreport.carry_job_region_info r2 ON l.target_region_id = r2.region_name
) tl on tl.start_point like CONCAT('%',t.start_point,'%') and tl.target_point like CONCAT('%',t.target_point,'%')
