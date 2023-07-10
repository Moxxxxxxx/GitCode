select tt.date_value,
       tt.upstream_order_no,
       tt.scene_type,
       tt.stat_time,
       tt.start_point,
       tt.start_area,
       tt.target_point,
       tt.target_area,
       tt.upstream_order_state,
       tl.id  as line_id,
       COALESCE(tl.line_name, '未配置')                                     as                           line_name,
       COALESCE(tp.name, '未配置')                                        as                           path_name,
       tp.estimate_move_time_consuming,
       case when tt.total_time_consuming > tp.estimate_move_time_consuming * 60 then 1 else 0 end overtime,
       tt.total_time_consuming - tp.estimate_move_time_consuming * 60 as                           timeout_duration,
       tt.dispatch_robot_code_num,
       tt.dispatch_robot_code_str,
       tt.dispatch_robot_classification_str,
       tt.total_time_consuming,
       tt.dispatch_order_no,
	   tt.order_no_num,
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
             tt.dispatch_order_no,
			 tt.order_no_num,
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
                   dispatch_order_no,
				   order_no_num,
                   upstream_order_create_time,
                   upstream_order_completed_time
            from qt_smartreport.qt_transport_upstream_order_detail_stat

            union all

            select current_date()                                                 as date_value,
                   t.upstream_order_no,
                   t1.scenario                                                    as scene_type,
                   date_format(t.upstream_order_create_time, '%Y-%m-%d %H:00:00') as stat_time,
                   case
                       when t1.start_point_code <> '' and t1.start_point_code is not null then t1.start_point_code
                       else 'unknow' end                                             start_point,
                   case
                       when t1.start_area_code <> '' and t1.start_area_code is not null then t1.start_area_code
                       else 'unknow' end                                             start_area,
                   case
                       when t1.target_point_code <> '' and t1.target_point_code is not null then t1.target_point_code
                       else 'unknow' end                                             target_point,
                   case
                       when t1.target_area_code <> '' and t1.target_area_code is not null then t1.target_area_code
                       else 'unknow' end                                             target_area,
                   case
                       when t1.order_state = 'WAITING_ROBOT_ASSIGN' then '待分车'
                       when t1.order_state = 'EXECUTING' then '正在执行'
                       when t1.order_state = 'COMPLETED' then '已完成'
                       when t1.order_state = 'CANCELED' then '取消'
                       when t1.order_state = 'PENDING' then '挂起'
                       when t1.order_state = 'ABNORMAL_COMPLETED' then '异常完成'
                       when t1.order_state = 'ABNORMAL_CANCELED' then '异常取消'
                       end                                                        as upstream_order_state,
                   t.dispatch_robot_code_num,
                   t.dispatch_robot_code_str,
                   t.dispatch_robot_classification_str,
                   unix_timestamp(t.upstream_order_update_time) -
                   unix_timestamp(t.upstream_order_create_time)                   as total_time_consuming,
                   t.dispatch_order_no,
				   t.order_no_num,
                   t.upstream_order_create_time,
                   t.upstream_order_completed_time
            from (select tu.upstream_order_no,
                         min(t.create_time)                                                as upstream_order_create_time,
                         max(t.update_time)                                                as upstream_order_update_time,
                         max(case when t.order_state = 'COMPLETED' then t.update_time end) as upstream_order_completed_time,
                         count(distinct tk.robot_code)                                     as dispatch_robot_code_num,
                         group_concat(distinct tk.robot_code)                              as dispatch_robot_code_str,
                         group_concat(distinct brt.first_classification) as dispatch_robot_classification_str,
                         count(distinct t.order_no)                                        as order_no_num,
                         group_concat(distinct t.order_no)                                 as dispatch_order_no,
                         max(t.id)                                                         as latest_id
                  from (select distinct upstream_order_no
                        from phoenix_rss.transport_order
                        where update_time BETWEEN {now_start_time} and {now_end_time}) tu
                           left join phoenix_rss.transport_order t on tu.upstream_order_no = t.upstream_order_no
                           left join (select current_date()                                                      as date_value,
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
                                     on tu.upstream_order_no = tk.upstream_order_no
                           left join phoenix_basic.basic_robot br on br.robot_code = tk.robot_code
                           left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
                  group by tu.upstream_order_no) t
                     left join phoenix_rss.transport_order t1
                               on t1.upstream_order_no = t.upstream_order_no and t.latest_id = t1.id) tt
               inner join (select td.upstream_order_no,
                                  date(td.update_time) as date_value
                           from (select upstream_order_no, max(update_time) as update_time
                                 from phoenix_rss.transport_order
                                 group by upstream_order_no) td) ttd
                          on ttd.upstream_order_no = tt.upstream_order_no and ttd.date_value = tt.date_value) tt
LEFT JOIN
    qt_smartreport.carry_job_path_info_v3 tp
ON
     tt.start_point = tp.start_point_code and tt.target_point = tp.target_point_code
LEFT JOIN
    qt_smartreport.carry_job_line_info_v3 tl
ON
     tp.line_id = tl.id