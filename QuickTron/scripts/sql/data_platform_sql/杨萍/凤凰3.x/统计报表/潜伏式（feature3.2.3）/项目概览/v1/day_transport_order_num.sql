select count(distinct tt.order_no)                                                                               as upstream_order_num,
       count(distinct case when tt.overtime = 1 then tt.order_no end)                                            as timeout_order_num,
       COALESCE(count(distinct case when tt.overtime = 1 then tt.order_no end) / count(distinct tt.order_no),
                0)                                                                                               AS timeout_order_rate
from (select t.order_no,
             t.create_time,
             t.update_time,
             unix_timestamp(t.update_time) - unix_timestamp(t.create_time)                                          as total_time_consuming,
             COALESCE(tl.line_name, '未配置')                                                                          as line_name,
             COALESCE(tp.name, '未配置')                                                                               as path_name,
             tp.estimate_move_time_consuming,
             case
                 when (unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) >
                      tp.estimate_move_time_consuming * 60 then 1
                 else 0 end                                                                                            overtime,
             (unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) - tp.estimate_move_time_consuming *
                                                                               60                                   as timeout_duration
      from phoenix_rss.transport_order t
               left join qt_smartreport.carry_job_path_info_v3 tp
                         on tp.start_point_code = t.start_point_code and tp.target_point_code = t.target_point_code
               left join qt_smartreport.carry_job_line_info_v3 tl on tp.line_id = tl.id
      where t.create_time BETWEEN {now_start_time} and {now_end_time}
	  ) tt 