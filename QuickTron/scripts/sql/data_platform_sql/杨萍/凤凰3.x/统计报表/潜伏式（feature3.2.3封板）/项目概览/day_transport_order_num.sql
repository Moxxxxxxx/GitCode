select count(distinct tt.order_no)                                                                               as upstream_order_num,
       count(distinct case when tt.overtime = 1 then tt.order_no end)                                            as timeout_order_num,
       COALESCE(count(distinct case when tt.overtime = 1 then tt.order_no end) / count(distinct tt.order_no),
                0)                                                                                               AS timeout_order_rate
from (select t.order_no,
             t.create_time,
             t.update_time,
             unix_timestamp(t.update_time) - unix_timestamp(t.create_time)                                          as total_time_consuming,
             COALESCE(tp.line_name, '未配置')                                                                          as line_name,
             COALESCE(CONCAT(t.start_point_code, ' - ', t.target_point_code), '未配置')                as path_name,
             tp.estimate_move_time_consuming,
             case
                 when (unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) >
                      tp.estimate_move_time_consuming * 60 then 1
                 else 0 end                                                                                            overtime,
             (unix_timestamp(t.update_time) - unix_timestamp(t.create_time)) - tp.estimate_move_time_consuming *
                                                                               60                                   as timeout_duration
      from phoenix_rss.transport_order t
      left join (
          SELECT DISTINCT tmp1.id AS line_id
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
                   tmp1.id = tmp3.line_id) tp
      ON t.start_point_code = tp.start_point_code AND t.target_point_code = tp.target_point_code
      where t.create_time BETWEEN {now_start_time} and {now_end_time}
	  ) tt 