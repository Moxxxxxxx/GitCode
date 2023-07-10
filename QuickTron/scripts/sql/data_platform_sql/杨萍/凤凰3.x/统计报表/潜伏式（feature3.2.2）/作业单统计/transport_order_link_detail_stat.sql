select t.upstream_order_no,
       t.order_no,
       t.link_create_time,
       t.execute_state,
       t.order_state,
       t.robot_code,
	   t.robot_classification,
       t.next_link_create_time,
       t.next_execute_state,
       t.next_order_state,
       t.to_next_link_time_consuming
from (select date_value,
             upstream_order_no,
             order_no,
             link_create_time,
             execute_state,
             order_state,
             robot_code,
			 robot_classification,
             next_link_create_time,
             next_execute_state,
             next_order_state,
             to_next_link_time_consuming
      from qt_smartreport.qt_transport_order_link_detail_stat
      union all
      select current_date()                                                      as date_value,
             t.upstream_order_no,
             t.order_no,
             t.link_create_time,
             t.execute_state,
             t.order_state,
             t.robot_code,
			  brt.first_classification                                        as robot_classification,
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
                               on t2.upstream_order_no = t1.upstream_order_no and t2.order_no = t1.order_no and
                                  t2.create_time > t1.create_time
            group by t.upstream_order_no, t.order_no, t1.id, t1.create_time, t1.execute_state, t1.order_state,
                     t1.robot_code) t
               left join phoenix_rss.transport_order_link t3 on t3.order_no = t.order_no and t3.id = t.next_id
			   		 left join phoenix_basic.basic_robot br on br.robot_code = t1.robot_code
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id) t
         inner join (select td.upstream_order_no,
                            date(td.update_time) as date_value
                     from (select upstream_order_no, max(update_time) as update_time
                           from phoenix_rss.transport_order
                           group by upstream_order_no) td) ttd
                    on ttd.upstream_order_no = t.upstream_order_no and ttd.date_value = t.date_value