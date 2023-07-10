select date_value,
             upstream_order_no,
             order_no,
             link_create_time,
             execute_state,
             order_state,
             robot_code,
             first_classification,
             robot_type_name,
             next_link_create_time,
             next_execute_state,
             next_order_state,
             to_next_link_time_consuming
      from qt_smartreport.qt_transport_order_link_detail_stat_his
      where upstream_order_no={upstream_order_no}
union
select current_date()                       as date_value,
tol.upstream_order_no,
tol.order_no,
tol.create_time as link_create_time,
tol.execute_state,
tol.order_state,
tol.robot_code,
brt.first_classification,
brt.robot_type_name,
t2.create_time                                                  as next_link_create_time,
t2.execute_state                                                as next_execute_state,
t2.order_state                                                  as next_order_state,
unix_timestamp(t2.create_time) - unix_timestamp(tol.create_time) as to_next_link_time_consuming  
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_link tol on tol.order_no = tc.order_no and tol.upstream_order_no={upstream_order_no}
left join 
(select 
t1.id,t1.order_no ,t1.create_time ,t2.id as next_id
from 
(select 
t.id,
t.order_no ,
t.create_time ,
CONCAT(order_no, '-', @rn := @rn + 1) current_id,
CONCAT(order_no, '-', @rn + 1)        next_id
from 
(select 
tol.id,
tol.order_no,
tol.create_time 
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_link tol on tol.order_no = tc.order_no
where tc.update_time >={now_start_time} and tc.update_time < {next_start_time}
and tc.upstream_order_no={upstream_order_no}
order by tol.order_no,tol.create_time asc)t,(SELECT @rn := 0) tmp)t1 
left join 
(select 
t.id,
t.order_no ,
t.create_time ,
CONCAT(order_no, '-', @rm := @rm + 1) current_id,
CONCAT(order_no, '-', @rm + 1)        next_id
from 
(select 
tol.id,
tol.order_no,
tol.create_time 
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_link tol on tol.order_no = tc.order_no
where tc.update_time >={now_start_time} and tc.update_time < {next_start_time}
and tc.upstream_order_no={upstream_order_no}
order by tol.order_no,tol.create_time asc)t,(SELECT @rm := 0) tmp)t2 on t1.next_id = t2.current_id
)tm on tm.id=tol.id
left join 
(select 
tol.*
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_link tol on tol.order_no = tc.order_no 
where tc.update_time >={now_start_time} and tc.update_time < {next_start_time}
and tc.upstream_order_no={upstream_order_no}
)t2  on t2.order_no = tm.order_no and t2.id = tm.next_id
left join phoenix_basic.basic_robot br on br.robot_code = tol.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tc.update_time >={now_start_time} and tc.update_time < {next_start_time}
and tc.upstream_order_no={upstream_order_no}
