select
	date_value,
	link_id,
	upstream_order_no,
	order_no,
	link_create_time,
	event_time,
	execute_state,
	order_state,
	robot_code,
	first_classification,
	robot_type_name,
	cost_time as to_next_link_time_consuming
from
	qt_smartreport.qt_day_transport_order_link_detail_stat_his
where
	upstream_order_no = {upstream_order_no}	
union
select current_date()                       as date_value,
tol.id as link_id,
tol.upstream_order_no,
tol.order_no,
tol.create_time as link_create_time,
tol.event_time,
tol.execute_state,
tol.order_state,
tol.robot_code,
brt.first_classification,
brt.robot_type_name,
tol.cost_time/1000 as to_next_link_time_consuming 
from phoenix_rss.transport_order_carrier_cost tc
inner join phoenix_rss.transport_order_link tol on tol.order_no = tc.order_no
left join phoenix_basic.basic_robot br on br.robot_code = tol.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where tc.update_time >={now_start_time} and tc.update_time < {next_start_time}