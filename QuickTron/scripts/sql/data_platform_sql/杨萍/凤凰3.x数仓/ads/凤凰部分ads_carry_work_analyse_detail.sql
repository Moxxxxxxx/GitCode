-- 凤凰3.X CARRIER逻辑
-- 暂时不做


select 
project_code,
id as link_id,
upstream_order_no,
order_no,
order_state,
execute_state, 
create_time,
update_time,
event_time,
robot_code,
lag(update_time,1) over(partition by project_code,upstream_order_no order by create_time,update_time asc) as pre1_update_time,
unix_timestamp(update_time)-unix_timestamp (lag(update_time,1) over(partition by project_code,upstream_order_no order by create_time,update_time asc)) as link_duration
from dwd.dwd_phx_rss_transport_order_link_info_di t
where order_no ='SIRack_167656386097405510'
order by id asc
