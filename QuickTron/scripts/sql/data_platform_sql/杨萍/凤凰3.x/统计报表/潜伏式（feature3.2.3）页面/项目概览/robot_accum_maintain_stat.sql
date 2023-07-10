select 
count(distinct robot_code) as total_maintenance_robot_num,
COALESCE(avg(unix_timestamp(coalesce(end_time,sysdate())) - unix_timestamp(start_time)),0) as avg_maintenance_time,
count(distinct id) as total_maintenance_num
from phoenix_rms.robot_maintain_record
