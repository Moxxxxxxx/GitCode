select 
rmr.robot_code,
brt.robot_type_code,
brt.robot_type_name,
rmr.id as  maintain_id,
rmr.start_time as maintain_start_time,
rmr.end_time as maintain_end_time,
unix_timestamp(coalesce(rmr.end_time,sysdate())) - unix_timestamp(rmr.start_time) as  maintain_duration,
rmr.reason as maintain_reason
from phoenix_rms.robot_maintain_record rmr
left join phoenix_basic.basic_robot br on br.robot_code = rmr.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id