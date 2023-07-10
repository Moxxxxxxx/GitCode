SELECT
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
ORDER BY coalesce(rmr.end_time,'2222-01-01 00:00:00') DESC, rmr.start_time DESC