select 
jaor.robot_code,
brt.robot_type_code,
brt.robot_type_name,
jaor.msg_id as doputdown_msg_id,
jaor.start_time as doputdown_start_time,
jaor.end_time as ddoputdown_end_time,
unix_timestamp(jaor.end_time) - unix_timestamp(jaor.start_time) as doputdown_duration
from phoenix_rms.job_action_operation_record jaor 
inner join phoenix_basic.basic_robot br on br.robot_code = jaor.robot_code and jaor.operation_name='DoPutDown' and br.usage_state = 'using'  and jaor.start_time between {start_time} and {end_time}
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
