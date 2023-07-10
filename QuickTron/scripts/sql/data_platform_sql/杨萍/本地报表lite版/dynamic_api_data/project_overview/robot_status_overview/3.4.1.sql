select 
	concat(brt.robot_type_name,' (',br.robot_type_code,')') as robot_type,
	count(distinct br.robot_code) as total_robot_num,
	count(distinct case when rsr.online_state='REGISTERED' then br.robot_code end) as online_robot_num,
	count(distinct case when rsr.online_state!='REGISTERED' then br.robot_code end) as offline_robot_num,
	count(distinct case when rmr.robot_code is not null then br.robot_code end) as offline_maintain_robot_num 
from phoenix_basic.basic_robot br
left join phoenix_basic.basic_robot_type brt on brt.robot_type_code = br.robot_type_code
left join phoenix_rms.robot_state_realtime rsr on rsr.robot_code = br.robot_code 
left join (select distinct robot_code from phoenix_rms.robot_maintain_record where end_time is null)rmr on rmr.robot_code=br.robot_code 
where br.usage_state ='using'
group by robot_type