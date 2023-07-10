set @start_time ='2023-03-01 00:00:00';
set @end_time ='2023-03-21 23:59:59';
select @start_time,@end_time;

select 
    concat(brt.robot_type_name,'(',br.robot_type_code,')') as robot_type,
	bn.robot_code,
	count(distinct bn.id) as error_num,
	sum(unix_timestamp(coalesce (bn.end_time,now()))-unix_timestamp(bn.start_time)) as total_error_duration,
	avg(unix_timestamp(coalesce (bn.end_time,now()))-unix_timestamp(bn.start_time)) as avg_error_duration
from phoenix_basic.basic_notification bn
inner join phoenix_basic.basic_robot br on br.robot_code =bn.robot_code and br.usage_state ='using'
left join phoenix_basic.basic_robot_type brt on brt.robot_type_code =br.robot_type_code
where bn.alarm_module ='robot' and bn.alarm_level >=3
and bn.start_time >= @start_time and bn.start_time <= @end_time
-- and br.robot_code in ('qilin31_14','qilin31_16')
-- and br.robot_type_code in ('H80A-HBDQR0N-91')
-- AND bn.error_code in ('DSP0x4903','DSP0x042D')
group by robot_type,bn.robot_code
order by error_num desc 




--------------------------------------------------------------
select 
    concat(brt.robot_type_name,'(',br.robot_type_code,')') as robot_type,
	bn.robot_code,
	count(distinct bn.id) as error_num,
	sum(unix_timestamp(coalesce (bn.end_time,now()))-unix_timestamp(bn.start_time)) as total_error_duration,
	avg(unix_timestamp(coalesce (bn.end_time,now()))-unix_timestamp(bn.start_time)) as avg_error_duration
from phoenix_basic.basic_notification bn
inner join phoenix_basic.basic_robot br on br.robot_code =bn.robot_code and br.usage_state ='using'
left join phoenix_basic.basic_robot_type brt on brt.robot_type_code =br.robot_type_code
where bn.alarm_module ='robot' and bn.alarm_level >=3
and bn.start_time >= '{{ start_time }}' and bn.start_time <= '{{ end_time }}'
{% if robot_code %} AND br.robot_code IN {{ ja_concat_in(robot_code) }} {% endif %}  -- 机器人编码
{% if robot_type_code %} AND br.robot_type_code IN {{ ja_concat_in(robot_type_code) }} {% endif %}  -- 机器人类型
{% if error_code %} AND bn.error_code IN {{ ja_concat_in(error_code) }} {% endif %}  -- 错误码
group by robot_type,bn.robot_code
order by error_num desc 