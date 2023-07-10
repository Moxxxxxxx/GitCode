select 
	t.error_start_time,
	t.robot_code,
	t.error_code,
	t.error_detail,
	t.error_end_time
from 
(
	select 
		bn.robot_code,
		bn.id,
		bn.error_code,
		bn.alarm_detail as error_detail,
		bei.alarm_name as error_name,
		bn.warning_spec,
		substring_index(substring_index(bn.point_location, "x=", -1), ",", 1) as x,
		substring_index(substring_index(replace (bn.point_location, ")", ""), "y=", -1), ",", 1) as y,
		substr(case when bn.point_location like "%pointCode=%" then substring_index(bn.point_location, "pointCode=", -1) end,1,case when bn.point_location like "%pointCode=%" and POSITION(',' in substring_index(bn.point_location, "pointCode=", -1)) !=0 and POSITION(',' in substring_index(bn.point_location, "pointCode=", -1))  < POSITION(')' in substring_index(bn.point_location, "pointCode=", -1)) then POSITION(',' in substring_index(bn.point_location, "pointCode=", -1)) when bn.point_location like "%pointCode=%"  then POSITION(')' in substring_index(bn.point_location, "pointCode=", -1)) else null end -1) as point_code,
		bn.start_time as error_start_time,
		bn.end_time as error_end_time
	from phoenix_basic.basic_notification bn
	left join phoenix_basic.basic_error_info bei on bei.error_code = bn.error_code
	inner join phoenix_basic.basic_robot br on br.robot_code =bn.robot_code and br.usage_state ='using'
	where bn.alarm_module ='robot' and bn.alarm_level >=3
	and bn.point_location is not null 
	-- and bn.start_time >= '{{ start_time }}' and bn.start_time <= '{{ end_time }}'
	-- {% if robot_code %} AND br.robot_code IN {{ ja_concat_in(robot_code) }} {% endif %}  -- 机器人编码
	-- {% if error_code %} AND bn.error_code IN {{ ja_concat_in(error_code) }} {% endif %}  -- 错误码
)t 
where 1=1
-- {% if x and y %} AND t.x = '{{ x }}' AND t.y = '{{ y }}' {% endif %}


--------------------------------------

select 
	t.error_start_time,
	t.robot_code,
	t.error_code,
	t.error_detail,
	t.error_end_time
from 
(
	select 
		bn.robot_code,
		bn.id,
		bn.error_code,
		bn.alarm_detail as error_detail,
		bei.alarm_name as error_name,
		bn.warning_spec,
		substring_index(substring_index(bn.point_location, "x=", -1), ",", 1) as x,
		substring_index(substring_index(replace (bn.point_location, ")", ""), "y=", -1), ",", 1) as y,
		substr(case when bn.point_location like "%pointCode=%" then substring_index(bn.point_location, "pointCode=", -1) end,1,case when bn.point_location like "%pointCode=%" and POSITION(',' in substring_index(bn.point_location, "pointCode=", -1)) !=0 and POSITION(',' in substring_index(bn.point_location, "pointCode=", -1))  < POSITION(')' in substring_index(bn.point_location, "pointCode=", -1)) then POSITION(',' in substring_index(bn.point_location, "pointCode=", -1)) when bn.point_location like "%pointCode=%"  then POSITION(')' in substring_index(bn.point_location, "pointCode=", -1)) else null end -1) as point_code,
		bn.start_time as error_start_time,
		bn.end_time as error_end_time
	from phoenix_basic.basic_notification bn
	left join phoenix_basic.basic_error_info bei on bei.error_code = bn.error_code
	inner join phoenix_basic.basic_robot br on br.robot_code =bn.robot_code and br.usage_state ='using'
	where bn.alarm_module ='robot' and bn.alarm_level >=3
	and bn.point_location is not null 
	and bn.start_time >= '{{ start_time }}' and bn.start_time <= '{{ end_time }}'
	{% if robot_code %} AND br.robot_code IN {{ ja_concat_in(robot_code) }} {% endif %}  -- 机器人编码
	{% if error_code %} AND bn.error_code IN {{ ja_concat_in(error_code) }} {% endif %}  -- 错误码
)t 
where 1=1
{% if x and y %} AND t.x = '{{ x }}' AND t.y = '{{ y }}' {% endif %}

