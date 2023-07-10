select
	COALESCE(ROUND(max(warehouse_area), 0),0) as warehouse_area,
	COALESCE(max(robot_num),0) as robot_num,
	COALESCE(max(charger_num),0) as charger_num
from
(
    select
		(map_length / 1000) * (map_width / 1000) as warehouse_area,
		null as robot_num,
		null as charger_num
    from phoenix_basic.basic_map
    where map_state='release'

    union all
	
    select
		null as warehouse_area,
		count(distinct robot_code) as robot_num,
		null as charger_num
    from phoenix_basic.basic_robot
    where usage_state='using'

    union all
	
    select
		null as warehouse_area,
		null as robot_num,
		count(distinct bc.charger_code) as charger_num
    from phoenix_basic.basic_charger bc
    inner join phoenix_basic.basic_map bm on bm.map_code=bc.map_code and bm.map_state='release'
) t
