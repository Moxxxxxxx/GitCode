select
	COALESCE(max(elevator_num),0) as elevator_num,
	COALESCE(null,0) as unloader_num,
	COALESCE(null,0) as door_num,
	COALESCE(max(other_device_num),0) as other_device_num
from
(
	select 
		count(distinct equipment_code) as elevator_num,
		null as unloader_num,
		null as door_num,
		null as other_device_num
	from phoenix_basic.basic_elevator
	
	union all 
	
	select 
		null as elevator_num,
		null as unloader_num,
		null as door_num,
		count(distinct equipment_code) as other_device_num
	from phoenix_basic.basic_equipment
) t
