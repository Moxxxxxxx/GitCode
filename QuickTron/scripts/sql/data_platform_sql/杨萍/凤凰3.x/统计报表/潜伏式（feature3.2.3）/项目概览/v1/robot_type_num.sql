select brt.robot_type_code,
       brt.robot_type_name,
       count(distinct br.robot_code) as robot_num,
       null                          as offline_maintenance_num
from phoenix_basic.basic_robot br
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where br.usage_state = 'using'
group by brt.robot_type_code, brt.robot_type_name