SELECT t.robot_code,
       brt.robot_type_code,
       brt.robot_type_name,
       t.hour_value,
	   t.create_charge_num,	   
       t.charge_num,
       t.charge_duration,
       t.charge_power_num,
       t.avg_charge_power_num
from qt_smartreport.qt_hour_robot_charge_stat_his t
         inner join phoenix_basic.basic_robot br on br.robot_code = t.robot_code and br.usage_state = 'using'
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where t.hour_value BETWEEN {start_time} and {end_time}
union all
select 
t.robot_code,
brt.robot_type_code,
brt.robot_type_name,
t.hour_value,
COALESCE(sum(create_charge_num),0) as create_charge_num,  
COALESCE(sum(end_charge_num),0) as charge_num,
COALESCE(sum(end_charge_duration),0) as charge_duration,
COALESCE(sum(end_charge_power_num),0) as  charge_power_num,
case when COALESCE(sum(end_charge_num),0)!=0 then COALESCE(sum(end_charge_power_num),0)/COALESCE(sum(end_charge_num),0) else 0 end as avg_charge_power_num
from 
(select 
DATE_FORMAT(coalesce(enter_charging_time,bind_robot_time), '%Y-%m-%d %H:00:00') as hour_value,
charging_robot as robot_code,
count(distinct id) as create_charge_num,
null as end_charge_num,
null as end_charge_duration,
null as end_charge_power_num
from phoenix_rms.robot_charging_history
where enter_charging_power is not null
and coalesce(enter_charging_time,bind_robot_time) >= {now_start_time}  and  coalesce(enter_charging_time,bind_robot_time) < {next_start_time}
and coalesce(enter_charging_time,bind_robot_time) BETWEEN {start_time} and {end_time}
group by hour_value,charging_robot
union all 
select 
DATE_FORMAT(recover_charger_time, '%Y-%m-%d %H:00:00') as hour_value,
charging_robot as robot_code,
null as create_charge_num,
count(distinct id) as end_charge_num,
sum(unix_timestamp(recover_charger_time)-unix_timestamp(coalesce (enter_charging_time,bind_robot_time))) as  end_charge_duration,
sum(recover_charger_power-coalesce (enter_charging_power,bind_robot_power)) as end_charge_power_num
from phoenix_rms.robot_charging_history
where enter_charging_power is not null
and recover_charger_time >= {now_start_time}  and recover_charger_time < {next_start_time}
and recover_charger_time BETWEEN {start_time} and {end_time}
group by hour_value,charging_robot)t
left join  phoenix_basic.basic_robot br on br.robot_code=t.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
group by t.robot_code,brt.robot_type_code,brt.robot_type_name,t.hour_value