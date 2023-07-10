-- 表1：qt_smartreport.qtr_hour_robot_charge_stat_his

-- step1:删除相关数据（qtr_hour_robot_charge_stat_his）
DELETE
FROM qt_smartreport.qtr_hour_robot_charge_stat_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);



-- step2:插入相关数据（qtr_hour_robot_charge_stat_his）
insert into qt_smartreport.qtr_hour_robot_charge_stat_his(create_time,update_time,date_value,robot_code,robot_type_code,robot_type_name,hour_value,create_charge_num,charge_num,charge_duration,charge_power_num,avg_charge_power_num)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
date_add(CURRENT_DATE(), interval -1 day) as date_value,
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
and coalesce(enter_charging_time,bind_robot_time) >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')  and  coalesce(enter_charging_time,bind_robot_time) < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')
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
where  enter_charging_power is not null
and recover_charger_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')  and recover_charger_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')
group by hour_value,charging_robot)t
left join  phoenix_basic.basic_robot br on br.robot_code=t.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
group by date_value,t.robot_code,brt.robot_type_code,brt.robot_type_name,t.hour_value



-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_hour_robot_charge_stat_his;
insert into qt_smartreport.qtr_hour_robot_charge_stat_his(create_time,update_time,date_value,robot_code,robot_type_code,robot_type_name,hour_value,create_charge_num,charge_num,charge_duration,charge_power_num,avg_charge_power_num)
select created_time as create_time,updated_time as update_time,date_value,robot_code,robot_type_code,robot_type_name,hour_value,create_charge_num,charge_num,charge_duration,charge_power_num,avg_charge_power_num
from qt_smartreport.qt_hour_robot_charge_stat_his;



--------------------------------------------------------------------------------
-- 表2：qt_smartreport.qtr_hour_charger_charge_stat_his


-- step1:删除相关数据（qtr_hour_charger_charge_stat_his）
DELETE
FROM qt_smartreport.qtr_hour_charger_charge_stat_his
where date_value=date_add(CURRENT_DATE(), interval -1 day);


	
-- step2:插入相关数据（qtr_hour_charger_charge_stat_his）
insert into qt_smartreport.qtr_hour_charger_charge_stat_his(create_time,update_time,date_value,charger_code,charger_port_type,hour_value,create_charge_num,charge_num,charge_duration,charge_power_num,avg_charge_power_num)
select 
CURRENT_TIMESTAMP as create_time,
CURRENT_TIMESTAMP as update_time,
date_add(CURRENT_DATE(), interval -1 day) as date_value,
t.charger_code,
bc.charger_port_type,
t.hour_value,
COALESCE(sum(create_charge_num),0) as create_charge_num,  
COALESCE(sum(end_charge_num),0) as charge_num,
COALESCE(sum(end_charge_duration),0) as charge_duration,
COALESCE(sum(end_charge_power_num),0) as  charge_power_num,
case when COALESCE(sum(end_charge_num),0)!=0 then COALESCE(sum(end_charge_power_num),0)/COALESCE(sum(end_charge_num),0) else 0 end as avg_charge_power_num
from 
(select 
DATE_FORMAT(coalesce(enter_charging_time,bind_robot_time), '%Y-%m-%d %H:00:00') as hour_value,
charger_code,
count(distinct id) as create_charge_num,
null as end_charge_num,
null as end_charge_duration,
null as end_charge_power_num
from phoenix_rms.robot_charging_history
where enter_charging_power is not null
and coalesce(enter_charging_time,bind_robot_time) >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')  and  coalesce(enter_charging_time,bind_robot_time) < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')
group by hour_value,charger_code
union all 
select 
DATE_FORMAT(recover_charger_time, '%Y-%m-%d %H:00:00') as hour_value,
charger_code,
null as create_charge_num,
count(distinct id) as end_charge_num,
sum(unix_timestamp(recover_charger_time)-unix_timestamp(coalesce (enter_charging_time,bind_robot_time))) as  end_charge_duration,
sum(recover_charger_power-coalesce (enter_charging_power,bind_robot_power)) as end_charge_power_num
from phoenix_rms.robot_charging_history
where enter_charging_power is not null
and recover_charger_time >= date_format(date_add(current_date(), interval -1 day), '%Y-%m-%d 00:00:00.000000000')  and recover_charger_time < date_format(current_date(), '%Y-%m-%d 00:00:00.000000000')
group by hour_value,charger_code)t
left join phoenix_basic.basic_charger bc on bc.charger_code=t.charger_code
group by date_value,t.charger_code,bc.charger_port_type,t.hour_value




-- 备注：老表数据同步
TRUNCATE TABLE qt_smartreport.qtr_hour_charger_charge_stat_his;
insert into qt_smartreport.qtr_hour_charger_charge_stat_his(create_time,update_time,date_value,charger_code,charger_port_type,hour_value,create_charge_num,charge_num,charge_duration,charge_power_num,avg_charge_power_num)
select created_time as create_time,updated_time as update_time,date_value,charger_code,charger_port_type,hour_value,create_charge_num,charge_num,charge_duration,charge_power_num,avg_charge_power_num
from qt_smartreport.qt_hour_charger_charge_stat_his;




