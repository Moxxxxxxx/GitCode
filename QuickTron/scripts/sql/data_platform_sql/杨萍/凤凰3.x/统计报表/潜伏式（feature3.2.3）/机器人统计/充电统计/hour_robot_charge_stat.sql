SELECT t.robot_code,
       brt.robot_type_code,
       brt.robot_type_name,
       t.hour_value,
       t.charge_num,
       t.charge_duration,
       t.charge_power_num,
       t.avg_charge_power_num
from qt_smartreport.qt_hour_robot_charge_stat_his t
         inner join phoenix_basic.basic_robot br on br.robot_code = t.robot_code and br.usage_state = 'using'
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where t.hour_value BETWEEN {start_time} and {end_time}
union all
select rch.charging_robot                                                                      as robot_code,
       brt.robot_type_code,
       brt.robot_type_name,
       DATE_FORMAT(rch.enter_charging_time, '%Y-%m-%d 00:00:00')                               as hour_value,
       count(distinct rch.job_sn_enter)                                                        as charge_num,
       sum(unix_timestamp(rch.recover_charger_time) - unix_timestamp(rch.enter_charging_time)) as charge_duration,
       sum(rch.recover_charger_power - rch.enter_charging_power)                               as charge_power_num,
       sum(rch.recover_charger_power - rch.enter_charging_power) /
       count(distinct rch.job_sn_enter)                                                        as avg_charge_power_num
from phoenix_rms.robot_charging_history rch
         left join phoenix_basic.basic_robot br on br.robot_code = rch.charging_robot
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where rch.enter_charging_time >= {now_start_time}
  and rch.enter_charging_time < {next_start_time}
  and rch.enter_charging_time BETWEEN {start_time} and {end_time}
group by rch.charging_robot, brt.robot_type_code, brt.robot_type_name, hour_value
