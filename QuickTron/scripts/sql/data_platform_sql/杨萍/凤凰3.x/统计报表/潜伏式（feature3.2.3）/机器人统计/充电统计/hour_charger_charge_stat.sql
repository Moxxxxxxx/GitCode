select t.charger_code,
       bc.charger_port_type,
       t.hour_value,
       t.charge_num,
       t.charge_duration,
       t.charge_power_num,
       t.avg_charge_power_num
from qt_smartreport.qt_hour_charger_charge_stat_his t
         left join phoenix_basic.basic_charger bc on bc.charger_code = t.charger_code
         inner join phoenix_basic.basic_map bm on bm.map_code = bc.map_code and bm.map_state = 'release'
where t.hour_value BETWEEN {start_time} and {end_time}
union all
select rch.charger_code,
       bc.charger_port_type,
       DATE_FORMAT(rch.enter_charging_time, '%Y-%m-%d 00:00:00')                                    as hour_value,
       count(distinct rch.job_sn_enter)                                                             as charge_num,
       sum(unix_timestamp(rch.recover_charger_time) - unix_timestamp(rch.enter_charging_time))      as charge_duration,
       sum(rch.recover_charger_power - rch.enter_charging_power)                                    as charge_power_num,
       sum(rch.recover_charger_power - rch.enter_charging_power) /
       count(distinct rch.job_sn_enter)                                                             as avg_charge_power_num
from phoenix_rms.robot_charging_history rch
         left join phoenix_basic.basic_charger bc on bc.charger_code = rch.charger_code
         inner join phoenix_basic.basic_map bm on bm.map_code = bc.map_code and bm.map_state = 'release'
where rch.enter_charging_time >= {now_start_time}
  and rch.enter_charging_time < {next_start_time}
  and rch.enter_charging_time BETWEEN {start_time} and {end_time}
group by rch.charger_code, bc.charger_port_type, hour_value
