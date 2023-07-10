####data1:qt_robot_charge_stat（包括当天数据）

select time_value,
       date_value,
       hour_value,
       robot_code,
       first_classification_name,
       charge_num,
       charge_time,
       add_power,
       avg_add_power,
       time_type
from qt_smartreport.qt_robot_charge_stat
union all 
select time_value,
       date_value,
       hour_value,
       robot_code,
       first_classification_name,
       charge_num,
       charge_time,
       add_power,
       avg_add_power,
       time_type
from qt_smartreport.qt_robot_charge_stat_realtime



####data2:qt_charger_charge_stat（包括当天数据）

select time_value,
       date_value,
       hour_value,
       charger_code,
       charge_num,
       charge_time,
       add_power,
       avg_add_power,
       time_type
from qt_smartreport.qt_charger_charge_stat
union all
select time_value,
       date_value,
       hour_value,
       charger_code,
       charge_num,
       charge_time,
       add_power,
       avg_add_power,
       time_type
from qt_smartreport.qt_charger_charge_stat_realtime
