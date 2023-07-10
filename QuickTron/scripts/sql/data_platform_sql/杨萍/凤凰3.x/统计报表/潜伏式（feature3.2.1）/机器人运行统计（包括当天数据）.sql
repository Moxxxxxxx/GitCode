####data1:qt_robot_state_time_stat（包括当天数据）
select time_value,
       date_value,
       hour_value,
       first_classification_name,
       robot_code,
       loading_busy_time,
       empty_busy_time,
       idle_time,
       charging_time,
       lock_time,
       error_time,
       offline_time,
       loading_busy_rate,
       time_type
from qt_smartreport.qt_robot_state_time_stat
union all 
select time_value,
       date_value,
       hour_value,
       first_classification_name,
       robot_code,
       loading_busy_time,
       empty_busy_time,
       idle_time,
       charging_time,
       lock_time,
       error_time,
       offline_time,
       loading_busy_rate,
       time_type
from qt_smartreport.qt_robot_state_time_stat_realtime 





####data2:qt_robot_state_time_rate_stat（包括当天数据）
select time_value,
       date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       total_time,
       state_durations,
       state_durations_rate,
       state_type
from qt_smartreport.qt_robot_state_time_rate_stat
union all
select time_value,
       date_value,
       hour_value,
       first_classification_name,
       robot_code,
       time_type,
       total_time,
       state_durations,
       state_durations_rate,
       state_type
from qt_smartreport.qt_robot_state_time_rate_stat_realtime


