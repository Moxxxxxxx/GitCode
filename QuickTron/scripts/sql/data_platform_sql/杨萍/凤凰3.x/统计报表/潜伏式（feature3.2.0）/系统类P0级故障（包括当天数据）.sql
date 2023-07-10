
####data1:qt_notification_system_module_p0_object_stat（包括当天数据）

select time_value,
       date_value,
       hour_value,
       object_code,
       object_type,
       first_classification_name,
       add_notification_num,
       notification_num,
       notification_time,
       notification_rate,
	   notification_rate_fenzi,
	   notification_rate_fenmu,
       mtbf,
	   mtbf_fenzi,
	   mtbf_fenmu,
       mttr,
	   mttr_fenzi,
	   mttr_fenmu,
       time_type
from qt_smartreport.qt_notification_system_module_p0_object_stat
--where notification_num>0
union all
select time_value,
       date_value,
       hour_value,
       object_code,
       object_type,
       first_classification_name,
       add_notification_num,
       notification_num,
       notification_time,
       notification_rate,
	   notification_rate_fenzi,
	   notification_rate_fenmu,
       mtbf,
	   mtbf_fenzi,
	   mtbf_fenmu,
       mttr,
	   mttr_fenzi,
	   mttr_fenmu,
       time_type
from qt_smartreport.qt_notification_system_module_p0_object_stat_realtime
--where notification_num>0





####data2:qt_notification_system_module_p0_index_stat（包括当天数据）

select time_value,
       date_value,
       hour_value,
       time_type,
       index_value,
	   index_value_fenzi,
	   index_value_fenmu,
       value_type
from qt_smartreport.qt_notification_system_module_p0_index_stat
union all
select time_value,
       date_value,
       hour_value,
       time_type,
       index_value,
	   index_value_fenzi,
	   index_value_fenmu,	   
       value_type
from qt_smartreport.qt_notification_system_module_p0_index_stat_realtime






####data3:系统统计时间段内P0级故障明细（包括当天数据）
select distinct hour_start_time       as time_value,
                date(hour_start_time) as date_value,
                HOUR(hour_start_time) as hour_value,
				object_code,
                notification_id,
                '小时'                  as time_type
from qt_smartreport.qt_notification_system_module_p0_time_hour_detail
where the_hour_cost_seconds is not null
union all
select distinct hour_start_time       as time_value,
                date(hour_start_time) as date_value,
                HOUR(hour_start_time) as hour_value,
				object_code,
                notification_id,
                '小时'                  as time_type
from qt_smartreport.qt_notification_system_module_p0_time_hour_detail_realtime
where the_hour_cost_seconds is not null
union all
select distinct hour_start_time       as time_value,
                date(hour_start_time) as date_value,
                null                  as hour_value,
				object_code,
                notification_id,
                '天'                   as time_type
from qt_smartreport.qt_notification_system_module_p0_time_hour_detail
where the_hour_cost_seconds is not null
union all
select distinct hour_start_time       as time_value,
                date(hour_start_time) as date_value,
                null                  as hour_value,
				object_code,
                notification_id,
                '天'                   as time_type
from qt_smartreport.qt_notification_system_module_p0_time_hour_detail_realtime
where the_hour_cost_seconds is not null



####data4:系统P0级故障明细（包括当天数据）
select distinct t.notification_id,
                t.error_code,
				b.alarm_detail,
                t.alarm_module,
                t.alarm_service,
                t.alarm_type,
                CONCAT(t.alarm_level,'级') AS alarm_level,
                t.object_type,
                t.object_code,
                t.robot_code,
                t.first_classification_name,
                t.job_order,
                t.device_code,
                t.server_code,
                t.start_time,
             coalesce(cast(b.end_time as datetime),'未结束') as end_time,
			 (UNIX_TIMESTAMP(coalesce (b.end_time,sysdate()))-UNIX_TIMESTAMP(t.start_time))*1000 as duration
from (select t1.notification_id,
             t1.error_code,
             t1.alarm_module,
             t1.alarm_service,
             t1.alarm_type,
             t1.alarm_level,
             t1.object_type,
             t1.object_code,
             t1.robot_code,
             t1.first_classification_name,
             t1.job_order,
             t1.device_code,
             t1.server_code,
             t1.start_time
      from qt_smartreport.qt_notification_system_module_p0_time_hour_detail t1
      where t1.the_hour_cost_seconds is not null
      union all
      select t2.notification_id,
             t2.error_code,
             t2.alarm_module,
             t2.alarm_service,
             t2.alarm_type,
             t2.alarm_level,
             t2.object_type,
             t2.object_code,
             t2.robot_code,
             t2.first_classification_name,
             t2.job_order,
             t2.device_code,
             t2.server_code,
             t2.start_time
      from qt_smartreport.qt_notification_system_module_p0_time_hour_detail_realtime t2
      where t2.the_hour_cost_seconds is not null) t
         left join phoenix_basic.basic_notification b on b.id = t.notification_id