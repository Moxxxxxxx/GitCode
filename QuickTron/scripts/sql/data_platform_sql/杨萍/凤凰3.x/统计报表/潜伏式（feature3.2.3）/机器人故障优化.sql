#step1:故障等级>=3（现场需要人工介入的机器人故障）

drop table if EXISTS qt_smartreport.qt_basic_notification_temp1;
create table if not EXISTS qt_smartreport.qt_basic_notification_temp1
as
select *
from phoenix_basic.basic_notification 
where alarm_module = 'robot'
and alarm_level >= 3
and (
(start_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and coalesce(end_time, sysdate()) <date_format(sysdate(), '%Y-%m-%d 00:00:00'))or 
(start_time >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and start_time < date_format(sysdate(), '%Y-%m-%d 00:00:00') and coalesce(end_time, sysdate()) >=date_format(sysdate(), '%Y-%m-%d 00:00:00'))or 
(start_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and coalesce(end_time, sysdate()) >= date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and coalesce(end_time, sysdate())< date_format(sysdate(), '%Y-%m-%d 00:00:00'))or 
(start_time < date_format(date_add(sysdate(), interval -1 day), '%Y-%m-%d 00:00:00') and coalesce(end_time, sysdate()) >= date_format(sysdate(), '%Y-%m-%d 00:00:00'))
)
;


#step2:按故障开始时间排序，相同故障码上一条的结束时间与下一条的开始时间间隔<60s，取第一条

drop table if EXISTS qt_smartreport.qt_basic_notification_temp2;
create table if not EXISTS qt_smartreport.qt_basic_notification_temp2
as
select 
t5.error_id,
t5.robot_code,
t5.error_code,
t5.start_time,
t5.end_time,
t5.pre_error_id,
t5.pre_start_time,
t5.pre_end_time,
t5.diff_seconds
from 
(select 
t3.*,
t4.start_time as pre_start_time,
t4.end_time as pre_end_time,
UNIX_TIMESTAMP(t3.start_time)-UNIX_TIMESTAMP(t4.end_time) as diff_seconds,
case
                                            when t3.pre_error_id is null then 1
											when UNIX_TIMESTAMP(t3.start_time)-UNIX_TIMESTAMP(t4.end_time) < 60 then 0
                                            else 1 end                                      is_effective																				
from 
(select t1.id as error_id,t1.robot_code,t1.error_code,t1.start_time,t1.end_time,
max(t2.id) as pre_error_id
from qt_smartreport.qt_basic_notification_temp1 t1 
left join qt_smartreport.qt_basic_notification_temp1 t2 on t2.robot_code=t1.robot_code and t2.error_code=t1.error_code and t2.start_time<t1.start_time
group by t1.id,t1.robot_code,t1.error_code,t1.start_time,t1.end_time)t3
left join qt_smartreport.qt_basic_notification_temp1 t4 on t4.robot_code=t3.robot_code and t4.id=t3.pre_error_id)t5 
where t5.is_effective=1
;

#step3:机器人多条故障均没有结束时间or结束时间相同，取第一条

drop table if EXISTS qt_smartreport.qt_basic_notification_temp3;
create table if not EXISTS qt_smartreport.qt_basic_notification_temp3
as
select 
t1.error_id,t1.robot_code,t1.error_code,t1.start_time,t1.end_time
from qt_smartreport.qt_basic_notification_temp2 t1 
inner join (select 
robot_code,end_time,min(error_id) as first_error_id 
from qt_smartreport.qt_basic_notification_temp2
group by robot_code,end_time)t on t.robot_code=t1.robot_code and t.first_error_id=t1.error_id
;


#step4:按故障开始时间排序，机器人多条故障结束时间间隔<3s，取第一条


drop table if EXISTS qt_smartreport.qt_basic_notification_temp4;
create table if not EXISTS qt_smartreport.qt_basic_notification_temp4
as
select 
t5.error_id,t5.robot_code,t5.error_code,t5.start_time,t5.end_time,t5.pre_start_time,t5.pre_end_time,t5.diff_seconds,t5.is_effective
from 
(select 
t3.*,
t4.start_time as pre_start_time,
t4.end_time as pre_end_time,
UNIX_TIMESTAMP(t3.end_time)-UNIX_TIMESTAMP(t4.end_time) as diff_seconds,
case
                                            when t3.pre_error_id is null then 1
											when UNIX_TIMESTAMP(t3.end_time)-UNIX_TIMESTAMP(t4.end_time) < 3 then 0
                                            else 1 end                                      is_effective	
from 
(select 
t1.error_id,t1.robot_code,t1.error_code,t1.start_time,t1.end_time,
max(t2.error_id) as pre_error_id
from qt_smartreport.qt_basic_notification_temp3 t1 
left join qt_smartreport.qt_basic_notification_temp3 t2 on t2.robot_code=t1.robot_code and t2.start_time<t1.start_time
group by t1.error_id,t1.robot_code,t1.error_code,t1.start_time,t1.end_time)t3 
left join qt_smartreport.qt_basic_notification_temp3 t4  on t4.robot_code=t3.robot_code and t4.error_id=t3.pre_error_id)t5 
where t5.is_effective=1
;


select * from qt_smartreport.qt_basic_notification_temp4



