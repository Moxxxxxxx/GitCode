set @now_time=sysdate();   --  当前时间
set @dt_hour_start_time=date_format(sysdate(), '%Y-%m-%d %H:00:00');   --  当前小时开始时间
set @dt_next_hour_start_time=date_format(DATE_ADD(sysdate(), INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00'); -- 下一个小时开始时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间
set @dt_week_start_time=date_format(DATE_SUB(sysdate(),INTERVAL WEEKDAY(sysdate()) + 0 DAY), '%Y-%m-%d 00:00:00'); -- 当前一周的开始时间
set @dt_next_week_start_time=date_format(DATE_SUB(CURRENT_DATE(),INTERVAL WEEKDAY(CURRENT_DATE()) -7 DAY), '%Y-%m-%d 00:00:00'); --  下一周的开始时间
select @now_time,@dt_hour_start_time,@dt_next_hour_start_time,@dt_day_start_time,@dt_next_day_start_time,@dt_week_start_time,@dt_next_week_start_time;


-- 插入数据（mysql参数）
-- insert into qt_smartreport.qtr_hour_robot_charge_stat_his(create_time,update_time,date_value,robot_code,robot_type_code,robot_type_name,hour_value,create_charge_num,charge_num,charge_duration,charge_power_num,avg_charge_power_num)
select 
@now_time as create_time,
@now_time as update_time,
date(@dt_day_start_time) as date_value,
t.robot_code,
brt.robot_type_code,
brt.robot_type_name,
DATE_FORMAT(t.hour_value, '%Y-%m-%d %H:00:00.000000') as hour_value,
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
and coalesce(enter_charging_time,bind_robot_time) >= @dt_day_start_time  and  coalesce(enter_charging_time,bind_robot_time) < @dt_next_day_start_time
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
and recover_charger_time >= @dt_day_start_time  and recover_charger_time < @dt_next_day_start_time
group by hour_value,charging_robot)t
left join  phoenix_basic.basic_robot br on br.robot_code=t.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
group by date_value,t.robot_code,brt.robot_type_code,brt.robot_type_name,t.hour_value



--------------------------------------------------------------------------------------------------------------------------
			
-- 插入数据（异步表）qt_smartreport.qtr_hour_robot_charge_stat_his	
-- {{ dt_relative_time(dt) }}
-- {{ now_time }}
-- {{ dt_hour_start_time }}
-- {{ dt_next_hour_start_time }}
-- {{ dt_day_start_time }}
-- {{ dt_next_day_start_time }}
-- {{ dt_week_start_time }}
-- {{ dt_next_week_start_time }}	


-- 定义时间参数
{% set now_time=datetime.datetime.now().strftime("'%Y-%m-%d %H:%M:%S.000000'") %}  -- 客观当前时间
{% set dt_hour_start_time=dt_relative_time(dt,default="%Y-%m-%d %H:00:00.000000") %}   -- dt所在小时的开始时间
{% set dt_next_hour_start_time=dt_relative_time(dt,hours=1,default="%Y-%m-%d %H:00:00.000000") %}  -- dt所在小时的下一个小时的开始时间
{% set dt_day_start_time=dt_relative_time(dt,default="%Y-%m-%d 00:00:00.000000") %}  -- dt所在天的开始时间
{% set dt_next_day_start_time=dt_relative_time(dt,days=1,default="%Y-%m-%d 00:00:00.000000") %}  -- dt所在天的下一天的开始时间
{% set dt_week_start_time=(dt - datetime.timedelta(days=dt.now().weekday())).strftime("'%Y-%m-%d 00:00:00.000000'") %}  -- dt所在周的开始时间
{% set dt_next_week_start_time=(dt + datetime.timedelta(days=7-dt.now().weekday())).strftime("'%Y-%m-%d 00:00:00.000000'") %}  -- dt所在周的下一周的开始时间



-- 插入逻辑 	
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
date({{ dt_day_start_time }}) as date_value,
t.robot_code,
brt.robot_type_code,
brt.robot_type_name,
DATE_FORMAT(t.hour_value, '%Y-%m-%d %H:00:00.000000') as hour_value,
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
and coalesce(enter_charging_time,bind_robot_time) >= {{ dt_day_start_time }}  and  coalesce(enter_charging_time,bind_robot_time) < {{ dt_next_day_start_time }}
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
and recover_charger_time >= {{ dt_day_start_time }}  and recover_charger_time < {{ dt_next_day_start_time }}
group by hour_value,charging_robot)t
left join  phoenix_basic.basic_robot br on br.robot_code=t.robot_code
left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
group by date_value,t.robot_code,brt.robot_type_code,brt.robot_type_name,t.hour_value