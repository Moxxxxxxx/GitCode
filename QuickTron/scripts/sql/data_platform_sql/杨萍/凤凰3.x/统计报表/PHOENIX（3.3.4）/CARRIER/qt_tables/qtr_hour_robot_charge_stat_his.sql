-- part1：mysql逻辑

-- mysql时间参数
set @now_time=sysdate();   --  当前时间
set @dt_day_start_time=date_format(sysdate(), '%Y-%m-%d 00:00:00');  -- 当天开始时间
set @dt_next_day_start_time=date_format(date_add(sysdate(), interval 1 day), '%Y-%m-%d 00:00:00'); --  明天开始时间

select 
@now_time as create_time,
@now_time as update_time,
date(@dt_day_start_time) as date_value,
t.robot_code,
br.robot_type_code,
brt.robot_type_name,
DATE_FORMAT(t.hour_value, '%Y-%m-%d %H:00:00.000000') as hour_value,
COALESCE(sum(create_charge_num),0) as create_charge_num,  
COALESCE(sum(end_charge_num),0) as charge_num,
COALESCE(sum(end_charge_duration),0) as charge_duration,
COALESCE(sum(end_charge_power_num),0) as  charge_power_num,
cast(case when COALESCE(sum(end_charge_num),0)!=0 then COALESCE(sum(end_charge_power_num),0)/COALESCE(sum(end_charge_num),0) else 0 end as decimal(20,10)) as avg_charge_power_num
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
left join phoenix_basic.basic_robot_type brt on brt.robot_type_code =br.robot_type_code
group by date_value,t.robot_code,br.robot_type_code,brt.robot_type_name,t.hour_value



-- part2：sqlserver逻辑

-- sqlserver时间参数
declare @now_time as datetime=sysdatetime() 
declare @dt_hour_start_time as datetime=FORMAT(sysdatetime(),'yyyy-MM-dd HH:00:00')
declare @dt_next_hour_start_time as datetime=FORMAT(DATEADD(hh,1,sysdatetime()),'yyyy-MM-dd HH:00:00')
declare @dt_day_start_time as datetime=FORMAT(sysdatetime(),'yyyy-MM-dd 00:00:00')
declare @dt_next_day_start_time as datetime=FORMAT(DATEADD(dd,1,sysdatetime()),'yyyy-MM-dd 00:00:00')
declare @dt_week_start_time as datetime=FORMAT(DATEADD(wk,datediff(wk,0,getdate()),0),'yyyy-MM-dd 00:00:00')
declare @dt_next_week_start_time as datetime=FORMAT(DATEADD(wk,datediff(wk,0,getdate()),7),'yyyy-MM-dd 00:00:00')


select 
@now_time as create_time,
@now_time as update_time,
FORMAT(cast(@dt_day_start_time as datetime),'yyyy-MM-dd') as date_value,
t.robot_code,
brt.robot_type_code,
br.robot_type_name,
FORMAT(cast(t.hour_value as datetime), 'yyyy-MM-dd HH:00:00.0000000') as hour_value,
COALESCE(sum(create_charge_num),0) as create_charge_num,  
COALESCE(sum(end_charge_num),0) as charge_num,
COALESCE(sum(end_charge_duration),0) as charge_duration,
COALESCE(sum(end_charge_power_num),0) as  charge_power_num,
cast(case when COALESCE(sum(end_charge_num),0)!=0 then cast(COALESCE(sum(end_charge_power_num),0) as decimal)/COALESCE(sum(end_charge_num),0) else 0 end as decimal(20,10)) as avg_charge_power_num
from 
(select 
FORMAT(cast(coalesce(enter_charging_time,bind_robot_time) as datetime), 'yyyy-MM-dd HH:00:00') as hour_value,
charging_robot as robot_code,
count(distinct id) as create_charge_num,
null as end_charge_num,
null as end_charge_duration,
null as end_charge_power_num
from phoenix_rms.dbo.robot_charging_history
where enter_charging_power is not null
and coalesce(enter_charging_time,bind_robot_time) >= @dt_day_start_time  and  coalesce(enter_charging_time,bind_robot_time) < @dt_next_day_start_time
group by FORMAT(cast(coalesce(enter_charging_time,bind_robot_time) as datetime), 'yyyy-MM-dd HH:00:00'),charging_robot
union all 
select 
FORMAT(cast(recover_charger_time as datetime), 'yyyy-MM-dd HH:00:00') as hour_value,
charging_robot as robot_code,
null as create_charge_num,
count(distinct id) as end_charge_num,
sum(DATEDIFF(ms,coalesce (enter_charging_time,bind_robot_time),recover_charger_time)/cast(1000 as decimal)) as  end_charge_duration,
-- sum(unix_timestamp(recover_charger_time)-unix_timestamp(coalesce (enter_charging_time,bind_robot_time))) as  end_charge_duration,
sum(recover_charger_power-coalesce (enter_charging_power,bind_robot_power)) as end_charge_power_num
from phoenix_rms.dbo.robot_charging_history
where  enter_charging_power is not null
and recover_charger_time >= @dt_day_start_time  and recover_charger_time < @dt_next_day_start_time
group by FORMAT(cast(recover_charger_time as datetime), 'yyyy-MM-dd HH:00:00'),charging_robot
)t
left join  phoenix_basic.dbo.basic_robot br on br.robot_code=t.robot_code
left join phoenix_basic.dbo.basic_robot_type brt on brt.robot_type_code =br.robot_type_code
group by t.robot_code,brt.robot_type_code,br.robot_type_name,t.hour_value





-- part3：异步表兼容逻辑

-- 定义时间参数
{% set now_time=datetime.datetime.now().strftime("'%Y-%m-%d %H:%M:%S'") %}  -- 客观当前时间
{% set dt_day_start_time=dt_relative_time(dt,default="%Y-%m-%d 00:00:00") %}  -- dt所在天的开始时间
{% set dt_next_day_start_time=dt_relative_time(dt,days=1,default="%Y-%m-%d 00:00:00") %}  -- dt所在天的下一天的开始时间


{% if db_type=="MYSQL" %}
-- mysql逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
date({{ dt_day_start_time }}) as date_value,
t.robot_code,
br.robot_type_code,
brt.robot_type_name,
DATE_FORMAT(t.hour_value, '%Y-%m-%d %H:00:00.000000') as hour_value,
COALESCE(sum(create_charge_num),0) as create_charge_num,
COALESCE(sum(end_charge_num),0) as charge_num,
COALESCE(sum(end_charge_duration),0) as charge_duration,
COALESCE(sum(end_charge_power_num),0) as  charge_power_num,
cast(case when COALESCE(sum(end_charge_num),0)!=0 then COALESCE(sum(end_charge_power_num),0)/COALESCE(sum(end_charge_num),0) else 0 end as decimal(20,10)) as avg_charge_power_num
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
left join phoenix_basic.basic_robot_type brt on brt.robot_type_code =br.robot_type_code
group by date_value,t.robot_code,br.robot_type_code,brt.robot_type_name,t.hour_value
{% elif db_type=="SQLSERVER" %}
-- sqlserver逻辑
select
{{ now_time }} as create_time,
{{ now_time }} as update_time,
FORMAT(cast({{ dt_day_start_time }} as datetime),'yyyy-MM-dd') as date_value,
t.robot_code,
br.robot_type_code,
brt.robot_type_name,
FORMAT(cast(t.hour_value as datetime), 'yyyy-MM-dd HH:00:00.0000000') as hour_value,
COALESCE(sum(create_charge_num),0) as create_charge_num,
COALESCE(sum(end_charge_num),0) as charge_num,
COALESCE(sum(end_charge_duration),0) as charge_duration,
COALESCE(sum(end_charge_power_num),0) as  charge_power_num,
cast(case when COALESCE(sum(end_charge_num),0)!=0 then cast(COALESCE(sum(end_charge_power_num),0) as decimal)/COALESCE(sum(end_charge_num),0) else 0 end as decimal(20,10)) as avg_charge_power_num
from 
(select
FORMAT(cast(coalesce(enter_charging_time,bind_robot_time) as datetime), 'yyyy-MM-dd HH:00:00') as hour_value,
charging_robot as robot_code,
count(distinct id) as create_charge_num,
null as end_charge_num,
null as end_charge_duration,
null as end_charge_power_num
from phoenix_rms.robot_charging_history
where enter_charging_power is not null
and coalesce(enter_charging_time,bind_robot_time) >= {{ dt_day_start_time }}  and  coalesce(enter_charging_time,bind_robot_time) < {{ dt_next_day_start_time }}
group by FORMAT(cast(coalesce(enter_charging_time,bind_robot_time) as datetime), 'yyyy-MM-dd HH:00:00'),charging_robot
union all
select
FORMAT(cast(recover_charger_time as datetime), 'yyyy-MM-dd HH:00:00') as hour_value,
charging_robot as robot_code,
null as create_charge_num,
count(distinct id) as end_charge_num,
sum(DATEDIFF(ms,coalesce (enter_charging_time,bind_robot_time),recover_charger_time)/cast(1000 as decimal)) as  end_charge_duration,
-- sum(unix_timestamp(recover_charger_time)-unix_timestamp(coalesce (enter_charging_time,bind_robot_time))) as  end_charge_duration,
sum(recover_charger_power-coalesce (enter_charging_power,bind_robot_power)) as end_charge_power_num
from phoenix_rms.robot_charging_history
where  enter_charging_power is not null
and recover_charger_time >= {{ dt_day_start_time }}  and recover_charger_time < {{ dt_next_day_start_time }}
group by FORMAT(cast(recover_charger_time as datetime), 'yyyy-MM-dd HH:00:00'),charging_robot
)t
left join  phoenix_basic.basic_robot br on br.robot_code=t.robot_code
left join phoenix_basic.basic_robot_type brt on brt.robot_type_code =br.robot_type_code
group by t.robot_code,br.robot_type_code,brt.robot_type_name,t.hour_value
{% endif %}