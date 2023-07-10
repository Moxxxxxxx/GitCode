-- 用于：统计报表->机器人统计->充电统计->充电桩

select t.charger_code,
       bc.charger_port_type,
       t.hour_value,
	   t.create_charge_num,
       t.charge_num,
       t.charge_duration,
       t.charge_power_num,
       t.avg_charge_power_num
from qt_smartreport.qtr_hour_charger_charge_stat_his t
         left join phoenix_basic.basic_charger bc on bc.charger_code = t.charger_code
         inner join phoenix_basic.basic_map bm on bm.map_code = bc.map_code and bm.map_state = 'release'
where t.hour_value BETWEEN { start_time } and { end_time }







#############################################################################################
---  检查
#############################################################################################
-- { now_time }
-- { start_time }
-- { end_time }
set @now_time = sysdate(); --  当前时间
set @start_time = date_format(sysdate(), '%Y-%m-%d 00:00:00.000000000'); -- 筛选框开始时间  默认当天开始时间
set @end_time = date_format(sysdate(), '%Y-%m-%d %H:59:59.999999999'); --  筛选框结束时间  默认当前小时结束时间
select @now_time, @start_time, @end_time;


select t.charger_code,
       bc.charger_port_type,
       t.hour_value,
	   t.create_charge_num,
       t.charge_num,
       t.charge_duration,
       t.charge_power_num,
       t.avg_charge_power_num
from qt_smartreport.qtr_hour_charger_charge_stat_his t
         left join phoenix_basic.basic_charger bc on bc.charger_code = t.charger_code
         inner join phoenix_basic.basic_map bm on bm.map_code = bc.map_code and bm.map_state = 'release'
where t.hour_value BETWEEN @start_time and @end_time