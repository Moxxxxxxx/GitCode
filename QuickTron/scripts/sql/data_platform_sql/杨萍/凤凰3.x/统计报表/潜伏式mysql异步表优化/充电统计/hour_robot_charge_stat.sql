-- 用于：统计报表->机器人统计->充电统计->机器人 

SELECT t.robot_code,
       brt.robot_type_code,
       brt.robot_type_name,
       t.hour_value,
	   t.create_charge_num,	   
       t.charge_num,
       t.charge_duration,
       t.charge_power_num,
       t.avg_charge_power_num
from qt_smartreport.qtr_hour_robot_charge_stat_his t
         inner join phoenix_basic.basic_robot br on br.robot_code = t.robot_code and br.usage_state = 'using'
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
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

SELECT t.robot_code,
       brt.robot_type_code,
       brt.robot_type_name,
       t.hour_value,
	   t.create_charge_num,	   
       t.charge_num,
       t.charge_duration,
       t.charge_power_num,
       t.avg_charge_power_num
from qt_smartreport.qtr_hour_robot_charge_stat_his t
         inner join phoenix_basic.basic_robot br on br.robot_code = t.robot_code and br.usage_state = 'using'
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
where t.hour_value BETWEEN @start_time and @end_time