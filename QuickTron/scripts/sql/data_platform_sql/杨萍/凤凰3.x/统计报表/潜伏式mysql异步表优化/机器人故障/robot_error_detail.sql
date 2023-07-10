-- 用于：统计报表->故障异常统计->机器人故障统计

select bn.robot_code,
       brt.robot_type_code,
       brt.robot_type_name,
       bn.id                                                                            as error_id,
       bn.error_code,
       bn.start_time                                                                    as error_start_time,
       bn.end_time                                                                      as error_end_time,
       unix_timestamp(COALESCE(bn.end_time, { now_time })) - unix_timestamp(bn.start_time) as error_time,
       bn.alarm_level,
       bn.alarm_detail,
       bn.alarm_service,
       bn.warning_spec
from (select distinct robot_code, error_id, start_time
      from qt_smartreport.qtr_day_robot_error_list_his
      where start_time BETWEEN { start_time } and { end_time }) te
         inner join phoenix_basic.basic_notification bn on bn.id = te.error_id
         left join phoenix_basic.basic_robot br on br.robot_code = bn.robot_code
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id





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

select bn.robot_code,
       brt.robot_type_code,
       brt.robot_type_name,
       bn.id                                                                            as error_id,
       bn.error_code,
       bn.start_time                                                                    as error_start_time,
       bn.end_time                                                                      as error_end_time,
       unix_timestamp(COALESCE(bn.end_time, @now_time)) - unix_timestamp(bn.start_time) as error_time,
       bn.alarm_level,
       bn.alarm_detail,
       bn.alarm_service,
       bn.warning_spec
from (select distinct robot_code, error_id, start_time
      from qt_smartreport.qtr_day_robot_error_list_his
      where start_time BETWEEN @start_time and @end_time) te
         inner join phoenix_basic.basic_notification bn on bn.id = te.error_id
         left join phoenix_basic.basic_robot br on br.robot_code = bn.robot_code
         left join phoenix_basic.basic_robot_type brt on brt.id = br.robot_type_id
            