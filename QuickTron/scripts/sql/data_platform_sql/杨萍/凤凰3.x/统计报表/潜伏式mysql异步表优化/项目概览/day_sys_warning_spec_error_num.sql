-- 用于：统计报表->项目概览->系统故障类型统计 

SELECT warning_spec,
       count(distinct id) as breakdown_num
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and start_time >= { now_start_time }
group by warning_spec 