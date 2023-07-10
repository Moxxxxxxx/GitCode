SELECT warning_spec,
       count(distinct id) as breakdown_num
from phoenix_basic.basic_notification
where alarm_module in ('system', 'server')
  and alarm_level >= 3
  and start_time BETWEEN {now_start_time} and {now_end_time}
group by warning_spec 