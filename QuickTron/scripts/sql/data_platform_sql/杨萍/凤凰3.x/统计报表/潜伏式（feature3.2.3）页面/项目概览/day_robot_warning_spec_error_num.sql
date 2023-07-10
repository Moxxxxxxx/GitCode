select warning_spec,
       error_code,
       alarm_detail       as error_detail,
       count(distinct id) as breakdown_num
from ({tb_day_robot_error_detail}) t -- day_robot_error_detail.sql
where start_time BETWEEN {now_start_time} and {now_end_time}
group by warning_spec, error_code, alarm_detail
order by breakdown_num desc
limit 5