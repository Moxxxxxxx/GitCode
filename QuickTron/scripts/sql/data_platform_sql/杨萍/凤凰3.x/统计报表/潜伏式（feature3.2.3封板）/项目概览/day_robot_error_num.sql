select robot_code,
       count(distinct id) as breakdown_num
from ({tb_day_robot_error_detail}) t   -- day_robot_error_detail.sql
where start_time BETWEEN {now_start_time} and {now_end_time}
group by robot_code
order by breakdown_num desc
limit 5