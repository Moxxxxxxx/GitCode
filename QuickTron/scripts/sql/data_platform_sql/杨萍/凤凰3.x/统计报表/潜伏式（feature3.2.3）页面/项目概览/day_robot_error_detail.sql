select t1.*
from (select *
      from phoenix_basic.basic_notification
      where alarm_module = 'robot'
        and alarm_level >= 3
        and (end_time is null or start_time >= {now_start_time} or
             (start_time < {now_start_time} and end_time >= {now_start_time}))) t1
         inner join (select robot_code,
                            end_time,
                            min(id) as first_error_id
                     from phoenix_basic.basic_notification
                     where alarm_module = 'robot'
                       and alarm_level >= 3
                       and (end_time is null or start_time >= {now_start_time} or
                            (start_time < {now_start_time} and end_time >= {now_start_time}))
                     group by robot_code, end_time) t2 on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id