select t.hour_start_time               as hour_value,
       COALESCE(t1.robot_error_num, 0) as robot_error_num,
       COALESCE(t1.sys_error_num, 0)   as sys_error_num
from (select th.day_hours                               as hour_start_time,
             DATE_ADD(th.day_hours, INTERVAL 60 MINUTE) as next_hour_start_time
      from (SELECT DATE_FORMAT(DATE_SUB(DATE_FORMAT({now_start_time}, '%Y-%m-%d 00:00:00'),
                                        INTERVAL
                                        (-(@u := @u + 1)) HOUR), '%Y-%m-%d %H:00:00') as day_hours
            FROM (SELECT a
                  FROM (SELECT '1' AS a UNION SELECT '2' UNION SELECT '3' UNION SELECT '4') AS a
                           JOIN(SELECT '1'
                                UNION
                                SELECT '2'
                                UNION
                                SELECT '3'
                                UNION
                                SELECT '4'
                                UNION
                                SELECT '5'
                                UNION
                                SELECT '6') AS b ON 1) AS b,
                 (SELECT @u := -1) AS i) th) t
         left join (select t.hour_value,
                           COALESCE(sum(t.robot_error_num), 0) as robot_error_num,
                           COALESCE(sum(t.sys_error_num), 0)   as sys_error_num
                    from (select DATE_FORMAT(t1.start_time, '%Y-%m-%d %H:00:00') as hour_value,
                                 count(distinct t1.id)                           as robot_error_num,
                                 null                                            as sys_error_num
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
                                               group by robot_code, end_time) t2
                                              on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
                                   inner join phoenix_basic.basic_robot br
                                              on br.robot_code = t1.robot_code and br.usage_state = 'using'
                          where t1.start_time >= {now_start_time}
                          group by hour_value
                          union all
                          select DATE_FORMAT(start_time, '%Y-%m-%d %H:00:00') as hour_value,
                                 null                                         as robot_error_num,
                                 count(distinct id)                           as sys_error_num
                          from phoenix_basic.basic_notification
                          where alarm_module in ('system', 'server')
                            and alarm_level >= 3
                            and start_time >= {now_start_time}
                          group by hour_value) t
                    group by t.hour_value) t1 on t1.hour_value = t.hour_start_time
					order by hour_value asc