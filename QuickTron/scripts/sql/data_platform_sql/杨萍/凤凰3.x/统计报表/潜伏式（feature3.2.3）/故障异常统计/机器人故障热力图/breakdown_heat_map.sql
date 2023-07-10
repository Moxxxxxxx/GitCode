SELECT b.x      AS x
     , b.y      AS y
     , count(1) AS num
FROM (select error_id
      from qt_smartreport.qt_day_robot_error_detail_his
      where start_time between {start_time} and {end_time}
      union
      select t1.id as error_id
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
               inner join phoenix_basic.basic_robot br on br.robot_code = t1.robot_code and br.usage_state = 'using'
      where t1.start_time between {start_time} and {end_time}) e
         LEFT JOIN
     (SELECT id
           , substring_index(substring_index(point_location, "x=", -1), ",", 1)                   as x
           , substring_index(substring_index(replace(point_location, ")", ""), "y=", -1), ",", 1) as y
      FROM phoenix_basic.basic_notification
      WHERE start_time between {start_time} and {end_time}
        AND point_location is not null
        AND point_location like '%x=%') b
     ON
         e.error_id = b.id
GROUP BY 1, 2
ORDER BY 3 DESC















###########################################################
######################################################################################################################

set @now_start_time = date_format(current_date(), '%Y-%m-%d 00:00:00.000000000');
set @now_end_time = date_format(current_date(), '%Y-%m-%d 23:59:59.999999999');
set @next_start_time = date_format(date_add(current_date(), interval 1 day), '%Y-%m-%d 00:00:00.000000000');
set @start_time = '2022-09-08 00:00:00.000000000';
set @end_time = '2022-09-08 09:59:59.999999999';


SELECT b.x      AS x
     , b.y      AS y
     , count(1) AS num
FROM (select error_id
      from qt_smartreport.qt_day_robot_error_detail_his
      where start_time between @start_time and @end_time
      union
      select t1.id as error_id
      from (select *
            from phoenix_basic.basic_notification
            where alarm_module = 'robot'
              and alarm_level >= 3
              and (end_time is null or start_time >= @now_start_time or
                   (start_time < @now_start_time and end_time >= @now_start_time))) t1
               inner join (select robot_code,
                                  end_time,
                                  min(id) as first_error_id
                           from phoenix_basic.basic_notification
                           where alarm_module = 'robot'
                             and alarm_level >= 3
                             and (end_time is null or start_time >= @now_start_time or
                                  (start_time < @now_start_time and end_time >= @now_start_time))
                           group by robot_code, end_time) t2
                          on t2.robot_code = t1.robot_code and t1.id = t2.first_error_id
               inner join phoenix_basic.basic_robot br on br.robot_code = t1.robot_code and br.usage_state = 'using'
      where t1.start_time between @start_time and @end_time) e
         LEFT JOIN
     (SELECT id
           , substring_index(substring_index(point_location, "x=", -1), ",", 1)                   as x
           , substring_index(substring_index(replace(point_location, ")", ""), "y=", -1), ",", 1) as y
      FROM phoenix_basic.basic_notification
      WHERE start_time between @start_time and @end_time
        AND point_location is not null
        AND point_location like '%x=%') b
     ON
         e.error_id = b.id
GROUP BY 1, 2
ORDER BY 3 DESC


######################################################################################################################

-------勇军原版逻辑-----------------------------------------------------------
SELECT
    b.x AS x
    ,b.y AS y
    ,count(1) AS num
FROM
    (
        SELECT error_id FROM qt_smartreport.qt_basic_notification_clear4 -- 历史
        WHERE start_time >= '{start_time}' AND start_time <= '{end_time}'
        UNION
        SELECT error_id FROM qt_smartreport.qt_basic_notification_clear4_realtime  -- 当天的
        WHERE start_time >= '{start_time}' AND start_time <= '{end_time}'
    ) e
LEFT JOIN
    (
        SELECT
            id
            ,substring_index(substring_index(point_location,"x=",-1),",",1) as x
            ,substring_index(substring_index(replace(point_location,")",""),"y=",-1),",",1) as y
        FROM
            phoenix_basic.basic_notification
        WHERE
            start_time >= '{start_time}' AND start_time <= '{end_time}'
            AND
            point_location is not null
            AND
            point_location like '%x=%'
    ) b
ON
    e.error_id = b.id
GROUP BY
    1,2
ORDER BY
    3 DESC
