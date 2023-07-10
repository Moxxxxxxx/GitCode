SELECT team_member AS team_member,
       DATE(work_date) AS work_date,
       day_type AS day_type,
       CASE
           WHEN WEEKDAY(work_date) = '0' THEN '周一'
           WHEN WEEKDAY(work_date) = '1' THEN '周二'
           WHEN WEEKDAY(work_date) = '2' THEN '周三'
           WHEN WEEKDAY(work_date) = '3' THEN '周四'
           WHEN WEEKDAY(work_date) = '4' THEN '周五'
           WHEN WEEKDAY(work_date) = '5' THEN '周六'
           WHEN WEEKDAY(work_date) = '6' THEN '周日'
       END AS `星期`,
       work_hour AS `登记日工时`,
       is_saturation AS `工时饱和度`,
       clock_in_work_hour AS `打卡工时`,
       CASE
           when attendance_working_time = attendance_off_time then concat(date_format(attendance_working_time, '%H:%i'), ' ~ N/A')
           else concat (date_format(attendance_working_time, '%H:%i'), ' ~ ', date_format(attendance_off_time, '%H:%i'))
       END AS `打卡范围`,
       date_format(attendance_working_time, '%H:%i:%s') AS `上班打卡时间`,
       attendance_working_place AS `上班打卡地点`,
       date_format(attendance_off_time, '%H:%i:%s') AS `下班打卡时间`,
       attendance_off_place AS `下班打卡地点`,
       travel_type AS `出勤类型`
FROM ads.ads_team_ft_role_member_work_efficiency
WHERE work_date >= STR_TO_DATE('2022-09-01', '%Y-%m-%d')
  AND work_date < STR_TO_DATE('2022-09-09', '%Y-%m-%d')
  AND team_ft IN ('系统中台')
  AND ((day_type IN ('工作日',
                     '工作日-哺乳假',
                     '调休',
                     '全天加班',
                     '下半天加班',
                     '上半天加班',
                     '上半天请假',
                     '下半天请假')
        or (day_type IN ('节假日',
                         '周末')
            and work_hour <>0))
       AND (is_need_fill_manhour = 1))
GROUP BY team_member,
         DATE(work_date),
         day_type
ORDER BY work_date DESC