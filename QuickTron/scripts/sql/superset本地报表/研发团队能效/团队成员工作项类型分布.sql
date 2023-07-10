--团队成员工作项类型分布 ads_team_ft_member_issue_type

with day_scope as
(
SELECT tud.team_ft,
       tud.team_group,
	   tud.team_sub_group,
       tud.user_name as team_member,
       tud.emp_position,
       tud.is_job,
       tud.role_type,
       '日' as run_type,
       tud.day_scope as time_value,
       IF(tud.work_overtime_type is null,IF(tud.leave_type is null,tud.day_type,IF(tud.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tud.leave_type),IF(tud.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tud.leave_type))),tud.work_overtime_type) as day_type,
       tud.work_type,
       cast(nvl(t.handle_ones_num, 0) as bigint) as handle_ones_num
FROM 
(
  SELECT tu.team_ft,
         tu.team_group,
	     tu.team_sub_group,
         tu.user_name,
         tu.user_email,
         tu.role_type,
         tu.is_job,
         tu.emp_position,
         td.day_scope,
         CASE when td.day_type = 0 then '工作日'
              when td.day_type = 1 then '周末'
              when td.day_type = 2 then '节假日'
              when td.day_type = 3 then '调休' end as day_type,  
         tmp.leave_type,
         tmp1.work_overtime_type,
         t.work_type
  FROM 
  (
    SELECT DISTINCT tg.org_name_2 as team_ft,
                    tg.org_name_3 as team_group,
                    tg.org_name_4 as team_sub_group,
                    te.emp_id,
                    te.emp_name   as user_name,
                    te.email      as user_email,
                    tmp.org_role_type as role_type,
                    te.is_job,
                    te.hired_date,
                    te.quit_date,
                    te.emp_position
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
    LEFT JOIN 
    (
      SELECT DISTINCT m.emp_id,
                      m.emp_name,
                      m.org_id,
                      m.org_role_type,
                      row_number()over(PARTITION by m.emp_id,m.emp_name order by m.org_role_type desc)rn
      FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
      WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.is_valid = 1
    )tmp
    ON te.emp_id = tmp.emp_id AND tmp.rn = 1
    LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg 
    ON tg.org_id = tmp.org_id AND tg.org_company_name = '上海快仓智能科技有限公司'
    WHERE 1 = 1
      AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
      AND (tg.org_name_2 IN ('AMR FT','智能搬运FT','硬件自动化','箱式FT','系统中台','制造部') OR (tg.org_name_2 is NULL AND te.is_job = 0))
  ) tu
  LEFT JOIN
  (
    SELECT DISTINCT TO_DATE(days) as day_scope,
                    day_type
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1 AND days >= '2021-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1) 
  ) td
  LEFT JOIN
  (
    SELECT DISTINCT if(t1.project_classify_name = '工单问题汇总','工单',t1.issue_type_cname) as work_type
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t1 
    WHERE 1 = 1 AND t1.status = 1 AND t1.issue_type_cname IN ('缺陷','任务','需求') 
  ) t
  LEFT JOIN 
  (
    SELECT l1.originator_user_id,
           l1.stat_date,
           case when l2.leave_type is null THEN l1.leave_type else '全天请假' END as leave_type
    FROM 
    (
      SELECT l.originator_user_id,
             cast(l.leave_date as date) as stat_date,
             CASE when l.period_type = '全天' THEN '全天请假'
                  when l.period_type = '下午' THEN '下半天请假'
                  when l.period_type = '上午' THEN '上半天请假' 
                  when l.period_type = '其它' THEN '哺乳假' end as leave_type,
             row_number()over(PARTITION by l.originator_user_id,cast(l.leave_date as date) order by CASE when l.period_type = '全天' THEN '全天请假'
                                                                                                         when l.period_type = '下午' THEN '下半天请假'
                                                                                                         when l.period_type = '上午' THEN '上半天请假' 
                                                                                                         when l.period_type = '其它' THEN '哺乳假' end asc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l
      WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1) 
    )l1
    LEFT JOIN 
    (
      SELECT l.originator_user_id,
             cast(l.leave_date as date) as stat_date,
             CASE when l.period_type = '全天' THEN '全天请假'
                  when l.period_type = '下午' THEN '下半天请假'
                  when l.period_type = '上午' THEN '上半天请假' 
                  when l.period_type = '其它' THEN '哺乳假' end as leave_type,
             row_number()over(PARTITION by l.originator_user_id,cast(l.leave_date as date) order by CASE when l.period_type = '全天' THEN '全天请假'
                                                                                                         when l.period_type = '下午' THEN '下半天请假'
                                                                                                         when l.period_type = '上午' THEN '上半天请假' 
                                                                                                         when l.period_type = '其它' THEN '哺乳假' end asc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l
      WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1)
    )l2
    ON l1.originator_user_id = l2.originator_user_id AND l1.stat_date = l2.stat_date AND l1.leave_type != l2.leave_type
    WHERE l1.rn = 1 
  )tmp
  ON td.day_scope = tmp.stat_date AND tu.emp_id = tmp.originator_user_id
  LEFT JOIN 
  (
    SELECT l1.applicant_userid,
           l1.stat_date,
           case when l2.work_overtime_type is null THEN l1.work_overtime_type else '全天加班' END as work_overtime_type
    FROM 
    (
      SELECT l.applicant_userid,
             cast(l.overtime_date as date) as stat_date,
             CASE when l.period_type = '全天' THEN '全天加班'
                  when l.period_type = '下午' THEN '下半天加班'
                  when l.period_type = '上午' THEN '上半天加班' end as work_overtime_type,
             row_number()over(PARTITION by l.applicant_userid,cast(l.overtime_date as date) order by CASE when l.period_type = '全天' THEN '全天加班'
                                                                                                          when l.period_type = '下午' THEN '下半天加班'
                                                                                                          when l.period_type = '上午' THEN '上半天加班' end asc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_work_overtime_dayily_info_df l
      WHERE l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1)
    )l1
    LEFT JOIN 
    (
      SELECT l.applicant_userid,
             cast(l.overtime_date as date) as stat_date,
             CASE when l.period_type = '全天' THEN '全天加班'
                  when l.period_type = '下午' THEN '下半天加班'
                  when l.period_type = '上午' THEN '上半天加班' end as work_overtime_type,
             row_number()over(PARTITION by l.applicant_userid,cast(l.overtime_date as date) order by CASE when l.period_type = '全天' THEN '全天加班'
                                                                                                          when l.period_type = '下午' THEN '下半天加班'
                                                                                                          when l.period_type = '上午' THEN '上半天加班' end asc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_work_overtime_dayily_info_df l
      WHERE l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1)
    )l2
    ON l1.applicant_userid = l2.applicant_userid AND l1.stat_date = l2.stat_date AND l1.work_overtime_type != l2.work_overtime_type
    WHERE l1.rn = 1 
  )tmp1
  ON td.day_scope = tmp1.stat_date AND tu.emp_id = tmp1.applicant_userid
  WHERE td.day_scope >= tu.hired_date AND td.day_scope <= IF(tu.is_job = 0,tu.quit_date,DATE_ADD(CURRENT_DATE(), -1)) 
) tud
LEFT JOIN
(
  SELECT TO_DATE(th.task_process_time) as day_scope,
         th.task_assign_uuid as ones_user_uuid,
	     t1.task_assign_email as user_email,
         if(t1.project_classify_name = '工单问题汇总','工单',t1.issue_type_cname) as work_type,
         count(distinct th.task_uuid) as handle_ones_num
  FROM ${dwd_dbname}.dwd_one_task_process_change_info_his th
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t1 
  ON t1.uuid = th.task_uuid
  WHERE 1 = 1 AND t1.status = 1 AND t1.issue_type_cname IN ('缺陷','任务','需求') 
    AND TO_DATE(th.task_process_time) >= '2021-01-01' AND TO_DATE(th.task_process_time) <= DATE_ADD(CURRENT_DATE(), -1)
  GROUP BY TO_DATE(th.task_process_time),th.task_assign_uuid,t1.task_assign_email,if(t1.project_classify_name = '工单问题汇总','工单',t1.issue_type_cname)
) t
ON t.day_scope = tud.day_scope and t.user_email = tud.user_email and t.work_type = tud.work_type
)

INSERT overwrite table ${ads_dbname}.ads_team_ft_member_issue_type
-- 日维度
SELECT '' as id,
       tud.*,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM day_scope tud

UNION ALL

-- 周维度
SELECT '' as id,
       tud.team_ft,
       tud.team_group,
	   tud.team_sub_group,
       tud.team_member,
       tud.emp_position,
       tud.is_job,
       tud.role_type,
       '周' as run_type,
       cast(tw.week_first_day as date) as time_value,
       NULL as day_type,
       tud.work_type,
       SUM(tud.handle_ones_num) as handle_ones_num,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM day_scope tud
LEFT JOIN
(
  SELECT concat(date_add(to_date(days), 1 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end),'~',date_add(to_date(days), 7 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end)) as week_scope,
         date_add(to_date(days), 1 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end) as week_first_day,
         date_add(to_date(days), 7 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end) as week_last_day
  FROM ${dim_dbname}.dim_day_date
  WHERE 1 = 1 AND days >= '2021-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1) AND date_add(to_date(days), 1 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end) >= '2021-01-01'
  GROUP BY concat(date_add(to_date(days), 1 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end),'~',date_add(to_date(days), 7 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end)),
           date_add(to_date(days), 1 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end),
           date_add(to_date(days), 7 - case when dayofweek(to_date(days)) = 1 then 7 else dayofweek(to_date(days)) - 1 end)
)tw
ON tud.time_value >= tw.week_first_day AND tud.time_value <= tw.week_last_day
GROUP BY tud.team_ft,tud.team_group,tud.team_sub_group,tud.team_member,tud.emp_position,tud.is_job,tud.role_type,tw.week_first_day,tud.work_type

UNION ALL

-- 月维度
SELECT '' as id,
       tud.team_ft,
       tud.team_group,
	   tud.team_sub_group,
       tud.team_member,
       tud.emp_position,
       tud.is_job,
       tud.role_type,
       '月' as run_type,
       cast(tm.month_first_day as date) as time_value,
       NULL as day_type,
       tud.work_type,
       SUM(tud.handle_ones_num) as handle_ones_num,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM day_scope tud
LEFT JOIN
(
  SELECT DISTINCT substr(d1.days, 1, 7)                as month_scope,
                  concat(substr(d1.days, 1, 7), '-01') as month_first_day,
                  d2.days as month_last_day
  FROM ${dim_dbname}.dim_day_date d1
  LEFT JOIN ${dim_dbname}.dim_day_date d2
  ON substr(d1.days, 1, 7) = substr(d2.days, 1, 7) and d2.is_month_end = 1
  WHERE 1 = 1 AND d1.days >= '2021-01-01' AND d1.days <= DATE_ADD(CURRENT_DATE(), -1) 
)tm
ON tud.time_value >= tm.month_first_day AND tud.time_value <= tm.month_last_day
GROUP BY tud.team_ft,tud.team_group,tud.team_sub_group,tud.team_member,tud.emp_position,tud.is_job,tud.role_type,tm.month_first_day,tud.work_type

UNION ALL

-- 季维度
SELECT '' as id,
       tud.team_ft,
       tud.team_group,
	   tud.team_sub_group,
       tud.team_member,
       tud.emp_position,
       tud.is_job,
       tud.role_type,
       '季' as run_type,
       cast(tq.quarter_first_day as date) as time_value,
       NULL as day_type,
       tud.work_type,
       SUM(tud.handle_ones_num) as handle_ones_num,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM day_scope tud
LEFT JOIN
(
  SELECT DISTINCT concat(year(d1.days), '-', quarter(d1.days)) as quarter_scope,
                  case when quarter(d1.days) = 1 then concat(year(d1.days), '-01-01')
                       when quarter(d1.days) = 2 then concat(year(d1.days), '-04-01')
                       when quarter(d1.days) = 3 then concat(year(d1.days), '-07-01')
                       when quarter(d1.days) = 4 then concat(year(d1.days), '-10-01') end as quarter_first_day,
                  d2.days as quarter_last_day
  FROM ${dim_dbname}.dim_day_date d1
  LEFT JOIN ${dim_dbname}.dim_day_date d2
  ON concat(year(d1.days), '-', quarter(d1.days)) = concat(year(d2.days), '-', quarter(d2.days)) and d2.month_date in(3,6,9,12) and d2.is_month_end = 1
  WHERE 1 = 1 AND d1.days >= '2021-01-01' AND d1.days <= DATE_ADD(CURRENT_DATE(), -1)
)tq
ON tud.time_value >= tq.quarter_first_day AND tud.time_value <= tq.quarter_last_day
GROUP BY tud.team_ft,tud.team_group,tud.team_sub_group,tud.team_member,tud.emp_position,tud.is_job,tud.role_type,tq.quarter_first_day,tud.work_type;