--FT团队成员达标明细 ads_team_ft_standard_reaching_detail

with day_scope as 
(
SELECT tud.team_ft,
       tud.team_group,
       tud.team_sub_group,
       tud.emp_position,
       tud.user_name                                           as team_member,
       tud.is_job,
       tud.is_need_fill_manhour,
       tud.role_type,
       '日'                                                     as run_type,
       IF(tud.leave_day_type is not null,tud.leave_day_type,IF(tud.work_overtime_type is not null,IF(tud.leave_type = '哺乳假',CONCAT(tud.work_overtime_type,'-','哺乳假'),tud.work_overtime_type),IF(tud.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) as day_type,
       cast(tud.day_scope as date)                             as time_value,
       cast(nvl(t2.add_lines_count, 0) as bigint)              as code_quantity,
       cast(IF(((tud.day_type = '工作日' or tud.day_type = '调休') AND tud.leave_day_type is null) or tud.work_overtime_type = '全天加班',1,IF(((tud.day_type = '工作日' or tud.day_type = '调休') AND tud.leave_day_type LIKE '%半天请假') or tud.work_overtime_type LIKE '%半天加班',0.5,0)) * 80 as decimal(10, 2))               as code_quantity_compliance_standard,
       cast(case when cast(nvl(t2.add_lines_count, 0) as bigint) < cast(IF(((tud.day_type = '工作日' or tud.day_type = '调休') AND tud.leave_day_type is null) or tud.work_overtime_type = '全天加班',1,IF(((tud.day_type = '工作日' or tud.day_type = '调休') AND tud.leave_day_type LIKE '%半天请假') or tud.work_overtime_type LIKE '%半天加班',0.5,0)) * 80 as decimal(10, 2)) then '不达标' else '达标' end as string) as is_code_quantity_compliance_standard,
       cast(nvl(t1.sum_task_spend_hours, 0) as decimal(10, 2)) as work_hour,
       IF(IF(tud.leave_day_type is not null,tud.leave_day_type,IF(tud.work_overtime_type is not null,IF(tud.leave_type = '哺乳假',CONCAT(tud.work_overtime_type,'-','哺乳假'),tud.work_overtime_type),IF(tud.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) in ('工作日-哺乳假','调休-哺乳假','上半天请假-哺乳假','下半天请假-哺乳假'),(cast(IF(((tud.day_type = '工作日' or tud.day_type = '调休') AND tud.leave_day_type is null) or tud.work_overtime_type = '全天加班',1,IF(tud.leave_day_type LIKE '%半天请假' or tud.work_overtime_type LIKE '%半天加班',0.5,0)) * 8 * 0.75 as decimal(10, 2)) - 1),cast(IF(((tud.day_type = '工作日' or tud.day_type = '调休') AND tud.leave_day_type is null) or tud.work_overtime_type = '全天加班',1,IF(((tud.day_type = '工作日' or tud.day_type = '调休') AND tud.leave_day_type LIKE '%半天请假') or tud.work_overtime_type LIKE '%半天加班',0.5,0)) * 8 * 0.75 as decimal(10, 2))) as work_hour_compliance_standard,
       cast(case when cast(nvl(t1.sum_task_spend_hours, 0) as decimal(10, 2)) < IF(IF(tud.leave_day_type is not null,tud.leave_day_type,IF(tud.work_overtime_type is not null,IF(tud.leave_type = '哺乳假',CONCAT(tud.work_overtime_type,'-','哺乳假'),tud.work_overtime_type),IF(tud.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) in ('工作日-哺乳假','调休-哺乳假','上半天请假-哺乳假','下半天请假-哺乳假'),(cast(IF(((tud.day_type = '工作日' or tud.day_type = '调休') AND tud.leave_day_type is null) or tud.work_overtime_type = '全天加班',1,IF(((tud.day_type = '工作日' or tud.day_type = '调休') AND tud.leave_day_type LIKE '%半天请假') or tud.work_overtime_type LIKE '%半天加班',0.5,0)) * 8 * 0.75 as decimal(10, 2)) - 1),cast(IF(((tud.day_type = '工作日' or tud.day_type = '调休') AND tud.leave_day_type is null) or tud.work_overtime_type = '全天加班',1,IF(((tud.day_type = '工作日' or tud.day_type = '调休') AND tud.leave_day_type LIKE '%半天请假') or tud.work_overtime_type LIKE '%半天加班',0.5,0)) * 8 * 0.75 as decimal(10, 2))) then '不达标' else '达标' end as string) as is_work_hour_compliance_standard
FROM 
(
  SELECT tu.team_ft,
         tu.team_group,
         tu.team_sub_group,
         tu.emp_id,
         tu.user_name,
         tu.user_email,
         tu.role_type,
         tu.is_job,
         tu.is_need_fill_manhour,
         tu.emp_position,
         td.day_scope,
         CASE when td.day_type = 0 then '工作日'
              when td.day_type = 1 then '周末'
              when td.day_type = 2 then '节假日'
              when td.day_type = 3 then '调休' end as day_type,
         tmp.day_type as leave_day_type,     
         tmp.leave_type,
         tmp1.work_overtime_type
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
                    te.emp_position,
                    tmp.is_need_fill_manhour
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
    LEFT JOIN 
    (
      SELECT DISTINCT m.emp_id,
                      m.emp_name,
                      m.org_id,
                      m.org_role_type, 
                      m.is_need_fill_manhour,
                      row_number()over(PARTITION by m.emp_id,m.emp_name order by m.is_need_fill_manhour desc,m.org_role_type desc,m.org_id asc)rn
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
    SELECT DISTINCT days as day_scope,
                    day_type
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1 AND days >= '2021-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
  ) td
  LEFT JOIN 
  (
    SELECT tmp1.stat_date,
           tmp1.originator_user_id,
           tmp1.email,
           IF(tmp2.stat_date is not null,IF(tmp2.day_type = '全天请假',tmp2.day_type,CONCAT(tmp2.day_type,'-',tmp1.leave_type)),tmp1.day_type) as day_type,
           tmp1.leave_type
    FROM
    (
      SELECT tud.days as stat_date,
             l.originator_user_id,
	    	 te.email,
  		     CASE when (tud.days > l.start_date and tud.days < l.end_date and l.leave_type != '哺乳假')
                    or (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '下午')
                    or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '上午') 
                    or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '下午') then '全天请假'
                  when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '上午')
                    or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '上午') then '上半天请假'
                  when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '下午' and l.end_time_period = '下午')
                    or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '下午') then '下半天请假'
             end as day_type,
             l.leave_type
      FROM 
      (
        SELECT *,
               row_number()over(PARTITION by l.business_id order by l.create_time desc)rn
        FROM ${dwd_dbname}.dwd_dtk_process_leave_info_df l
        WHERE l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1) 
      )l
      LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
      ON l.originator_user_id = te.emp_id	
      LEFT JOIN ${dim_dbname}.dim_day_date tud
      ON l.start_date <= tud.days and l.end_date >= tud.days
      WHERE l.rn = 1 
        AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
    )tmp1
    LEFT JOIN 
    (
      SELECT tud.days as stat_date,
             l.originator_user_id,
	  	     te.email,
		     CASE when (tud.days > l.start_date and tud.days < l.end_date and l.leave_type != '哺乳假')
                    or (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '下午')
                    or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '上午') 
                    or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '下午') then '全天请假'
                  when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '上午')
                    or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '上午') then '上半天请假'
                  when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '下午' and l.end_time_period = '下午')
                    or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '下午') then '下半天请假'
             end as day_type,
             l.leave_type
      FROM 
      (
        SELECT *,
               row_number()over(PARTITION by l.business_id order by l.create_time desc)rn
        FROM ${dwd_dbname}.dwd_dtk_process_leave_info_df l
        WHERE l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1) 
      )l
      LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
      ON l.originator_user_id = te.emp_id	
      LEFT JOIN ${dim_dbname}.dim_day_date tud
      ON l.start_date <= tud.days and l.end_date >= tud.days
      WHERE l.rn = 1 
        AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
    )tmp2
    ON tmp1.stat_date = tmp2.stat_date and tmp1.originator_user_id = tmp2.originator_user_id and tmp1.email = tmp2.email and tmp1.leave_type != tmp2.leave_type
  )tmp
  ON td.day_scope = tmp.stat_date AND tu.emp_id = tmp.originator_user_id
  LEFT JOIN 
  (
    SELECT l.applicant_userid,
             cast(l.overtime_date as date) as stat_date,
             CASE when l.period_type = '全天' THEN '全天加班'
                  when l.period_type = '下午' THEN '下半天加班'
                  when l.period_type = '上午' THEN '上半天加班' end as work_overtime_type
      FROM ${dwd_dbname}.dwd_dtk_process_work_overtime_dayily_info_df l
      WHERE l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1)
  )tmp1
  ON td.day_scope = tmp1.stat_date AND tu.emp_id = tmp1.applicant_userid
  WHERE td.day_scope >= tu.hired_date AND td.day_scope <= IF(tu.is_job = 0,tu.quit_date,DATE_ADD(CURRENT_DATE(), -1)) 
) tud
--工时 
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as day_scope,
         t.user_uuid,
		 tou.user_email,
         round(COALESCE(sum(t.task_spend_hours), 0), 2) as sum_task_spend_hours
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND project_type_name is not null
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email
) t1 
ON t1.user_email = tud.user_email AND t1.day_scope = tud.day_scope
--代码量 
LEFT JOIN 
(
  SELECT to_date(t1.git_create_date) as day_scope,
         t1.git_user_email as user_email,
         sum(t1.add_lines_count)     as add_lines_count
  FROM ${dwd_dbname}.dwd_git_app_git_stats_info_da t1
  LEFT JOIN ${dim_dbname}.dim_git_auth_user t2
  ON t2.git_user_email = t1.git_user_email
  WHERE 1 = 1 AND t1.git_repo != 'software/phoenix/aio/phoenix-rcs-aio.git'
  GROUP BY to_date(t1.git_create_date), t1.git_user_email
) t2
ON t2.user_email = tud.user_email AND t2.day_scope = tud.day_scope
)

INSERT overwrite table ${ads_dbname}.ads_team_ft_standard_reaching_detail
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
       tud.emp_position,
       tud.team_member,
       tud.is_job,
       tud.is_need_fill_manhour,
       tud.role_type,
       '周' as run_type,
       null as day_type,
       cast(tw.week_first_day as date) as time_value,
       SUM(tud.code_quantity) as code_quantity,
       SUM(tud.code_quantity_compliance_standard) as code_quantity_compliance_standard,
       case when SUM(tud.code_quantity) < SUM(tud.code_quantity_compliance_standard) then '不达标' else '达标' end as is_code_quantity_compliance_standard,
       SUM(tud.work_hour) as work_hour,
       SUM(tud.work_hour_compliance_standard) as work_hour_compliance_standard,
       case when SUM(tud.work_hour) < SUM(tud.work_hour_compliance_standard) then '不达标' else '达标' end as is_work_hour_compliance_standard,
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
GROUP BY tud.team_ft,tud.team_group,tud.team_sub_group,tud.emp_position,tud.team_member,tud.is_job,tud.is_need_fill_manhour,tud.role_type,tw.week_first_day

UNION ALL

-- 月维度
SELECT '' as id,
       tud.team_ft,
       tud.team_group,
       tud.team_sub_group,
       tud.emp_position,
       tud.team_member,
       tud.is_job,
       tud.is_need_fill_manhour,
       tud.role_type,
       '月' as run_type,
       null as day_type,
       cast(tm.month_first_day as date) as time_value,
       SUM(tud.code_quantity) as code_quantity,
       SUM(tud.code_quantity_compliance_standard) as code_quantity_compliance_standard,
       case when SUM(tud.code_quantity) < SUM(tud.code_quantity_compliance_standard) then '不达标' else '达标' end as is_code_quantity_compliance_standard,
       SUM(tud.work_hour) as work_hour,
       SUM(tud.work_hour_compliance_standard) as work_hour_compliance_standard,
       case when SUM(tud.work_hour) < SUM(tud.work_hour_compliance_standard) then '不达标' else '达标' end as is_work_hour_compliance_standard,
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
GROUP BY tud.team_ft,tud.team_group,tud.team_sub_group,tud.emp_position,tud.team_member,tud.is_job,tud.is_need_fill_manhour,tud.role_type,tm.month_first_day

UNION ALL

-- 季维度
SELECT '' as id,
       tud.team_ft,
       tud.team_group,
       tud.team_sub_group,
       tud.emp_position,
       tud.team_member,
       tud.is_job,
       tud.is_need_fill_manhour,
       tud.role_type,
       '季' as run_type,
       null as day_type,
       cast(tq.quarter_first_day as date) as time_value,
       SUM(tud.code_quantity) as code_quantity,
       SUM(tud.code_quantity_compliance_standard) as code_quantity_compliance_standard,
       case when SUM(tud.code_quantity) < SUM(tud.code_quantity_compliance_standard) then '不达标' else '达标' end as is_code_quantity_compliance_standard,
       SUM(tud.work_hour) as work_hour,
       SUM(tud.work_hour_compliance_standard) as work_hour_compliance_standard,
       case when SUM(tud.work_hour) < SUM(tud.work_hour_compliance_standard) then '不达标' else '达标' end as is_work_hour_compliance_standard,
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
GROUP BY tud.team_ft,tud.team_group,tud.team_sub_group,tud.emp_position,tud.team_member,tud.is_job,tud.is_need_fill_manhour,tud.role_type,tq.quarter_first_day

UNION ALL

-- 年维度
SELECT '' as id,
       tud.team_ft,
       tud.team_group,
       tud.team_sub_group,
       tud.emp_position,
       tud.team_member,
       tud.is_job,
       tud.is_need_fill_manhour,
       tud.role_type,
       '年' as run_type,
       null as day_type,
       cast(ty.year_first_day as date) as time_value,
       SUM(tud.code_quantity) as code_quantity,
       SUM(tud.code_quantity_compliance_standard) as code_quantity_compliance_standard,
       case when SUM(tud.code_quantity) < SUM(tud.code_quantity_compliance_standard) then '不达标' else '达标' end as is_code_quantity_compliance_standard,
       SUM(tud.work_hour) as work_hour,
       SUM(tud.work_hour_compliance_standard) as work_hour_compliance_standard,
       case when SUM(tud.work_hour) < SUM(tud.work_hour_compliance_standard) then '不达标' else '达标' end as is_work_hour_compliance_standard,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM day_scope tud
LEFT JOIN
(
  SELECT DISTINCT year(d1.days) as year_scope,
                  concat(year(d1.days), '-01-01') as year_first_day,
                  concat(year(d1.days), '-12-31') as year_last_day
  FROM ${dim_dbname}.dim_day_date d1
  WHERE 1 = 1 AND d1.days >= '2021-01-01' AND d1.days <= DATE_ADD(CURRENT_DATE(), -1)
)ty
ON tud.time_value >= ty.year_first_day AND tud.time_value <= ty.year_last_day
GROUP BY tud.team_ft,tud.team_group,tud.team_sub_group,tud.emp_position,tud.team_member,tud.is_job,tud.is_need_fill_manhour,tud.role_type,ty.year_first_day