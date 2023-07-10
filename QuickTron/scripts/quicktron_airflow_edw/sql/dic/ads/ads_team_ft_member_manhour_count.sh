#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads




    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-------------------------------------------------------------------------------------------------------------00
--团队小组成员工时日统计 ads_team_ft_member_manhour_count

INSERT overwrite table ${ads_dbname}.ads_team_ft_member_manhour_count
SELECT '' as id,
       tud.team_ft,
       tud.team_group,
	   tud.team_sub_group,
	   tud.emp_position,
       tud.user_name as team_member,
       tud.is_job,
       tud.is_need_fill_manhour,
       tud.role_type,
       '内部工作项目' as project_classify_name,
       cast(tud.days as date) as work_date,
       IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) as day_type,
       tud.issue_type_cname,
       case when tud.issue_type_cname = '缺陷' then IF(t3.check_num is null,0,t3.check_num)
            when tud.issue_type_cname = '任务' then IF(t6.check_num is null,0,t6.check_num)
            when tud.issue_type_cname = '需求' then IF(t9.check_num is null,0,t9.check_num)
            when tud.issue_type_cname = '工单' then IF(t12.check_num is null,0,t12.check_num)
       end as check_num,
       case when tud.issue_type_cname = '缺陷' then IF(t3.work_hours is null,0,t3.work_hours)
            when tud.issue_type_cname = '任务' then IF(t6.work_hours is null,0,t6.work_hours)
            when tud.issue_type_cname = '需求' then IF(t9.work_hours is null,0,t9.work_hours)
            when tud.issue_type_cname = '工单' then IF(t12.work_hours is null,0,t12.work_hours)
       end as work_hours,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
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
         td.days,
         CASE when td.day_type = 0 then '工作日'
              when td.day_type = 1 then '周末'
              when td.day_type = 2 then '节假日'
              when td.day_type = 3 then '调休' end as day_type,
         task.issue_type_cname
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
                    tmp.is_need_fill_manhour,
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
    SELECT DISTINCT days,
                    day_type
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1 AND days >= '2021-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
  ) td
  LEFT JOIN 
  (
    SELECT DISTINCT IF(t1.project_classify_name = '工单问题汇总','工单',t1.issue_type_cname) as issue_type_cname
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
    WHERE t1.status = 1 AND t1.issue_type_cname IN ('需求','任务','缺陷')
  ) task
  WHERE td.days >= tu.hired_date AND td.days <= IF(tu.is_job = 0,tu.quit_date,DATE_ADD(CURRENT_DATE(), -1)) 
) tud
-- 缺陷类型
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         t.user_uuid,
		 tou.user_email,
		 t1.issue_type_cname,
		 COUNT(DISTINCT t.task_uuid) as check_num,
		 SUM(t.task_spend_hours) as work_hours
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t1
  ON t.task_uuid = t1.uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除无效工时和违规登记
    AND t1.status = 1 AND t1.issue_type_cname = '缺陷' AND t1.project_classify_name != '工单问题汇总'
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email,t1.issue_type_cname
) t3 
ON t3.user_email = tud.user_email AND t3.issue_type_cname = tud.issue_type_cname AND t3.stat_date = tud.days
-- 任务类型
LEFT JOIN
(
  SELECT to_date(t.task_start_time) as stat_date,
         t.user_uuid,
		 tou.user_email,
		 t1.issue_type_cname,
		 COUNT(DISTINCT t.task_uuid) as check_num,
		 SUM(t.task_spend_hours) as work_hours
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t1
  ON t.task_uuid = t1.uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除无效工时和违规登记
    AND t1.status = 1 AND t1.issue_type_cname = '任务' AND t1.project_classify_name != '工单问题汇总'
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email,t1.issue_type_cname
) t6 
ON t6.user_email = tud.user_email AND t6.issue_type_cname = tud.issue_type_cname AND t6.stat_date = tud.days
-- 需求类型
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         t.user_uuid,
		 tou.user_email,
		 t1.issue_type_cname,
		 COUNT(DISTINCT t.task_uuid) as check_num,
		 SUM(t.task_spend_hours) as work_hours
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t1
  ON t.task_uuid = t1.uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除无效工时和违规登记
    AND t1.status = 1 AND t1.issue_type_cname = '需求' AND t1.project_classify_name != '工单问题汇总'
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email,t1.issue_type_cname
) t9 
ON t9.user_email = tud.user_email AND t9.issue_type_cname = tud.issue_type_cname AND t9.stat_date = tud.days
-- 工单类型
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         t.user_uuid,
		 tou.user_email,
		 '工单' as issue_type_cname,
		 COUNT(DISTINCT t.task_uuid) as check_num,
		 SUM(t.task_spend_hours) as work_hours
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t1
  ON t.task_uuid = t1.uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除无效工时和违规登记
    AND t1.status = 1 AND t1.project_classify_name = '工单问题汇总'
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email,t1.issue_type_cname
) t12
ON t12.user_email = tud.user_email AND t12.issue_type_cname = tud.issue_type_cname AND t12.stat_date = tud.days
-- 请假统计
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
) tt1
ON tt1.originator_user_id = tud.emp_id AND tt1.stat_date = tud.days
-- 加班统计
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
) tt2
ON tt2.applicant_userid = tud.emp_id AND tt2.stat_date = tud.days;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"