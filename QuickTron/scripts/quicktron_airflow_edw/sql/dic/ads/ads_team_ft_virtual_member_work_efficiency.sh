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
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--团队小组成员效能 ads_team_ft_virtual_member_work_efficiency （研发团队能效）

INSERT overwrite table ${ads_dbname}.ads_team_ft_virtual_member_work_efficiency
SELECT ''                                                      as id,
       tud.team_ft,
       tud.team_group,
	   tud.team_sub_group,
       tud.user_name                                           as team_member,
       tud.is_job,
       tud.role_type,
       tud.module_branch,
       tud.virtual_org_name,
       '[系统中台]3.0凤凰项目'                                 as project_classify_name,
       cast(tud.days as date)                                  as work_date,
       IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) as day_type,
       cast(nvl(t1.code_quantity, 0) as bigint)                as code_quantity,
       cast(nvl(t2.work_hour, 0) as decimal(10, 2))            as work_hour,
       cast(nvl(t3.newly_increased_defect_num, 0) as bigint)   as newly_increased_defect_num,
       cast(nvl(t4.solve_defect_num, 0) as bigint)             as solve_defect_num,
       cast(nvl(t5.close_defect_num, 0) as bigint)             as close_defect_num,
       cast(nvl(t6.newly_increased_task_num, 0) as bigint)     as newly_increased_task_num,
       cast(nvl(t7.solve_task_num, 0) as bigint)               as solve_task_num,
       cast(nvl(t8.close_task_num, 0) as bigint)               as close_task_num,
       cast(nvl(t9.newly_increased_demand_num, 0) as bigint)   as newly_increased_demand_num,
       cast(nvl(t10.solve_demand_num, 0) as bigint)            as solve_demand_num,
       cast(nvl(t11.close_demand_num, 0) as bigint)            as close_demand_num,
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
         tu.module_branch,
         tu.virtual_org_name,
         tu.is_job,
         td.days,
         CASE when td.day_type = 0 then '工作日'
              when td.day_type = 1 then '周末'
              when td.day_type = 2 then '节假日'
              when td.day_type = 3 then '调休' end as day_type   
  FROM
  (
     SELECT DISTINCT tg.org_name_2 as team_ft,
                    tg.org_name_3 as team_group,
                    tg.org_name_4 as team_sub_group,
                    tmp.emp_id,
                    tmp.emp_name   as user_name,
                    tmp.email      as user_email,
                    tt.role_type,
                    tt.module_branch,
                    tt.virtual_org_name,
                    te.is_job,
                    te.hired_date,
                    te.quit_date
    FROM
    (
      SELECT i.emp_code,
             i.role_type,
             i.module_branch,
             i.virtual_org_name
      FROM ${dim_dbname}.dim_virtual_org_emp_info_offline i
      WHERE i.is_active = 1 AND i.virtual_org_name = '凤凰项目'
    )tt
    LEFT JOIN 
    (
      SELECT DISTINCT m.emp_id,
                      m.emp_name,
                      m.email,
                      m.org_id,
                      m.org_role_type,
                      row_number()over(PARTITION by m.emp_id,m.emp_name order by m.org_role_type desc)rn
      FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
      WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.is_valid = 1
    )tmp
    ON tt.emp_code = tmp.emp_id AND tmp.rn = 1
    LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg 
    ON tg.org_id = tmp.org_id AND tg.org_company_name = '上海快仓智能科技有限公司' 
    LEFT JOIN  ${dwd_dbname}.dwd_dtk_emp_info_df te
    ON tt.emp_code = te.emp_id
    WHERE 1 = 1
      AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
  ) tu
  LEFT JOIN 
  (
    SELECT DISTINCT days,
                    day_type
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1 AND days >= '2021-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
  ) td
  WHERE td.days >= tu.hired_date AND td.days <= IF(tu.is_job = 0,tu.quit_date,DATE_ADD(CURRENT_DATE(), -1)) 
) tud
LEFT JOIN 
(
  SELECT to_date(t1.git_commit_date) as stat_date,
         t1.git_author_email as true_email,
         SUM(IF(nvl(t1.add_lines_count,0) >= 2000,2000,nvl(t1.add_lines_count,0))) as code_quantity
  FROM ${dwd_dbname}.dwd_git_commit_detail_info_da t1
  WHERE (split(SUBSTRING_INDEX(split(t1.git_repository,'/opt/gitlab/data/repositories/')[1],'/',2),'/')[1]  = 'phoenix' OR t1.git_repository = 'hardware/upper_computer/upper_computer.git') AND t1.git_repository NOT LIKE '%software/phoenix/aio/phoenix-rcs-aio.git'
  GROUP BY to_date(t1.git_commit_date),t1.git_author_email
) t1
ON tud.user_email = t1.true_email AND t1.stat_date = tud.days
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         t.user_uuid,
		 tou.user_email,
         round(COALESCE(sum(t.task_spend_hours), 0), 2) as work_hour
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid AND tou.user_status = 1
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_classify_name = '[系统中台]3.0凤凰项目' 
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email
) t2 
ON t2.user_email = tud.user_email AND t2.stat_date = tud.days
LEFT JOIN 
(
  SELECT to_date(t1.task_create_time) as stat_date,
         t1.task_assign_email,
         count(distinct t1.uuid)      as newly_increased_defect_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '缺陷' AND t1.project_classify_name = '[系统中台]3.0凤凰项目'
  GROUP BY to_date(t1.task_create_time),t1.task_assign_email
) t3 
ON t3.task_assign_email = tud.user_email AND t3.stat_date = tud.days
LEFT JOIN 
(
  SELECT to_date(t1.server_update_time) as stat_date,
         t1.task_solver_email,
         count(distinct t1.uuid)        as solve_defect_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '缺陷' AND t1.project_classify_name = '[系统中台]3.0凤凰项目' AND t1.task_status_cname in ('单功能通过', '回归通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中') --这些状态被认为是工单解决状态
  GROUP BY to_date(t1.server_update_time),t1.task_solver_email
) t4 
ON t4.task_solver_email = tud.user_email AND t4.stat_date = tud.days
LEFT JOIN 
(
  SELECT to_date(t1.server_update_time) as stat_date,
         t1.task_close_email,
         count(distinct t1.uuid)        as close_defect_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '缺陷' AND t1.project_classify_name = '[系统中台]3.0凤凰项目' AND t1.task_status_cname in ('关闭', '完成', '已关单', '已关闭', '已发布', '已完成', '已实现', '项目验证通过') --这些状态被认为是工单关闭状态
  GROUP BY to_date(t1.server_update_time),t1.task_close_email
) t5 
ON t5.task_close_email = tud.user_email AND t5.stat_date = tud.days 
LEFT JOIN
(
  SELECT to_date(t1.task_create_time) as stat_date,
         t1.task_assign_email,
         count(distinct t1.uuid)      as newly_increased_task_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '任务' AND t1.project_classify_name = '[系统中台]3.0凤凰项目'
  GROUP BY to_date(t1.task_create_time),t1.task_assign_email
) t6 
ON t6.task_assign_email = tud.user_email AND t6.stat_date = tud.days
LEFT JOIN
( 
  SELECT to_date(t1.server_update_time) as stat_date,
         t1.task_solver_email,
         count(distinct t1.uuid)        as solve_task_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '任务' AND t1.project_classify_name = '[系统中台]3.0凤凰项目' AND t1.task_status_cname in ('单功能通过', '回归通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中') --这些状态被认为是工单解决状态
  GROUP BY to_date(t1.server_update_time),t1.task_solver_email
) t7 
ON t7.task_solver_email = tud.user_email AND t7.stat_date = tud.days
LEFT JOIN
(
  SELECT to_date(t1.server_update_time) as stat_date,
         t1.task_close_email,
         count(distinct t1.uuid)        as close_task_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '任务' AND t1.project_classify_name = '[系统中台]3.0凤凰项目' AND t1.task_status_cname in ('关闭', '完成', '已关单', '已关闭', '已发布', '已完成', '已实现', '项目验证通过') --这些状态被认为是工单关闭状态
  GROUP BY to_date(t1.server_update_time),t1.task_close_email
) t8 
ON t8.task_close_email = tud.user_email AND t8.stat_date = tud.days
LEFT JOIN 
(
  SELECT to_date(t1.task_create_time) as stat_date,
         t1.task_assign_email,
         count(distinct t1.uuid)      as newly_increased_demand_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '需求' AND t1.project_classify_name = '[系统中台]3.0凤凰项目'
  GROUP BY to_date(t1.task_create_time),t1.task_assign_email
) t9 
ON t9.task_assign_email = tud.user_email AND t9.stat_date = tud.days
LEFT JOIN 
(
  SELECT to_date(t1.server_update_time) as stat_date,
         t1.task_solver_email,
         count(distinct t1.uuid)        as solve_demand_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '需求' AND t1.project_classify_name = '[系统中台]3.0凤凰项目' AND t1.task_status_cname in ('单功能通过', '回归通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中') --这些状态被认为是工单解决状态
  GROUP BY to_date(t1.server_update_time),t1.task_solver_email
) t10 
ON t10.task_solver_email = tud.user_email AND t10.stat_date = tud.days
LEFT JOIN 
(
  SELECT to_date(t1.server_update_time) as stat_date,
		 t1.task_close_email,
         count(distinct t1.uuid)        as close_demand_num
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
  WHERE t1.status = 1 AND t1.issue_type_cname = '需求' AND t1.project_classify_name = '[系统中台]3.0凤凰项目' AND t1.task_status_cname in ('关闭', '完成', '已关单', '已关闭', '已发布', '已完成', '已实现', '项目验证通过') --这些状态被认为是工单关闭状态
  GROUP BY to_date(t1.server_update_time),t1.task_close_email
) t11 
ON t11.task_close_email = tud.user_email AND t11.stat_date = tud.days
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