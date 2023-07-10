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
--缺陷任务需求明细 ads_team_ft_virtual_ones_work_detail （研发团队能效）

INSERT overwrite table ${ads_dbname}.ads_team_ft_virtual_ones_work_detail
SELECT '' as id,
       tu.team_ft,
       tu.team_group,
       tu.team_sub_group,
       tu.user_name as team_member,
       tu.is_job,
       tu.org_role_type,
       tu.virtual_role_type,
       tu.module_branch,
       tu.virtual_org_name,
       t1.project_classify_name,
       t1.sprint_classify_name,
       t1.\`number\` as ones_work_id,
       cast(to_date(t1.task_create_time) as date) as ones_create_date,
       cast(to_date(t1.open_time) as date) as ones_open_date,
       cast(to_date(t1.server_update_time) as date) as ones_update_date,
       t1.issue_type_cname as work_type,
       t1.task_status_cname as work_status,
       t1.summary as work_summary,
       t1.task_desc as work_desc,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
LEFT JOIN 
(
  SELECT DISTINCT tg.org_name_2 as team_ft,
                  tg.org_name_3 as team_group,
                  tg.org_name_4 as team_sub_group,
                  te.emp_id,
                  te.emp_name   as user_name,
                  te.email      as user_email,
                  tmp.org_role_type as org_role_type,
                  tt.role_type as virtual_role_type,
                  tt.module_branch,
                  tt.virtual_org_name,
                  te.is_job
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
  LEFT JOIN 
  (
    SELECT i.emp_code,
           i.role_type,
           i.module_branch,
           i.virtual_org_name
    FROM ${dim_dbname}.dim_virtual_org_emp_info_offline i
    WHERE i.is_active = 1 AND i.virtual_org_name = '凤凰项目'
  )tt
  ON tt.emp_code = te.emp_id
  LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg 
  ON tg.org_id = tmp.org_id AND tg.org_company_name = '上海快仓智能科技有限公司'
  WHERE te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
    AND IF(te.is_job = 1,tg.org_name_2 IN ('AMR FT','智能搬运FT','硬件自动化','箱式FT','系统中台') OR (tg.org_name_2 IN ('制造部') AND tg.org_name_3 IN ('工程组','测试')) OR (tg.org_name_2 IN ('研发管理部') AND tg.org_name_3 IN ('产研质量组','效能工具组')),te.dept_name LIKE '%AMR FT%' OR te.dept_name LIKE '%智能搬运FT%' OR te.dept_name LIKE '%硬件自动化%' OR te.dept_name LIKE '%箱式FT%' OR te.dept_name LIKE '%系统中台%' OR te.dept_name LIKE '%制造部%' OR te.dept_name LIKE '%研发管理部%') -- 只筛选AMR FT、智能搬运FT、硬件自动化、箱式FT、系统中台、制造部、研发管理部
) tu 
ON tu.user_email = t1.task_assign_email
WHERE t1.status = 1
  AND t1.issue_type_cname in ('缺陷','任务','需求');
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"