#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-12-19 增加研发管理部人员
#-- 2 wangyingying 2022-12-26 增加commit_id字段
# ------------------------------------------------------------------------------------------------


hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi

    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-------------------------------------------------------------------------------------------------------------00
--研发团队小组成员git提交明细 ads_team_ft_virtual_member_git_detail （研发团队能效）

INSERT overwrite table ${ads_dbname}.ads_team_ft_virtual_member_git_detail
SELECT '' AS id,
       tud.team_ft,
       tud.team_group,
       tud.team_sub_group,
       tud.user_name AS team_member,
       tud.is_job,
       tud.org_role_type,
       tud.virtual_role_type,
       tud.module_branch,
       tud.virtual_org_name,
       CAST(tud.work_date AS DATE) AS work_date,
       tud.day_type,
	   tg.git_commit_id,
       split(tg.git_repository,'/opt/gitlab/data/repositories/')[1] AS git_repository,
       SUBSTRING_INDEX(split(tg.git_repository,'/opt/gitlab/data/repositories/')[1],'/',1) AS root_directory,
       split(SUBSTRING_INDEX(split(tg.git_repository,'/opt/gitlab/data/repositories/')[1],'/',2),'/')[1] AS second_level_directory,
       tg.git_branch,
       tg.git_commit_desc,
	   CAST(nvl(tg.add_lines_count, 0) AS bigint) AS add_lines_count,
       CAST(nvl(tg.removed_lines_count, 0) AS bigint) AS removed_lines_count,
       CAST(nvl(tg.total_lines_count, 0) AS bigint) AS total_lines_count,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM 
(
  SELECT tu.*,
         td.days as work_date,
         CASE when td.day_type = 0 then '工作日'
              when td.day_type = 1 then '周末'
              when td.day_type = 2 then '节假日'
              when td.day_type = 3 then '调休' end as day_type
  FROM 
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
    WHERE te.d = '${pre1_date}' AND te.org_company_name = '上海快仓智能科技有限公司'
      AND IF(te.is_job = 1,tg.org_name_2 IN ('AMR FT','智能搬运FT','硬件自动化','箱式FT','系统中台') OR (tg.org_name_2 = '研发管理部' AND tg.org_name_3 = '效能工具组' AND te.emp_name != '奚静思'),te.dept_name LIKE '%AMR FT%' OR te.dept_name LIKE '%智能搬运FT%' OR te.dept_name LIKE '%硬件自动化%' OR te.dept_name LIKE '%箱式FT%' OR te.dept_name LIKE '%系统中台%')
  ) tu
  LEFT JOIN
  (
    SELECT DISTINCT days,
                    day_type
    FROM ${dim_dbname}.dim_day_date
    WHERE days >= '2021-01-01' and days <= '${pre1_date}'
  ) td
) tud
LEFT JOIN
(   
  SELECT to_date(t1.git_commit_date) as work_date,
         t1.git_commit_id,
         t1.git_author_email,
         t1.git_repository,
         t1.git_branch,
		 t1.git_commit_desc,
         t1.add_lines_count,
         t1.removed_lines_count,
         t1.total_lines_count
  FROM ${dwd_dbname}.dwd_git_commit_detail_info_da t1
  WHERE t1.add_lines_count > 0 AND t1.git_repository != '/opt/gitlab/data/repositories/software/phoenix/aio/phoenix-rcs-aio.git'
) tg
ON tg.git_author_email = tud.user_email and tg.work_date = tud.work_date;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"