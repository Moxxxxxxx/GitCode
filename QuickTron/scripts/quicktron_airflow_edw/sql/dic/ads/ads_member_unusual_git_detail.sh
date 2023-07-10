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
--git提交异常明细 ads_member_unusual_git_detail

INSERT overwrite table ${ads_dbname}.ads_member_unusual_git_detail
SELECT '' as id,
       to_date(t1.git_commit_date) as work_date,
       t1.git_author,
       t1.git_author_email,
       split(t1.git_repository,'/opt/gitlab/data/repositories/')[1] as git_repository,
       SUBSTRING_INDEX(split(t1.git_repository,'/opt/gitlab/data/repositories/')[1],'/',1) as root_directory,
       split(SUBSTRING_INDEX(split(t1.git_repository,'/opt/gitlab/data/repositories/')[1],'/',2),'/')[1] as second_level_directory,
       t1.add_lines_count,
       t1.removed_lines_count,
       t1.total_lines_count,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${dwd_dbname}.dwd_git_commit_detail_info_da t1
LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
ON te.email = t1.git_author_email AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
WHERE t1.add_lines_count > 0 AND t1.git_repository != '/opt/gitlab/data/repositories/software/phoenix/aio/phoenix-rcs-aio.git'
  AND te.email is NULL;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"