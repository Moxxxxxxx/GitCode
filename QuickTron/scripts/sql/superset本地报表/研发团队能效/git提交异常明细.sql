--git提交异常明细 ads_member_unusual_git_detail

INSERT overwrite table ${ads_dbname}.ads_member_unusual_git_detail
SELECT ''                                                      as id,
       tg.work_date,
       tg.user_uuid,
       tg.git_author,
       tg.git_last_name,
       tg.git_user_email,
       tg.git_author_email,
       tg.git_repository,
       tg.root_directory,
       tg.second_level_directory,
       tg.add_lines_count,
       tg.removed_lines_count,
       tg.total_lines_count,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(   
  SELECT DISTINCT to_date(t1.git_create_date)   as work_date,
                  t2.ones_user_uuid             as user_uuid,
                  t1.git_author,
                  t2.git_last_name,
                  t1.git_user_email,
                  t1.git_author_email,
                  IF(t1.git_user_email = te.email,t1.git_user_email,t1.git_author_email) as true_email,
                  t1.git_repo                   as git_repository,
                  SUBSTRING_INDEX(t1.git_repo,'/',1)              as root_directory,
                  SUBSTRING_INDEX(SUBSTRING_INDEX(t1.git_repo,'/',2),'/',-1) as second_level_directory,
                  t1.add_lines_count,
                  t1.removed_lines_count,
                  t1.total_lines_count
  FROM ${dwd_dbname}.dwd_git_app_git_stats_info_da t1
  LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
  ON te.email = IF(t1.git_user_email = te.email,t1.git_user_email,t1.git_author_email) AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
  LEFT JOIN ${dim_dbname}.dim_git_auth_user t2
  ON t2.git_user_email = IF(t1.git_user_email = te.email,t1.git_user_email,t1.git_author_email)
  WHERE 1 = 1
    AND t1.add_lines_count > 0 
    AND t1.git_repo != 'software/phoenix/aio/phoenix-rcs-aio.git'
) tg
LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df m
ON tg.true_email = m.emp_email AND m.d = DATE_ADD(CURRENT_DATE(), -1)
WHERE 1 = 1 AND m.emp_email is NULL;