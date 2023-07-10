--研发团队小组成员git提交明细 ads_team_ft_virtual_member_git_detail （研发团队能效）

INSERT overwrite table ${ads_dbname}.ads_team_ft_virtual_member_git_detail
SELECT ''                                                      as id,
       tud.team_ft,
       tud.team_group,
       tud.team_sub_group,
       tud.user_name                                           as team_member,
       tud.is_job,
       tud.org_role_type,
       tud.virtual_role_type,
       tud.module_branch,
       tud.virtual_org_name,
       cast(tud.work_date as date)                             as work_date,
       tud.day_type,
       tg.git_repository,
       SUBSTRING_INDEX(tg.git_repository,'/',1)              as root_directory,
       SUBSTRING_INDEX(SUBSTRING_INDEX(tg.git_repository,'/',2),'/',-1) as second_level_directory,
       cast(nvl(tg.add_lines_count, 0) as bigint)              as add_lines_count,
       cast(nvl(tg.removed_lines_count, 0) as bigint)          as removed_lines_count,
       cast(nvl(tg.total_lines_count, 0) as bigint)            as total_lines_count,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
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
    WHERE 1 = 1
      AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
      AND (tg.org_name_2 IN ('AMR FT','智能搬运FT','硬件自动化','箱式FT','系统中台') OR (tg.org_name_2 is NULL AND te.is_job = 0))
  ) tu
  LEFT JOIN
  (
    SELECT DISTINCT days,
                    day_type
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1
      AND days >= '2021-01-01' and days <= DATE_ADD(CURRENT_DATE(), -1)
  ) td
) tud
LEFT JOIN
(   
  SELECT DISTINCT to_date(t1.git_commit_date)   as work_date,
                  t2.ones_user_uuid             as user_uuid,
                  IF(t1.git_user_email = te.email,t1.git_user_email,t1.git_author_email) as true_email,
                  t1.git_repository,
                  t1.add_lines_count,
                  t1.removed_lines_count,
                  t1.total_lines_count
  FROM ${dwd_dbname}.dwd_git_commit_detail_info_da t1
  LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
  ON te.email = IF(t1.git_user_email = te.email,t1.git_user_email,t1.git_author_email) AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
  LEFT JOIN ${dim_dbname}.dim_git_auth_user t2
  ON t2.git_user_email = IF(t1.git_user_email = te.email,t1.git_user_email,t1.git_author_email)
  WHERE 1 = 1
    AND t1.add_lines_count > 0 
    AND t2.ones_user_uuid is not null
    AND t1.git_repository != 'software/phoenix/aio/phoenix-rcs-aio.git'
) tg
ON tg.true_email = tud.user_email and tg.work_date = tud.work_date       
WHERE 1 = 1 ;