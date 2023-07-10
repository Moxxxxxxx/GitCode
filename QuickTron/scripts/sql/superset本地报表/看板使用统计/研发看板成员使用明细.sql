--研发看板成员使用明细 ads_superset_rd_used_detail

INSERT overwrite table ${ads_dbname}.ads_superset_rd_used_detail
SELECT '' as id,
       tt.id as log_id,
       tt.d as days,
       tt.record_start_time,
       tt.duration_ms,
       tt.user_id,
       tt.user_cname,
       tt.role_id,
       tt.role_cname,
       tt.user_email,
       tt.user_action,
       tt.user_action_name,
       tt.dashboard_id,
       tt.dashboard_name,
       tt.is_dashboard_publish,
       tt.url_address,
       tu.team_ft,
       tu.emp_position,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
( 
  SELECT tmp.id,
         DATE_FORMAT(tmp.record_start_time,'yyyy-MM-dd') as d,
         tmp.record_start_time,
         tmp.duration_ms,
         tmp.user_id,
         tmp.user_cname,
         tmp.role_id,
         tmp.role_cname,
         tmp.user_email,
         tmp.user_action,
         tmp.user_action_name,
         tmp.dashboard_id,
         tmp.dashboard_name,
         tmp.is_dashboard_publish,
         tmp.url_address
  FROM 
  (
    SELECT row_number() over(partition by ali.user_id,ali.dashboard_id,DATE_FORMAT(ali.record_start_time,'yyyy-MM-dd HH:mm:00') order by ali.record_start_time desc) row_number1,
           ali.id,
           ali.d,
           from_unixtime(unix_timestamp(ali.record_start_time)+28800,'yyyy-MM-dd HH:mm:ss') as record_start_time,
           ali.duration_ms,
           ali.user_id,
           ali.user_cname,
           ui.role_id,
           ui.role_cname,
           ui.user_email,
           ali.user_action,
           ali.user_action_name,
           ali.dashboard_id,
           dsi.dashboard_name,
           dsi.is_dashboard_publish,
           ali.url_address
    FROM ${dwd_dbname}.dwd_report_action_log_info_da ali
    LEFT JOIN 
    (
      select distinct dashboard_id,
                      dashboard_name,
                      is_dashboard_publish
      from ${dim_dbname}.dim_report_dashboard_slices_info
    ) dsi 
    ON ali.dashboard_id  = dsi.dashboard_id
    LEFT JOIN ${dim_dbname}.dim_report_dashboard_user_info ui
    ON ali.user_id = ui.id
    WHERE ali.dashboard_id is not null AND ui.role_id regexp '(7|88)'
  )tmp
  WHERE tmp.row_number1 = 1 
  order by record_start_time desc
) tt
LEFT JOIN
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
                    row_number()over(PARTITION by m.emp_id,m.emp_name order by m.is_need_fill_manhour desc,m.org_role_type desc)rn
    FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
    WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.is_valid = 1
  )tmp
  ON te.emp_id = tmp.emp_id AND tmp.rn = 1
  LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg 
  ON tg.org_id = tmp.org_id AND tg.org_company_name = '上海快仓智能科技有限公司'  
  WHERE 1 = 1
    AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
) tu
ON tt.user_email = tu.user_email
WHERE tt.dashboard_name LIKE '%【大研发体系%';