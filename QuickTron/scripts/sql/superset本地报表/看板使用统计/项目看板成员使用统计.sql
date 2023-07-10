--项目看板成员使用统计 ads_superset_project_used_count

INSERT overwrite table ${ads_dbname}.ads_superset_project_used_count
SELECT '' as id,
       DATE_FORMAT(days_tab.days,'yyyy-MM-dd HH:mm:ss.000000') as days, 
       rst.record_start_time,
       days_tab.role_id,
       days_tab.user_cname,
       days_tab.team_ft,
       days_tab.emp_position,
       days_tab.dashboard_id,
       days_tab.dashboard_name,
       days_tab.is_dashboard_publish,
       CASE WHEN log_count_tab.num IS NULL THEN 0 ELSE log_count_tab.num END AS page_count,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT ddd.days,
         ab.id, 
         ab.role_id,
         ab.user_cname,
         ab.team_ft,
         ab.emp_position,
         dd.dashboard_id,
         dd.dashboard_name,
         dd.is_dashboard_publish
  FROM ${dim_dbname}.dim_day_date ddd
  JOIN 
  (
    SELECT auu.*,
           tu.team_ft,
           tu.emp_position
    FROM 
    (
      SELECT aa.*
      FROM ${dim_dbname}.dim_report_dashboard_user_info aa
      WHERE aa.role_id regexp '(7|96|94|110|109|85|77|104|97|108|105|107|101|106|102|43|75|47|56|33|65|62|50|31|28|34|39|48|74|63|61|38|46|52|37|57|40|68|72|32|53|58|41|67|70|54|35|51|64|49|59|71|29|42|80|82|78|83|81|79|76)'
    ) auu
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
    ON auu.user_email = tu.user_email 
  ) ab
  JOIN  
  (
    SELECT DISTINCT d.dashboard_id,d.dashboard_name,is_dashboard_publish
    FROM ${dim_dbname}.dim_report_dashboard_slices_info d
    WHERE d.dashboard_name LIKE '%【项目】%'
  ) dd
  WHERE DATE(ddd.days) >= '2021-01-01' AND DATE(ddd.days) <= DATE_ADD(CURRENT_DATE(), -1)
) days_tab
LEFT JOIN 
(
  SELECT tt.d,
         tt.user_id,
         tt.dashboard_id,
         COUNT(tt.id) AS num
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
           tmp.user_action,
           tmp.user_action_name,
           tmp.dashboard_id,
           tmp.dashboard_name,
           tmp.url_address
    FROM 
    (
      SELECT row_number() over(partition by ali.user_id,ali.dashboard_id,DATE_FORMAT(ali.record_start_time,'yyyy-MM-dd HH:mm:00')  order by ali.record_start_time desc) row_number1,
             ali.id,
             ali.d,
             from_unixtime(unix_timestamp(ali.record_start_time)+28800,'yyyy-MM-dd HH:mm:ss') as record_start_time,
             ali.duration_ms,
             ali.user_id,
             ali.user_cname,
             ui.role_id,
             ui.role_cname,
             ali.user_action,
             ali.user_action_name,
             ali.dashboard_id,
             dsi.dashboard_name,
             ali.url_address
      FROM ${dwd_dbname}.dwd_report_action_log_info_da ali
      LEFT JOIN 
      (
        select distinct dashboard_id,
                        dashboard_name 
        from ${dim_dbname}.dim_report_dashboard_slices_info
      ) dsi 
      ON ali.dashboard_id  = dsi.dashboard_id
      LEFT JOIN ${dim_dbname}.dim_report_dashboard_user_info ui
      ON ali.user_id = ui.id
    )tmp
    WHERE tmp.row_number1 = 1 
    order by record_start_time desc
  ) tt
  GROUP BY tt.d,tt.user_id,tt.dashboard_id
) log_count_tab
ON days_tab.days = log_count_tab.d AND days_tab.id = log_count_tab.user_id AND days_tab.dashboard_id = log_count_tab.dashboard_id
LEFT JOIN
(
  SELECT tt.d,
         max(tt.record_start_time) as record_start_time,
         tt.user_id,
         tt.user_cname
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
           tmp.user_action,
           tmp.user_action_name,
           tmp.dashboard_id,
           tmp.dashboard_name,
           tmp.url_address
    FROM 
    (
      SELECT row_number() over(partition by ali.user_id,ali.dashboard_id,DATE_FORMAT(ali.record_start_time,'yyyy-MM-dd HH:mm:00')  order by ali.record_start_time desc) row_number1,
             ali.id,
             ali.d,
             from_unixtime(unix_timestamp(ali.record_start_time)+28800,'yyyy-MM-dd HH:mm:ss') as record_start_time,
             ali.duration_ms,
             ali.user_id,
             ali.user_cname,
             ui.role_id,
             ui.role_cname,
             ali.user_action,
             ali.user_action_name,
             ali.dashboard_id,
             dsi.dashboard_name,
             ali.url_address
      FROM ${dwd_dbname}.dwd_report_action_log_info_da ali
      LEFT JOIN 
      (
        select distinct dashboard_id,
                        dashboard_name
        from ${dim_dbname}.dim_report_dashboard_slices_info
        WHERE is_dashboard_publish = 1
      ) dsi 
      ON ali.dashboard_id  = dsi.dashboard_id
      LEFT JOIN ${dim_dbname}.dim_report_dashboard_user_info ui
      ON ali.user_id = ui.id
    )tmp
    WHERE tmp.row_number1 = 1 
    order by record_start_time desc
  ) tt
  WHERE tt.dashboard_name LIKE '%【项目】%'
  group by tt.d,tt.user_id,tt.user_cname
  ORDER BY max(tt.record_start_time) desc
)rst
ON log_count_tab.d = rst.d AND log_count_tab.user_id = rst.user_id;