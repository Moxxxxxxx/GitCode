--ads_share_project_base_info_df    --������Դ������Ŀ������Ϣ��

INSERT overwrite table ${ads_dbname}.ads_share_project_base_info_df
SELECT NULL as id,p.project_code,p.project_name,
       IF(p.project_custormer_code = 'UNKNOWN','δ֪',p.project_custormer_code) as project_custormer_code,
       IF(p.project_company_name = 'UNKNOWN','δ֪',p.project_company_name) as project_company_name,
       p.project_custormer_level,
       IF(p.project_operation_state = 'UNKNOWN','δ֪',p.project_operation_state) as project_operation_state,
       p.project_context,
       IF(p.project_area = 'UNKNOWN','δ֪',p.project_area) as project_area,
       p.project_industry_type,
       IF(p.project_product_name = 'UNKNOWN','δ֪',p.project_product_name) as project_product_name,
       IF(p.project_product_type = 'UNKNOWN','δ֪',p.project_product_type) as project_product_type,
       IF(p.project_attr_ft is null,'δ֪',p.project_attr_ft) as project_attr_ft,
       CASE WHEN p.project_current_version = 'UNKNOWN' THEN '����'
              WHEN p.project_current_version = '2.82' THEN '2.8.2'
              WHEN p.project_current_version = '2.91' THEN '2.9.1'
              ELSE p.project_current_version END as project_current_version,
       p.project_approval_time,
       project_update_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')                               as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')                               as update_time
  FROM ${dwd_dbname}.dwd_share_project_base_info_df p
  WHERE p.d = DATE_ADD(CURRENT_DATE(), -1) and p.is_filter_project='1';