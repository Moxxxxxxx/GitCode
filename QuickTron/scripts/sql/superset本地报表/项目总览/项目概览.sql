-- ads_project_count_info    --��Ŀ����

INSERT overwrite table ${ads_dbname}.ads_project_count_info
SELECT '' as id,
       tmp.cur_date, -- ͳ������
       tmp.belong_ft_name, -- ����ft
       tmp.project_operation_state, -- ��Ŀ��Ӫ�׶�
       tmp.project_current_version, -- ��Ŀ�汾
       COUNT(DISTINCT project_code) as project_total_num, -- ��Ŀ����
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT p.d as cur_date, --ͳ������
         p.project_code, --��Ŀ����
         IF(p.project_attr_ft is null,'δ֪',p.project_attr_ft) as belong_ft_name, --��Ŀ����ft
         IF(trim(p.project_operation_state) = 'UNKNOWN','δ֪',trim(p.project_operation_state)) as project_operation_state, --��Ŀ��Ӫ�׶Σ�ȥ���ֶ�ֵ��β�ո�
         CASE WHEN p.project_current_version = 'UNKNOWN' THEN '����'
              WHEN p.project_current_version = '2.82' THEN '2.8.2'
              WHEN p.project_current_version = '2.91' THEN '2.9.1'
              ELSE p.project_current_version END as project_current_version --��Ŀ�汾
  FROM ${dwd_dbname}.dwd_share_project_base_info_df p
  WHERE p.d = DATE_ADD(CURRENT_DATE(), -1) and p.is_filter_project = '1'
)tmp
GROUP BY tmp.cur_date,tmp.belong_ft_name,tmp.project_operation_state,tmp.project_current_version
ORDER BY tmp.cur_date,tmp.belong_ft_name,tmp.project_operation_state,tmp.project_current_version;