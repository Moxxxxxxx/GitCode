-- ads_project_count_info    --项目概览

INSERT overwrite table ${ads_dbname}.ads_project_count_info
SELECT '' as id,
       tmp.cur_date, -- 统计日期
       tmp.belong_ft_name, -- 所属ft
       tmp.project_operation_state, -- 项目运营阶段
       tmp.project_current_version, -- 项目版本
       COUNT(DISTINCT project_code) as project_total_num, -- 项目数量
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT p.d as cur_date, --统计日期
         p.project_code, --项目编码
         IF(p.project_attr_ft is null,'未知',p.project_attr_ft) as belong_ft_name, --项目所属ft
         IF(trim(p.project_operation_state) = 'UNKNOWN','未知',trim(p.project_operation_state)) as project_operation_state, --项目运营阶段，去掉字段值结尾空格
         CASE WHEN p.project_current_version = 'UNKNOWN' THEN '其它'
              WHEN p.project_current_version = '2.82' THEN '2.8.2'
              WHEN p.project_current_version = '2.91' THEN '2.9.1'
              ELSE p.project_current_version END as project_current_version --项目版本
  FROM ${dwd_dbname}.dwd_share_project_base_info_df p
  WHERE p.d = DATE_ADD(CURRENT_DATE(), -1) and p.is_filter_project = '1'
)tmp
GROUP BY tmp.cur_date,tmp.belong_ft_name,tmp.project_operation_state,tmp.project_current_version
ORDER BY tmp.cur_date,tmp.belong_ft_name,tmp.project_operation_state,tmp.project_current_version;