#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2023-01-05 创建
#-- 2 wangyingying 2023-01-30 修正异常判断逻辑
#-- 3 wangyingying 2023-02-22 增加物料名称字段
# ------------------------------------------------------------------------------------------------


hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp


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
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--生产制造部生产数据统计异常明细表 ads_team_ft_engineer_member_work_unusual_detail

INSERT overwrite table ${ads_dbname}.ads_team_ft_engineer_member_work_unusual_detail
SELECT '' AS id, -- 主键
       r.process_instance_id, -- 数据ID
       r.business_id, -- 审批编号
       r.process_start_time, -- 工单创建时间
       r.applicant_user_name, -- 工单创建人
       r.production_date, -- 生产日期
       r.work_order_number, -- 工单号
       r.project_code, -- 项目编码
       r.project_name, -- 项目名称
       r.project_attr_ft AS project_ft, -- 项目所属ft
       r.product_process, -- 组别
       r.product_part_number, -- 产品料号
       k.material_name, -- 物料名称
       r.model_code, -- 车型代号
       r.product_name, -- 产品名称
       nvl(r.agv_standard_time,r.harness_or_parts_standard_time) AS standard_time_minutes, -- 标准工时（分钟）
       ROW_NUMBER()OVER(PARTITION BY r.production_date,r.work_order_number,r.product_part_number,r.product_name,r.business_id ORDER BY r.applicant_user_name ASC) AS work_order_sort, -- 工单排序
	   ROW_NUMBER()OVER(PARTITION BY r.production_date,r.work_order_number,r.product_part_number,r.product_name ORDER BY r.applicant_user_name ASC) AS product_part_sort, -- 物料排序
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM 
(
  SELECT *
  FROM ${dwd_dbname}.dwd_dtk_daily_production_report_info_df r
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b 
  ON b.d = '${pre1_date}' AND (r.project_code = b.project_code OR r.project_code = b.project_sale_code)
  WHERE r.d = '${pre1_date}' AND r.approval_result = 'agree' AND r.approval_status = 'COMPLETED' AND r.is_valid = 1 -- 审批状态:已结束 且 审批结果:审批通过 且 有效记录
    AND r.product_process NOT LIKE '%返工%' 
)r
LEFT JOIN 
(
  SELECT *,ROW_NUMBER()OVER(PARTITION BY w.product_process,w.product_part_number,w.model_code ORDER BY w.start_date ASC,w.end_date ASC) AS rn
  FROM ${dwd_dbname}.dwd_dtk_standard_working_hour_info_df w 
  WHERE w.d = '${pre1_date}' 
)w
ON ((r.production_date >= w.start_date AND r.production_date <= w.end_date) OR (w.rn = 1 AND r.production_date < w.start_date)) AND IF(w.product_part_number IS NULL,r.product_process = w.product_process AND r.model_code = w.model_code,r.product_part_number = w.product_part_number)
-- 物料名称映射表
LEFT JOIN 
(
  SELECT *,ROW_NUMBER()OVER(PARTITION BY k.material_number ORDER BY k.material_id ASC) AS rn
  FROM ${dwd_dbname}.dwd_kde_bd_material_info_df k
  WHERE k.d = '${pre1_date}'
)k
ON k.rn = 1 AND r.product_part_number = k.material_number
WHERE w.standard_working_hour IS NULL 

UNION ALL

SELECT '' AS id, -- 主键
       NULL AS process_instance_id, -- 数据ID
       NULL AS business_id, -- 审批编号
       NULL AS process_start_time, -- 工单创建时间
       NULL AS applicant_user_name, -- 工单创建人
       r.start_date AS production_date, -- 生产日期
       r.work_order_number, -- 工单号
       r.project_code, -- 项目编码
       r.project_name, -- 项目名称
       r.project_attr_ft AS project_ft, -- 项目所属ft
       r.group_name AS product_process, -- 组别
       r.material_number AS product_part_number, -- 产品料号
	   k.material_name, -- 物料名称
       r.model_code, -- 车型代号
       r.product_name, -- 产品名称
       NULL AS standard_time_minutes, -- 标准工时（分钟）
       ROW_NUMBER()OVER(PARTITION BY r.start_date,r.work_order_number,r.material_number,r.machine_type,r.business_id ORDER BY r.applicant_user_name ASC) AS work_order_sort, -- 工单排序
	   ROW_NUMBER()OVER(PARTITION BY r.start_date,r.work_order_number,r.material_number,r.machine_type ORDER BY r.applicant_user_name ASC) AS product_part_sort, -- 物料排序
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM 
(
  SELECT p.*,
         IF(p.group_name IN ('部品','线束'),NULL,p.machine_type) AS model_code,
         IF(p.group_name IN ('部品','线束'),p.name,NULL) AS product_name,
         r.business_id,
         r.applicant_user_name,
         b.project_name,
         b.project_attr_ft
  FROM ${dim_dbname}.dim_product_plan_info_offline p
  LEFT JOIN 
  (
    SELECT r.*
    FROM ${dwd_dbname}.dwd_dtk_daily_production_report_info_df r
    WHERE r.d = '${pre1_date}' AND r.approval_result = 'agree' AND r.approval_status = 'COMPLETED' AND r.is_valid = 1 -- 审批状态:已结束 且 审批结果:审批通过 且 有效记录 
  )r
  ON p.work_order_number = r.work_order_number AND p.start_date = r.production_date AND p.material_number = r.product_part_number AND (IF(p.group_name IN ('部品','线束'),p.name,NULL) = r.product_name OR IF(p.group_name IN ('部品','线束'),NULL,p.machine_type) = r.model_code)
  LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b 
  ON b.d = '${pre1_date}' AND (p.project_code = b.project_code OR p.project_code = b.project_sale_code)
  WHERE p.group_name NOT LIKE '%返工%' AND r.process_instance_id IS NULL 
)r
LEFT JOIN 
(
  SELECT *,ROW_NUMBER()OVER(PARTITION BY w.product_process,w.product_part_number,w.model_code ORDER BY w.start_date ASC,w.end_date ASC) AS rn
  FROM ${dwd_dbname}.dwd_dtk_standard_working_hour_info_df w 
  WHERE w.d = '${pre1_date}' 
)w
ON ((r.start_date >= w.start_date AND r.start_date <= w.end_date) OR (w.rn = 1 AND r.start_date < w.start_date)) AND IF(w.product_part_number IS NULL,r.group_name = w.product_process AND r.model_code = w.model_code,r.material_number = w.product_part_number)
-- 物料名称映射表
LEFT JOIN 
(
  SELECT *,ROW_NUMBER()OVER(PARTITION BY k.material_number ORDER BY k.material_id ASC) AS rn
  FROM ${dwd_dbname}.dwd_kde_bd_material_info_df k
  WHERE k.d = '${pre1_date}'
)k
ON k.rn = 1 AND r.material_number = k.material_number
WHERE w.standard_working_hour IS NULL;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"      