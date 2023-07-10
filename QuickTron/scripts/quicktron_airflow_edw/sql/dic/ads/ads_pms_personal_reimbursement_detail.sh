#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-12-17 新建
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
--pms个人报销明细 ads_pms_personal_reimbursement_detail

INSERT overwrite table ${ads_dbname}.ads_pms_personal_reimbursement_detail
SELECT '' AS id, -- 主键
       p.flow_id AS reimburse_code, -- 报销单编号
       p.flow_name AS reimburse_name, -- 报销单名称
       p.start_time AS reimburse_start_date, -- 开始时间
       p.end_time AS reimburse_update_date, -- 结束时间
       p.reimburse_categories AS reimburse_type, -- 报销类型
       p.apply_user_name AS applicant_user_name, -- 申请人
       p.reimburse_user_name, -- 报销人
       TO_DATE(p.reimburse_date) AS reimburse_date, -- 报销日期
       pvd.project_code,  -- 项目编码
       pvd.project_name, -- 项目名称
       i.start_date, -- 费用开始日期
       i.end_date, -- 费用结束日期
       i.place, -- 地点
       i.cost_categories, -- 费用类别
       i.total_days, -- 总计天数
       i.summary, -- 摘要（是由）
       i.total_amount AS amount, -- 金额
       'BPM' AS data_source, -- 数据来源
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM ${dwd_dbname}.dwd_bpm_personal_expense_account_info_ful p
LEFT JOIN ${dwd_dbname}.dwd_bpm_personal_expense_account_item_info_ful i
ON p.flow_id = i.flow_id
LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail pvd
ON i.project_code = pvd.project_code OR i.project_code = pvd.project_sale_code
WHERE p.approve_status = 30
  AND pvd.project_code IS NOT NULL

UNION ALL 

SELECT '' AS id, -- 主键
       p.reimburse_code, -- 报销单编号
       p.reimburse_form_name AS reimburse_name, -- 报销单名称
       p.submit_time AS reimburse_start_date, -- 开始时间
       p.reimburse_last_update_time AS reimburse_update_date, -- 结束时间
       p.reimburse_form_type AS reimburse_type, -- 报销类型
       p.applicant_name AS applicant_user_name, -- 申请人
       p.submitter_name AS reimburse_user_name, -- 报销人
       TO_DATE(p.submit_time) AS reimburse_date, -- 报销日期
       pvd.project_code,  -- 项目编码
       pvd.project_name, -- 项目名称
       NULL AS start_date, -- 费用开始日期
       NULL AS end_date, -- 费用结束日期
       NULL AS place, -- 地点
       NULL AS cost_categories, -- 费用类别
       NULL AS total_days, -- 总计天数
       p.title AS summary, -- 摘要（是由）
       p.functional_currency_amount AS amount, -- 金额
       'HLY' AS data_source, -- 数据来源
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM ${dwd_dbname}.dwd_hly_personal_reimbursement_info_df p
LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail pvd
ON p.reimburse_project_codes = pvd.project_code OR p.reimburse_project_codes = pvd.project_sale_code
WHERE p.d = '${pre1_date}' AND p.reimburse_status IN ('待付款','已付款') AND p.reimburse_project_codes IS NOT NULL AND p.data_source = 'HLY' 
  AND pvd.project_code IS NOT NULL;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"