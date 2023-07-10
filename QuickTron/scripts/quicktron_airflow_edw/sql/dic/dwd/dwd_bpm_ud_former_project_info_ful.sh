#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： bpm 旧项目的信息记录
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : ods.ods_qkt_bpm_ud_former_project_ful
#-- 输出表 ：dim.dwd_bpm_ud_former_project_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-04-18 CREATE 
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
dwd_dbname=dwd
tmp_dbname=tmp
hive=/opt/module/hive-3.1.2/bin/hive


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else 
    pre1_date=`date -d "-1 day" +%F`
fi

if [ -n "$1" ] ;then
    pre2_date=`date -d "-1 day $1" +%F`
else
    pre2_date=`date -d "-2 day" +%F`
fi

echo "##############################################hive:{start executor dwd}####################################################################"



sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

insert overwrite table ${dwd_dbname}.dwd_bpm_ud_former_project_info_ful 
select
  id,
  project_code,
  project_name,
  project_status,
  project_member,
  gantt_chart,
  project_cost,
  pm_name ,
  pmo_name,
  project_grade,
  finance_terms ,
  settlement_currency,
  currency_exchange_rates,
  total_contract_amount_exclusive,
  total_contract_amount,
  amounts_remitted,
  amounts_no_remittance,
  remittance_ratio,
  contract_amount_exclusive_of_tax,
  contract_amount,
  contract_amount_exclud_rmb,
  contract_amount_of_rmb,
  supplement_contract_amount_exclusive,
  supplement_contract_amount,
  supplement_contract_amount_exclusive_of_rmb,
  supplement_contract_amount_of_rmb,
  customer_name,
  customer_level,
  customer_classify,
  final_customer,
  implementation_area,
  system,
  is_chapter_two_contract,
  contract_sign_date,
  plan_online_date_1,
  actual_online_date,
  plan_preliminary_acceptance_date_2,
  actual_preliminary_accetance_date,
  plan_final_accetance_date,
  actual_final_accetance_date,
  plan_post_project_date,
  actual_post_project_date,
  performance_score,
  performance_rating,
  is_advance_payment ,
  is_payment_for_goods,
  is_acceptance_of_payment,
  agv1_name,
  agv1_type,
  agv1_number,
  agv2_name,
  agv2_type,
  agv2_number,
  agv3_name,
  agv3_type,
  agv3_number,
  agv4_name,
  agv4_type,
  agv4_number,
  agv5_name,
  agv5_type,
  agv5_number,
  project_explain,
  change_number,
  technical_evaluation,
  technical_evaluation_signature
from 
${ods_dbname}.ods_qkt_bpm_ud_former_project_ful
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

