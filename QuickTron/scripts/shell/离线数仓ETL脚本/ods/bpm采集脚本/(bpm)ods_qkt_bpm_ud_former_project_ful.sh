#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 采集 bpm 老项目的信息记录
#-- 注意 ： 每天全量
#-- 输入表 : bpm.ud_former_project
#-- 输出表 ：ods.ods_qkt_bpm_ud_former_project_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-04-18 CREATE 

# ------------------------------------------------------------------------------------------------

## 设置环境变量
export HCAT_HOME=/opt/module/hive-3.1.2/hcatalog
export PATH=$PATH:$HCAT_HOME/bin

######### 设置表的变量
ods_dbname=ods
table=ods_qkt_bpm_ud_former_project_ful
outdir=/data/sqoop/logs/hcatalog
target_dir=/warehouse/hive/ods/
mysql_dbname=bpm
mysql_table=ud_former_project
datax_incre_column=datax_update_time
hive=/opt/module/hive-3.1.2/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else 
    pre1_date=`date -d "-1 day" +%F`
fi

## hcatalog不支持文件覆盖，为了避免重跑导致数据重复，先判断后是否存在再删除hdfs上的文件
hdfs dfs -test -d $target_dir$table
if [ $? -eq 0 ] ;then 
    hdfs dfs -rm -r $target_dir$table/*
    echo 'clean up'
else 
    echo 'not clean up' 
fi



 /opt/module/sqoop-1.4.7/bin/sqoop import -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://008.bg.qkt:3306/$mysql_dbname?useUnicode=true&characterEncoding=utf-8&tinyInt1isBit=false" \
--username data_sqoop \
--password quicktron_sqoop \
--query "select
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
from $mysql_table  where 1=1 and \$CONDITIONS"  \
--num-mappers 1 \
--split-by id \
--hcatalog-database ${ods_dbname} \
--hcatalog-table ${table} \
--null-string '\\N' \
--null-non-string '\\N' \
--outdir "$outdir"

echo "#########################ods成功导入分区数据###############################"
