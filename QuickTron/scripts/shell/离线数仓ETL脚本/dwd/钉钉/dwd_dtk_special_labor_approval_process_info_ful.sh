#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ：  钉钉特批劳务运维审批单记录
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_dtk_process_special_labor_approval_df
#-- 输出表 ：dwd.dwd_dtk_special_labor_approval_process_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-02-17 CREATE 
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dwd_dbname=dwd
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


insert overwrite table ${dwd_dbname}.dwd_dtk_special_labor_approval_process_info_ful
select
org_name, 
process_instance_id, 
attached_process_instance_ids, 
biz_action, 
business_id, 
create_time, 
originator_dept_id as dept_id, 
originator_dept_name as dept_name, 
originator_userid as user_id, 
result as  process_result,
status as process_status, 
title as process_title, 
process_project_code as project_code, 
process_project_name as project_name, 
project_addre as project_address,
project_manage, 
approval_reason, 
job_content, 
cost, 
payer, 
problem_type, 
total_approved_days, 
ft, 
region_project, 
region_supply_chain, 
sale, 
sale_region
from 
${ods_dbname}.ods_qkt_dtk_process_special_labor_approval_df 
where d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

