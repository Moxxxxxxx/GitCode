#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： bpm dwd层  流程表单信息数据
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_bpm_app_k3flow_df、
#-- 输出表 ：dwd.dwd_bpm_app_k3flow_info_ful、dwd_bpm_final_verification_report_milestone_info_ful、dwd_bpm_equipment_arrival_confirmation_milestone_info_ful、dwd_bpm_external_project_handover_info_ful、dwd_bpm_technical_scheme_review_info_ful、dwd_bpm_contract_review_info_ful、dwd_bpm_supplementary_contract_review_info_ful、dwd_bpm_contract_change_review_info_ful、dwd.dwd_bpm_external_project_pre_apply_info_ful 、dwd_bpm_personal_expense_account_item_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-02-28 CREATE 
#-- 2 wangziming 2022-03-24 modify 增加dwd_bpm_technical_scheme_review_info_ful 字段 project_ft 项目ft
#-- 3 wangziming 2022-03-25 modify 增加字段状态值（部分表）,增加字段正式项目编码dwd_bpm_contract_review_info_ful
#-- 4 wangziming 2022-03-28 modify 增加表 dwd_bpm_external_project_pre_apply_info_ful
#-- 5 wangziming 2022-04-19 modify 增加表 dwd_bpm_project_delivery_approval_info_ful、dwd_bpm_materials_purchase_request_info_ful、dwd_bpm_purchase_request_change_info_ful
#-- 6 wangziming 2022-04-20 modify 增加表字段 dwd_bpm_online_report_milestone_info_ful 实际上线时间
#-- 7 wangziming 2022-05-06 modify 增加表 dwd_bpm_personal_expense_account_info_ful
#-- 8 wangziming 2022-05-26 modify 增加设备到货里程碑新的字段
#-- 9 wangziming 2022-06-07 modify 增加bpm 项目暂停申请表
#-- 10 wangziming 2022-06-29 modify 增加 dwd_bpm_personal_expense_account_item_info_ful（个人报销明细行表）
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


-- 82034
insert overwrite table ${dwd_dbname}.dwd_bpm_materials_purchase_request_info_ful
select 
a.FlowID as flow_id,
FlowStatus as flow_status,
FlowModelID as flow_model_id,
ApplyID as apply_user_id,
ApplyName as apply_user_name,
DeptID as dept_id,
DeptName as dept_name,
OrgID as org_id,
OrgName as org_name,
FlowName as flow_name,
StartDate as start_time,
EndDate as end_time,
upper(string37) as project_code,
string16 as project_name,
string17 as project_type,
string33 as pm_name,
string1 as k3_org_name,
string2 as k3_org_isn,
string11 as k3_org_code,
string3 as applicant_name,
string4 as applicant_id,
string5 as applicant_dept_name,
string6 as applicant_dept_id,
string7 as k3_personnel,
string8 as k3_personnel_code,
string9 as k3_dept_name,
string10 as k3_dept_code,
substr(date1,1,10) as apply_date,
string34 as pm_id,
string31 as k3_pm_id,
string18 as subscribe_type,
string13 as subscribe_type_code,
string12 as k3_apply_type_flag,
substr(date4,1,10) as enquiry_date,
string49 as sales_manager,
string19 as k3_purchase_apply_work_order,
string48 as orders_type,
string14 as k3_currency_code,
string43 as material_borrow_id,
string44 as material_borrow_flow_id,
string45 as material_transaction_work_order,
string47 as expiration_date_status,
string20 as shipping_address,
string52 as sales_area,
string51 as additional_fields,
string50 as sales_manager_id,
string35 as ids,
string36 as gantt_type,
string38 as flow_id_1,
string21 as desc_comment,
string15 as project_code_1,
b.status as approval_staus -- 审批状态（20代表进行中，30代表已完成）
from
${ods_dbname}.ods_qkt_bpm_app_k3flow_df a
left join 
(
select 
flowid,
status
from 
${ods_dbname}.ods_qkt_bpm_es_flow_df 
where d='${pre1_date}'
) b on a.flowid=b.flowid 
where a.oFlowModelID = '82034' and a.d='${pre1_date}'
;

"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"

