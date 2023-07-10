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

-- 81822
insert overwrite table ${dwd_dbname}.dwd_bpm_personal_expense_account_info_ful
select 
a.flowid as flow_id,
flowstatus as flow_status,
flowmodelid as flow_model_id,
applyid as apply_user_id,
applyname as apply_user_name,
deptid as dept_id,
deptname as dept_name,
orgid as org_id,
orgname as org_name,
flowname as flow_name,
startdate as start_time,
enddate as end_time, 
string12 as reimburse_categories,
string1 as reimburse_user_name,
string66 as reimburse_user_k3_code,
string29 as org_name_2,
string3 as cost_attr_org,
string41 as cost_tenant,
string42 as cost_tenant_code,
string24 as cost_tenant_bpm_code,
string43 as cost_undertake_dept_name,
string44 as cost_undertake_dept_code,
string23 as cost_undertake_dept_bpm_code,
substr(date1,1,10) as reimburse_date,
number1 as total_reimburse_amount,
number5 as total_reimburse_amount_exclud,
number14 as total_reimburse_tax_amount,
case when string80='有' then '1'
     when string80=''   then '0'
     else null end as is_have_electronic_invoice,
string38 as string38,
string45 as currency,
string74 as credit_ratings,
string52 as k3_handle_order,
string51 as cost_org_code,
string53 as exchange_rate,
string55 as exchange_rate_type,
case when upper(string56)='A' then '1'
     else null end as is_cancel,
string61 as reimburse_flow_number,
substr(date4,1,10) as professional_work_date,
string18 as remark,
string64 as filter_cost_undertake_dept_code,
string54 as cost_tenant_type,
string60 as pm_codes,
case when string69='是' then '1'
     when string69='否' then '0'
     else null end as is_executives,
case when string71='是' then '1'
     when string71='否' then '0'
     else null end as is_cost_tenant,
string72 as assessors,
string70 as draft_leader_code,
string73 as cost_tenant_leader_code,
case when string65='是' then '1'
     when string65='否' then '0'
     else null end as is_owner_project,
upper(string22) as project_code,
string25 as project_name,
string26 as pm_name,
string27 as pm_code,
string30 as project_initiate_dept_isncode,
string31 as project_initiate_dept_name,
string28 as project_isncode,
string46 as currency_code,
string13 as cost_attr_org_code,
string37 as draft_user_isncode,
string21 as position,
number2 as reimburse_expense_summary,
string40 as order_type_code,
string62 as business_number,
string39 as order_type,
number6 as reimburse_budget,
number8 as subsidy_cost_total_amount,
number9 as other_cost_total_amount,
upper(string75) as project_code_5,
string76 as project_name_5,
string77 as pm_name_5,
number11 as tax_amount,
number12 as deduct_tax_after_amount,
string19 as invoice_number,
string57 as is_vat_special_invoice,
string63 as business_number_1,
string9 as remark_2,
string47 as cost_project_name,
string48 as cost_project_code,
string49 as cost_undertake_dept_name_1,
string50 as cost_undertake_dept_code_1,
string6 as customer,
string7 as customer_code,
string8 as region,
string10 as region_code,
string11 as private_warehouse_name,
string20 as private_warehouse_code,
string16 as subsidy_type,
string67 as person_approving_id,
string81 as area,
b.status as approve_status,
c.row_project_codes,
c.row_reimburse_amounts
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
left join (
select 
flow_id,
concat_ws(',',collect_list(project_code)) as row_project_codes,
concat_ws(',',collect_list(reimburse_amount)) as row_reimburse_amounts
from 
(
select 
flowid as flow_id,
string(number4) as reimburse_amount,
upper(string14) as project_code
from ${ods_dbname}.ods_qkt_bpm_app_k3flowentry_df
 where d='${pre1_date}' and nvl(number4,'')<>'' and nvl(string14,'')<>''
 ) a
 group by flow_id
) c on a.flowid=c.flow_id
where a.d='${pre1_date}' 
and a.oflowmodelid='81822'
;


"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"

