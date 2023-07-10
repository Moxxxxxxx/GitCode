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


-- 81668
insert overwrite table ${dwd_dbname}.dwd_bpm_contract_review_info_ful
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
string1 as contract_code,
string2 as contract_name,
string4 as org_inner_code,
string3 as sales_org,
string5 as customer_name,
string6 as customer_code,
string8 as dept,
string9 as dept_code,
string10 as settlement_currency,
string11 as currency_code,
string12 as sales_group,
string13 as sales_manager,
string14 as salesman_code,
string15 as note,
string16 as final_user_industry_type,
string34 as levied_total,
string37 as org_code,
string38 as total_contract_amount_exclusive_of_tax,
string45 as customer_classification,
string46 as robots_count,
string48 as currency_exchange_rates,
string49 as contract_category,
upper(string50) as pre_sale_code,
string51 as project_location,
string52 as sort_goods_by_type,
case when string53 in('否','无','0') then '0'
     when string53 in ('是','有','1') then '1'
     else '-1' end as is_guaranteed_orders,
case when string54 in('否','无','0') then '0'
     when string54 in ('是','有','1') then '1'
     else '-1' end as is_guaranteed_orders_amount, 
string57 as customer_level,
string58 as contract_type,
case when string61 in('否','无','0') then '0'
     when string61 in ('是','有','1') then '1'
     else '-1' end as is_export_item,
string62 as total_contract_amount_of_rmb,
string63 as total_contract_amount_exclud_rmb,
string64 as contact,
string65 as phone,
string66 as mobile_phone,
string67 as guaranteed_orders_quantity,
string68 as final_user,
string70 as technology_review,
string71 as technical_review_signature_and_seal,
string72 as landing_area,
string76 as flow_id_tecgnical_scheme_review,
string77 as regional_director,
string78 as regional_director_id,
case when string79 in('否','无','0') then '0'
     when string79 in ('是','有','1') then '1'
     else '-1' end as is_grant,
number1 as contract_validity,
number19 as guaranteed_orders_quantity_amount,
number21 as quality_assuraance_period,
number22 as warehouse_area,
date1 as review_date,
date2 as service_start_date,
date3 as service_end_date,
date4 as expected_delivery_date,
date5 as signature_date,
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
where a.oFlowModelID = '81668' and a.d='${pre1_date}'
;


"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"

