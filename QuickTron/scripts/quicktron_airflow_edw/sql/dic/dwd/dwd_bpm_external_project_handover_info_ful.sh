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


-- 81687
insert overwrite table ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful
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
string32 as contract_code,
string89 as contract_category,
upper(string9) as pre_sale_code,
string57 as contract_type,
string28 as consumer_name,
string82 as consumer_class,
string64 as consumer_contract,
string91 as consumer_phone,
string65 as consumer_telephone,
date5 as delivery_date,
string3 as sales_org,
string5 as sales_dept,
string13 as saler,
string29 as final_user,
string10 as final_user_industry_type,
string17 as delivery_way,
string51 as delivery_address,
string12 as settlement_currency,
number31 as currency_exchange_rates,
string56 as total_amount,
string76 as total_without_tax_amount,
number14 as shelf_life,
case when string52='否' then '0'
     when string52='是' then '1'
     else '-1' end as is_export,
string98 as total_amount_of_rmb,
string99 as total_amount_of_rmb_without_tax,
number36 as labor_cost_budget,
number13 as warehouse_area,
string93 as sort_goods_by_type,
date6 as service_start_time,
date7 as service_end_time,
case when string94='否' then '0'
     when string94='是' then '1'
     else '-1' end as is_guaranteed_orders,
string95 as guaranteed_orders_quantity,
case when string96='否' then '0'
     when string96='是' then '1'
     else '-1' end as is_guaranteed_orders_amount,
string97 as guaranteed_orders_quantity_amount,
number34 as implementation_area,
string88 as landing_area,
string35 as spm,
string14 as project_manager,
string54 as project_consultant,
string72 as after_purchase_project,
upper(string31) as project_code,
string30 as project_name,
string69 as pmo,
string68 as project_priority_class,
string67 as project_type,
case when string87 = '标准项目' then '标准项目' 
     when string87 = '其余项目' then '非标项目' 
     when string87 = '硬件项目' then '硬件项目' 
     else '未分类项目' end as project_classification,
string86 as system_classification,
case when string19='无双章' then '0'
     when string19='有双章' then '1'
     else '-1' end as is_chapter_two_contract,
date2 as sign_contract_date,
string77 as pre_sales_consultant,
string78 as product_consultant,
case when string92='否' then '0'
     when string92='是' then '1'
     else '-1' end as is_technical_review,
string21 as project_introduction,
string26 as consumer_expectations,
b.status as approve_status
from
${ods_dbname}.ods_qkt_bpm_app_k3flow_df a
left join 
(
select 
flowid,
status
from 
${ods_dbname}.ods_qkt_bpm_es_flow_df a
where d='${pre1_date}'
) b on a.flowid=b.flowid 
where oFlowModelID = '81687' and d='${pre1_date}'
;

"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"

