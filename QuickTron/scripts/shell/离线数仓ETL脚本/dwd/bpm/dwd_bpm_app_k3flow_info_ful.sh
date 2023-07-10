#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： bpm dwd层  流程表单信息数据
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_bpm_app_k3flow_df、
#-- 输出表 ：dwd.dwd_bpm_app_k3flow_info_ful、dwd_bpm_final_verification_report_milestone_info_ful、dwd_bpm_equipment_arrival_confirmation_milestone_info_ful、dwd_bpm_external_project_handover_info_ful、dwd_bpm_technical_scheme_review_info_ful、dwd_bpm_contract_review_info_ful、dwd_bpm_supplementary_contract_review_info_ful、dwd_bpm_contract_change_review_info_ful、dwd.dwd_bpm_external_project_pre_apply_info_ful 
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-02-28 CREATE 
#-- 2 wangziming 2022-03-24 modify 增加dwd_bpm_technical_scheme_review_info_ful 字段 project_ft 项目ft
#-- 3 wangziming 2022-03-25 modify 增加字段状态值（部分表）,增加字段正式项目编码dwd_bpm_contract_review_info_ful
#-- 4 wangziming 2022-03-28 modify 增加表 dwd_bpm_external_project_pre_apply_info_ful
#-- 5 wangziming 2022-04-19 modify 增加表 dwd_bpm_project_delivery_approval_info_ful、dwd_bpm_materials_purchase_request_info_ful、dwd_bpm_purchase_request_change_info_ful
#-- 6 wangziming 2022-04-20 modify 增加表字段 dwd_bpm_online_report_milestone_info_ful 实际上线时间
#-- 7 wangziming 2022-05-06 modify 增加表 dwd_bpm_personal_expense_account_info_ful
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

columns=id,FlowID,FlowStatus,FlowModelID,ApplyID,ApplyName,DeptID,DeptName,OrgID,OrgName,FlowName,StartDate,EndDate,date1,date2,date3,date4,date5,date6,date7,date8,date9,date10,date11,date12,date13,date14,date15,string1,string2,string3,string4,string5,string6,string7,string8,string9,string10,string11,string12,string13,string14,string15,Number1,Number2,Number3,Number4,Number5,Number6,Number7,Number8,Number9,Number10,Number11,Number12,Number13,Number14,Number15,Number16,Number17,Number18,Number19,Number20,Number21,Number22,Number23,Number24,bool1,bool2,bool3,bool4,bool5,bool6,bool7,bool8,bool9,bool10,remark1,Description,BackSucess,string16,string17,string18,string19,string20,string21,string22,string23,string24,string25,string26,string27,string28,string29,string30,string31,string32,string33,string34,string35,string36,string37,string38,string39,string40,string41,string42,string43,string44,string45,string46,string47,string48,string49,string50,string51,string52,string53,string54,string55,string56,string57,string58,string59,string60,string61,string62,string63,string64,string65,string66,string67,string68,string69,string70,string71,string72,string73,string74,string75,string76,string77,string78,string79,string80,string81,string82,string83,string84,string85,string86,string87,string88,string89,string90,string91,string92,string93,string94,string95,string96,string97,string98,string99,string100,bool11,bool12,bool13,bool14,bool15,bool16,bool17,bool18,bool19,bool20,text1,oFlowModelID,ApplyAcc,ErpMsgID,VoucherID,CheckID,text2,text3,text4,text5,cash,budget,cashflow,zdbh,cashflow2,cashflow1,GUID,PrintCount,Office1,FileType,FileSize,Number25,Number26,Number27,Number28,Number29,Number30,Number31,Number32,Number33,Number34,Number35,Number36,Number37,Number38,Number39,Number40,Number41,Number42,Number43,Number44,Number45,backSql


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_bpm_app_k3flow_info_ful
select 
${columns}
from 
${ods_dbname}.ods_qkt_bpm_app_k3flow_df
where d='${pre1_date}'
;

--81733 
insert overwrite table ${dwd_dbname}.dwd_bpm_online_report_milestone_info_ful
select 
flowid as flow_id,
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
upper(string1) as project_code,
string2 as project_name,
string3 as project_manager,
string4 as project_inner_code,
string5 as project_manager_code,
string13 as pmo_id,
string14 as spm_id,
substr(date1,0,10) as online_date,
string12 as note,
string6 as user_apply_flow_id,
string8 as ids,
string9 as gantt_type,
string10 as prompt,
string11 as process_end_copy_personnel,
substr(date1,1,10) as fact_online_date
from 
${ods_dbname}.ods_qkt_bpm_app_k3flow_df
where oFlowModelID = '81733' and d='${pre1_date}'
;

--82023
insert overwrite table ${dwd_dbname}.dwd_bpm_final_verification_report_milestone_info_ful
select 
flowid as flow_id,
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
upper(string1) as project_code,
string2 as project_name,
string3 as project_manager,
string4 as project_inner_code,
string5 as project_manager_code,
string6 as pmo_id,
string13 as spm_id,
substr(date1,0,10) as project_final_inspection_date,
string12 as note,
string8 as user_apply_flow_id,
string7 as ids,
string9 as gantt_type,
string10 as prompt,
string11 as process_end_copy_personnel
from 
${ods_dbname}.ods_qkt_bpm_app_k3flow_df
where oFlowModelID = '82023' and d='${pre1_date}'
;

--82017
insert overwrite table ${dwd_dbname}.dwd_bpm_equipment_arrival_confirmation_milestone_info_ful
select 
flowid as flow_id,
flowstatus as flow_status_id,
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
upper(string1) as project_code,
string2 as project_name,
string3 as project_manager,
string4 as project_inner_code,
string5 as project_manager_code,
string13 as pmo_id,
string14 as spm_id,
date1 as equitment_arrival_date,
string12 as note,
string8 as ids,
string6 as user_apply_flow_id,
string9 as gantt_type,
string10 as prompt,
string11 as process_end_copy_personnel
from 
${ods_dbname}.ods_qkt_bpm_app_k3flow_df
where oFlowModelID = '82017' and d='${pre1_date}'
;


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


-- 81988
insert overwrite table ${dwd_dbname}.dwd_bpm_technical_scheme_review_info_ful
select 
FlowID as flow_id,
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
upper(string1) as pre_sale_code,
string2 as customer_name,
string3 as customer_level,
string4 as industry_type,
string5 as product_hardware_reviewer,
string6 as final_user,
string7 as final_user_industry_type,
string8 as product_module,
string9 as product_line,
case when string10='无' then '0'
     when string10='是' then '1'
     else '-1' end as is_hardware,
string11 as product_hardware_auditor_id,
string12 as product_hardware_auditor,
string13 as annotation,
string14 as sales_manager,
string15 as salesman_id,
string16 as product_software_reviewer_id,
string17 as sales_area,
string18 as project_expert_reviewer,
string19 as project_expert_reviewer_id,
string26 as industry_expert_reviewer,
string27 as project_name,
string28 as industry_expert_reviewer_id,
string29 as product_dimension,
string30 as scene_level,
string31 as area_delivery_auditor,
string32 as area_delivery_auditor_id,
string33 as pre_sales_planning_principal,
string42 as org_code,
string52 as landing_area,
case when string53='否' then '0'
     when string53='是' then '1'
     else '-1' end as is_pro_approval_upgrade,
string54 as pro_approval_man,
string55 as pro_approval_man_id,
string59 as client_code,
string61 as project_ft
from
${ods_dbname}.ods_qkt_bpm_app_k3flow_df
where oFlowModelID = '81988' and d='${pre1_date}'
;

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


-- 82134
insert overwrite table ${dwd_dbname}.dwd_bpm_supplementary_contract_review_info_ful
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
string1 as supplementary_contract_code,
string2 as supplementary_contract_name,
string3 as sales_org,
string4 as org_inner_code,
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
string37 as org_code,
string45 as customer_classification,
string46 as robots_count,
string48 as currency_exchange_rates,
string49 as supplementary_contract_category,
string50 as regional_director,
string51 as project_location,
string52 as sort_goods_by_type,
case when string53 in('否','无','0') then '0'
     when string53 in ('是','有','1') then '1'
     else '-1' end as is_guaranteed_orders,
case when string54 in('否','无','0') then '0'
     when string54 in ('是','有','1') then '1'
     else '-1' end as is_guaranteed_orders_amount,
string57 as customer_level,
string58 as supplementary_contract_type,
case when string61 in('否','无','0') then '0'
     when string61 in ('是','有','1') then '1'
     else '-1' end as is_export_item,
string62 as regional_director_id,
string64 as contact,
string65 as phone,
string66 as mobile_phone,
string67 as guaranteed_orders_quantity,
string68 as final_user,
string70 as principal_contract_code,
string71 as principal_contract_name,
string72 as contract_review_number,
number1 as supplementary_contract_validity,
number18 as supplementary_times,
number19 as guaranteed_orders_quantity_amount,
number21 as quality_assuraance_period,
number22 as warehouse_area,
Number24 as includ_tax_change_amount,
Number25 as exclud_tax_change_amount,
Number26 as rmb_includ_tax_change_amount,
Number27 as rmb_exclud_tax_change_amount,
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
where a.oFlowModelID = '82134' and a.d='${pre1_date}'
;



-- 81904
insert overwrite table ${dwd_dbname}.dwd_bpm_contract_change_review_info_ful
select
FlowID as flow_id,
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
string3 as sales_org,
string4 as org_inner_code,
string5 as customer_name,
string6 as customer_code,
string8 as dept_name,
string9 as dept_code,
string10 as settlement_currency,
string11 as currenct_code,
string12 as sales_group,
string13 as sales_manager,
string14 as salesman_code,
string15 as note,
string16 as final_user_industry_type,
string34 as levied_in_total,
string37 as org_code,
string38 as included_tax_total_amount,
string40 as industry_code,
string45 as customer_classification,
string46 as robots_count,
string47 as robot_category,
string48 as currency_exchange_rates,
string49 as contract_category,
string50 as warehouse_area,
string51 as warehouse_address,
string52 as sort_goods_by_type,
case when string53 in('否','无','0') then '0'
     when string53 in ('是','有','1') then '1'
     else '-1' end as is_guaranteed_orders,
case when string54 in('否','无','0') then '0'
     when string54 in ('是','有','1') then '1'
     else '-1' end as is_guaranteed_orders_amount,
string57 as customer_level,
string58 as contract_type,
string60 as contract_process_number,
string66 as total_rmb_tax,
string67 as rmb_excluding_total_tax,
date1 as review_date,
date2 as service_start_date,
date3 as service_end_date,
date4 as expected_delivery_date,
date5 as signature_date,
number1 as contract_validity,
number18 as guaranteed_orders_quantity,
number19 as guaranteed_orders_quantity_amount
from
${ods_dbname}.ods_qkt_bpm_app_k3flow_df
where oFlowModelID = '81904' and d='${pre1_date}'
;



-- 82582
insert overwrite table ${dwd_dbname}.dwd_bpm_external_project_pre_apply_info_ful
select 
a.id,
a.flowid as flow_id,
a.flowstatus as flow_status, 
a.flowmodelid as flow_model_id,
a.applyid as apply_user_id,
a.applyname as apply_user_name,
a.deptid as dept_id ,
a.deptname as dept_name,
a.orgid as org_id,
a.orgname as org_name,
a.flowname as flow_name,
a.startdate as start_time,
a.enddate as end_time,
upper(a.string1) as pre_sale_code,
a.string2 as pre_sale_name,
a.string3 as current_status,
a.number1 as amount,
a.string6 as remark,
a.string4 as spm_name,
a.string7 as spm_id,
a.string5 as pm_name,
a.string8 as pm_id,
a.string9 as slaes_area,
a.string10 as region_manager,
a.string11 as region_manager_id,
b.status as approve_status
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
where a.oFlowModelID = '82582' and a.d='${pre1_date}'
;

-- 82034
insert overwrite table ${dwd_dbname}.dwd_bpm_project_delivery_approval_info_ful
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
upper(string21) as project_code,
string2 as project_name,
substr(date1,1,10) as plan_shipments_date,
string3 as shipments_order_no,
string4 as customer_name,
string33 as customer_code,
string5 as customer_contacts,
string6 as telephone,
number4 as shipments_time,
string8 as pack_lwhth_requirement,
string9 as pack_tonnage_requirement,
string10 as scene_forklift_status,
string11 as scene_distribution_mode,
string12 as transport_mode,
string13 as pack_mode,
string7 as shipping_address,
string39 as require_source,
string35 as after_sale_work_order,
string42 as mode_of_trade,
string43 as foreign_trade_clause,
string44 as sales_area,
string41 as additional_fields,
string23 as desc_comment,
string16 as ids,
string18 as gantt_type,
string1 as project_code_1,
string36 as draftsman_id,
string38 as draftsman_name,
string22 as project_type,
string24 as pmo_id,
string25 as spm_id,
string26 as receipt_type,
substr(date3,1,10) as make_receipt_date,
string34 as k3_shipments_order_no,
string37 as shipments_tail_after,
string27 as sales_org_name,
string28 as sale_org_code,
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
where a.oFlowModelID = '82040' and a.d='${pre1_date}'
;




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


-- 82550
insert overwrite table ${dwd_dbname}.dwd_bpm_purchase_request_change_info_ful
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
string60 as apply_work_order,
string18 as subscribe_type,
string13 as subscribe_type_code,
upper(string37) as project_code,
string16 as project_name,
string17 as project_type,
string33 as pm_name,
string34 as pm_id,
string1 as k3_org_name,
string2 as k3_org_isn,
string4 as k3_org_code,
string3 as applicant_name,
string4 as applicant_id,
string5 as applicant_dept_name,
string6 as applicant_dept_id,
substr(date1,1,10) as apply_date,
string7 as purchase_apply_flow_id,
string21 as desc_comment,
string22 as material_code,
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
where a.oFlowModelID = '82550' and a.d='${pre1_date}'
;




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

echo "##############################################hive:{end executor dwd}####################################################################"