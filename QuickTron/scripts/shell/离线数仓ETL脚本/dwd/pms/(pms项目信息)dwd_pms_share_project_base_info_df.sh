#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： pms项目基础信息表
#-- 注意 ： 每日全量分区
#-- 输入表 : ods.ods_qkt_pms_project_info_df、ods.ods_qkt_pms_wbs_info_df、ods.ods_qkt_pms_user_info_df,ods.ods_qkt_pms_sales_territory_df,ods.ods_qkt_pms_wbs_info_df、dwd_share_project_base_info_df、dim_bpm_ud_spm_mapping_info_ful、ods_qkt_bpm_pm_project_df
#-- dwd_dtk_version_evaluation_info_df
#-- 输出表 ：dwd.dwd_pms_share_project_base_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-09-17 CREATE 
#-- 2 wangziming 2022-10-12 modify pms项目基础信息表新增字段
#-- 3 wangziming 2022-10-13 modify pms项目增加wbs计划阶段相关时间字段以及物料采购和发货
#-- 4 wangziming 2022-10-17 modify 增加销售经理和方案顾问字段
#-- 5 wangziming 2022-10-19 modify 融合bpm数据源的项目基本信息数据
#-- 6 wangziming 2022-10-20 modify 增加项目合同信息数据以及审批流数据
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
set hive.vectorized.execution.enabled = false; -- 解决Output column number expected to be 0 when isRepeating

with tmp_pms_user_str as (
select 
id,
lastname as user_name
from 
${ods_dbname}.ods_qkt_pms_user_info_df
where d='${pre1_date}'
),
tmp_pms_project_str as (
select 
id,
upper(xmbm) as project_code,
if(nvl(upper(sqbh),'')='','UNKNOWN',upper(sqbh)) as pre_project_code
from ${ods_dbname}.ods_qkt_pms_project_info_df 
where d='${pre1_date}'
),
tmp_wbs_date_str as (
select 
xmbm1 as id,
str_to_map(concat_ws(',',collect_list(concat_ws(':',(case when wbsmc rlike '^转售后' then 'after_sale'
					when wbsmc rlike '^项目交接' then 'handover'
					when wbsmc rlike '^第一次实施入场' then 'sap_entry'
					when wbsmc rlike '^上线验收' then 'inspection'
					when wbsmc rlike '^终验' then 'final_inspection'
					else 'UNKNOWN' end),jhksrq))),',',':') as start_date_map,
					
str_to_map(concat_ws(',',collect_list(concat_ws(':',(case when wbsmc rlike '^转售后' then 'after_sale'
					when wbsmc rlike '^项目交接' then 'handover'
					when wbsmc rlike '^第一次实施入场' then 'sap_entry'
					when wbsmc rlike '^上线验收' then 'inspection'
					when wbsmc rlike '^终验' then 'final_inspection'
					else 'UNKNOWN' end),jhjsrq))),',',':') as end_date_map,	
					
str_to_map(concat_ws(',',collect_list(concat_ws(':',(case when wbsmc rlike '^转售后' then 'after_sale'
					when wbsmc rlike '^项目交接' then 'handover'
					when wbsmc rlike '^第一次实施入场' then 'sap_entry'
					when wbsmc rlike '^上线验收' then 'inspection'
					when wbsmc rlike '^终验' then 'final_inspection'
					else 'UNKNOWN' end),sjgbrq))),',',':') as close_date_map
from ods.ods_qkt_pms_wbs_info_df
where d='${pre1_date}' and wbsmc rlike '^(转售后|项目交接|第一次实施入场|上线验收|终验)'
group by xmbm1
),
tmp_material_quantity_str as (
select 
xmbm as id,
sum(sqcgsl) as material_purchase_quantity,
sum(ljyfhsl) as material_shipments_quantity
from ${ods_dbname}.ods_qkt_pms_purchas_info_df 
where d='${pre1_date}'  and nvl(xmbm,'')<>''
group by xmbm
),
tmp_sap_counselor_str as ( -- 方案顾问
select 
t.id,
concat_ws(',',collect_list(rt.user_name)) as sap_counselor
from 
(
select 
a.id,
b.gw
from 
${ods_dbname}.ods_qkt_pms_project_info_df a
lateral view explode(split(if(nvl(fagw,'')='','UNKNOWN',fagw),',')) b as gw
where a.d='${pre1_date}'
) t
left join tmp_pms_user_str rt on t.gw=rt.id
group by t.id
),
tmp_pms_pe_user_str as ( -- pe成员
select 
t.id,
concat_ws(',',collect_list(rt.user_name)) as pe
from 
(
select 
a.id,
b.pe
from 
${ods_dbname}.ods_qkt_pms_project_info_df a
lateral view explode(split(if(nvl(pe,'')='','UNKNOWN',pe),',')) b as pe
where a.d='${pre1_date}'
) t
left join tmp_pms_user_str rt on t.pe=rt.id
group by t.id
),
tmp_pms_contract_str as (
select 
coalesce(b.id,c.id,'UNKNOWN') as id,
coalesce(b.project_code,c.project_code) as project_code,
a.htqdrq as  contract_signed_date
from ${ods_dbname}.ods_qkt_pms_contract_df a
left join tmp_pms_project_str b on upper(a.xmbm)=b.project_code
left join tmp_pms_project_str c on upper(a.xmbm)=c.pre_project_code
where a.d='${pre1_date}'
),
tmp_pms_workflow_str as (
select 
coalesce(b.id,c.id,'UNKNOWN') as id,
coalesce(b.project_code,c.project_code) as project_code,
a.workflow_map
from 
(
select 
project_code,
str_to_map(concat_ws(',',collect_set(concat_ws(':',(case when workflow_name ='上线报告里程碑流程' then 'online_process_approval_date'
					when workflow_name='终验报告里程碑流程' then 'final_inspection_process_approval_date'
					when workflow_name='项目交接流程' then 'external_project_handover_approval_date'
					else 'UNKNOWN' end),workflow_date))),',',':') as workflow_map
from 
(
select 
split(requestnamenew,'-')[0] as workflow_name,
upper(regexp_replace(regexp_extract(requestnamenew,'(?<=项目编码:).*,{1}',0),',','')) as project_code,
lastoperatedate as workflow_date
from 
${ods_dbname}.ods_qkt_pms_workflow_df 
where d='${pre1_date}'
) t
group by project_code
) a
left join tmp_pms_project_str b on a.project_code=b.project_code
left join tmp_pms_project_str c on a.project_code=c.pre_project_code
),
tmp_dtk_project_product as( -- 项目产品线
select 
project_code,
project_product_name,
project_product_type,
project_current_version
from 
(
select 
project_code ,
product_name as project_product_name,
project_type as project_product_type,
if(nvl(upgrade_version,'')='',null,upgrade_version) as project_current_version,
row_number() over(partition by project_code order by upgrade_date desc) as rn
from 
${dwd_dbname}.dwd_dtk_version_evaluation_info_df 
where d='${pre1_date}'
) t
where t.rn=1
),
tmp_pms_share_project_str as ( -- pms项目基础信息数据
select 
upper(a.xmbm) as project_code,
regexp_replace(a.xmmc,'\r|\n|\t','') as project_name,
a.khbm2 as project_custormer_code,
regexp_replace(a.khmc,'\r|\n|\t','') as project_company_name,
a.khdj as project_custormer_level,
a.xmjd as project_operation_state,
regexp_replace(a.xmjj,'\r|\n|\t','') as project_context,
split(e.mc,' ')[0] as project_area,
regexp_replace(a.hylx,'\r|\n|\t','') as project_industry_type,
dtk_product.project_product_name,
a.gzft as project_attr_ft,
a.xt as project_current_version,
b.user_name as project_manager,
-- case sfqzxm when '否'  then '0'
--		    when '是'  then '1'
--		    else '-1' end as is_pre_project,
null as is_pre_project,
upper(a.sqbh) as project_sale_code,
upper(a.shbh) as project_after_sale_code,
a.sxqdrq as online_date,
a.zybgqdrq as final_inspection_date,
case a.xmlx when '常规项目' then 0
			when '自营仓项目' then 1
			when '纯硬件项目' then 2
			when '租赁项目' then 3
			when '售前项目' then 4
			when '通用项目' then 5
			when '公司级项目' then 6
			when '硬件部项目' then 7
			when '市场部项目' then 8
			when '集成类项目' then 9
			when '质量专案项目' then 10
			when '软件部项目' then 11
		else -1 end as project_type_id,
a.xmlx as project_type_name,
a.xmdj as project_priority,
a.xmzt as project_dispaly_state,
case when a.xmnwblb='内部项目' then '0' 
	 when a.xmnwblb='外部项目' then '1'
	 else null end as is_external_project,
c.user_name as spm_name,
a.sbdhqdrq as equitment_arrival_date,
a.htzje as contract_amount,
a.scrcrq as sap_entry_date,
wbs.start_date_map['after_sale'] as after_sale_plan_start_date,
wbs.end_date_map['after_sale'] as after_sale_plan_end_date,
wbs.close_date_map['after_sale'] as after_sale_actual_close_date,
wbs.start_date_map['handover'] as handover_plan_start_date,
wbs.end_date_map['handover'] as handover_plan_end_date,
wbs.close_date_map['handover'] as handover_actual_close_date,
wbs.start_date_map['sap_entry'] as sap_entry_plan_start_date,
wbs.end_date_map['sap_entry'] as sap_entry_plan_end_date,
wbs.close_date_map['sap_entry'] as sap_entry_actual_close_date,
wbs.start_date_map['inspection'] as inspection_plan_start_date,
wbs.end_date_map['inspection'] as inspection_plan_end_date,
wbs.close_date_map['inspection'] as inspection_actual_close_date,
wbs.start_date_map['final_inspection'] as final_inspection_plan_start_date,
wbs.end_date_map['final_inspection'] as final_inspection_plan_end_date,
wbs.close_date_map['final_inspection'] as final_inspection_actual_close_date,
material.material_purchase_quantity,
material.material_shipments_quantity,
sales.user_name as sales_manager,
null as pre_sales_consultant,
if(nvl(sap.sap_counselor,'')='',null,sap.sap_counselor) as sap_counselor,
sales_areaon.user_name as sales_area_director,
'PMS' as data_source,
null as is_business_project,
null as project_area_place,
contract.contract_signed_date,
workflow.workflow_map['online_process_approval_date'] as online_process_approval_date,
workflow.workflow_map['final_inspection_process_approval_date'] as final_inspection_process_approval_date,
null as pre_project_approval_date,
workflow.workflow_map['external_project_handover_approval_date']  as external_project_handover_approval_date,
null as post_project_date,
if(nvl(pms_pe.pe,'')='',null,pms_pe.pe) as pe_members,
case when (upper(a.xmbm) rlike '^(C|D|H|M|N|Q|SCM-|SW|FB-)' and  upper(a.xmbm)<>'C35052') then '0'
	else '1' end as is_filter_project,
regexp_replace(a.cpx1,'\r|\n|\t','') as project_agv_product_name
from 
${ods_dbname}.ods_qkt_pms_project_info_df a
left join tmp_pms_user_str b  on a.xmjl=b.id
left join tmp_pms_user_str c on a.spm=c.id
left join ${ods_dbname}.ods_qkt_pms_sales_territory_df e on a.xsqy=e.id and e.d='${pre1_date}'
left join tmp_wbs_date_str wbs on a.id=wbs.id
left join tmp_material_quantity_str material on a.id=material.id
left join tmp_sap_counselor_str sap on a.id=sap.id
left join tmp_pms_user_str sales on if(nvl(a.xsjl,'')='','UNKNOWN',a.xsjl)=sales.id

left join tmp_pms_user_str sales_areaon on if(nvl(a.xsqyzj,'')='','UNKNOWN',a.xsqyzj)=sales_areaon.id --

left join tmp_pms_pe_user_str pms_pe on a.id=pms_pe.id

left join tmp_pms_contract_str contract on a.id=contract.id
left join tmp_pms_workflow_str workflow on a.id=workflow.id
left join tmp_dtk_project_product dtk_product on upper(a.xmbm)=dtk_product.project_code
where a.d='${pre1_date}'
),
tmp_bpm_share_project_str as ( -- 去除与pms的交集的bpm项目表基础信息数据
select 
project_code, 
project_name, 
project_custormer_code, 
project_company_name, 
project_custormer_level, 
project_operation_state, 
project_context, 
project_area, 
project_industry_type, 
project_product_name, 
project_attr_ft, 
project_current_version,
project_manager, 
is_pre_project, 
project_sale_code, 
null as project_after_sale_code, 
online_date, 
final_inspection_date, 
project_type_id, 
project_type_name, 
project_priority, 
project_dispaly_state, 
null as is_external_project, 
c.spm_name, 
null as equitment_arrival_date, 
null as contract_amount, 
sap_entry_date, 
null as after_sale_plan_start_date, 
null as after_sale_plan_end_date, 
null as after_sale_actual_close_date, 
null as handover_plan_start_date, 
null as handover_plan_end_date, 
null as handover_actual_close_date, 
null as sap_entry_plan_start_date, 
null as sap_entry_plan_end_date, 
null as sap_entry_actual_close_date, 
null as inspection_plan_start_date, 
null as inspection_plan_end_date, 
null as inspection_actual_close_date, 
null as final_inspection_plan_start_date, 
null as final_inspection_plan_end_date, 
null as final_inspection_actual_close_date, 
null as material_purchase_quantity, 
null as material_shipments_quantity, 
e.salespersonnel as sales_manager, 
e.shouqianguwen as pre_sales_consultant, 
sap_counselor, 
e.salesareadirector as sales_area_director, 
'BPM' as data_source, 
is_business_project, 
project_area_place, 
contract_signed_date, 
substr(online_process_approval_time,1,10) as online_process_approval_date, 
substr(final_inspection_process_approval_time,1,10) as final_inspection_process_approval_date,
substr(pre_project_approval_time,1,10) as pre_project_approval_date,
substr(external_project_handover_approval_time,1,10) as external_project_handover_approval_date,
post_project_date, 
pe_members, 
is_filter_project,
null as project_agv_product_name
from 
${dwd_dbname}.dwd_share_project_base_info_df a
left join ${ods_dbname}.ods_qkt_pms_project_info_df b on a.project_code=upper(b.xmbm) and b.d='${pre1_date}'
left join ${dim_dbname}.dim_bpm_ud_spm_mapping_info_ful c on a.project_manager=c.pm_name
left join 
(
select 
*
from 
(
select 
*,
row_number() over(partition by upper(pcode) order by id desc) as rn
from 
${ods_dbname}.ods_qkt_bpm_pm_project_df
where d='${pre1_date}'
) t
where t.rn=1
) e on a.project_code=upper(e.pcode)
where a.d='${pre1_date}' and a.is_fh_pre_transform=0 and b.id is null
)
insert overwrite table ${dwd_dbname}.dwd_pms_share_project_base_info_df partition(d='${pre1_date}')
select 
project_code, 
project_name, 
project_custormer_code, 
project_company_name, 
project_custormer_level, 
project_operation_state, 
project_context, 
project_area, 
project_industry_type, 
project_product_name, 
project_attr_ft, 
project_current_version,
project_manager, 
is_pre_project, 
project_sale_code, 
project_after_sale_code, 
online_date, 
final_inspection_date, 
project_type_id, 
project_type_name, 
project_priority, 
project_dispaly_state, 
is_external_project, 
spm_name, 
equitment_arrival_date, 
contract_amount, 
sap_entry_date, 
after_sale_plan_start_date, 
after_sale_plan_end_date, 
after_sale_actual_close_date, 
handover_plan_start_date, 
handover_plan_end_date, 
handover_actual_close_date, 
sap_entry_plan_start_date, 
sap_entry_plan_end_date, 
sap_entry_actual_close_date, 
inspection_plan_start_date, 
inspection_plan_end_date, 
inspection_actual_close_date, 
final_inspection_plan_start_date, 
final_inspection_plan_end_date, 
final_inspection_actual_close_date, 
material_purchase_quantity, 
material_shipments_quantity, 
sales_manager, 
pre_sales_consultant, 
sap_counselor, 
sales_area_director, 
data_source, 
is_business_project, 
project_area_place, 
contract_signed_date, 
online_process_approval_date, 
final_inspection_process_approval_date,
pre_project_approval_date, 
external_project_handover_approval_date, 
post_project_date, 
pe_members, 
is_filter_project,
project_agv_product_name
from 
tmp_pms_share_project_str

union all
select 
project_code, 
project_name, 
project_custormer_code, 
project_company_name, 
project_custormer_level, 
project_operation_state, 
project_context, 
project_area, 
project_industry_type, 
project_product_name, 
project_attr_ft, 
project_current_version,
project_manager, 
is_pre_project, 
project_sale_code, 
project_after_sale_code, 
online_date, 
final_inspection_date, 
project_type_id, 
project_type_name, 
project_priority, 
project_dispaly_state, 
is_external_project, 
spm_name, 
equitment_arrival_date, 
contract_amount, 
sap_entry_date, 
after_sale_plan_start_date, 
after_sale_plan_end_date, 
after_sale_actual_close_date, 
handover_plan_start_date, 
handover_plan_end_date, 
handover_actual_close_date, 
sap_entry_plan_start_date, 
sap_entry_plan_end_date, 
sap_entry_actual_close_date, 
inspection_plan_start_date, 
inspection_plan_end_date, 
inspection_actual_close_date, 
final_inspection_plan_start_date, 
final_inspection_plan_end_date, 
final_inspection_actual_close_date, 
material_purchase_quantity, 
material_shipments_quantity, 
sales_manager, 
pre_sales_consultant, 
sap_counselor, 
sales_area_director, 
data_source, 
is_business_project, 
project_area_place, 
contract_signed_date, 
online_process_approval_date, 
final_inspection_process_approval_date,
pre_project_approval_date, 
external_project_handover_approval_date, 
post_project_date, 
pe_members, 
is_filter_project,
project_agv_product_name
from 
tmp_bpm_share_project_str
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
