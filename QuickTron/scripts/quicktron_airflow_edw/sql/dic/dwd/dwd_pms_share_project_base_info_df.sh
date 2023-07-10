#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： pms项目基础信息表
#-- 注意 ： 每日全量分区
#-- 输入表 : ods.ods_qkt_pms_project_info_df、ods.ods_qkt_pms_wbs_info_df、ods.ods_qkt_pms_user_info_df,ods.ods_qkt_pms_sales_territory_df,ods.ods_qkt_pms_wbs_info_df、dwd_share_project_base_info_df、dim_bpm_ud_spm_mapping_info_ful、ods_qkt_bpm_pm_project_df
#-- dwd_dtk_version_evaluation_info_df、dwd_bpm_equipment_arrival_confirmation_milestone_info_ful
#-- 输出表 ：dwd.dwd_pms_share_project_base_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-09-17 CREATE 
#-- 2 wangziming 2022-10-12 modify pms项目基础信息表新增字段
#-- 3 wangziming 2022-10-13 modify pms项目增加wbs计划阶段相关时间字段以及物料采购和发货
#-- 4 wangziming 2022-10-17 modify 增加销售经理和方案顾问字段
#-- 5 wangziming 2022-10-19 modify 融合bpm数据源的项目基本信息数据
#-- 6 wangziming 2022-10-20 modify 增加项目合同信息数据以及审批流数据
#-- 7 wangziming 2022-10-21 modify 项目审批流程增加项目编码以及新增两个工作流以及合同日期使用合同编号进行关联获取
#-- 8 wangziming 2022-10-25 modify 增加项目阶段组 映射字段 project_dispaly_state_group
#-- 9 wangziming 2022-10-26 modify 修改bpm的project_code与pms的project_sale_code进行去重
#-- 10 wangziming 2022-10-29 modify 新增币种和rmb合同金额字段
#-- 11 wangziming 2022-11-07 modify 增加项目区域类型字段（国内，国外）
#-- 12 wangziming 2022-11-17 modify 修改pms的项目区域取值字段 为  sjqy
#-- 13 wangziming 2022-11-18 modify 增加uat、da相关的计划日期字段
#-- 14 wangziming 2022-11-29 modify 修改pms源
#-- 15 wangziming 2023-01-04 modify 增加设备到货签收计划日期
#-- 16 wangziming 2023-01-05 modify 增加项目地区城市等字段 project_country、project_province、project_city
#-- 17 wangziming 2023-03-06 modify 判断如果金额0and没有合同则把金额置为null
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
htbh as contract_code,
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
					when wbsmc rlike '^(接口业务场景联调|业务场景联调|Interface service scenario joint debugging)' then 'uat'
					when wbsmc rlike '^下发发货通知' then 'da'
					when wbsmc rlike '^设备到货签收' then 'equitment_arrival'
					else 'UNKNOWN' end),jhksrq))),',',':') as start_date_map,
					
str_to_map(concat_ws(',',collect_list(concat_ws(':',(case when wbsmc rlike '^转售后' then 'after_sale'
					when wbsmc rlike '^项目交接' then 'handover'
					when wbsmc rlike '^第一次实施入场' then 'sap_entry'
					when wbsmc rlike '^上线验收' then 'inspection'
					when wbsmc rlike '^终验' then 'final_inspection'
					when wbsmc rlike '^(接口业务场景联调|业务场景联调|Interface service scenario joint debugging)' then 'uat'
					when wbsmc rlike '^下发发货通知' then 'da'
					when wbsmc rlike '^设备到货签收' then 'equitment_arrival'
					else 'UNKNOWN' end),jhjsrq))),',',':') as end_date_map,	
					
str_to_map(concat_ws(',',collect_list(concat_ws(':',(case when wbsmc rlike '^转售后' then 'after_sale'
					when wbsmc rlike '^项目交接' then 'handover'
					when wbsmc rlike '^第一次实施入场' then 'sap_entry'
					when wbsmc rlike '^上线验收' then 'inspection'
					when wbsmc rlike '^终验' then 'final_inspection'
					when wbsmc rlike '^(接口业务场景联调|业务场景联调|Interface service scenario joint debugging)' then 'uat'
					when wbsmc rlike '^下发发货通知' then 'da'
					when wbsmc rlike '^设备到货签收' then 'equitment_arrival'
					else 'UNKNOWN' end),sjgbrq))),',',':') as close_date_map
from ods.ods_qkt_pms_wbs_info_df
where d='${pre1_date}' and wbsmc rlike '^(转售后|项目交接|第一次实施入场|上线验收|终验|接口业务场景联调|业务场景联调|Interface service scenario joint debugging|下发发货通知|设备到货签收)'
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
coalesce(b.id,'UNKNOWN') as id,
htqdrq as contract_signed_date
from ${ods_dbname}.ods_qkt_pms_contract_df a
left join tmp_pms_project_str b on a.htbh=b.contract_code
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
					when workflow_name='设备到货签收里程碑流程' then 'equitment_arrival_approval_date'
					else 'UNKNOWN' end),workflow_date))),',',':') as workflow_map
from 
(
select 
split(requestnamenew,'-')[0] as workflow_name,
xmbm as project_code,
-- upper(regexp_replace(regexp_extract(requestnamenew,'(?<=项目编码:).*,{1}',0),',','')),
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
split(e.sjqy,' ')[0] as project_area,
regexp_replace(a.hylx,'\r|\n|\t','') as project_industry_type,
dtk_product.project_product_name,
case when a.gzft='箱式' then '箱式FT'
	 when a.gzft='智能搬运' then '智能搬运FT'
	else a.gzft end as project_attr_ft,
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
concat(a.xmdj,'级') as project_priority,
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
if(nvl(a.lxrq,'')<>'',a.lxrq,workflow.workflow_map['external_project_handover_approval_date']) as external_project_handover_approval_date,
null as post_project_date,
if(nvl(pms_pe.pe,'')='',null,pms_pe.pe) as pe_members,
case when (upper(a.xmbm) rlike '^(C|D|H|M|N|Q|SCM-|SW|FB-)' and  upper(a.xmbm)<>'C35052') then '0'
	else '1' end as is_filter_project,
regexp_replace(a.cpx1,'\r|\n|\t','') as project_agv_product_name,
workflow.workflow_map['equitment_arrival_approval_date'] as equitment_arrival_approval_date,
case when a.xmzt='已取消' then '项目取消' 
	 when a.xmzt='已暂停' then '项目暂停'
	 when a.xmjd in ('项目启动','蓝图规划') then '需求确认/分解阶段'
	 when a.xmjd in ('现场实施','到货签收') then '发货阶段'
	 when a.xmjd='上线' then '上线实施阶段'
	 when a.xmjd='终验' then '验收阶段'
	 when a.xmjd='移交运维/转售后' then '售后移交阶段'
	 when a.xmjd='项目结项' then '项目结项'
	 else null end as project_dispaly_state_group,
a.bz as currency, 
if(a.rmbhtzje=0 and contract.id is null,null,a.rmbhtzje) as contract_rmb_amount,
e.qylx as project_area_type,
wbs.start_date_map['uat'] as uat_plan_start_date,
wbs.end_date_map['uat'] as uat_plan_end_date,
wbs.close_date_map['uat'] as uat_actual_close_date,
wbs.start_date_map['da'] as da_plan_start_date,
wbs.end_date_map['da'] as da_plan_end_date,
wbs.close_date_map['da'] as da_actual_close_date,
wbs.start_date_map['equitment_arrival'] as equitment_arrival_plan_start_date,
wbs.end_date_map['equitment_arrival'] as equitment_arrival_plan_end_date,
wbs.close_date_map['equitment_arrival'] as equitment_arrival_actual_close_date,
a.szgj as project_country,
a.szsf as project_province,
a.szcs as project_city
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
tmp_bpm_equitment_approval_date as ( -- bpm 设备到货签收审批日期
select 
tmp.project_code,
tmp.equitment_arrival_approval_date
from 
(
select 
e.project_code,
substr(e.equitment_arrival_date,1,10) as equitment_arrival_approval_date, -- 设备到货签订日期
row_number() over(PARTITION by e.project_code order by e.start_time desc) rn
from ${dwd_dbname}.dwd_bpm_equipment_arrival_confirmation_milestone_info_ful e
where e.approve_status = 30 
) tmp
where tmp.rn = 1
),
tmp_bpm_share_project_str as ( -- 去除与pms的交集的bpm项目表基础信息数据
select 
a.project_code, 
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
null as project_agv_product_name,
f.equitment_arrival_approval_date,
case when a.project_dispaly_state = '0.未开始' OR a.project_dispaly_state = '1.立项/启动阶段' OR a.project_dispaly_state = '2.需求确认/分解' OR a.project_dispaly_state = '3.设计开发/测试' then '需求确认/分解阶段'
                when a.project_dispaly_state = '4.采购/生产' OR a.project_dispaly_state = '5.发货/现场实施' then '发货阶段'
                when a.project_dispaly_state = '6.上线/初验/用户培训' then '上线实施阶段'
                when a.project_dispaly_state = '7.终验' then '验收阶段'
                when a.project_dispaly_state like '8.移交运维/转售后' then '售后移交阶段'
                when a.project_dispaly_state = '9.项目结项' then '项目结项'
                when a.project_dispaly_state = '10.项目暂停' then '项目暂停'
                when a.project_dispaly_state = '11.项目取消' then '项目取消'
                else NULL end as project_dispaly_state_group,
null as currency, 
null as contract_rmb_amount,
null as project_area_type,
null as uat_plan_start_date,
null as uat_plan_end_date,
null as uat_actual_close_date,
null as da_plan_start_date,
null as da_plan_end_date,
null as da_actual_close_date,
null as equitment_arrival_plan_start_date,
null as equitment_arrival_plan_end_date,
null as equitment_arrival_actual_close_date,
null as project_country,
null as project_province,
null as project_city
from 
${dwd_dbname}.dwd_share_project_base_info_df a
left join ${ods_dbname}.ods_qkt_pms_project_info_df b on a.project_code=upper(b.xmbm) and b.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_pms_project_info_df b1 on a.project_code=upper(b1.sqbh) and b1.d='${pre1_date}'
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
left join tmp_bpm_equitment_approval_date f on a.project_code=f.project_code
where a.d='${pre1_date}' and a.is_fh_pre_transform=0 and b.id is null and b1.id is null
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
project_agv_product_name,
equitment_arrival_approval_date,
project_dispaly_state_group,
currency, 
contract_rmb_amount,
project_area_type,
uat_plan_start_date,
uat_plan_end_date,
uat_actual_close_date,
da_plan_start_date,
da_plan_end_date,
da_actual_close_date,
equitment_arrival_plan_start_date,
equitment_arrival_plan_end_date,
equitment_arrival_actual_close_date,
project_country,
project_province,
project_city
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
project_agv_product_name,
equitment_arrival_approval_date,
project_dispaly_state_group,
currency, 
contract_rmb_amount,
project_area_type,
uat_plan_start_date,
uat_plan_end_date,
uat_actual_close_date,
da_plan_start_date,
da_plan_end_date,
da_actual_close_date,
equitment_arrival_plan_start_date,
equitment_arrival_plan_end_date,
equitment_arrival_actual_close_date,
project_country,
project_province,
project_city
from 
tmp_bpm_share_project_str
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"




#!/bin/bash

start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select project_code,project_name from dwd.dwd_pms_share_project_base_info_df where d='\${pre1_date}'
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin mysqlwriter 
\--column project_code,project_name
\--ipAddress 008.bg.qkt 
\--port 3306 
\--dataBase bpm
\--table dic_share_project_info 
\--preSql truncate table dic_share_project_info 
\--passWord quicktron123456 
\--userName root 
\--channel 1" "dic_share_project_info" "${pre1_date}"