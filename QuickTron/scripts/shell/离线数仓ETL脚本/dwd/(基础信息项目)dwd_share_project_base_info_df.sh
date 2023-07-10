#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 多数据源项目基础信息表
#-- 注意 ： 每日全量分区
#-- 输入表 : ods.ods_qkt_bpm_pm_project_df、ods.ods_qkt_ones_project_base_info_df、
#-- dwd.dwd_dtk_version_evaluation_info_df、ods.ods_qkt_bpm_app_k3flow_df、ods.ods_qkt_bpm_es_flow_df、dwd.dwd_bpm_es_ganttchart_info_df、dwd.dwd_bpm_technical_scheme_review_info_ful,dwd.dwd_ones_work_order_info_df
#-- 输出表 ：dwd.dwd_share_project_base_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-19 CREATE 
#-- 2 wangziming 2021-11-30 modify 增加立项字段、新增逻辑
#-- 3 wangziming 2021-12-09 modify project_context字段进行换行符替换
#-- 4 wangziming 2021-12-25 modify 根据bpm作为主表，进行项目主表过滤以及逻辑判断
#-- 5 wangziming 2021-12-27 modify 添加一个标识字段，判断是否逻辑判断后的数据 is_filter_project (1是、0否)
#-- 6 wangziming 2022-01-20 modify 添加一个 project_operation_state_group （用于表示项目处于哪个阶段组上）,并过滤到行（项目取消、取消、停止）这三个项目阶段的项目数据
#-- 7 wangziming 2022-03-03 modify 添加字段 项目经理
#-- 8 wangziming 2022-03-15 modify 修改项目所属ft线字段取数来源（和并）
#-- 9 wangziming 2022-03-24 modify 增加项目大表的逻辑，增加字段
#-- 10 wangziming 2022-03-25 modify 修改 bpm C开头表的取数逻辑
#-- 11 wangziming 2022-03-28 modify 增加字段 project_type_id，project_type_name,is_fh_pre_transform
#-- 12 wangziming 2022-03-29 modify 增加字段 project_priority sap_counselor sap_entry_time
#-- 13 wangziming 2022-03-30 modify 修改项目版本的取值的逻辑，优先取工单、再取钉钉版本升级的
#-- 14 wangziming 2022-04-12 modify 增加字段 项目所在区域 project_area_place
#-- 15 wangziming 2022-04-19 modify 增加字段 online_process_approval_time，final_inspection_process_approval_time
#-- 16 wangziming 2022-04-20 modify 修改项目版本的逻辑取值,增加字段、pre_project_approval_time、external_project_handover_approval_time,fact_online_date
#-- 17 wangziming 2022-04-21 modify 修改实施入场时间逻辑
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
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




with ones_project_ft as (
select 
upper(project_code) as project_code,
ft_group as project_attr_ft,
REGEXP_EXTRACT(rcs_version,'([0-9]{1}[\\.]{1}[0-9]{1}[\\.]{0,1}[0-9]{0,1})',0) as project_current_version
from 
${ods_dbname}.ods_qkt_ones_project_base_info_df
where d='${pre1_date}'
),
ones_work_order as (
select 
project_code,
project_current_version
from
(
select 
project_code,
if(nvl(project_sys_version,'')='','UNKNOWN',project_sys_version) as project_current_version,
row_number() over(partition by project_code order by created_time desc) as rn
from 
${dwd_dbname}.dwd_ones_work_order_info_df
where d='${pre1_date}'
) t
where t.rn=1
),
dtk_project_product as(
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
if(nvl(upgrade_version,'')='','UNKNOWN',upgrade_version) as project_current_version,
row_number() over(partition by project_code order by upgrade_date desc) as rn
from 
${dwd_dbname}.dwd_dtk_version_evaluation_info_df 
where d='${pre1_date}'
) t
where t.rn=1


),
bpm_project_base as (
select 
*
from 
(
select 
*,
case when pstatus='-' then '0.未开始'
	 when nvl(pstatus,'')='' then 'UNKNOWN'
	 when pstatus='1.项目前期' then '0.未开始'
	 when nvl(regexp_replace(pstatus,'[0-9\.-]',''),'')='' then'UNKNOWN'
	 else pstatus end  as project_dispaly_state,

case regexp_replace(ptype,'-','') when '外部项目' then 0
								  when '外部产品项目' then 1
								  when '内部项目' then 2
								  when '公司级项目' then 3
								  when '售前项目' then 4
								  when '工程ECN项目' then 5
								  when '市场部项目' then 6
								  when '硬件部项目' then 7
								  when '纯硬件项目' then 8
								  when '自营仓项目' then 9
								  when '质量专案项目' then 10
								  when '软件部项目' then 11
								  when '通用项目' then 12
								else -1 end as project_type_id,
row_number() over(partition by pcode order by if(upper(pcode) rlike '^C',case when ptype in ('外部项目','外部产品项目') then 6
	 when ptype='纯硬件项目' then 5
	 when ptype='售前项目' then 4
	 when ptype='质量专案项目' then 3
	 when ptype='公司级项目' then 2
	 else 0 end,id) desc) as rn
from 
${ods_dbname}.ods_qkt_bpm_pm_project_df
where d='${pre1_date}' 
) t
where t.rn=1
),
pp_code as ( --合同签订日期，外部项目交接单审批完成时间
select 
a.flowid as flow_id,
a.string9 as pre_sales_code,
a.string31 as project_code,
a.string32 as contract_code,
substr(a.date2,0,10) as contract_signed_date,
if(a.enddate is null,null,b.endtime) as external_project_handover_approval_time,
row_number() over(partition by a.string9,a.string31 order by a.flowid desc) as rn 
from 
${ods_dbname}.ods_qkt_bpm_app_k3flow_df a
inner join ${ods_dbname}.ods_qkt_bpm_es_flow_df b on a.flowid=b.flowid and b.d='${pre1_date}'
where b.status ='30' and a.d='${pre1_date}' and a.oflowmodelid='81687'
),
p_approval as (select 
project_code,
start_date,
end_date
from 
(
select 
*,row_number() over(partition by project_code order by end_date desc ) as rn
from 
(
select 
t1.project_code,
t2.start_date,
t2.end_date
from 
(select * from pp_code where nvl(pre_sales_code,'')<>'') t1
inner join 
(select 
a.flowid as flow_id,
a.string9 as pre_sales_code,
a.startdate as start_date,
a.enddate as end_date
from 
${ods_dbname}.ods_qkt_bpm_app_k3flow_df a
inner join ${ods_dbname}.ods_qkt_bpm_es_flow_df b on a.flowid=b.flowid and b.d='${pre1_date}'
where b.status='30' and a.d='${pre1_date}' and a.oflowmodelid='82274') t2 on t1.pre_sales_code=t2.pre_sales_code

union all
select 
t1.project_code,
t2.start_date,
t2.end_date
from 
(select * from pp_code where nvl(pre_sales_code,'')='') t1
inner join 
(
select 
a.flowid as flow_id,
a.string1 as contract_code,
a.string50 as pre_sales_code,
a.startdate as start_date,
a.enddate as end_date
from 
${ods_dbname}.ods_qkt_bpm_app_k3flow_df a
inner join ${ods_dbname}.ods_qkt_bpm_es_flow_df b on a.flowid=b.flowid and b.d='${pre1_date}'
where b.status='30' and a.d='${pre1_date}' and a.oflowmodelid='81668'
) t2 on t1.contract_code=t2.contract_code
) rt
) rtt
where rn=1
),
technical_scheme_review_str1 as ( -- ft、技术评审结束时间
select 
project_code,
mprojectcode,
pcode,
project_ft,
technical_end_time
from 
(
select 
project_code,
mprojectcode,
pcode,
project_ft,
technical_end_time,
row_number() over(partition by project_code order by technical_end_time desc) as rn 
from 
(
select 
upper(if(b.mprojectcode is not null or pcode is not null,pcode,a.pre_sale_code)) as project_code,
b.mprojectcode,
b.pcode,
a.project_ft,
a.end_time as technical_end_time
from 
${dwd_dbname}.dwd_bpm_technical_scheme_review_info_ful a 
left join bpm_project_base b on upper(if(nvl(b.mprojectcode,'')<>'',b.mprojectcode,b.pcode))=upper(a.pre_sale_code)
where nvl(a.end_time,'')<>'' and b.id is not null
) t
) rt 
where rt.rn=1

),
tmp_share_project_base_str1 as ( -- 是否前置，是否商机, 前置项目申请完成时间 
select 
upper(a.string1) as project_code,
b.status,
b.name,
'1' as is_pre_project,
if(a.enddate is null,null, b.endtime) as pre_project_approval_time
from 
(
select 
*,
row_number() over(partition by upper(string1) order by startdate desc ) as rn
from
${ods_dbname}.ods_qkt_bpm_app_k3flow_df
where oFlowModelID = '82582' and d='${pre1_date}'
) a
left join ${ods_dbname}.ods_qkt_bpm_es_flow_df b on a.flowid =b.flowid and b.d='${pre1_date}'
where b.status='30' and a.rn=1
),
tmp_share_project_base_str2 as ( -- 现场实施完成评审单完成时间
select 
upper(a.string1) as project_code,
max(a.enddate) as  field_conduct_end_time 
from
${ods_dbname}.ods_qkt_bpm_app_k3flow_df a
left join ${ods_dbname}.ods_qkt_bpm_es_flow_df b on a.flowid=b.flowid and b.d='${pre1_date}'
where a.oFlowModelID = '82574' and a.d='${pre1_date}' and b.status='30'
group by upper(a.string1)
),
tmp_share_project_base_str3 as ( --预计上线时间，预计终验时间
select 
upper(project_code) as project_code,
max(expect_online_date) as expect_online_date,
max(expect_final_inspection_date) as expect_final_inspection_date
from 
(
select 
project_code,
gantt_end_date as expect_online_date,
null as expect_final_inspection_date
from 
${dwd_dbname}.dwd_bpm_es_ganttchart_info_df 
where d='${pre1_date}' and gantt_name='上线'

union all
select 
project_code,
null as expect_online_date,
gantt_end_date as expect_final_inspection_date
from 
${dwd_dbname}.dwd_bpm_es_ganttchart_info_df 
where d='${pre1_date}' and gantt_name='终验'
) t
group by project_code
),
tmp_pre_ttransform_str1 as (
select 
distinct upper(mprojectcode) as pre_sales_code
from 
${ods_dbname}.ods_qkt_bpm_pm_project_df
where d='${pre1_date}'
),
tmp_share_project_base_str4 as ( -- 项目实施顾问
 select 
 upper(a.string1) as project_code,
 concat_ws(',',collect_list(b.string9)) as sap_counselor
 from 
 ods.ods_qkt_bpm_app_k3flow_df a left join ods.ods_qkt_bpm_app_k3flowentry_df b ON a.flowid = b.flowid and b.d='${pre1_date}'
 where a.oFlowModelID = '81679' 
 and a.d='${pre1_date}'
 and b.string7 = '实施顾问' 
 and a.enddate is not null 
 group by  upper(a.string1)

),
tmp_share_project_base_str5 as( --项目实施第一次入场时间
select 
case when project_code rlike '^(A|C|E)' and length(project_code)>=9 then concat(substr(project_code,1,6),'-',substr(project_code,-3))
	else project_code end as project_code,
sap_entry_date
from 
(
select 
case when nvl(t1.project_code,'')<>'' then t1.project_code
	 when nvl(t2.project_code,'')<>'' then t2.project_code
	 else 'UNKNOWN' end project_code,
case when nvl(t1.sap_entry_date,'')<>'' then t1.sap_entry_date
	 when nvl(t2.sap_entry_date,'')<>'' then t2.sap_entry_date
	 else null end sap_entry_date

from 
(
select
project_code,
min(sap_entry_date) as sap_entry_date
from 
(
select 
regexp_extract((case when  project_code rlike '^(AFH)' then substr(project_code,2)
	 when project_code rlike '^(A|C|E)' then regexp_replace(regexp_replace(project_code,'[—-…－_（）.]',''),'[^A-Z0-9]','')
	 else regexp_replace(project_code,'[—…－_（）]','-') end),'(S{1}-{1}[AC]{1}[0-9]{5,6}-{1}[0-9]{1,3}|S{1}-{1}[AC]{1}[0-9]{5,6}|[ACE]{1}[0-9]{8}|[ACE]{1}[0-9]{5}|FH{1}-{1}[A-Z0-9]{5,6}-{1}[A-Z0-9]{4,5}-{1}[0-9]{1,3}|FH{1}-{1}[A-Z0-9]{5,6}-{1}[A-Z0-9]{4,5}|FB{1}-{1}[A-Z0-9]{5,6}-{1}[A-Z0-9]{4,5}-{1}[0-9]{1,3}|FB{1}-{1}[A-Z0-9]{5,6}-{1}[A-Z0-9]{4,5}|HW{1}[0-9]{4,5}-{1}[0-9]{1,3}|HW{1}[0-9]{4,5}|MD{1}-{1}[A-Z0-9]{4}-{1}[0-9]{1,3}|MD{1}-{1}[A-Z0-9]{4}|D{1}[0-9]{5}-{1}[0-9]{1,3}|D{1}[0-9]{5}|N{1}-{1}[A-Z0-9]{5}-{1}[0-9]{1,3}|N{1}-{1}[A-Z0-9]{5})',0) as project_code,
sap_entry_date
from 
(
select 
regexp_replace(upper(a.string2),'[\u4e00-\u9fa5\\s+]','') as project_code,
a.EndDate as sap_entry_date 
from
${ods_dbname}.ods_qkt_bpm_app_k3flow_df a
left join ${ods_dbname}.ods_qkt_bpm_es_flow_df b on a.flowid=b.flowid and b.d='${pre1_date}'
where a.oFlowModelID = '81674'
 and a.d='${pre1_date}' 
 and nvl(a.string2,'')<>''
 and b.status='30'
 and a.enddate is not null
 
 union all
select 
regexp_replace(upper(pcode),'[\u4e00-\u9fa5\\s+]','') as project_code,
submit_date as sap_entry_date
from 
${ods_dbname}.ods_qkt_bpm_ud_peattendancerecord_df
where d='${pre1_date}'
) t
) rt
group by project_code
) t1
full join
(
select 
project_code,
min(sap_entry_date) as sap_entry_date
from 
(
select 
regexp_extract((case when project_code rlike '^(AFH)' then substr(project_code,2)
	 when project_code rlike '^(A|C|E)' then regexp_replace(regexp_replace(project_code,'[—-…－_（）.S]',''),'[^A-Z0-9]','')
	 else regexp_replace(project_code,'[—…－_（）]','-') end),'(S{1}-{1}[AC]{1}[0-9]{5,6}-{1}[0-9]{1,3}|S{1}-{1}[AC]{1}[0-9]{5,6}|[ACE]{1}[0-9]{8}|[ACE]{1}[0-9]{5}|FH{1}-{1}[A-Z0-9]{5,6}-{1}[A-Z0-9]{4,5}-{1}[0-9]{1,3}|FH{1}-{1}[A-Z0-9]{5,6}-{1}[A-Z0-9]{4,5}|FB{1}-{1}[A-Z0-9]{5,6}-{1}[A-Z0-9]{4,5}-{1}[0-9]{1,3}|FB{1}-{1}[A-Z0-9]{5,6}-{1}[A-Z0-9]{4,5}|HW{1}[0-9]{4,5}-{1}[0-9]{1,3}|HW{1}[0-9]{4,5}|MD{1}-{1}[A-Z0-9]{4}-{1}[0-9]{1,3}|MD{1}-{1}[A-Z0-9]{4}|D{1}[0-9]{5}-{1}[0-9]{1,3}|D{1}[0-9]{5}|N{1}-{1}[A-Z0-9]{5}-{1}[0-9]{1,3}|N{1}-{1}[A-Z0-9]{5})',0) as project_code,
sap_entry_date
from 
(
select 
regexp_replace(upper(a.string1),'[\u4e00-\u9fa5\\s+]','') as project_code,
a.date1 as sap_entry_date 
from
${ods_dbname}.ods_qkt_bpm_app_k3flow_df a
left join ${ods_dbname}.ods_qkt_bpm_es_flow_df b on a.flowid=b.flowid and b.d='${pre1_date}'
where a.oFlowModelID = '82622'
 and a.d='${pre1_date}' 
 and b.status='30'
 and a.enddate is not null

union all
select 
regexp_replace(upper(pcode),'[\u4e00-\u9fa5\\s+]','') as project_code,
sign_in_date as sap_entry_date
from 
${ods_dbname}.ods_qkt_bpm_ud_peattendancerecord_df
where d='${pre1_date}'
) t
) rt
group by project_code
 ) t2 on t1.project_code=t2.project_code
 ) resut
where project_code<>'UNKNOWN'
),
tmp_project_area_place as (
select 
a.project_code,
a.pm_name,
b.spm as spm_name,
c.string14 as project_area_place
from 
(
select 
upper(pcode) project_code,
pm as pm_name,
pmid
from ${ods_dbname}.ods_qkt_bpm_pm_project_df
where d='${pre1_date}'
) a
left join ${ods_dbname}.ods_qkt_bpm_ud_spm_df b on a.pmid=b.pmid and b.d='${pre1_date}'
left join 
(select 
distinct string1,string9,string14
from 
${ods_dbname}.ods_qkt_bpm_ud_jsfaps_ful 
where nvl(string9,'')<>'' and nvl(string14,'')<>''
)c on b.spmid=c.string9
where c.string1 in('大区','FT')
)
insert overwrite table ${dwd_dbname}.dwd_share_project_base_info_df partition(d='${pre1_date}')
select 
t1.project_code,
t1.project_name,
t1.project_custormer_code,
t1.project_company_name,
t1.project_custormer_level,
t1.project_operation_state,
t1.project_context,
t1.project_area,
t1.project_industry_type,
t1.project_product_name,
t1.project_product_type,
t1.project_attr_ft,
t1.project_current_version,
t1.project_approval_time,
t1.project_update_time,
t1.is_filter_project,
t1.project_operation_state_group,
t1.project_manager,
case when t2.project_code is not null then is_pre_project
	 else '0' end as is_pre_project, --是否前置
t3.project_ft, -- ft线
case when t2.project_code is null and t1.project_code rlike '^FH' then '1'
	 else '0' end as is_business_project, --是否商机
case when t1.project_sale_code is not null then t1.project_sale_code
	 when t1.project_sale_code is null and t1.project_code rlike '^FH' then t1.project_code
	else null end as project_sale_code,--售前项目编码
case when t4.project_code is not null then t4.contract_signed_date
	 when t5.pre_sales_code is not null then t5.contract_signed_date
	else null end as contract_signed_date,--合同签订日期
t6.field_conduct_end_time,--现场实施评审单完成时间
t7.online_date, -- 上线时间
t8.project_final_inspection_date as final_inspection_date,-- 终验时间
t9.expect_online_date, -- 预计上线时间
t9.expect_final_inspection_date,-- 预计终验时间
t3.technical_end_time, -- 技术评审完成时间
t1.project_type_id,
t1.project_type_name,
if(t10.pre_sales_code is not null,1,0) as is_fh_pre_transform, -- 
t1.project_priority,
t11.sap_counselor,
t12.sap_entry_date,
t1.project_dispaly_state,
t13.project_area_place,
t7.online_process_approval_time,
t8.final_inspection_process_approval_time,
case when t2.project_code is not null and t2.pre_project_approval_time is not null then  t2.pre_project_approval_time
	 when t2.project_code is not null and t2.pre_project_approval_time is null  then 'UNKNOWN'
	 else null end as pre_project_approval_time, 
t4.external_project_handover_approval_time,
t14.fact_online_date
from 
(
select
project_code,
project_name,
project_custormer_code,
project_company_name,
project_custormer_level,
if(project_code rlike '^FH-','0.前置',project_operation_state) as project_operation_state,
project_context,
project_area,
project_industry_type,
project_product_name,
project_product_type,
project_attr_ft,
project_current_version,
project_approval_time,
project_update_time,
case when (project_code rlike '^(C|D|H|M|N|Q|SCM-|SW|FB-)' and  project_code<>'C35052') then '0'
	 when project_operation_state  rlike '(取消|停止)' then '0'
	 when  project_name rlike '测试'  then '0'
	else '1' end as is_filter_project,
case when project_code rlike '^FH-' then '1-前置'
	 when project_operation_state rlike '(项目前期|未开始)$' and  project_code not like 'FH-%' then '0-前期'
	 when project_operation_state rlike '(前置)$' then '1-前置'
	 when project_operation_state rlike '(立项/启动阶段|启动)$' then '2-项目启动'
	 when project_operation_state rlike '(需求确认/分解|采购/生产)$' then '3-蓝图规划'
	 when project_operation_state rlike '(发货/现场实施|设计开发/测试)$' then '4-现场实施'
	 when project_operation_state rlike '(UAT/用户培训/初验/项目交付物评审|上线/初验/用户培训)$' then '5-上线初验'
	 when project_operation_state rlike '(项目结项|移交运维/发布/项目结项|终验|完成|移交运维/转售后)$' then '6-服务支持'
	 when project_operation_state rlike '(项目暂停)$' then '9-项目暂停'
	 else 'UNKNOWN' end as project_operation_state_group,
project_manager,
project_sale_code,
project_type_id,
project_type_name,
project_priority,
project_dispaly_state
from 
(
select 
upper(a.pcode) as project_code,
a.pname as project_name,
if(length(a.ccode)=1 and  a.ccode rlike '[a-zA-Z]{1}','UNKNOWN',if(nvl(a.ccode,'')='','UNKNOWN',a.ccode)) as project_custormer_code,
if(nvl(regexp_replace(a.cname,'-',''),'')='','UNKNOWN', regexp_replace(a.cname,'-',''))as project_company_name,
if(nvl(a.cclass,'')='' and length(a.ccode)=1 and  a.ccode rlike '[a-zA-Z]{1}',a.ccode, a.cclass) as project_custormer_level,
if(nvl(regexp_replace(a.pstatus,'[0-9\.-]',''),'')='','UNKNOWN',a.pstatus) as project_operation_state,
regexp_replace(a.briefinfo,'\r|\n|\t','') as project_context,
if(nvl(a.area,'')='','UNKNOWN',a.area) as project_area,
if((nvl(a.pclass,'')='' or (length(a.pclass)=1 and a.pclass='-')),null,pclass) as  project_industry_type,
if(d.project_code is not null,d.project_product_name,'UNKNOWN') as project_product_name,
if(d.project_code is not null,d.project_product_type,'UNKNOWN') as project_product_type,
if(nvl(a.ft,'')='',c.project_attr_ft,a.ft) as project_attr_ft,
case when f.project_code is not null and f.project_current_version<>'UNKNOWN' then f.project_current_version
	 when c.project_code is not null and nvl(c.project_current_version,'')<>'' then c.project_current_version
	 when d.project_code is not null and d.project_current_version<>'UNKNOWN'  then d.project_current_version
	else 'UNKNOWN' end as project_current_version,
e.end_date as project_approval_time,
null as project_update_time,
a.pm as project_manager,
upper(a.mprojectcode) as project_sale_code,
a.project_type_id,
regexp_replace(a.ptype,'-','') as project_type_name,
a.priority as project_priority,
a.project_dispaly_state
from 
bpm_project_base a
left join ones_project_ft c on upper(a.pcode)=c.project_code
left join dtk_project_product d on upper(a.pcode)=d.project_code
left join p_approval e on upper(a.pcode)=e.project_code
left join ones_work_order f on upper(a.pcode)= f.project_code
where nvl(a.pcode,'')<>''
) a
where  project_code not rlike '[\u4e00-\u9fa5]'
-- and project_operation_state not rlike '(取消|停止)'
) t1
left join tmp_share_project_base_str1 t2 on t1.project_code=t2.project_code
left join technical_scheme_review_str1 t3 on t1.project_code=t3.project_code
left join 
(
select 
upper(project_code) as project_code,
max(contract_signed_date) as contract_signed_date,
max(external_project_handover_approval_time) as external_project_handover_approval_time
from 
pp_code
group by upper(project_code)
) t4 on t1.project_code=t4.project_code
left join 
(
select 
upper(pre_sales_code) as pre_sales_code,
max(contract_signed_date) as contract_signed_date
from 
pp_code
group by upper(pre_sales_code) 
) t5 on t1.project_code=t5.pre_sales_code
left join tmp_share_project_base_str2 t6 on t1.project_code=t6.project_code
left join 
(
select 
upper(a.project_code) as project_code ,
max(a.online_date) as online_date ,
max(endtime) as online_process_approval_time
from 
dwd.dwd_bpm_online_report_milestone_info_ful a
left join  ${ods_dbname}.ods_qkt_bpm_es_flow_df b on a.flow_id=b.flowid and b.d='${pre1_date}'
where b.status='30'
group by upper(a.project_code) 
)t7 on t1.project_code=t7.project_code
left join 
(
select 
upper(a.project_code) as project_code,
max(a.project_final_inspection_date) as project_final_inspection_date,
max(endtime) as final_inspection_process_approval_time
from 
${dwd_dbname}.dwd_bpm_final_verification_report_milestone_info_ful a
left join  ${ods_dbname}.ods_qkt_bpm_es_flow_df b on a.flow_id=b.flowid and b.d='${pre1_date}'
where b.status='30'
group by upper(a.project_code)
) t8 on t1.project_code=t8.project_code
left join tmp_share_project_base_str3 t9 on t1.project_code=t9.project_code
left join tmp_pre_ttransform_str1 t10 on t1.project_code=t10.pre_sales_code
left join tmp_share_project_base_str4 t11 on t1.project_code=t11.project_code
left join tmp_share_project_base_str5 t12 on t1.project_code=t12.project_code
left join tmp_project_area_place t13 on t1.project_code=t13.project_code
left join
(
select 
upper(a.project_code) as project_code ,
max(fact_online_date) as fact_online_date
from 
dwd.dwd_bpm_online_report_milestone_info_ful a
left join  ${ods_dbname}.ods_qkt_bpm_es_flow_df b on a.flow_id=b.flowid and b.d='${pre1_date}'
group by upper(a.project_code) 
)t14 on t1.project_code=t14.project_code
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
