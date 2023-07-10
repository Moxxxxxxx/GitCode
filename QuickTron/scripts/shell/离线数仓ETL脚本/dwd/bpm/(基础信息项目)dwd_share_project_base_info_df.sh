#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 多数据源项目基础信息表
#-- 注意 ： 每日全量分区
#-- 输入表 : ods.ods_qkt_bpm_pm_project_df、ods.ods_qkt_ones_project_base_info_df、
#-- dwd.dwd_dtk_version_evaluation_info_df、ods.ods_qkt_bpm_app_k3flow_df、ods.ods_qkt_bpm_es_flow_df、dwd.dwd_bpm_es_ganttchart_info_df、dwd.dwd_bpm_technical_scheme_review_info_ful
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
project_code,
ft_group as project_attr_ft
from 
${ods_dbname}.ods_qkt_ones_project_base_info_df
where d='${pre1_date}'
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
upgrade_version as project_current_version,
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
row_number() over(partition by pcode order by id desc) as rn
from 
${ods_dbname}.ods_qkt_bpm_pm_project_df
where d='${pre1_date}' 
) t
where t.rn=1
),
pp_code as ( --合同签订日期
select 
a.flowid as flow_id,
a.string9 as pre_sales_code,
a.string31 as project_code,
a.string32 as contract_code,
substr(a.date2,0,10) as contract_signed_date,
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
tmp_share_project_base_str1 as ( -- 是否前置，是否商机
select 
upper(a.string1) as project_code,
b.status,
b.name,
'1' as is_pre_project
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
	 when t2.project_code is null and t1.project_code rlike '^FH' then '-1'
	 else '0' end as is_pre_project, --是否前置
t3.project_ft, -- ft线
case when t2.project_code is null and t1.project_code rlike '^FH' then '1'
	 when t2.project_code is not null then '0' 
	 else '-1' end as is_business_project, --是否商机
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
t3.technical_end_time -- 技术评审完成时间
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
project_sale_code
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
if(d.project_code is not null,d.project_current_version,'UNKNOWN') as project_current_version,
e.end_date as project_approval_time,
null as project_update_time,
a.pm as project_manager,
upper(a.mprojectcode) as project_sale_code
from 
bpm_project_base a
left join ones_project_ft c on a.pcode=c.project_code
left join dtk_project_product d on a.pcode=d.project_code
left join p_approval e on a.pcode=e.project_code
where nvl(a.pcode,'')<>''
) a
where  project_code not rlike '[\u4e00-\u9fa5]'
and project_operation_state not rlike '(取消|停止)'
) t1
left join tmp_share_project_base_str1 t2 on t1.project_code=t2.project_code
left join technical_scheme_review_str1 t3 on t1.project_code=t3.project_code
left join 
(
select 
upper(project_code) as project_code,
max(contract_signed_date) as contract_signed_date
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
max(a.online_date) as online_date 
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
max(a.project_final_inspection_date) as project_final_inspection_date
from 
${dwd_dbname}.dwd_bpm_final_verification_report_milestone_info_ful a
left join  ${ods_dbname}.ods_qkt_bpm_es_flow_df b on a.flow_id=b.flowid and b.d='${pre1_date}'
where b.status='30'
group by upper(a.project_code)
) t8 on t1.project_code=t8.project_code
left join tmp_share_project_base_str3 t9 on t1.project_code=t9.project_code
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
