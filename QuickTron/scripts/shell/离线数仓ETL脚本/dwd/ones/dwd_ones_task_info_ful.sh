#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： ones 任务明细数据记录，每天全量
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_ones_task_df、dwd.dwd_ones_project_classify_info_ful、ods.ods_qkt_ones_field_df、ods.ods_qkt_ones_field_value_df、ods.ods_qkt_ones_field_option_df、ods.ods_qkt_ones_field_multi_option_df
#-- dwd.dwd_one_task_process_change_info_his、dim.dim_ones_issue_type、dwd.dwd_ones_org_user_info_ful、dim.dim_ones_sprint_info
#-- 输出表 ：dwd.dwd_ones_task_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-04 CREATE 
#-- 2 wangziming 2021-11-05 modify 去除 中文字段的\r\t\n字段
#-- 3 wangziming 2021-12-01 modify 增加过滤逻辑，过滤掉status不是1的数据
#-- 4 wangziming 2021-12-01 modify 修改时间转换函数 from_uninxtime 到 to_utc_timestamp
#-- 5 wangziming 2022-02-10 modify 增加 project_classify_name 字段（项目名称分类，判断ones任务隶属哪个项目下）
#-- 6 wangziming 2022-02-11 modify 增加工单额外类型map集合字段 property_value_map
#-- 7 wangziming 2022-02-21 modify 增加字段任务类型名称 issue_type_cname，关联 dim_ones_issue_type获取、增加任务状态名称，关联dim_ones_task_status获取、增加字段task_owner_cname、task_owner_email、task_assign_cname、task_assign_email、task_solver_cname、task_solver_email、is_task_solved、is_task_closed
#-- 8 wangziming 2022-02-22 modify 增加字段sprint_classify_name 项目迭代名称字段
#-- 9 wangziming 2022-04-09 modify 增加新需求字段，修改逻辑
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
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

with project_classify as (
select 
t1.task_uuid,
t1.project_uuid,
t2.project_classify_name,
t2.project_bpm_code,
t2.project_type_name,
t2.org_name_1,
t3.org_id_2 as dtk_org_id_1,
if(t2.project_type_name='外部客户项目',
regexp_extract(regexp_replace(regexp_replace(upper(t1.summary),' ',''),'_','-'),'(S{1}-{1}A{1}[0-9]{5,6}-{1}[0-9]{1,3}|S{1}-{1}A{1}[0-9]{5,6}|[AC]{1}[0-9]{5,6}-{1}[0-9]{1,3}|[AC]{1}[0-9]{5,6}|FH{1}-{1}[A-Z0-9]{5,6}-{1}[A-Z0-9]{4,5}-{1}[0-9]{1,3}|FH{1}-{1}[A-Z0-9]{5,6}-{1}[A-Z0-9]{4,5}|HW{1}[0-9]{4,5}-{1}[0-9]{1,3}|HW{1}[0-9]{4,5}|MD{1}-{1}[A-Z0-9]{4}-{1}[0-9]{1,3}|MD{1}-{1}[A-Z0-9]{4}|D{1}[0-9]{5}-{1}[0-9]{1,3}|D{1}[0-9]{5}|N{1}-{1}[A-Z0-9]{5}-{1}[0-9]{1,3}|N{1}-{1}[A-Z0-9]{5})',0)
,null) as external_project_code_title,
t1.summary,
if(t2.project_type_name='外部客户项目',
regexp_extract(regexp_replace(regexp_replace(upper(t1.sprint_classify_name),' ',''),'_','-'),'(S{1}-{1}A{1}[0-9]{5,6}-{1}[0-9]{1,3}|S{1}-{1}A{1}[0-9]{5,6}|[AC]{1}[0-9]{5,6}-{1}[0-9]{1,3}|[AC]{1}[0-9]{5,6}|FH{1}-{1}[A-Z0-9]{5,6}-{1}[A-Z0-9]{4,5}-{1}[0-9]{1,3}|FH{1}-{1}[A-Z0-9]{5,6}-{1}[A-Z0-9]{4,5}|HW{1}[0-9]{4,5}-{1}[0-9]{1,3}|HW{1}[0-9]{4,5}|MD{1}-{1}[A-Z0-9]{4}-{1}[0-9]{1,3}|MD{1}-{1}[A-Z0-9]{4}|D{1}[0-9]{5}-{1}[0-9]{1,3}|D{1}[0-9]{5}|N{1}-{1}[A-Z0-9]{5}-{1}[0-9]{1,3}|N{1}-{1}[A-Z0-9]{5})',0)
,null) as external_project_code_sprint,
t1.sprint_classify_name
from 
(
select 
a.uuid as task_uuid,
a.project_uuid,
a.summary,
b.sprint_classify_name
from ${ods_dbname}.ods_qkt_ones_task_df a
left join ${dim_dbname}.dim_ones_sprint_info b on a.sprint_uuid=b.sprint_uuid
where a.d='2022-04-07'
) t1
left join 
(
select 
uuid as project_uuid,
project_classify_name,
project_bpm_code,
project_type_name,
case when project_type_name in('外部客户项目','内部研发项目','技术&管理工作')
	 then substr(project_classify_name,2,instr(project_classify_name,']')-2)
	 else null end org_name_1
from
${dwd_dbname}.dwd_ones_project_classify_info_ful
where  project_status='1'
) t2 on t1.project_uuid=t2.project_uuid
left join 
(
select 
distinct org_id_2,org_name_2 
from ${dim_dbname}.dim_dtk_org_level_info
where org_id_2 is not null
) t3 on t3.org_name_2=t2.org_name_1
),
task_field as (
select 
uuid,
name as field_name,
case when name in('外部项目名称','处理描述','工单号') then 1
	 when name in('一级分类','二级分类','三级分类','定位方式-是否可通过工具','解决方式-是否可通过工具') then 2
	 else 3 end as flag
from 
${ods_dbname}.ods_qkt_ones_field_df
where d='${pre1_date}' 
and status='1'
and name in ('外部项目名称','处理描述','一级分类','定位方式-是否可通过工具',
'所属FT','二级分类','解决方式-是否可通过工具','工单号','三级分类')
),
task_field_value as (
select 
task_uuid,
property_value_map,
regexp_extract(regexp_replace(regexp_replace(upper(property_value_map['external_project_name']),'',''),'_','-'),'(S{1}-{1}A{1}[0-9]{5,6}-{1}[0-9]{1,3}|S{1}-{1}A{1}[0-9]{5,6}|[AC]{1}[0-9]{5,6}-{1}[0-9]{1,3}|[AC]{1}[0-9]{5,6}|FH{1}-{1}[A-Z0-9]{5,6}-{1}[A-Z0-9]{4,5}-{1}[0-9]{1,3}|FH{1}-{1}[A-Z0-9]{5,6}-{1}[A-Z0-9]{4,5}|HW{1}[0-9]{4,5}-{1}[0-9]{1,3}|HW{1}[0-9]{4,5}|MD{1}-{1}[A-Z0-9]{4}-{1}[0-9]{1,3}|MD{1}-{1}[A-Z0-9]{4}|D{1}[0-9]{5}-{1}[0-9]{1,3}|D{1}[0-9]{5}|N{1}-{1}[A-Z0-9]{5}-{1}[0-9]{1,3}|N{1}-{1}[A-Z0-9]{5})',
0) as external_project_code_column
from 
(
select 
task_uuid,
str_to_map(concat_ws(',',collect_set(concat(field_key,':',field_vaule))),',',':') as property_value_map
from 
(
select 
b.task_uuid,
b.field_uuid,
if(a.flag=1,b.value,c.value) as field_vaule,
case when a.field_name='外部项目名称' then 'external_project_name'
	 when a.field_name='处理描述' then 'process_desc'
	 when a.field_name='工单号'  then 'work_order'
	 when a.field_name='一级分类' then 'first_category'
	 when a.field_name='二级分类' then 'second_category'
	 when a.field_name='三级分类' then 'third_category'
	 when a.field_name='定位方式-是否可通过工具' then 'is_by_tool_for_location'
	 when a.field_name='解决方式-是否可通过工具' then 'is_by_tool_for_solve'
	 else 'UNKNOWN' END field_key
from 
task_field a 
left join ${ods_dbname}.ods_qkt_ones_field_value_df b on a.uuid=b.field_uuid and b.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_ones_field_option_df c on c.uuid=if(a.flag=2,b.value,'UNKNOWN')
where a.flag in (1,2) and b.status='1' 
-- and  if(a.flag=1,'1',c.status) ='1'

union all
select 
b.task_uuid,
b.field_uuid,
concat_ws('、',collect_set(c.value)) as field_vaule,
'belong_to_FT' as field_key
from 
task_field a
left join ${ods_dbname}.ods_qkt_ones_field_multi_option_df b on a.uuid=b.field_uuid and b.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_ones_field_option_df c on b.value=c.uuid and c.d='${pre1_date}'
where a.flag =3 and b.status='1'  and c.status='1'
group by b.task_uuid,
b.field_uuid
) rt
group by task_uuid
) rt1
),
task_extra_property as (
select 
rt.*,
issue.issue_type_cname,
users.user_email as task_process_user_email,
if(issue.issue_type_cname='任务','-1',
case when rt.new_task_field_value in('单功能通过', '回归通过', '已修复', '已提测', '已解决', '已验证', '待回归', '待测试', '待验证', '测试通过', '研发完成', '非Bug','验证中') then '1'
	 else 0 end 
) as is_task_solved,
case when rt.new_task_field_value in('关闭', '完成', '已关单', '已关闭', '已发布', '已完成', '已实现', '项目验证通过') then '1'
	else 0 end as  is_task_closed
from 
(
select 
task_uuid,task_issue_type_uuid,task_process_user_uuid,task_process_user,old_task_field_value,new_task_field_value,task_process_time
from 
(
select 
*,
row_number() over(partition by task_uuid,task_process_field_type order by task_process_time desc) as rn 
from 
${dwd_dbname}.dwd_one_task_process_change_info_his
where task_process_field='field005'
) t
where t.rn=1
) rt 
left join ${dim_dbname}.dim_ones_issue_type issue on rt.task_issue_type_uuid=issue.uuid and issue.status='1'
left join ${dwd_dbname}.dwd_ones_org_user_info_ful users on rt.task_process_user_uuid=users.uuid
)
insert overwrite table ${dwd_dbname}.dwd_ones_task_info_ful
select
a.uuid,
a.team_uuid,
to_utc_timestamp (floor(a.create_time/1000),'GMT-8') as task_create_time,
a.priority as task_priority,
a.owner as task_owner,
a.assign as task_assign,
a.tags as task_tags,
a.sprint_uuid,
a.project_uuid,
a.issue_type_uuid,
a.status_uuid,
to_utc_timestamp (a.deadline*1000,'GMT-8') as deadline_time,
a.status,
regexp_replace(a.summary,'\n|\r|\t','') as summary,
regexp_replace(\`desc\`,'\n|\r|\t','') as task_desc,
regexp_replace(a.desc_rich,'\n|\r|\t','') as task_desc_rich ,
to_utc_timestamp (floor(a.server_update_stamp/1000),'GMT-8') as  server_update_time,
a.complete_time,
to_utc_timestamp (floor(a.open_time/1000),'GMT-8') as open_time,
a.score,
a.parent_uuid,
a.position,
a.number,
a.assess_manhour,
a.path as task_path,
a.sub_issue_type_uuid,
a.related_count,
a.remaining_manhour,
a.issue_type_scope_uuid,
b.project_classify_name,
c.property_value_map,
e.issue_type_cname,
f.task_cname as task_status_cname,
u1.user_name as task_owner_cname,
u1.user_email as task_owner_email,
u2.user_name as task_assign_cname,
u2.user_email as task_assign_email,
case when e.issue_type_cname<>'任务' and (pe.is_task_closed='1' or pe.is_task_solved='1') then pe.task_process_user
	 else 'UNKNOWN' end as task_solver_cname,
case when e.issue_type_cname<>'任务' and (pe.is_task_closed='1' or pe.is_task_solved='1') then pe.task_process_user_email
	 else 'UNKNOWN' end as task_solver_email,
case when pe.is_task_closed='1' then pe.task_process_user
	 else 'UNKNOWN' end as task_close_cname,
case when pe.is_task_closed='1' then pe.task_process_user_email
	 else 'UNKNOWN' end as task_close_email,
case when e.issue_type_cname<>'任务' and (pe.is_task_closed='1' or pe.is_task_solved='1') then '1'
	 when e.issue_type_cname='任务'  then '-1'
	 else '0' end as is_task_solved,
if(nvl(pe.is_task_closed,'')<>'',pe.is_task_closed,0) as is_task_closed,
b.sprint_classify_name,
b.org_name_1,
b.dtk_org_id_1,
coalesce(b.external_project_code_title,b.external_project_code_sprint,c.external_project_code_column) as external_project_code,
b.project_bpm_code,
b.project_type_name
from 
${ods_dbname}.ods_qkt_ones_task_df a
left join project_classify b on a.uuid=b.task_uuid
left join task_field_value c on a.uuid=c.task_uuid
left join ${dim_dbname}.dim_ones_issue_type e on a.issue_type_uuid=e.uuid
left join ${dim_dbname}.dim_ones_task_status f on a.status_uuid=f.uuid
left join ${dwd_dbname}.dwd_ones_org_user_info_ful u1 on a.owner=u1.uuid
left join ${dwd_dbname}.dwd_ones_org_user_info_ful u2 on a.assign=u2.uuid
left join task_extra_property pe on a.uuid=pe.task_uuid
where a.d='${pre1_date}' and a.status='1'

;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
