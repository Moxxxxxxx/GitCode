#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： ones 任务流程历史状态数据
#-- 注意 ： 
#-- 输入表 : dwd.dwd_ones_org_user_info_ful、dwd_ones_task_message_info_di、ods.ods_qkt_ones_task_df
#-- 输出表 ：dwd.dwd_ones_task_process_comments_change_info_his
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-07-09 CREATE 
#-- 2 wangziming 2022-07-11 modify 增加增量数据和并流水逻辑以及评论的uuid替换人名
# -----------------------------------------------------------------------------------------------

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


init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

with tmp_ones_task_message_str1 as (
select  
*
from 
${dwd_dbname}.dwd_ones_task_message_info_di 
where reference_type ='2'
),
user_info as (
select 
*
from 
${dwd_dbname}.dwd_ones_org_user_info_ful
)
insert overwrite table ${dwd_dbname}.dwd_ones_task_process_comments_change_info_his
select 
task_uuid, 
task_owner_uuid, 
task_owner_name, 
task_assign_uuid, 
task_assign_name, 
task_summary, 
task_reviewer_uuid, 
task_reviewer_name, 
task_process_time,
replace_uuid(task_comment_content,uuid,user_name) as task_comment_content, 
task_message_staus, 
task_change_time, 
replace_uuid(task_change_content,uuid,user_name) as task_change_content
from 
(
select 
a.reference_id as task_uuid,
b.owner as task_owner_uuid,
c1.user_name as task_owner_name,
b.assign as task_assign_uuid,
c2.user_name as task_assign_name,
b.summary as task_summary,
a.from_uuid as task_reviewer_uuid,
c3.user_name as task_reviewer_name,
a.send_time as task_process_time,
regexp_replace(a.message,'\t|\n|\r','') as task_comment_content,
get_json_object(a.ext_json,'$.message_status') as task_message_staus,
to_utc_timestamp (floor(cast(get_json_object(a.ext_json,'$.update_time') as bigint)/1000),'GMT-8') as  task_change_time,
regexp_replace(concat(get_json_object(a.ext_json,'$.replied_message_uuid'),'#',get_json_object(a.ext_json,'$.replied_message_history_text')),'\t|\r|\n','') as task_change_content
from 
tmp_ones_task_message_str1 a
left join ${ods_dbname}.ods_qkt_ones_task_df b on a.reference_id=b.uuid and b.d='${pre1_date}'
left join user_info c1 on b.owner=c1.uuid
left join user_info c2 on b.assign=c2.uuid
left join user_info c3 on a.from_uuid=c3.uuid
where nvl(a.message,'')<>'' or nvl(get_json_object(a.ext_json,'$.message_status'),'')<>''
) t
left join (select org_uuid ,concat_ws(',',collect_list(uuid)) as uuid,concat_ws(',',collect_list(user_name)) as user_name from  dwd.dwd_ones_org_user_info_ful group by org_uuid) b on 1=1

"

columns=uuid,task_uuid,task_owner_uuid,task_owner_name,task_assign_uuid,task_assign_name,task_issue_type_uuid,task_summary,task_process_user_uuid,task_process_user,task_process_time,task_process_field,task_process_field_type,old_task_field_value,new_task_field_value,task_owner_email,task_assign_email,task_process_user_email
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;



with tmp_ones_task_message_str1 as (
select  
*
from 
${dwd_dbname}.dwd_ones_task_message_info_di 
where reference_type ='2' and d='${pre1_date}'
),
user_info as (
select 
*
from 
${dwd_dbname}.dwd_ones_org_user_info_ful
)
insert overwrite table ${dwd_dbname}.dwd_ones_task_process_comments_change_info_his
select 
task_uuid, 
task_owner_uuid, 
task_owner_name, 
task_assign_uuid, 
task_assign_name, 
task_summary, 
task_reviewer_uuid, 
task_reviewer_name, 
task_process_time,
replace_uuid(task_comment_content,uuid,user_name) as task_comment_content, 
task_message_staus, 
task_change_time, 
replace_uuid(task_change_content,uuid,user_name) as task_change_content
from 
(
select 
a.reference_id as task_uuid,
b.owner as task_owner_uuid,
c1.user_name as task_owner_name,
b.assign as task_assign_uuid,
c2.user_name as task_assign_name,
b.summary as task_summary,
a.from_uuid as task_reviewer_uuid,
c3.user_name as task_reviewer_name,
a.send_time as task_process_time,
regexp_replace(a.message,'\t|\n|\r','') as task_comment_content,
get_json_object(a.ext_json,'$.message_status') as task_message_staus,
cast(to_utc_timestamp (floor(cast(get_json_object(a.ext_json,'$.update_time') as bigint)/1000),'GMT-8') as string) as  task_change_time,
regexp_replace(concat(get_json_object(a.ext_json,'$.replied_message_uuid'),'#',get_json_object(a.ext_json,'$.replied_message_history_text')),'\t|\r|\n','') as task_change_content
from 
tmp_ones_task_message_str1 a
left join ${ods_dbname}.ods_qkt_ones_task_df b on a.reference_id=b.uuid and b.d='${pre1_date}'
left join user_info c1 on b.owner=c1.uuid
left join user_info c2 on b.assign=c2.uuid
left join user_info c3 on a.from_uuid=c3.uuid
where nvl(a.message,'')<>'' or nvl(get_json_object(a.ext_json,'$.message_status'),'')<>''
) a
left join (select org_uuid ,concat_ws(',',collect_list(uuid)) as uuid,concat_ws(',',collect_list(user_name)) as user_name from  dwd.dwd_ones_org_user_info_ful group by org_uuid) b on 1=1


union all
select
task_uuid, 
task_owner_uuid, 
task_owner_name, 
task_assign_uuid, 
task_assign_name, 
task_summary, 
task_reviewer_uuid, 
task_reviewer_name, 
task_process_time, 
task_comment_content, 
task_message_staus, 
task_change_time, 
task_change_content
from 
${tmp_dbname}.tmp_ones_task_process_comments_change_info_df
where d='${pre2_date}'
;


insert overwrite table tmp.tmp_ones_task_process_comments_change_info_df partition(d='${pre1_date}')
select
task_uuid, 
task_owner_uuid, 
task_owner_name, 
task_assign_uuid, 
task_assign_name, 
task_summary, 
task_reviewer_uuid, 
task_reviewer_name, 
task_process_time, 
task_comment_content, 
task_message_staus, 
task_change_time, 
task_change_content
from 
${dwd_dbname}.dwd_ones_task_process_comments_change_info_his
;
"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

