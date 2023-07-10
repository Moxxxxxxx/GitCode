#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： ones项目一级分类项目信息记录表
#-- 注意 ： 每天全量跑数据
#-- 输入表 : ods.ods_qkt_ones_task_project_classify_df,ods.ods_qkt_ones_org_user_df,ods.ods_qkt_ones_ods_qkt_ones_project_field_value_df,dim.dim_ones_project_field_option_value_info、dim.dim_ones_project_field_info、dim_dtk_org_level_info
#-- 输出表 dwd.dwd_ones_project_classify_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-04-07 CREATE 
#-- 2 wangziming 2022-07-04 modify 增加字段 org_name_1(不包含project_type_name is null 和 UNKNOWN)的行,dtk_org_id_1
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


with tmp_ones_org_user_str1 as (
select 
uuid as user_uuid,
name as user_name
from 
${ods_dbname}.ods_qkt_ones_org_user_df
where d='${pre1_date}'
),
tmp_ones_project_classify_str2 as (
select 
project_uuid,
str_to_map(concat_ws(',',collect_set(concat_ws(':',field_name,cast(field_value as string)))))  as project_columns
from
(
select 
a.project_uuid,
b.field_name,
nvl(if(b.field_name='项目类型',c.option_value,a.value),'UNKNOWN') as field_value
from 
(
select 
project_uuid,alias as field_alias,value
from
${ods_dbname}.ods_qkt_ones_project_field_value_df
where d='${pre1_date}'
) a
left join 
(
select
distinct 
field_alias,
field_name
from 
${dim_dbname}.dim_ones_project_field_info 
where field_status='1'
) b on a.field_alias=b.field_alias
left join ${dim_dbname}.dim_ones_project_field_option_value_info c on if(b.field_name in('项目类型','项目状态'),a.value=c.option_uuid,1=0)
) rt
group by project_uuid
)
insert overwrite table ${dwd_dbname}.dwd_ones_project_classify_info_ful
select 
uuid,
project_classify_name,
project_owner_uuid,
project_owner_name,
project_assign_uuid,
project_assign_name,
team_uuid,
create_time,
access_mode,
project_progress_status,
project_status,
plan_start_time,
plan_end_time,
is_open_email_notify,
is_sample,
is_archive,
project_archive_user_uuid,
project_archive_user_name,
archive_time,
project_bpm_code,
project_type_name,
if(nvl(project_type_name,'')<>'' and project_type_name<>'UNKNOWN',substr(project_classify_name,2,instr(project_classify_name,']')-2),null) as org_name_1,
t1.org_id_2 as dtk_org_id_1 
from 
(
select 
a.uuid,
a.name as project_classify_name,
a.owner as project_owner_uuid,
u1.user_name as project_owner_name,
a.assign as project_assign_uuid,
u2.user_name as project_assign_name,
a.team_uuid,
if(nvl(substr(a.create_time,11),'')='',from_unixtime(cast(substr(a.create_time,1,10) as bigint),'yyyy-MM-dd HH:mm:ss'),concat(from_unixtime(cast(substr(a.create_time,1,10) as bigint),'yyyy-MM-dd HH:mm:ss'),'.',substr(a.create_time,11))) as create_time,
a.access_mode,
c.option_value as project_progress_status,
a.status as project_status,
from_unixtime(a.plan_start_time,'yyyy-MM-dd HH:mm:ss') as plan_start_time,
from_unixtime(a.plan_end_time,'yyyy-MM-dd HH:mm:ss') as plan_end_time,
a.is_open_email_notify,
a.is_sample,
a.is_archive,
a.archive_user as project_archive_user_uuid,
u3.user_name as project_archive_user_name,
if(nvl(substr(a.archive_time,11),'')='',from_unixtime(cast(substr(a.archive_time,1,10) as bigint),'yyyy-MM-dd HH:mm:ss'),concat(from_unixtime(cast(substr(a.archive_time,1,10) as bigint),'yyyy-MM-dd HH:mm:ss'),'.',substr(a.archive_time,11))) as archive_time,
if(b.project_columns[\"BPM项目编号\"]='UNKNOWN',null,b.project_columns[\"BPM项目编号\"]) as project_bpm_code,
if(b.project_columns[\"项目类型\"]='UNKNOWN',null,b.project_columns[\"项目类型\"]) as project_type_name
from 
${ods_dbname}.ods_qkt_ones_task_project_classify_df a
left join tmp_ones_org_user_str1 u1 on a.owner=u1.user_uuid
left join tmp_ones_org_user_str1 u2 on a.assign=u2.user_uuid
left join tmp_ones_project_classify_str2 b on a.uuid=b.project_uuid
left join tmp_ones_org_user_str1 u3 on a.archive_user=u3.user_uuid
left join ${dim_dbname}.dim_ones_project_field_option_value_info c on a.status_uuid=c.option_uuid
where a.d='${pre1_date}'
) t
left join (
select 
distinct org_id_2,org_name_2 
from ${dim_dbname}.dim_dtk_org_level_info
where org_id_2 is not null
) t1 on t1.org_name_2=if(nvl(t.project_type_name,'')<>'' and t.project_type_name<>'UNKNOWN',substr(t.project_classify_name,2,instr(t.project_classify_name,']')-2),null) 
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

