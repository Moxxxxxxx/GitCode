#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： sonar事件通知记录宽边
#-- 注意 ：  每日t-1增量分区
#-- 输入表 : ods.ods_qkt_sonar_issues_di、ods.ods_qkt_sonar_project_df、ods.ods_qkt_sonar_rules_df、ods.ods_qkt_sonar_project_branches_df、ods.ods_qkt_sonar_users_df
#-- 输出表 ：dwd.dwd_sonar_issues_ful_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-29 CREATE 
#-- 2 wangziming 2021-12-01 modify 修改时间转换函数 from_uninxtime 到 to_utc_timestamp
#-- 3 wangziming 2021-12-02 modify 增加timestamp类型的时间字段created_time_v1
#-- 4 wangziming 2021-12-13 modify 修改逻辑，将每天变更的数据加上原来分区的数据取最新的那条数据追加到哪个分区内
#-- 5 wangziming 2021-12-15 modify 增加二级项目分区（根据project_uuid进行二级分区）
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

init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_sonar_issues_ful_di partition(d)
select 
a.kee as kee_uuid,
a.rule_uuid,
b.name as rule_name,
b.scope as rule_scope,
b.priority as rule_priority,
b.status as rule_status,
b.is_external as is_external_rule,
to_utc_timestamp(cast(b.created_at as bigint),'GMT-8') as rule_created_time,
to_utc_timestamp(cast(b.updated_at as bigint),'GMT-8') as rule_updated_time,
a.severity as severity_level,
case when a.severity ='MAJOR' then '主要'
     when a.severity ='BLOCKER' then '阻断'
     when a.severity ='CRITICAL' then '严重'
     when a.severity ='MINOR' then '次要'
     else '提示' end as severity_level_name,
a.manual_severity,
regexp_replace(a.message,'\r|\n|\t','') as issues_message,
a.line,
a.gap,
a.status as issues_status,
case a.status when 'OPEN' then '打开'
              when 'CLOSED' then '关闭'
              when 'RESOLVED' then '解决'
              when 'REOPENED' then '重开'
              when 'TO_REVIEW' then '确认'
              else 'UNKNOWN' end as issues_status_name,
a.resolution as resolution_status,
case a.resolution when 'FIXED' then '解决' 
                  when 'WONTFIX' then '不会修复'
                  when 'FALSE-POSITIVE' then '误判'
                  else 'UNKNOWN' end as resolution_name,
a.checksum,
a.reporter as report_user_uuid,
f.name as report_user_name,
a.assignee as assignee_user_uuid,
g.name as assignee_user_name,
a.author_login as author_email,
a.effort,
to_utc_timestamp(cast(a.created_at as bigint),'GMT-8') as created_time,
to_utc_timestamp(cast(a.updated_at as bigint),'GMT-8') as updated_time,
to_utc_timestamp(cast(a.issue_creation_date as bigint),'GMT-8') as issue_creation_date,
to_utc_timestamp(cast(a.issue_update_date as bigint),'GMT-8') as issue_update_date,
to_utc_timestamp(cast(a.issue_close_date as bigint),'GMT-8') as issue_close_date,
a.tags as issues_tags,
a.component_uuid,
a.project_uuid,
c.name as project_name,
a.issue_type,
case a.issue_type when '1' then '异味'
                  when '2' then 'bug'
                  when '3' then '漏洞'
                  else 'UNKNOWN' end as issue_type_name,
a.from_hotspot,
e.kee as project_branch_name,
to_utc_timestamp(cast(a.created_at as bigint),'GMT-8') as created_time_v1,
substr(to_utc_timestamp(cast(a.created_at as bigint),'GMT-8'),1,10) as d
from 
${ods_dbname}.ods_qkt_sonar_issues_di a
left join ${ods_dbname}.ods_qkt_sonar_rules_df b on a.rule_uuid=b.uuid and b.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_sonar_project_df c on a.project_uuid =c.uuid and c.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_sonar_project_branches_df e on a.project_uuid=e.uuid and e.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_sonar_users_df f on a.reporter=f.uuid and f.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_sonar_users_df g on a.assignee=g.uuid and g.d='${pre1_date}'
where a.d='${pre1_date}'
"

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;



with ods_sonar_di as (
select 
*
from 
(
select 
*
,row_number() over(partition by kee order by updated_at desc) as rn
from ${ods_dbname}.ods_qkt_sonar_issues_di
where d='${pre1_date}'
) t
where t.rn=1
),
dwd_sonar_di as (
select
*
from
${dwd_dbname}.dwd_sonar_issues_ful_di 
where d in (select distinct substr(to_utc_timestamp(cast(created_at as bigint),'GMT-8'),1,10) from ods_sonar_di) and d!='${pre1_date}'
)
insert overwrite table ${dwd_dbname}.dwd_sonar_issues_ful_di partition(d,pt)
select 
kee_uuid,
rule_uuid,
rule_name,
rule_scope,
rule_priority,
rule_status,
is_external_rule,
rule_created_time,
rule_updated_time,
severity_level,
severity_level_name,
manual_severity,
issues_message,
line,
gap,
issues_status,
issues_status_name,
resolution_status,
resolution_name,
checksum,
report_user_uuid,
report_user_name,
assignee_user_uuid,
assignee_user_name,
author_email,
effort,
created_time,
updated_time,
issue_creation_date,
issue_update_date,
issue_close_date,
issues_tags,
component_uuid,
project_uuid,
project_name,
issue_type,
issue_type_name,
from_hotspot,
project_branch_name,
created_time_v1,
d,
project_uuid as pt
from 
(
select 
*,
row_number() over(partition by kee_uuid,d order by flag asc) as rn
from 
(
select 
a.kee as kee_uuid,
a.rule_uuid,
b.name as rule_name,
b.scope as rule_scope,
cast(b.priority as string) as rule_priority,
b.status as rule_status,
b.is_external as is_external_rule,
cast(to_utc_timestamp(cast(b.created_at as bigint),'GMT-8') as string) as rule_created_time,
cast(to_utc_timestamp(cast(b.updated_at as bigint),'GMT-8') as string) as rule_updated_time,
a.severity as severity_level,
case when a.severity ='MAJOR' then '主要'
     when a.severity ='BLOCKER' then '阻断'
     when a.severity ='CRITICAL' then '严重'
     when a.severity ='MINOR' then '次要'
     else '提示' end as severity_level_name,
a.manual_severity,
regexp_replace(a.message,'\r|\n|\t','') as issues_message,
a.line,
a.gap,
a.status as issues_status,
case a.status when 'OPEN' then '打开'
              when 'CLOSED' then '关闭'
              when 'RESOLVED' then '解决'
              when 'REOPENED' then '重开'
              when 'TO_REVIEW' then '确认'
              else 'UNKNOWN' end as issues_status_name,
a.resolution as resolution_status,
case a.resolution when 'FIXED' then '解决' 
                  when 'WONTFIX' then '不会修复'
                  when 'FALSE-POSITIVE' then '误判'
                  else 'UNKNOWN' end as resolution_name,
a.checksum,
a.reporter as report_user_uuid,
f.name as report_user_name,
a.assignee as assignee_user_uuid,
g.name as assignee_user_name,
a.author_login as author_email,
cast(a.effort as string) as effort,
cast(to_utc_timestamp(cast(a.created_at as bigint),'GMT-8') as string) as created_time,
cast(to_utc_timestamp(cast(a.updated_at as bigint),'GMT-8') as string) as updated_time,
cast(to_utc_timestamp(cast(a.issue_creation_date as bigint),'GMT-8') as string) as issue_creation_date,
cast(to_utc_timestamp(cast(a.issue_update_date as bigint),'GMT-8') as string) as issue_update_date,
cast(to_utc_timestamp(cast(a.issue_close_date as bigint),'GMT-8') as string) as issue_close_date,
a.tags as issues_tags,
a.component_uuid,
a.project_uuid,
c.name as project_name,
a.issue_type,
case a.issue_type when '1' then '异味'
                  when '2' then 'bug'
                  when '3' then '漏洞'
                  else 'UNKNOWN' end as issue_type_name,
a.from_hotspot,
e.kee as project_branch_name,
to_utc_timestamp(cast(a.created_at as bigint),'GMT-8') as created_time_v1,
substr(to_utc_timestamp(cast(a.created_at as bigint),'GMT-8'),1,10)  as d,
1 as flag
from 
ods_sonar_di a
left join ${ods_dbname}.ods_qkt_sonar_rules_df b on a.rule_uuid=b.uuid and b.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_sonar_project_df c on a.project_uuid =c.uuid and c.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_sonar_project_branches_df e on a.project_uuid=e.uuid and e.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_sonar_users_df f on a.reporter=f.uuid and f.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_sonar_users_df g on a.assignee=g.uuid and g.d='${pre1_date}'

union all
select 
kee_uuid,
rule_uuid,
rule_name,
rule_scope,
rule_priority,
rule_status,
is_external_rule,
rule_created_time,
rule_updated_time,
severity_level,
severity_level_name,
manual_severity,
issues_message,
line,
gap,
issues_status,
issues_status_name,
resolution_status,
resolution_name,
checksum,
report_user_uuid,
report_user_name,
assignee_user_uuid,
assignee_user_name,
author_email,
effort,
created_time,
updated_time,
issue_creation_date,
issue_update_date,
issue_close_date,
issues_tags,
component_uuid,
project_uuid,
project_name,
issue_type,
issue_type_name,
from_hotspot,
project_branch_name,
created_time_v1,
d,
2 as flag
from 
dwd_sonar_di
) t1
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


