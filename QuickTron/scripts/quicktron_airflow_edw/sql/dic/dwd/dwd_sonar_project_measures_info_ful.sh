#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： sonar项目指标数据记录表
#-- 注意 ：  每日t-1增量分区
#-- 输入表 : dwd.dwd_sonar_live_measures_info_ful、ods.ods_qkt_sonar_project_df
#-- 输出表 ：dwd.dwd_sonar_project_measures_info
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-29 CREATE 
#-- 2 wangziming 2021-12-06 modify 增加时间字段分区 d，增加map类型字段metric_value_map
#-- 3 wangziming 2021-12-10 modify 增加项目指标（主要，阻断，严重）这是那个指标
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

with metrics as (
select 
*
from 
(
select 
value,
component_uuid,
metric_uuid,
case metric_uuid when 'AXquLXV0JkokZbyWp-Ll' then '代码行数量'
                 when 'AXquLXWPJkokZbyWp-Ms' then 'bug数量'
                 when 'AXquLXWQJkokZbyWp-Mu' then '漏洞数量'
                 when 'AXquLXWPJkokZbyWp-Mq' then '异味数量'
                 when 'AXquLXWXJkokZbyWp-NF' then '复查热点率'
                 when 'AXquLXWCJkokZbyWp-MD' then '覆盖率'
                 when 'AXquLXWIJkokZbyWp-MW' then '重复行率'
                 when 'AXquLXWHJkokZbyWp-MT' then '重复块数量'
                 when 'AXquLXWKJkokZbyWp-Ma' then '阻断数量'
                 when 'AXquLXWKJkokZbyWp-Mb' then '严重数量'
                 when 'AXquLXWKJkokZbyWp-Mc' then '主要数量'
                 end as metric_name,
row_number() over(partition by component_uuid,metric_uuid order by analysis_uuid desc ) as rn
from 
${ods_dbname}.ods_qkt_sonar_project_measures_df
where d='${pre1_date}' and metric_uuid in ('AXquLXV0JkokZbyWp-Ll','AXquLXWPJkokZbyWp-Ms','AXquLXWQJkokZbyWp-Mu','AXquLXWPJkokZbyWp-Mq','AXquLXWXJkokZbyWp-NF','AXquLXWCJkokZbyWp-MD','AXquLXWIJkokZbyWp-MW','AXquLXWHJkokZbyWp-MT','AXquLXWKJkokZbyWp-Mc','AXquLXWKJkokZbyWp-Ma','AXquLXWKJkokZbyWp-Mb')
) t
where t.rn=1
)
insert overwrite table ${dwd_dbname}.dwd_sonar_project_measures_info_ful partition(d='${pre1_date}')
select 
r1.uuid,
r1.kee as kee_name,
r1.qualifier,
r1.name as project_name,
r1.description as desc,
r1.private as is_private,
r1.tags as project_tags,
r2.component_uuid,
r2.metric_uuid,
r2.metric_name,
r2.metric_value,
r2.metric_value_map
from 
${ods_dbname}.ods_qkt_sonar_project_df r1
left join (
select 
project_uuid,
concat_ws(',',collect_list(component_uuid)) as component_uuid,
collect_list(metric_uuid) as metric_uuid,
collect_list(metric_name) as metric_name,
collect_list(value) as metric_value,
str_to_map(concat_ws(',',collect_set(concat_ws(':',metric_name,value)))) as metric_value_map
from
(
select 
b.project_uuid,
a.component_uuid,
a.metric_uuid,
a.metric_name,
if(metric_uuid in('AXquLXWXJkokZbyWp-NF','AXquLXWCJkokZbyWp-MD','AXquLXWIJkokZbyWp-MW'),concat(round(a.value,2),'%'),a.value) as value
from 
metrics a
left join ${ods_dbname}.ods_qkt_sonar_components_di b on a.component_uuid=b.uuid
) t
group by project_uuid
) r2 on r1.uuid=r2.project_uuid
where r1.d='${pre1_date}'
;
"
sql_0="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

with live_measures as (
select 
measures_value as value,
project_uuid,
component_uuid,
metric_uuid,
case metric_uuid when 'AXquLXV0JkokZbyWp-Ll' then '代码行数量'
                 when 'AXquLXWPJkokZbyWp-Ms' then 'bug数量'
                 when 'AXquLXWQJkokZbyWp-Mu' then '漏洞数量'
                 when 'AXquLXWPJkokZbyWp-Mq' then '异味数量'
                 when 'AXquLXWXJkokZbyWp-NF' then '复查热点率'
                 when 'AXquLXWCJkokZbyWp-MD' then '覆盖率'
                 when 'AXquLXWIJkokZbyWp-MW' then '重复行率'
                 when 'AXquLXWHJkokZbyWp-MT' then '重复块数量'
                 end as metric_name
from
(
select 
*,
row_number() over(partition by  project_uuid,metric_uuid order by updated_time desc) as rn
from 
${dwd_dbname}.dwd_sonar_live_measures_info_ful
where metric_uuid in ('AXquLXV0JkokZbyWp-Ll','AXquLXWPJkokZbyWp-Ms','AXquLXWQJkokZbyWp-Mu','AXquLXWPJkokZbyWp-Mq','AXquLXWXJkokZbyWp-NF','AXquLXWCJkokZbyWp-MD','AXquLXWIJkokZbyWp-MW','AXquLXWHJkokZbyWp-MT')
) t
where t.rn=1
)
insert overwrite table ${dwd_dbname}.dwd_sonar_project_measures_info_ful
select 
r1.uuid,
r1.kee as kee_name,
r1.qualifier,
r1.name as project_name,
r1.description as desc,
r1.private as is_private,
r1.tags as project_tags,
r2.component_uuid,
r2.metric_uuid,
r2.metric_name,
r2.metric_value
from 
${ods_dbname}.ods_qkt_sonar_project_df r1
left join (
select 
project_uuid,
concat_ws(',',collect_list(component_uuid)) as component_uuid,
collect_list(metric_uuid) as metric_uuid,
collect_list(metric_name) as metric_name,
collect_list(value) as metric_value
from
(
select 
project_uuid,
component_uuid,
metric_uuid,
metric_name,
if(metric_uuid in('AXquLXWXJkokZbyWp-NF','AXquLXWCJkokZbyWp-MD','AXquLXWIJkokZbyWp-MW'),concat(round(value,2),'%'),value) as value
from 
live_measures 
) t
group by project_uuid
) r2 on r1.uuid=r2.project_uuid
where r1.d='${pre1_date}'

"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

