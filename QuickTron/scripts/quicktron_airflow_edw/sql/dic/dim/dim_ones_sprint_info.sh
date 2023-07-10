#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： ones项目大类对应的项目迭代小类记录表
#-- 注意 ： 全量
#-- 输入表 : ods.ods_qkt_ones_sprint_df、ods.ods_qkt_ones_task_project_classify_df
#-- 输出表 ：dim.dim_ones_sprint_info
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-02-22 CREATE 
#-- 2 wangziming 2022-07-04 modify 增加开始时间，结束时间以及状态字段
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
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
uuid as project_uuid,
name as project_classify_name
from
${ods_dbname}.ods_qkt_ones_task_project_classify_df
where d='${pre1_date}' and status='1'
)
insert overwrite table ${dim_dbname}.dim_ones_sprint_info
select 
a.uuid as sprint_uuid,
a.title as sprint_classify_name,
a.project_uuid,
b.project_classify_name,
to_utc_timestamp (a.start_time*1000,'GMT-8')  as start_time,
to_utc_timestamp (a.end_time*1000,'GMT-8')  as end_time,
a.status
from 
${ods_dbname}.ods_qkt_ones_sprint_df a
left join project_classify b on a.project_uuid=b.project_uuid
where a.d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

