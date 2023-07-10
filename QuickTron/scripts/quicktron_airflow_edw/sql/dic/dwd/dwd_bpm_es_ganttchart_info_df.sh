#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： bpm甘特图信息记录
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : ods.ods_qkt_bpm_es_ganttchart_df
#-- 输出表 ：dim.dwd_bpm_es_ganttchart_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-22 CREATE 
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

insert overwrite table ${dwd_dbname}.dwd_bpm_es_ganttchart_info_df partition(d='${pre1_date}')
select 
id, 
ganttname as gantt_name, 
level as gantt_level, 
haschild as is_has_child, 
status as gantt_status, 
progress, 
progressbyworklog as is_work_log_of_progress, 
relevance, 
description,  
depends as depend_ids, 
canwrite as is_can_write, 
substr(ganttchartstart,0,10) as gantt_start_date, 
duration as duration_days, 
substr(ganttchartend,0,10) as gantt_end_date, 
startismilestone as is_start_milestone, 
endismilestone as is_end_milestone,
collapsed, 
ganttid as gantt_id, 
flowmodelid as flow_model_id, 
upper(gantttype) as project_code, 
ganttsort as gantt_sort, 
flowid as flow_id,
ganttmark as gantt_mark, 
tname as project_name, 
taskreport as is_task_report, 
nametype as gantt_task_type, 
repeat_start as is_repeat_start, 
associated_contract as is_associated_contract
from 
${ods_dbname}.ods_qkt_bpm_es_ganttchart_df
where d='${pre1_date}'
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

