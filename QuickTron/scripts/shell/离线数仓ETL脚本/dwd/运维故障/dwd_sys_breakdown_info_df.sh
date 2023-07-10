#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： ones 任务工时状态记录，每天全量
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_collect_sys_breakdown_df
#-- 输出表 ：dwd.dwd_sys_breakdown_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-10 CREATE 

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

echo "$pre1_date"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_sys_breakdown_info_df partition(d='${pre1_date}',pt)
select
id,
host,
item_name,
breakdown_level,
item_status,
project_code,
project_name,
breakdown_end_time,
breakdown_time as breakdown_start_time,
breakdown_description as breakdown_desc,
instance as extra_breakdown_info,
work_order_code,
work_order_status,
dingtalk_status,
project_code as pt
from
${ods_dbname}.ods_qkt_collect_sys_breakdown_df 
where d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

