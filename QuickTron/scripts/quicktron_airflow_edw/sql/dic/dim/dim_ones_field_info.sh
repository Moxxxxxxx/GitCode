#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： ones全局属性维度表
#-- 注意 ： 每天全量覆盖
#-- 输入表 : ods.ods_qkt_ones_field_df
#-- 输出表 ：dim.dim_ones_field_info
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-29 CREATE 

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


insert overwrite table ${dim_dbname}.dim_ones_field_info
select
uuid as field_uuid,
team_uuid as field_team_uuid,
\`type\` as field_type, 
name as field_name, 
renderer as field_renderer, 
filter_option as field_filter_option, 
search_option as field_search_option, 
from_unixtime(create_time,'yyyy-MM-dd HH:mm:ss') as field_create_time, 
built_in as is_in_built,
stay_settings,
related_type,
related_uuid,
status as field_status
from 
${ods_dbname}.ods_qkt_ones_field_df
where d='${pre1_date}'

;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

