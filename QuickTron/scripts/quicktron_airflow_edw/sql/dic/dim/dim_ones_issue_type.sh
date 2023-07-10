#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： ones任务类型维度表，每天全量
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_ones_issue_type_df
#-- 输出表 ：dwd.dim_ones_issue_type
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-04 CREATE 
#-- 2 wangziming 2022-08-25 modify 修改成临时的 2022-08-21分区

# ------------------------------------------------------------------------------------------------


dbname=ods
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


use $dbname;
insert overwrite table dim.dim_ones_issue_type
select
uuid,
team_uuid,
name as task_cname,
name_pinyin as task_ename,
icon as issue_type_icon,
built_in as task_in_built,
default_selected as is_default_selected,
from_unixtime(create_time) as create_time,
status,
default_configs,
type as issue_type,
detail_type as issue_type_detail
from 
ods_qkt_ones_issue_type_df 
where d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

