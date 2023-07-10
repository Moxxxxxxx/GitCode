#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： ones 测试任务案例数据记录，每天全量
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_ones_testcase_case_df
#-- 输出表 ：dwd.dwd_ones_testcase_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-04 CREATE 

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
insert overwrite table dwd.dwd_ones_testcase_info_ful
select
uuid,
team_uuid,
library_uuid,
path as case_path,
module_uuid,
name as case_cname,
priority as case_priority,
type as case_type,
assign,
desc as case_desc,
from_unixtime(create_time) as create_time,
number,
status,
condition,
name_pinyin as case_ename,
from_unixtime(update_time) as update_time,
updater
from 
ods_qkt_ones_testcase_case_df 
where d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

