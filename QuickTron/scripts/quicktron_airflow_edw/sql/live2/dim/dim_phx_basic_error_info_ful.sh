#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 3.x项目错误码维表
#-- 注意 ： 每日按天全量分区
#-- 输入表 : ods.ods_qkt_phx_basic_error_info_df
#-- 输出表 ：dim.dim_phx_basic_error_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-03 CREATE 

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



insert overwrite table ${dim_dbname}.dim_phx_basic_error_info_ful
select 
id,
state,
module,
error_code,
level as error_level,
solution as error_solution,
alarm_detail as error_detail,
alarm_module as error_module,
alarm_name as error_name,
alarm_type as error_type,
warning_spec as error_spec,
need_update as is_need_update,
alarm_way as error_alarm_way,
project_code
from 
${ods_dbname}.ods_qkt_phx_basic_error_info_df
where d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



