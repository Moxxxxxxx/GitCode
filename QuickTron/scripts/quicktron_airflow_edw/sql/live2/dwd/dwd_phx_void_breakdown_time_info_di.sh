#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 3.x项目的机器人故障end_time 为null的数据记录
#-- 注意 ： 每日增量
#-- 输入表 : dwd.dwd_phx_basic_notification_info_di
#-- 输出表 : dwd.dwd_phx_void_breakdown_time_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-21 CREATE 

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


insert overwrite table ${dwd_dbname}.dwd_phx_void_breakdown_time_info_di partition(d,pt)
select 
id, 
state, 
warehouse_id, 
error_level, 
error_module, 
error_service, 
error_type, 
error_code, 
param_value, 
error_start_time, 
error_end_time, 
is_read, 
job_order, 
robot_code, 
error_detail, 
point_x, 
point_y, 
point_code,
robot_job,
transport_object, 
error_spec, 
project_code, 
d, 
pt
from 
${dwd_dbname}.dwd_phx_basic_notification_info_di
where d>=date_sub('${pre1_date}',10)
and nvl(error_end_time,'')='' 
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



