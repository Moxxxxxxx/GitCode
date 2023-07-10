#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 3.x项目潜伏式作业单表
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_phx_rss_transport_order_carrier_di
#-- 输出表 : dwd.dwd_phx_rss_transport_order_carrier_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-07 CREATE 

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


insert overwrite table ${dwd_dbname}.dwd_phx_rss_transport_order_carrier_info_di partition(d,pt)
select 
id,
check_code as is_check_code,
execute_state,
need_operation as is_need_operation,
need_out as is_need_out,
need_reset as is_need_reset,
put_down as is_put_down,
rack_work_heading,
start_point_name as start_point,
target_point_name as target_point,
map_code,
order_group_mode,
event_name,
put_down_code,
x,
y,
event_time,
business_type,
project_code,
substr(event_time,1,10) as d,
project_code as pt
from 
${ods_dbname}.ods_qkt_phx_rss_transport_order_carrier_di
where d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



