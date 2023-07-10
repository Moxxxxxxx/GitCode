#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 3.x项目搬运作业单的耗时记录
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_phx_rss_transport_order_carrier_cost_di
#-- 输出表 : dwd.dwd_phx_rss_transport_order_carrier_cost_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-07 CREATE 
#-- 2 wangziming 2023-02-23 modify 增加去重

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


insert overwrite table ${dwd_dbname}.dwd_phx_rss_transport_order_carrier_cost_info_di partition(d,pt)
select 
id,
order_update_time,
upstream_order_no,
order_no,
execute_state,
total_cost,
assign_cost,
move_cost,
lift_cost,
rack_move_cost,
put_cost,
project_code,
d,
pt
from 
(
select 
id,
order_update_time,
upstream_order_no,
order_no,
execute_state,
total_cost,
assign_cost,
move_cost,
lift_cost,
rack_move_cost,
put_cost,
project_code,
substr(order_update_time,1,10) as d,
project_code as pt,
row_number() over(partition by id,project_code order by update_time desc) as rn
from 
${ods_dbname}.ods_qkt_phx_rss_transport_order_carrier_cost_di
where d>=date_sub('${pre1_date}',10) 
and substr(order_update_time,1,10)>=date_sub('${pre1_date}',10)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



