#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 3.x项目作业单状态记录表
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_phx_rss_transport_order_link_di
#-- 输出表 : dwd.dwd_phx_rss_transport_order_link_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-06 CREATE 

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



insert overwrite table ${dwd_dbname}.dwd_phx_rss_transport_order_link_info_di partition(d,pt)
select 
id,
create_time,
update_time,
error_code,
error_desc,
execute_state,
order_no,
order_state,
order_type,
is_report,
robot_code,
upstream_order_no,
warehouse_id,
cost_time,
event_time,
project_code,
d,
pt
from 
(
select 
id,
create_time,
update_time,
error_code,
error_msg as error_desc,
execute_state,
order_no,
order_state,
order_type,
report as is_report,
robot_code,
upstream_order_no,
warehouse_id,
cost_time,
event_time,
project_code,
substr(event_time,1,10) as d,
project_code as pt,
row_number() over(partition by id,project_code order by update_time desc) as rn
from 
${ods_dbname}.ods_qkt_phx_rss_transport_order_link_di
where d>=date_sub('${pre1_date}',10)
and substr(event_time,1,10)>=date_sub('${pre1_date}',10)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



