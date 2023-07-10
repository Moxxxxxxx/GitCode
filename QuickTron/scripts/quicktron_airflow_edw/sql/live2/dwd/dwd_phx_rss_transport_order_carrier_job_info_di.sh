#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 3.x项目潜伏式机器人任务表
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_phx_rss_transport_order_carrier_job_di
#-- 输出表 : dwd.dwd_phx_rss_transport_order_carrier_job_info_di
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


insert overwrite table ${dwd_dbname}.dwd_phx_rss_transport_order_carrier_job_info_di partition(d,pt)
select 
id,
job_create_time,
job_update_time,
job_sn,
job_type,
order_id,
order_no,
robot_code,
robot_type_code,
trace,
warehouse_id,
zone_code,
is_calibrate_code,
is_check_code,
job_group_id,
job_state,
job_lock_time,
is_need_operation,
priority,
is_put_down,
rack_code,
rack_move_type,
rack_type_code,
robot_end_point,
sequence,
source,
source_point_code,
station_code,
target_point_code,
line_code,
map_code,
source_x,
source_y,
target_x,
target_y,
ticket_code,
business_type,
project_code,
d,
pt
from 
(
select 
id,
create_time as job_create_time,
update_time as job_update_time,
job_sn,
job_type,
order_id,
order_no,
robot_code,
robot_type_code,
trace,
warehouse_id,
zone_code,
calibrate_code as is_calibrate_code,
check_code as is_check_code,
job_group_id,
job_state,
lock_time as job_lock_time,
need_operation as is_need_operation,
priority,
put_down as is_put_down,
rack_code,
rack_move_type,
rack_type_code,
robot_end_point,
sequence,
source,
source_point_code,
station_code,
target_point_code,
line_code,
map_code,
source_x,
source_y,
target_x,
target_y,
ticket_code,
business_type,
project_code,
substr(create_time,1,10) as d,
project_code as pt,
row_number() over(partition by id,project_code order by update_time desc) as rn
from 
${ods_dbname}.ods_qkt_phx_rss_transport_order_carrier_job_di
where d>=date_sub('${pre1_date}',10)
and substr(create_time,1,10)>=date_sub('${pre1_date}',10)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



