#!/bin/bash


# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  出库单拣货明细
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_picking_order_fulfill_detail_di
#-- 输出表 ：dwd.dwd_picking_order_fulfill_detail_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-02 CREATE 
#-- 2 wangziming 2022-03-09 modify 新增字段兼容2.9.1

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



init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;



insert overwrite table ${dwd_dbname}.dwd_picking_order_fulfill_detail_info_di partition(d,pt)
select 
id,
picking_order_detail_id,
tenant_id,
state as order_state,
sku_id,
owner_code,
pack_id,
lot_id,
station_slot_id,
station_slot_code,
station_code,
quantity,
short_pick,
container_code,
package_uuid,
level3_inventory_id,
bucket_code,
bucket_slot_code,
job_id,
short_pick_flag,
short_pick_reason,
location_container_code,
version,
operator,
warehouse_id,
delete_flag,
created_date as order_created_time,
created_user as order_created_user,
created_app as order_created_app,
last_updated_date as order_updated_time,
last_updated_user as order_updated_user,
last_updated_app as order_updated_app,
frozen_flag,
project_code,
level2_location_container_code,
level1_location_container_code,
substr(created_date,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc ) as rn 
from
${ods_dbname}.ods_qkt_picking_order_fulfill_detail_di 
) t
where t.rn=1
;
"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

insert overwrite table ${dwd_dbname}.dwd_picking_order_fulfill_detail_info_di partition(d,pt)
select 
id,
picking_order_detail_id,
tenant_id,
state as order_state,
sku_id,
owner_code,
pack_id,
lot_id,
station_slot_id,
station_slot_code,
station_code,
quantity,
short_pick,
container_code,
package_uuid,
level3_inventory_id,
bucket_code,
bucket_slot_code,
job_id,
short_pick_flag,
short_pick_reason,
location_container_code,
version,
operator,
warehouse_id,
delete_flag,
created_date as order_created_time,
created_user as order_created_user,
created_app as order_created_app,
last_updated_date as order_updated_time,
last_updated_user as order_updated_user,
last_updated_app as order_updated_app,
frozen_flag,
project_code,
level2_location_container_code,
level1_location_container_code,
substr(created_date,0,10) as d,
project_code as pt
from (
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc) as rn
from ${ods_dbname}.ods_qkt_picking_order_fulfill_detail_di
where d>=date_sub('${pre1_date}',30) and substr(created_date,0,10)>=date_sub('${pre1_date}',30)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

