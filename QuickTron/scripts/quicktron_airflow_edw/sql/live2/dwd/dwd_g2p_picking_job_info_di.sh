#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  wcs拣货任务表
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_g2p_picking_job_di、ods.ods_qkt_g2p_w2p_picking_job_di、ods.ods_qkt_picking_w2p_picking_job_di
#-- 输出表 ：dwd.dwd_g2p_picking_job_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-01 CREATE 
#-- 2 wangziming 2022-03-08 modify 修改字段逻辑错误
#-- 3 wangziming 2022-10-09 modify 增加表ods.ods_qkt_picking_w2p_picking_job_di
#-- 4 wangziming 2023-02-24 modify 回流状态七天数据
# ------------------------------------------------------------------------------------------------

ods_dbname=ods
dwd_dbname=dwd
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



sql0="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;



with tmp_picking_job_str1 as (
select 
distinct d,project_code
from 
(
select 
distinct substr(created_date,0,10) as d,project_code
from
${ods_dbname}.ods_qkt_g2p_picking_job_di
where d='${pre1_date}' 
and substr(created_date,0,10)<>'${pre1_date}'

UNION all
select 
distinct substr(created_date,0,10) as d,project_code
from
${ods_dbname}.ods_qkt_g2p_w2p_picking_job_di
where d='${pre1_date}' 
and substr(created_date,0,10)<>'${pre1_date}'

union all
select 
distinct substr(created_date,0,10) as d,project_code
from 
${ods_dbname}.ods_qkt_picking_w2p_picking_job_di
where d='${pre1_date}'
and substr(created_date,0,10)<>'${pre1_date}'
) t
),
tmp_picking_job_str2 as (
select 
b.*
from 
tmp_picking_job_str1 a
inner join ${dwd_dbname}.dwd_g2p_picking_job_info_di b on a.d=b.d and a.project_code=b.pt
)
insert overwrite table ${dwd_dbname}.dwd_g2p_picking_job_info_di partition(d,pt)
select 
id,
warehouse_id,
zone_code,
job_id,
job_type,
job_state,
agv_code,
agv_type,
priority,
priority_type,
picking_order_group_id,
order_id,
order_detail_id,
picking_work_id,
picking_work_detail_id,
sku_id,
lot_id,
pack_id,
frozen,
package_type,
customer_code,
level3_inventory_id,
quantity,
actual_quantity,
qty_mismatch_reason,
bucket_slot_code,
bucket_code,
bucket_waypoint_code,
bucket_face_num,
target_face_num,
station_code,
station_slot_code,
station_waypoint_code,
bucket_move_job_id,
job_mode,
job_created_app,
job_created_time,
job_updated_app,
job_updated_time,
order_type,
order_group_type,
project_code,
container_code,
container_transfer_job_id,
container_move_job_id,
source_way_point_code,
product_type,
container_slot_code,
d,
project_code as pt
from 
(
select 
*,
row_number() over(partition by id,project_code order by job_updated_time desc) as rn
from 
(
select  
id,
warehouse_id,
zone_code,
job_id,
job_type,
state as job_state,
agv_code,
agv_type,
priority,
priority_type,
picking_order_group_id,
order_id,
order_detail_id,
picking_work_id,
picking_work_detail_id,
sku_id,
lot_id,
pack_id,
frozen,
package_type,
customer_code,
level3_inventory_id,
quantity,
actual_quantity,
qty_mismatch_reason,
bucket_slot_code,
bucket_code,
bucket_waypoint_code,
bucket_face_num,
target_face_num,
station_code,
station_slot_code,
station_waypoint_code,
bucket_move_job_id,
job_mode,
created_app as job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
order_type,
order_group_type,
a.project_code,
null as container_code,
null as container_transfer_job_id,
null as container_move_job_id,
null as source_way_point_code,
b.product_type,
null as container_slot_code,
substr(created_date,0,10) as d
from 
${ods_dbname}.ods_qkt_g2p_picking_job_di a
left join ${dim_dbname}.dim_project_product_type b on a.project_code=b.project_code
where a.d='${pre1_date}'

union all
select 
id,
warehouse_id,
zone_code,
job_id,
job_type,
state as job_state,
agv_code,
agv_type,
priority,
priority_type,
picking_order_group_id,
order_id,
order_detail_id,
picking_work_id,
picking_work_detail_id,
sku_id,
lot_id,
pack_id,
frozen,
package_type,
customer_code,
level3_inventory_id,
quantity,
actual_quantity,
qty_mismatch_reason,
bucket_slot_code,
bucket_code,
null as bucket_waypoint_code,
null as bucket_face_num,
null as target_face_num,
station_code,
station_slot_code,
station_waypoint_code,
null as bucket_move_job_id,
job_mode,
created_app as job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
order_type,
order_group_type,
a.project_code,
container_code,
container_transfer_job_id,
container_move_job_id,
source_way_point_code,
b.product_type,
null as container_slot_code,
substr(created_date,0,10) as d
from 
${ods_dbname}.ods_qkt_g2p_w2p_picking_job_di a
left join ${dim_dbname}.dim_project_product_type b on a.project_code=b.project_code
where a.d='${pre1_date}'


union all 
select 
cast(id as string) as id,
cast(warehouse_id as string) as warehouse_id,
zone_code,
job_id,
job_type,
state as job_state,
agv_code,
agv_type,
priority,
priority_type,
picking_order_group_id,
order_id,
order_detail_id,
picking_work_id,
picking_work_detail_id,
sku_id,
lot_id,
pack_id,
frozen,
package_type,
customer_code,
level3_inventory_id,
quantity,
actual_quantity,
qty_mismatch_reason,
bucket_slot_code,
bucket_code,
null as bucket_waypoint_code,
null as bucket_face_num,
null as target_face_num,
station_code,
station_slot_code,
station_waypoint_code,
null as bucket_move_job_id,
job_mode,
created_app as job_created_app,
created_date as job_created_time,
last_updated_app as job_updated_app,
last_updated_date as job_updated_time,
order_type,
order_group_type,
a.project_code,
container_code,
null as container_transfer_job_id,
null as container_move_job_id,
source_way_point_code,
b.product_type,
container_slot_code,
substr(created_date,0,10) as d
from 
${ods_dbname}.ods_qkt_picking_w2p_picking_job_di a
left join ${dim_dbname}.dim_project_product_type b on a.project_code=b.project_code
where a.d='${pre1_date}'


union all
select 
id,
warehouse_id,
zone_code,
job_id,
job_type,
job_state,
agv_code,
agv_type,
priority,
priority_type,
picking_order_group_id,
order_id,
order_detail_id,
picking_work_id,
picking_work_detail_id,
sku_id,
lot_id,
pack_id,
frozen,
package_type,
customer_code,
level3_inventory_id,
quantity,
actual_quantity,
qty_mismatch_reason,
bucket_slot_code,
bucket_code,
bucket_waypoint_code,
bucket_face_num,
target_face_num,
station_code,
station_slot_code,
station_waypoint_code,
bucket_move_job_id,
job_mode,
job_created_app,
job_created_time,
job_updated_app,
job_updated_time,
order_type,
order_group_type,
project_code,
container_code,
container_transfer_job_id,
container_move_job_id,
source_way_point_code,
product_type,
container_slot_code,
d
from 
tmp_picking_job_str2
) t
) rt 
where rt.rn=1
;
"


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;



insert overwrite table ${dwd_dbname}.dwd_g2p_picking_job_info_di partition(d,pt)
select 
id,
warehouse_id,
zone_code,
job_id,
job_type,
job_state,
agv_code,
agv_type,
priority,
priority_type,
picking_order_group_id,
order_id,
order_detail_id,
picking_work_id,
picking_work_detail_id,
sku_id,
lot_id,
pack_id,
frozen,
package_type,
customer_code,
level3_inventory_id,
quantity,
actual_quantity,
qty_mismatch_reason,
bucket_slot_code,
bucket_code,
bucket_waypoint_code,
bucket_face_num,
target_face_num,
station_code,
station_slot_code,
station_waypoint_code,
bucket_move_job_id,
job_mode,
job_created_app,
job_created_time,
job_updated_app,
job_updated_time,
order_type,
order_group_type,
project_code,
container_code,
container_transfer_job_id,
container_move_job_id,
source_way_point_code,
product_type,
container_slot_code,
d,
project_code as pt
from 
(
select 
id,
warehouse_id,
zone_code,
job_id,
job_type,
state as job_state,
agv_code,
agv_type,
priority,
priority_type,
picking_order_group_id,
order_id,
order_detail_id,
picking_work_id,
picking_work_detail_id,
sku_id,
lot_id,
pack_id,
frozen,
package_type,
customer_code,
level3_inventory_id,
quantity,
actual_quantity,
qty_mismatch_reason,
bucket_slot_code,
bucket_code,
bucket_waypoint_code,
bucket_face_num,
target_face_num,
station_code,
station_slot_code,
station_waypoint_code,
bucket_move_job_id,
job_mode,
created_app as job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
order_type,
order_group_type,
a.project_code,
null as container_code,
null as container_transfer_job_id,
null as container_move_job_id,
null as source_way_point_code,
b.product_type,
null as container_slot_code,
substr(created_date,0,10) as d,
row_number() over(partition by a.id,a.project_code order by a.updated_date desc) as rn
from 
${ods_dbname}.ods_qkt_g2p_picking_job_di a
left join ${dim_dbname}.dim_project_product_type b on a.project_code=b.project_code
where a.d>=date_sub('${pre1_date}',7) and substr(a.created_date,0,10)>=date_sub('${pre1_date}',7)

union all
select 
id,
warehouse_id,
zone_code,
job_id,
job_type,
state as job_state,
agv_code,
agv_type,
priority,
priority_type,
picking_order_group_id,
order_id,
order_detail_id,
picking_work_id,
picking_work_detail_id,
sku_id,
lot_id,
pack_id,
frozen,
package_type,
customer_code,
level3_inventory_id,
quantity,
actual_quantity,
qty_mismatch_reason,
bucket_slot_code,
bucket_code,
null as bucket_waypoint_code,
null as bucket_face_num,
null as target_face_num,
station_code,
station_slot_code,
station_waypoint_code,
null as bucket_move_job_id,
job_mode,
created_app as job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
order_type,
order_group_type,
a.project_code,
container_code,
container_transfer_job_id,
container_move_job_id,
source_way_point_code,
b.product_type,
null as container_slot_code,
substr(created_date,0,10) as d,
row_number() over(partition by a.id,a.project_code order by a.updated_date desc) as rn
from 
${ods_dbname}.ods_qkt_g2p_w2p_picking_job_di a
left join ${dim_dbname}.dim_project_product_type b on a.project_code=b.project_code
where a.d>=date_sub('${pre1_date}',7) and substr(a.created_date,0,10)>=date_sub('${pre1_date}',7)


union all 
select 
cast(id as string) as id,
cast(warehouse_id as string) as warehouse_id,
zone_code,
job_id,
job_type,
state as job_state,
agv_code,
agv_type,
priority,
priority_type,
picking_order_group_id,
order_id,
order_detail_id,
picking_work_id,
picking_work_detail_id,
sku_id,
lot_id,
pack_id,
frozen,
package_type,
customer_code,
level3_inventory_id,
quantity,
actual_quantity,
qty_mismatch_reason,
bucket_slot_code,
bucket_code,
null as bucket_waypoint_code,
null as bucket_face_num,
null as target_face_num,
station_code,
station_slot_code,
station_waypoint_code,
null as bucket_move_job_id,
job_mode,
created_app as job_created_app,
created_date as job_created_time,
last_updated_app as job_updated_app,
last_updated_date as job_updated_time,
order_type,
order_group_type,
a.project_code,
container_code,
null as container_transfer_job_id,
null as container_move_job_id,
source_way_point_code,
b.product_type,
container_slot_code,
substr(created_date,0,10) as d,
row_number() over(partition by a.id,a.project_code order by a.last_updated_date desc) as rn
from 
${ods_dbname}.ods_qkt_picking_w2p_picking_job_di a
left join ${dim_dbname}.dim_project_product_type b on a.project_code=b.project_code
where a.d>=date_sub('${pre1_date}',7) and substr(a.created_date,0,10)>=date_sub('${pre1_date}',7)
) t
where t.rn=1
;

"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

