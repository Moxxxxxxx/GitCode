#!/bin/bash

dbname=ods
#hive=/opt/module/hive-3.1.2/scripts/hive
hive=/opt/module/hive-3.1.2/bin/hive
hive_username=wangziming
hive_passwd=wangziming1


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
insert overwrite table dwd.dwd_g2p_guided_putaway_job_info partition(d,pt)
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
order_id,
work_id,
detail_id,
sku_id,
lot_id,
pack_id,
frozen,
package_type,
owner_code,
quantity,
fullfill_quantity,
qty_mismatch_reason,
new_bucket_slot_code,
bucket_code,
bucket_slot_code,
bucket_slot_type_id,
bucket_waypoint_code,
bucket_face_num,
target_face_num,
station_code,
station_waypoint_code,
bucket_move_job_id,
created_app as job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
project_code,
null as order_type,
null as level3_inventory_id,
null as container_code,
null as container_transfer_job_id,
null as container_move_job_id,
null as station_slot_code,
null as source_way_point_code,
product_type,
substr(created_date,0,10) as d,
project_code as pt 
from (
select 
a.*,b.product_type
,row_number() over(partition by a.id,a.project_code order by updated_date desc) as rn
from ods_qkt_g2p_guided_putaway_job a
left join dim.dim_project_product_type b on a.project_code = b.project_code
where d>=date_sub('$pre1_date',7) and substr(created_date,0,10)>=date_sub('$pre1_date',7)
) t 
where t.rn=1

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
order_id,
work_id,
detail_id,
sku_id,
lot_id,
pack_id,
frozen,
package_type,
owner_code,
quantity,
fullfill_quantity,
qty_mismatch_reason,
new_bucket_slot_code,
bucket_code,
bucket_slot_code,
null as bucket_slot_type_id,
null as bucket_waypoint_code,
null as bucket_face_num,
null as target_face_num,
station_code,
station_waypoint_code,
null as bucket_move_job_id,
created_app as job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
project_code,
order_type,
level3_inventory_id,
container_code,
container_transfer_job_id,
container_move_job_id,
station_slot_code,
source_way_point_code,
product_type,
substr(created_date,0,10) as d,
project_code as pt 
from  (
select 
a.*,b.product_type
,row_number() over(partition by a.id,a.project_code order by updated_date desc) as rn
from ods_qkt_g2p_w2p_guided_putaway_job a
left join dim.dim_project_product_type b on a.project_code = b.project_code
where d>=date_sub('$pre1_date',7) and substr(created_date,0,10)>=date_sub('$pre1_date',7)
) t 
where t.rn=1
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"


echo '##############################################hive:{end executor ods}####################################################################'

