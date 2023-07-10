#!/bin/bash
dbname=ods
#hive=/opt/module/hive-3.1.2/scripts/hive
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


use $dbname;
insert overwrite table dwd.dwd_replenish_order_fulfill_detail_info_di partition(d,pt)
select 
id,
replenish_order_id,
replenish_order_detail_id,
tenant_id,
state as order_state,
owner_code,
sku_id,
lot_id,
pack_id,
unit_id,
use_frozen_flag,
level1_container_code,
level2_container_code,
level3_container_code,
bucket_code,
bucket_slot_code,
fulfill_quantity,
mismatch_quantity,
quantity_mismatch_reason,
actual_put_away,
operator,
version,
zone_id,
warehouse_id,
delete_flag,
created_date as order_created_time,
created_user as order_created_user,
created_app as order_created_app,
last_updated_date as order_updated_time,
last_updated_user as order_updated_user,
last_updated_app as order_updated_app,
station_code,
done_flag,
job_id,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc ) as rn 
from
ods_qkt_replenish_order_fulfill_detail_di 
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


use $dbname;
insert overwrite table dwd.dwd_replenish_order_fulfill_detail_info_di partition(d,pt)
select 
id,
replenish_order_id,
replenish_order_detail_id,
tenant_id,
state as order_state,
owner_code,
sku_id,
lot_id,
pack_id,
unit_id,
use_frozen_flag,
level1_container_code,
level2_container_code,
level3_container_code,
bucket_code,
bucket_slot_code,
fulfill_quantity,
mismatch_quantity,
quantity_mismatch_reason,
actual_put_away,
operator,
version,
zone_id,
warehouse_id,
delete_flag,
created_date as order_created_time,
created_user as order_created_user,
created_app as order_created_app,
last_updated_date as order_updated_time,
last_updated_user as order_updated_user,
last_updated_app as order_updated_app,
station_code,
done_flag,
job_id,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc) as rn
from ods_qkt_replenish_order_fulfill_detail_di
where d>=date_sub('$pre1_date',7) and substr(created_date,0,10)>=date_sub('$pre1_date',7)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


