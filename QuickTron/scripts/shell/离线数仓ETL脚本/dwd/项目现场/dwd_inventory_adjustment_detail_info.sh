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
insert overwrite table dwd.dwd_inventory_adjustment_detail_info partition(d,pt)
select 
id,
warehouse_id,
inventory_adjustment_id,
from_owner_code,
from_sku_id,
from_lot_id,
from_pack_id,
from_frozen_flag,
from_bucket_slot_code,
from_level1_container_code,
from_level2_container_code,
from_quantity,
from_feedback_quantity,
to_owner_code,
to_sku_id,
to_lot_id,
to_pack_id,
to_frozen_flag,
to_bucket_slot_code,
to_level1_container_code,
to_level2_container_code,
to_level3_inventory_id,
to_inventory_profit_version,
to_quantity,
to_feedback_quantity,
version,
remark,
delete_flag,
created_date as adjustment_created_time,
created_user as adjustment_created_user,
created_app as adjustment_created_app,
last_updated_date as adjustment_updated_time,
last_updated_user as adjustment_updated_user,
last_updated_app as adjustment_updated_app,
to_zone_code,
from_zone_code,
from_bucket_code,
to_bucket_code,
state as adjustment_state,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc ) as rn 
from
ods_qkt_inventory_adjustment_detail 
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
insert overwrite table dwd.dwd_inventory_adjustment_detail_info partition(d,pt)
select 
id,
warehouse_id,
inventory_adjustment_id,
from_owner_code,
from_sku_id,
from_lot_id,
from_pack_id,
from_frozen_flag,
from_bucket_slot_code,
from_level1_container_code,
from_level2_container_code,
from_quantity,
from_feedback_quantity,
to_owner_code,
to_sku_id,
to_lot_id,
to_pack_id,
to_frozen_flag,
to_bucket_slot_code,
to_level1_container_code,
to_level2_container_code,
to_level3_inventory_id,
to_inventory_profit_version,
to_quantity,
to_feedback_quantity,
version,
remark,
delete_flag,
created_date as adjustment_created_time,
created_user as adjustment_created_user,
created_app as adjustment_created_app,
last_updated_date as adjustment_updated_time,
last_updated_user as adjustment_updated_user,
last_updated_app as adjustment_updated_app,
to_zone_code,
from_zone_code,
from_bucket_code,
to_bucket_code,
state as adjustment_state,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc) as rn
from ods_qkt_inventory_adjustment_detail
where d>=date_sub('$pre1_date',7) and substr(created_date,0,10)>=date_sub('$pre1_date',7)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"


