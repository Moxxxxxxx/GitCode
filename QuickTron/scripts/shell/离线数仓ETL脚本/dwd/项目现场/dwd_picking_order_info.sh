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
insert overwrite table dwd.dwd_picking_order_info partition(d,pt)
select 
id,
picking_order_number,
sn_unique_assist_key,
tenant_id,
owner_code,
external_id,
order_type,
state asorder_state,
printing_times,
out_of_stock_flag,
priority_type,
priority_value,
picking_order_group_id,
order_date,
ship_deadline,
done_date,
splittable,
station_id,
station_code,
station_slot_id,
station_slot_code,
work_count,
manual_allot,
remark,
udf1,
udf2,
udf3,
udf4,
udf5,
version,
warehouse_id,
delete_flag,
created_date as order_created_time,
created_user as order_created_user,
created_app as order_created_app,
last_updated_date as order_updated_time,
last_updated_user as order_updated_user,
last_updated_app as order_updated_app,
force_work_flag,
short_pick_deliver,
create_type,
urgent_flag,
cancel_reason,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc ) as rn 
from
ods_qkt_picking_order 
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
insert overwrite table dwd.dwd_picking_order_info partition(d,pt)
select 
id,
picking_order_number,
sn_unique_assist_key,
tenant_id,
owner_code,
external_id,
order_type,
state asorder_state,
printing_times,
out_of_stock_flag,
priority_type,
priority_value,
picking_order_group_id,
order_date,
ship_deadline,
done_date,
splittable,
station_id,
station_code,
station_slot_id,
station_slot_code,
work_count,
manual_allot,
remark,
udf1,
udf2,
udf3,
udf4,
udf5,
version,
warehouse_id,
delete_flag,
created_date as order_created_time,
created_user as order_created_user,
created_app as order_created_app,
last_updated_date as order_updated_time,
last_updated_user as order_updated_user,
last_updated_app as order_updated_app,
force_work_flag,
short_pick_deliver,
create_type,
urgent_flag,
cancel_reason,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc) as rn
from ods_qkt_picking_order
where d>=date_sub('$pre1_date',7) and substr(created_date,0,10)>=date_sub('$pre1_date',7)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

