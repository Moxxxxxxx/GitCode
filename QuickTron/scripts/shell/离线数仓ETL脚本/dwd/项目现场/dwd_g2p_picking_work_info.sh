#!/bin/bash
#################################################################dwd#################################################################


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



init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


use $dbname;
insert overwrite table dwd.dwd_g2p_picking_work_info partition(d,pt)
select 
id,
warehouse_id,
zone_code,
order_id,
order_type,
picking_order_group_id,
wave_order_type,
tenant_id,
picking_work_id,
station_code,
priority_type,
priority_value,
station_slot_code,
ship_date,
bucket_code,
bucket_slot_code,
work_station_match_type,
cross_zone,
picking_available_flag,
match_station_field1,
match_station_field2,
match_station_field3,
state as job_state,
created_app as work_created_app,
created_date as work_created_time,
updated_app as work_updated_app,
updated_date as work_updated_time,
owner_code,
t.project_code,
null as container_code,
b.product_type,
substr(created_date,0,10) as d,
t.project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by updated_date desc ) as rn 
from
ods_qkt_g2p_picking_work 
) t
left join dim_project_product_type b on t.project_code=b.project_code
where t.rn=1

union all
select 
id,
warehouse_id,
zone_code,
order_id,
order_type,
picking_order_group_id,
wave_order_type,
tenant_id,
picking_work_id,
station_code,
priority_type,
priority_value,
station_slot_code,
ship_date,
null as bucket_code,
null as bucket_slot_code,
work_station_match_type,
cross_zone,
picking_available_flag,
match_station_field1,
match_station_field2,
match_station_field3,
state as job_state,
created_app as work_created_app,
created_date as work_created_time,
updated_app as work_updated_app,
updated_date as work_updated_time,
owner_code,
t.project_code,
container_code,
b.product_type,
substr(created_date,0,10) as d,
t.project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by updated_date desc ) as rn 
from
ods_qkt_g2p_w2p_picking_work 
) t
left join dim_project_product_type b on t.project_code=b.project_code
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
insert overwrite table dwd.dwd_g2p_picking_work_info partition(d,pt)
select 
id,
warehouse_id,
zone_code,
order_id,
order_type,
picking_order_group_id,
wave_order_type,
tenant_id,
picking_work_id,
station_code,
priority_type,
priority_value,
station_slot_code,
ship_date,
bucket_code,
bucket_slot_code,
work_station_match_type,
cross_zone,
picking_available_flag,
match_station_field1,
match_station_field2,
match_station_field3,
state as job_state,
created_app as work_created_app,
created_date as work_created_time,
updated_app as work_updated_app,
updated_date as work_updated_time,
owner_code,
t.project_code,
null as container_code,
b.product_type,
substr(created_date,0,10) as d,
t.project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by updated_date desc) as rn
from ods_qkt_g2p_picking_work
where d>=date_sub('$pre1_date',7) and substr(created_date,0,10)>=date_sub('$pre1_date',7)
) t
left join dim.dim_project_product_type b on t.project_code=b.project_code
where t.rn=1

union all
select 
id,
warehouse_id,
zone_code,
order_id,
order_type,
picking_order_group_id,
wave_order_type,
tenant_id,
picking_work_id,
station_code,
priority_type,
priority_value,
station_slot_code,
ship_date,
null as bucket_code,
null as bucket_slot_code,
work_station_match_type,
cross_zone,
picking_available_flag,
match_station_field1,
match_station_field2,
match_station_field3,
state as job_state,
created_app as work_created_app,
created_date as work_created_time,
updated_app as work_updated_app,
updated_date as work_updated_time,
owner_code,
t.project_code,
container_code,
b.product_type,
substr(created_date,0,10) as d,
t.project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by updated_date desc) as rn
from ods_qkt_g2p_w2p_picking_work
where d>=date_sub('$pre1_date',7) and substr(created_date,0,10)>=date_sub('$pre1_date',7)
) t
left join dim.dim_project_product_type b on t.project_code=b.project_code
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"


echo "##############################################hive:{end executor dwd}####################################################################"
