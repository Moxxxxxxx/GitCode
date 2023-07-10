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
insert overwrite table dwd.dwd_g2p_countcheck_job_info partition(d,pt)
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
detail_id,
work_id,
station_code,
bucket_code,
source_waypoint_code,
target_waypoint_code,
bucket_face_num,
bucket_init_face,
bucket_target_face,
bucket_move_job_id,
created_app as  job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
t.project_code,
null as container_code,
null as station_waypoint_code,
null as level3_inventory_id,
null as container_transfer_job_id,
null as container_move_job_id,
b.product_type,
substr(created_date,0,10) as d,
t.project_code as pt
from ( 
select 
*
,row_number() over(partition by id,project_code order by updated_date desc ) as rn 
from
ods_qkt_g2p_countcheck_job 
) t 
left join dim_project_product_type b on t.project_code=b.project_code
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
detail_id,
work_id,
station_code,
bucket_code,
source_way_point_code as source_waypoint_code,
target_waypoint_code,
null as bucket_face_num,
null as bucket_init_face,
null as bucket_target_face,
null as bucket_move_job_id,
created_app as  job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
t.project_code,
container_code,
station_waypoint_code,
level3_inventory_id,
container_transfer_job_id,
container_move_job_id,
b.product_type,
substr(created_date,0,10) as d,
t.project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by updated_date desc) as rn
from ods_qkt_g2p_w2p_countcheck_job
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
insert overwrite table dwd.dwd_g2p_countcheck_job_info partition(d,pt)
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
detail_id,
work_id,
station_code,
bucket_code,
source_waypoint_code,
target_waypoint_code,
bucket_face_num,
bucket_init_face,
bucket_target_face,
bucket_move_job_id,
created_app as  job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
t.project_code,
null as container_code,
null as station_waypoint_code,
null as level3_inventory_id,
null as container_transfer_job_id,
null as container_move_job_id,
b.product_type,
substr(created_date,0,10) as d,
t.project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by updated_date desc) as rn
from ods_qkt_g2p_countcheck_job
where d>=date_sub('$pre1_date',7) and substr(created_date,0,10)>=date_sub('$pre1_date',7)
) t 
left join dim.dim_project_product_type b on t.project_code=b.project_code
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
detail_id,
work_id,
station_code,
bucket_code,
source_way_point_code as source_waypoint_code,
target_waypoint_code,
null as bucket_face_num,
null as bucket_init_face,
null as bucket_target_face,
null as bucket_move_job_id,
created_app as  job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
t.project_code,
container_code,
station_waypoint_code,
level3_inventory_id,
container_transfer_job_id,
container_move_job_id,
b.product_type,
substr(created_date,0,10) as d,
t.project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by updated_date desc) as rn
from ods_qkt_g2p_w2p_countcheck_job
where d>=date_sub('$pre1_date',7) and substr(created_date,0,10)>=date_sub('$pre1_date',7)
) t 
left join dim.dim_project_product_type b on t.project_code=b.project_code
where t.rn=1
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo '##############################################hive:{end executor dwd}####################################################################'
