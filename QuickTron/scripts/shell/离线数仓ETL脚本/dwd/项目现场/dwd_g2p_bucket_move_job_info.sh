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
insert overwrite table dwd.dwd_g2p_bucket_move_job_info partition(d,pt)
select 
id,
warehouse_id,
zone_code,
job_id,
lift_group_id ,
busi_group_id ,
left_group_id ,
\`floor\`,
job_type,
state as job_state,
\`source\`,
agv_code,
agv_type,
priority_type,
priority,
bucket_move_type,
bucket_code,
station_code,
source_waypoint_code,
target_waypoint_code,
top_face,
bucket_face_num,
put_down,
need_operation,
agv_end_point,
lock_flag,
bucket_type_code,
check_code,
stand_by_flag,
created_app as job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
flag,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by updated_date desc ) as rn 
from
ods_qkt_g2p_bucket_move_job 
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
insert overwrite table dwd.dwd_g2p_bucket_move_job_info partition(d,pt)
select 
id,
warehouse_id,
zone_code,
job_id,
lift_group_id ,
busi_group_id ,
left_group_id ,
\`floor\`,
job_type,
state as job_state,
\`source\`,
agv_code,
agv_type,
priority_type,
priority,
bucket_move_type,
bucket_code,
station_code,
source_waypoint_code,
target_waypoint_code,
top_face,
bucket_face_num,
put_down,
need_operation,
agv_end_point,
lock_flag,
bucket_type_code,
check_code,
stand_by_flag,
created_app as job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
flag,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by updated_date desc) as rn
from ods_qkt_g2p_bucket_move_job
where d>=date_sub('$pre1_date',7) and substr(created_date,0,10)>=date_sub('$pre1_date',7)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor ods}####################################################################"

