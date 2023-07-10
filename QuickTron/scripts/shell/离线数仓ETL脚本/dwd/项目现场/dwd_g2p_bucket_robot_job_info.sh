#!/bin/bash
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
insert overwrite table dwd.dwd_g2p_bucket_robot_job_info partition(d,pt)
select 
id,
warehouse_id,
zone_code,
job_id,
robot_job_id,
priority_type,
priority,
bucket_code,
state as job_state,
source as job_source,
work_mode,
push_flag,
bucket_slot_code,
target_bucket_slot_code,
start_point,
start_point_name,
work_face,
work_faces,
end_area,
target_point,
target_point_name,
agv_end_point,
put_down,
need_operation,
need_reset,
lock_flag,
bucket_type_code,
need_out,
check_code,
stand_by_flag,
job_type,
up_job_type,
dispatch_state,
remark,
agv_code,
agv_type,
business_type,
device_code,
cancel_strategy,
deadline,
busi_group_id,
robot_job_group_id,
sequence as job_sequence,
flag,
created_app as job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
hds_group_id,
bucket_type,
start_area,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by updated_date desc ) as rn 
from
ods_qkt_g2p_bucket_robot_job 
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
insert overwrite table dwd.dwd_g2p_bucket_robot_job_info partition(d,pt)
select 
id,
warehouse_id,
zone_code,
job_id,
robot_job_id,
priority_type,
priority,
bucket_code,
state as job_state,
source as job_source,
work_mode,
push_flag,
bucket_slot_code,
target_bucket_slot_code,
start_point,
start_point_name,
work_face,
work_faces,
end_area,
target_point,
target_point_name,
agv_end_point,
put_down,
need_operation,
need_reset,
lock_flag,
bucket_type_code,
need_out,
check_code,
stand_by_flag,
job_type,
up_job_type,
dispatch_state,
remark,
agv_code,
agv_type,
business_type,
device_code,
cancel_strategy,
deadline,
busi_group_id,
robot_job_group_id,
sequence as job_sequence,
flag,
created_app as job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
hds_group_id,
bucket_type,
start_area,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from (
select 
*
,row_number() over(partition by id,project_code order by updated_date desc) as rn
from ods_qkt_g2p_bucket_robot_job
where d>=date_sub('$pre1_date',7) and substr(created_date,0,10)>=date_sub('$pre1_date',7)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

