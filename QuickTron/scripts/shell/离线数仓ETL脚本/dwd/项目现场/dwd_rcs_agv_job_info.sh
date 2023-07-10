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
insert overwrite table dwd.dwd_rcs_agv_job_info partition(d,pt)
select 
id,
action_point_code,
action_state,
coalesce(agv_id,agv_code) as agv_code,
bucket_id,
bucket_point_code,
can_interrupt,
dest_point_code,
is_let_down,
is_report_event,
job_context,
job_id,
job_mark,
job_priority,
job_state,
job_type,
own_job_type,
src_job_type,
top_face_list,
top_face,
warehouse_id,
zone_code,
gmt_create_user as job_created_user,
gmt_modified_user as job_updated_user,
coalesce(create_time,gmt_create) as job_created_time,
coalesce(update_time,gmt_modified) as job_updated_time,
let_down_flag,
mark_canceling,
project_code,
substr(coalesce(create_time,gmt_create),0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by if(nvl(gmt_modified,'')<>'',gmt_modified,update_time) desc ) as rn 
from
ods_qkt_rcs_agv_job 
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
insert overwrite table dwd.dwd_rcs_agv_job_info partition(d,pt)
select 
id,
action_point_code,
action_state,
coalesce(agv_id,agv_code) as agv_code,
bucket_id,
bucket_point_code,
can_interrupt,
dest_point_code,
is_let_down,
is_report_event,
job_context,
job_id,
job_mark,
job_priority,
job_state,
job_type,
own_job_type,
src_job_type,
top_face_list,
top_face,
warehouse_id,
zone_code,
gmt_create_user as job_created_user,
gmt_modified_user as job_updated_user,
coalesce(create_time,gmt_create) as job_created_time,
coalesce(update_time,gmt_modified) as job_updated_time,
let_down_flag,
mark_canceling,
project_code,
substr(coalesce(create_time,gmt_create),0,10) as d,
project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by if(nvl(gmt_modified,'')<>'',gmt_modified,update_time) desc) as rn
from ods_qkt_rcs_agv_job
where d>=date_sub('$pre1_date',7) and substr(coalesce(create_time,gmt_create),0,10)>=date_sub('$pre1_date',7)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"


echo "##############################################hive:{end executor dwd}####################################################################"

