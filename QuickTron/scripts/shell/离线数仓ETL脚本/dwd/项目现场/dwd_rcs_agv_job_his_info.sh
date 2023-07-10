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



init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


use $dbname;
insert overwrite table dwd.dwd_rcs_agv_job_his_info partition(d,pt)
select 
id,
agv_code,
bucket_id,
bucket_point_code,
dest_point_code,
create_time as job_create_time,
null as job_create_user,
update_time as job_updated_time,
null as job_updated_user,
job_id,
job_mark,
job_priority,
job_state,
job_type,
is_let_down as let_down,
own_job_type,
null as top_face,
top_face_list,
warehouse_id,
zone_code,
action_point_code,
action_state,
can_interrupt,
is_report_event,
job_context,
src_job_type,
project_code,
null as job_priority_type,
null as priority_create,
substr(create_time,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by update_time desc ) as rn 
from
ods_qkt_rcs_agv_job_history 
) t
where t.rn=1

union all
select 
id,
agv_id as agv_code,
bucket_id,
bucket_pointcode as bucket_point_code,
dest_pointcode as dest_point_code,
gmt_create as job_create_time,
gmt_create_user as job_create_user,
gmt_modified as job_updated_time,
gmt_modified_user as job_updated_user,
job_id,
job_mark,
job_priority,
job_state,
job_type,
let_down,
own_job_type,
top_face,
null as top_face_list,
warehouse_id,
null as zone_code,
action_point_code,
action_state,
can_interrupt,
is_report_event,
job_context,
src_job_type,
project_code,
job_priority_type,
priority_create,
substr(gmt_create,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by gmt_modified desc ) as rn 
from
ods_qkt_rcs_agv_history_job 
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
insert overwrite table dwd.dwd_rcs_agv_job_his_info partition(d,pt)
select 
id,
agv_code,
bucket_id,
bucket_point_code,
dest_point_code,
create_time as job_create_time,
null as job_create_user,
update_time as job_updated_time,
null as job_updated_user,
job_id,
job_mark,
job_priority,
job_state,
job_type,
is_let_down as let_down,
own_job_type,
null as top_face,
top_face_list,
warehouse_id,
zone_code,
action_point_code,
action_state,
can_interrupt,
is_report_event,
job_context,
src_job_type,
project_code,
null as job_priority_type,
null as priority_create,
substr(create_time,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by update_time desc ) as rn 
from
ods_qkt_rcs_agv_job_history 
where d>=date_sub('$pre1_date',7) and substr(create_time,0,10)>=date_sub('$pre1_date',7)
) t
where t.rn=1

union all
select 
id,
agv_id as agv_code,
bucket_id,
bucket_pointcode as bucket_point_code,
dest_pointcode as dest_point_code,
gmt_create as job_create_time,
gmt_create_user as job_create_user,
gmt_modified as job_updated_time,
gmt_modified_user as job_updated_user,
job_id,
job_mark,
job_priority,
job_state,
job_type,
let_down,
own_job_type,
top_face,
null as top_face_list,
warehouse_id,
null as zone_code,
action_point_code,
action_state,
can_interrupt,
is_report_event,
job_context,
src_job_type,
project_code,
job_priority_type,
priority_create,
substr(gmt_create,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by gmt_modified desc ) as rn 
from
ods_qkt_rcs_agv_history_job 
where d>=date_sub('$pre1_date',7) and substr(gmt_create,0,10)>=date_sub('$pre1_date',7)
) t
where t.rn=1
;
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"


