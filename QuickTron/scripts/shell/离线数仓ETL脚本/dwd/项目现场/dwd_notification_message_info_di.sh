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
insert overwrite table dwd.dwd_notification_message_info_di partition(d,pt)
select 
  id,
  unit_id as agv_code, 
  message_id,
  unit_type,
  warning_type,
  title as message_title,
  service_name,
  read_status,
  status as notify_status,
  event as notify_event_type,
  notify_level,
  happen_at as notify_start_time,
  close_at as notify_close_time,
  message_body,
  compress_message_body,
  warehouse_id,
  created_user as notify_created_user,
  created_app as notify_created_app,
  created_time as notify_created_time,
  last_updated_user as notify_updated_user,
  last_updated_app as notify_updated_app,
  last_updated_time as notify_updated_time,
  project_code,
substr(happen_at,0,10) as d,
t.project_code as pt
from ( 
select 
*
,row_number() over(partition by id,project_code order by last_updated_time desc ) as rn 
from
ods_qkt_notification_message_di 
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
insert overwrite table dwd.dwd_notification_message_info_di partition(d,pt)
select 
  id,
  unit_id as agv_code, 
  message_id,
  unit_type,
  warning_type,
  title as message_title,
  service_name,
  read_status,
  status as notify_status,
  event as notify_event_type,
  notify_level,
  happen_at as notify_start_time,
  close_at as notify_close_time,
  message_body,
  compress_message_body,
  warehouse_id,
  created_user as notify_created_user,
  created_app as notify_created_app,
  created_time as notify_created_time,
  last_updated_user as notify_updated_user,
  last_updated_app as notify_updated_app,
  last_updated_time as notify_updated_time,
  project_code,
substr(happen_at,0,10) as d,
t.project_code as pt
from (
select 
*
,row_number() over(partition by id,project_code order by last_updated_time desc) as rn
from ods_qkt_notification_message_di
where d>=date_sub('$pre1_date',30) and substr(happen_at,0,10)>=date_sub('$pre1_date',30)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
