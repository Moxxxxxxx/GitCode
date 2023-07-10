#!/bin/bash

dbname=quicktronft_db
hive=/opt/module/hive/bin/hive


##时间
if [ -n "$1" ] ;then
   pre1_date=$1

else 
    pre1_date=`date -d "-1 day" +%F`
fi

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

use $dbname;
insert overwrite table dim_rcs_basic_agv_ful
select 
id,
agv_code,
warehouse_id,
zone_code,
zone_collection,
agv_type_id,
drive_unit_version,
ip,
dsp_version,
battery_version,
radar_version,
camera_version,
os,
dbox_version,
iot_version,
disk_space_percent,
state as agv_state,
created_time as agv_created_time,
created_user as agv_created_user,
created_app as agv_created_app, 
last_updated_time as agv_updated_time, 
last_updated_user as agv_updated_user,
last_updated_app as agv_updated_app,
bucket_code,
project_code
from 
ods_qkt_rcs_basic_agv
where d='$pre1_date'
;


"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


echo "##############################################hive:{end executor dim}####################################################################"
