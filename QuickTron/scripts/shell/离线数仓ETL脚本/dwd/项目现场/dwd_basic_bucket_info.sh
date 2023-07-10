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


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


use $dbname;
insert overwrite table dwd.dwd_basic_bucket_info partition(d,pt)
select 
  id,
  warehouse_id,
  zone_id,
  bucket_template_id,
  bucket_code,
  bucket_type_id,
  enabled,
  station_id,
  owner_id,
  point_code,
  top_face,
  destination,
  alias,
  digital_code,
  cage_car_state,
  sku_mix_limit,
  attribute1,
  attribute2,
  attribute3,
  attribute4,
  attribute5,
  state as bucket_state ,
  validate_state,
  validate_time,
  created_user as bucket_created_user,
  created_app as bucket_created_app,
  created_time as bucket_created_time,
 last_updated_user as bucket_updated_user,
 last_updated_app as bucket_updated_app,
 last_updated_time as bucket_updated_time,
  extended_field,
  project_code,
  '$pre1_date' as d, 
project_code as pt
from 
ods_qkt_basic_bucket
where d='$pre1_date' 
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
