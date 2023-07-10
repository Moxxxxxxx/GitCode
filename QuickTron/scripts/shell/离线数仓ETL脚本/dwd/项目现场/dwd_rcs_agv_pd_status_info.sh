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
insert overwrite table dwd.dwd_rcs_agv_pd_status_info partition(d,pt)
select 
  id,
  coalesce(agv_id,agv_code) as agv_code,
  agv_mac_address,
  ap_mac_address,
  ap_radio_id,
  ap_service_id,
  battery_temperature,
  bucket_heading,
  bucket_id,
  direction,
  disk_space_percent,
  exception_code,
  ground_code_bias,
  ground_decoded,
  is_barrier,
  is_return_home,
  liftup_number,
  load_mileage,
  loading_bucket,
  no_load_mileage,
  over_all_mileage,
  point_code,
  power,
  robot_state,
  signal_strength,
  speed,
  warehouse_id,
  x as ponit_x,
  y as point_y,
  gyro_temperature,
  layerelectric_temperature,
  leftelectric_temperature,
  rightelectric_temperature,
  liftelectric_temperature,
  waypoint_id,
  coalesce(create_time,create_date,gmt_create) as status_created_time,
  coalesce(update_time,gmt_modified,modified_date)  as status_updated_time,
  create_user as status_created_user,
  modified_user as status_updated_user,
  project_code,
  substr(coalesce(create_time,create_date,gmt_create),0,10) as d,
  project_code as pt
from ( 
select 
*
,row_number() over(partition by id,project_code order by if(nvl(modified_date,'')<>'',modified_date,update_time)  desc ) as rn 
from
ods_qkt_rcs_agv_pd_status 
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
insert overwrite table dwd.dwd_rcs_agv_pd_status_info partition(d,pt)
select 
  id,
  coalesce(agv_id,agv_code) as agv_code,
  agv_mac_address,
  ap_mac_address,
  ap_radio_id,
  ap_service_id,
  battery_temperature,
  bucket_heading,
  bucket_id,
  direction,
  disk_space_percent,
  exception_code,
  ground_code_bias,
  ground_decoded,
  is_barrier,
  is_return_home,
  liftup_number,
  load_mileage,
  loading_bucket,
  no_load_mileage,
  over_all_mileage,
  point_code,
  power,
  robot_state,
  signal_strength,
  speed,
  warehouse_id,
  x as ponit_x,
  y as point_y,
  gyro_temperature,
  layerelectric_temperature,
  leftelectric_temperature,
  rightelectric_temperature,
  liftelectric_temperature,
  waypoint_id,
  coalesce(create_time,create_date,gmt_create) as status_created_time,
  coalesce(update_time,gmt_modified,modified_date)  as status_updated_time,
  create_user as status_created_user,
  modified_user as status_updated_user,
  project_code,
  substr(coalesce(create_time,create_date,gmt_create),0,10) as d,
  project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by if(nvl(modified_date,'')<>'',modified_date,update_time) desc) as rn
from ods_qkt_rcs_agv_pd_status
where d>=date_sub('$pre1_date',7) and substr(coalesce(create_time,create_date,gmt_create),0,10)>=date_sub('$pre1_date',7)
) t
where t.rn=1
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"


echo "##############################################hive:{end executor dwd}####################################################################"

