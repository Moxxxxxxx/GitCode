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
insert overwrite table dim_rcs_basic_agv_type_ful
select 
  id,
  warehouse_id,
  agv_type_code,
  agv_type_name,
  first_classification,
  second_classification,
  size_information,
  specified_load,
  jacking_height,
  no_load_rated_speed,
  full_load_rated_speed,
  navigation_method,
  positioning_accuracy,
  stop_accuracy,
  stop_angle_accuracy,
  battery_type,
  battery_capacity,
  rated_battery_life,
  charging_time,
  battery_life,
  ditch_capacity,
  crossing_slope_capacity,
  crossing_hom_capacity,
  operating_temperature,
  noise,
  charger_port_type,
  walk_face,
  agv_camera_distance,
  state as type_state,
  created_time as type_created_time, 
  created_user as type_created_user,
  created_app as type_created_app,
  last_updated_time as type_updated_time,
  last_updated_user as type_updated_user,
  last_updated_app as type_updated_app,
  project_code
from 
ods_qkt_rcs_basic_agv_type
where d='$pre1_date'
;


"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


echo "##############################################hive:{end executor dim}####################################################################"

