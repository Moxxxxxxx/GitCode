#!/bin/bash


# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  'rcs-agv小车类型信息'
#-- 注意 ： 每日按天全量分区
#-- 输入表 : ods.ods_qkt_rcs_basic_agv_type_df
#-- 输出表 ：dwd.dwd_rcs_basic_agv_type_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-02 CREATE 

# ------------------------------------------------------------------------------------------------

ods_dbname=ods
dwd_dbname=dwd
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



insert overwrite table ${dwd_dbname}.dwd_rcs_basic_agv_type_info_df partition(d,pt)
select 
id,
warehouse_id,
agv_type_code,
agv_type_name,
agv_image,
first_classification,
second_classification,
size_information,
self_weight,
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
slam_bucket_guide_front_wide_detection_added_value,
slam_bucket_guide_deep_detection_added_value,
speed,
acceleration,
angular_speed,
angular_acceleration,
reflector_guide_base_width,
reflector_guide_base_depth,
camera_guide_base_width,
camera_guide_base_height,
leave_guide_distance_off_main_line,
vertical_distance_off_main_line,
project_code,
'${pre1_date}' as d,
project_code as pt
from 
${ods_dbname}.ods_qkt_rcs_basic_agv_type_df
where d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


