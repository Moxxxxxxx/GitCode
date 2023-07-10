#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 仿真报告小车基础信息表（包含小车类型）等记录
#-- 注意 ：  每日t-1增量分区
#-- 输入表 : ods.ods_qkt_simulation_basic_agv_type_di、ods.ods_qkt_simulation_basic_agv_di
#-- 输出表 ：dwd.dwd_simulation_agv_base_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-01-18 CREATE 
#-- 2 wangziming 2022-01-19 modify 增加时间字段
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

insert overwrite table ${dwd_dbname}.dwd_simulation_agv_base_info_di partition(d='${pre1_date}')
select
  a.id,
  a.agv_code,
  a.warehouse_id,
  a.zone_code,
  a.zone_collection,
  a.agv_frame_code,
  a.drive_unit_version,
  a.ip as agv_ip,
  a.dsp_version,
  a.battery_version,
  a.radar_version,
  a.camera_version,
  a.os as agv_os,
  a.command_version,
  a.product_version,
  a.dbox_version,
  a.iot_version,
  a.disk_space_percent,
  a.state as agv_state,
  a.bucket_code,
  a.agv_type_id,
  b.agv_type_code,
  b.agv_type_name,
  b.agv_image,
  b.first_classification,
  b.second_classification,
  b.size_information as agv_size_information,
  b.size_information as agv_self_weight,
  b.specified_load as agv_specified_load,
  b.jacking_height as agv_jacking_height,
  b.no_load_rated_speed as agv_no_load_rated_speed,
  b.full_load_rated_speed as agv_full_load_rated_speed,
  b.navigation_method as agv_navigation_method,
  b.positioning_accuracy,
  b.stop_accuracy,
  b.stop_angle_accuracy,
  b.battery_type as agv_battery_type,
  b.battery_capacity as agv_battery_capacity,
  b.rated_battery_life as agv_rated_battery_life,
  b.charging_time as agv_charging_time,
  b.battery_life as agv_battery_life,
  b.ditch_capacity as agv_ditch_capacity,
  b.crossing_slope_capacity as agv_crossing_slope_capacity,
  b.crossing_hom_capacity as agv_crossing_hom_capacity,
  b.operating_temperature as agv_operating_temperature,
  b.noise as agv_noise,
  b.charger_port_type as agv_charger_port_type,
  b.walk_face as agv_walk_face,
  b.agv_camera_distance as agv_agv_camera_distance,
  b.state as agv_type_state,
  b.slam_bucket_guide_front_wide_detection_added_value,
  b.slam_bucket_guide_deep_detection_added_value,
  b.speed as agv_speed,
  b.acceleration as agv_acceleration,
  b.angular_speed as agv_angular_speed,
  b.angular_acceleration as agv_angular_acceleration,
  b.reflector_guide_base_width,
  b.reflector_guide_base_depth,
  b.camera_guide_base_width,
  b.camera_guide_base_height,
  b.leave_guide_distance_off_main_line,
  b.vertical_distance_off_main_line,
  a.created_time as agv_created_time,
  b.created_time as agv_type_created_time,
  a.service_ip,
  a.simulation_id,
  a.simulation_job_created_id
from 
${ods_dbname}.ods_qkt_simulation_basic_agv_di a
left join ${ods_dbname}.ods_qkt_simulation_basic_agv_type_di b 
on a.agv_type_id=b.id 
and a.service_ip=b.service_ip 
and a.simulation_id=b.simulation_id 
and a.simulation_job_created_id=b.simulation_job_created_id
and b.d='${pre1_date}'
where a.d='${pre1_date}'
;
"



printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
