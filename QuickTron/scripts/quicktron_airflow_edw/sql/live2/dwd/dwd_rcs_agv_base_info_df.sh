#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 现场项目小车基础信息表（包含小车类型）等记录
#-- 注意 ：  每日t-1增量分区,项目编码分区
#-- 输入表 : ods.ods_qkt_rcs_basic_agv、ods_qkt_rcs_basic_agv_type
#-- 输出表 ：dwd.dwd_rcs_agv_base_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-08-02 CREATE 
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

insert overwrite table ${dwd_dbname}.dwd_rcs_agv_base_info_df partition(d='${pre1_date}',pt)
select
  a.id,
  a.warehouse_id,
  a.agv_code,
  a.zone_code,
  a.zone_collection,
  a.agv_type_id,
  a.drive_unit_version,
  a.ip as agv_ip,
  a.dsp_version,
  a.battery_version,
  a.radar_version,
  a.camera_version,
  a.os as agv_os,
  a.dbox_version,
  a.iot_version,
  a.disk_space_percent,
  a.state as agv_state,
  a.project_code,
  b.agv_type_code,
  b.agv_type_name,
  b.first_classification,
  b.second_classification,
  b.size_information as agv_size_information,
  b.self_weight as agv_self_weight,
  b.specified_load as agv_specified_load,
  b.jacking_height as agv_jacking_height,
  b.no_load_rated_speed as agv_no_load_rated_speed,
  b.full_load_rated_speed as agv_full_load_rated_speed,
  b.navigation_method as agv_navigation_method,
  b.positioning_accuracy,
  b.stop_accuracy,
  b.stop_angle_accuracy,
  b.battery_type,
  b.battery_capacity,
  b.rated_battery_life,
  b.charging_time,
  b.battery_life,
  b.ditch_capacity,
  b.crossing_slope_capacity,
  b.crossing_hom_capacity,
  b.operating_temperature,
  b.noise as agv_noise,
  b.charger_port_type,
  b.walk_face,
  b.agv_camera_distance,
  b.state as agv_type_state,
  a.project_code as pt 
from 
${ods_dbname}.ods_qkt_rcs_basic_agv_df a
left join ${ods_dbname}.ods_qkt_rcs_basic_agv_type_df b on a.agv_type_id=b.id and a.project_code=b.project_code and b.d='${pre1_date}'
where a.d='${pre1_date}'
;
"



printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


