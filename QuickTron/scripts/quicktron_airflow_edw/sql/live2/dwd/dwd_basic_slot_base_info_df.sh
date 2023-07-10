#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目现场的库位基本信息
#-- 注意 ： 每天T-1以及根据项目编码分区
#-- 输入表 : ods.ods_qkt_basic_slot_df、ods_qkt_basic_slot_type_df
#-- 输出表 ：dwd.dwd_basic_slot_base_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-02-18 CREATE 
#-- 2 wangziming 2022-03-09 modify 新增字段兼容2.9.1

# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
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


insert overwrite table ${dwd_dbname}.dwd_basic_slot_base_info_df partition(d='${pre1_date}',pt)
select
a.id, 
a.warehouse_id, 
a.slot_code, 
a.bucket_id, 
a.slot_type_id, 
a.enabled as is_enabled, 
a.owner_id, 
a.bucket_face, 
a.bucket_layer, 
a.front_ptl_code, 
a.back_ptl_code, 
a.rfid_code, 
a.ground_height, 
a.roadway_point_code,
a.x as slot_point_x, 
a.x as slot_point_y, 
a.dispersion_type,
a.slot_hot, 
a.slot_number, 
a.extension_distance, 
a.digital_code,
a.state as slot_state, 
a.created_user as slot_created_user, 
a.created_app as slot_created_app, 
a.created_time as slot_created_time,
a.last_updated_user as slot_updated_user, 
a.last_updated_app as slot_updated_app, 
a.last_updated_time as slot_updated_time,
b.slot_type_code, 
b.apply_type as slot_apply_type, 
b.layer_count as slot_layer_count, 
b.slot_count,
b.height as slot_height, 
b.width as slot_width,
b.depth as slot_depth,
b.view_distinguish as slot_view_distinguish, 
b.group_layer_count, 
b.group_slot_count,
b.guide_way, 
b.reflect_distance,
b.reflect_insider_distance,
b.state as slot_type_state, 
b.created_time as slot_type_created_time,
b.created_user as slot_type_created_user,
b.created_app as slot_type_created_app, 
b.last_updated_time as slot_type_updated_time,
b.last_updated_user as slot_type_updated_user, 
b.last_updated_app as slot_type_updated_app, 
a.project_code,
a.group_slot_code,
a.project_code as pt
from 
${ods_dbname}.ods_qkt_basic_slot_df a
left join ${ods_dbname}.ods_qkt_basic_slot_type_df b on a.slot_type_id=b.id and a.project_code=b.project_code and b.d='${pre1_date}'
where a.d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

