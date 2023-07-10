#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  rcs-小车基本信息
#-- 注意 ： 每日按天全量分区
#-- 输入表 : ods.ods_qkt_rcs_basic_agv_df
#-- 输出表 ：dwd.dwd_rcs_basic_agv_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-02 CREATE 
#-- 2 wangziming 2023-01-04 modify 修正无网项目基础信息表回流七天的分区数据

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


insert overwrite table ${dwd_dbname}.dwd_rcs_basic_agv_info_df partition(d,pt)
select 
id,
agv_code,
warehouse_id,
zone_code,
zone_collection,
agv_type_id,
agv_frame_code,
drive_unit_version,
ip as agv_ip,
dsp_version,
battery_version,
radar_version,
camera_version,
os,
command_version,
product_version,
dbox_version,
iot_version,
disk_space_percent,
state,
created_time as agv_created_time,
created_user as agv_created_user,
created_app as agv_created_app,
last_updated_time as agv_updated_time,
last_updated_user as agv_updated_user,
last_updated_app as agv_updated_app,
bucket_code,
project_code,
'${pre1_date}' as d,
project_code as pt
from 
${ods_dbname}.ods_qkt_rcs_basic_agv_df
where d='${pre1_date}' 
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


