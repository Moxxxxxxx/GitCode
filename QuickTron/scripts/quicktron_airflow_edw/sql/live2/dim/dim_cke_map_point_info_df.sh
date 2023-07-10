#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目现场地图码点维度表
#-- 注意 ： 每日T-1全量分区，pt分区
#-- 输入表 : ods.ods_qkt_cke_map_point_info_df
#-- 输出表 ：dim.dim_cke_map_point_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-01-24 CREATE 

# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
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


insert overwrite table ${dim_dbname}.dim_cke_map_point_info_df partition(d='${pre1_date}',pt)
select
map_id, 
project_code, 
project_name, 
effective_state, 
point_code, 
point_x, 
point_y, 
type_path, 
type_storage, 
type_turning, 
type_forbidden, 
type_station_human, 
type_station_waiting, 
type_station_working, 
type_charger_pole, 
type_charger_station, 
type_bucket_putdown_working, 
type_bucket_entrance, 
type_device_interaction, 
type_assist, 
type_other_device,
project_code as pt
from 
${ods_dbname}.ods_qkt_cke_map_point_info_df
where d='${pre1_date}'
and lower(project_name) not rlike '(测试|test)'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


