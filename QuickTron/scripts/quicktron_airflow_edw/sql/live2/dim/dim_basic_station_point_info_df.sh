#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目basic工作站停靠点维度表
#-- 注意 ： 每日T-1全量分区，pt分区
#-- 输入表 : ods.ods_qkt_basic_station_df、ods.ods_qkt_basic_station_point_df
#-- 输出表 ：dim.dim_basic_station_point_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-12-30 CREATE 

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


insert overwrite table ${dim_dbname}.dim_basic_station_point_info_df partition(d='${pre1_date}',pt)
select
a.id,
a.point_code, 
a.warehouse_id, 
a.station_id, 
b.station_code, 
b.station_name, 
a.work_face as point_work_face, 
a.point_type, 
a.map_code_and_version, 
a.enabled as is_enabled, 
a.state as point_state, 
a.project_code,
a.project_code as pt
from 
${ods_dbname}.ods_qkt_basic_station_point_df a 
left join ${ods_dbname}.ods_qkt_basic_station_df b on a.station_id=b.id and b.d='${pre1_date}'
where a.d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


