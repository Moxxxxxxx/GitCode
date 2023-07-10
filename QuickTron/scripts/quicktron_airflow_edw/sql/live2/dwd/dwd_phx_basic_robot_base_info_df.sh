#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 3.x项目机器人基本信息表
#-- 注意 ： 每日按天全量分区
#-- 输入表 : ods.ods_qkt_phx_basic_robot_df、ods.ods_qkt_phx_basic_robot_type_df
#-- 输出表 ：dwd.dwd_phx_basic_robot_base_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-03 CREATE 

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


insert overwrite table ${dwd_dbname}.dwd_phx_basic_robot_base_info_df partition(d='${pre1_date}',pt)
select 
a.id,
a.state as robot_state,
a.ip as robot_ip,
a.robot_code,
a.warehouse_id,
null as zone_code,
a.zone_collection,
a.running_map as robot_running_map,
a.usage_state as robot_usage_state,
a.robot_type_code,
b.robot_type_name,
b.first_classification,
b.second_classification,
b.charger_port_type,
b.navigation_method,
b.state as robot_type_state,
a.project_code,
a.project_code as pt
from 
${ods_dbname}.ods_qkt_phx_basic_robot_df a
left join ${ods_dbname}.ods_qkt_phx_basic_robot_type_df b on a.robot_type_code=b.robot_type_code and a.project_code=b.project_code and b.d='${pre1_date}'
where a.d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



