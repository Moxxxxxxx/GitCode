#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  ones项目基础信息
#-- 注意 ： 每日按天全量分区
#-- 输入表 : ods.ods_qkt_ones_project_base_info_df
#-- 输出表 ：dwd.dwd_ones_project_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-02 CREATE 
#-- 2 wangziming 2022-04-20 modify 增加 版本字段

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



insert overwrite table ${dwd_dbname}.dwd_ones_project_info_df partition(d='${pre1_date}')
select 
ones_project_uuid as project_uuid,
project_code,
project_name,
if(nvl(project_status,'')='','UNKNOWN',project_status) as project_status,
if(nvl(project_system,'') in ('','/','其他','无'),'UNKNOWN',project_system) as project_system,
if(nvl(agv_type,'')='','UNKNOWN',agv_type) as agv_type,
created_time as project_created_time,
if(nvl(owner_name,'')='','UNKNOWN',owner_name) as project_owner_name,
IF(nvl(ft_group,'')='','UNKNOWN',ft_group) as ft_group,
REGEXP_EXTRACT(sys_version,'([0-9]{1}[\\.]{1}[0-9]{1}[\\.]{0,1}[0-9]{0,1})',0) as sys_version,
REGEXP_EXTRACT(rcs_version,'([0-9]{1}[\\.]{1}[0-9]{1}[\\.]{0,1}[0-9]{0,1})',0) as rcs_version,
REGEXP_EXTRACT(wes_version,'([0-9]{1}[\\.]{1}[0-9]{1}[\\.]{0,1}[0-9]{0,1})',0) as wes_version,
REGEXP_EXTRACT(station_version,'([0-9]{1}[\\.]{1}[0-9]{1}[\\.]{0,1}[0-9]{0,1})',0) as station_version
from 
${ods_dbname}.ods_qkt_ones_project_base_info_df
where d='${pre1_date}' 
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


