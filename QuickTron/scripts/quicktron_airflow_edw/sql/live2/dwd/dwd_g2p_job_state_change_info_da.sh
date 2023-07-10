#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  任务状态变更记录
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_g2p_job_state_change_da
#-- 输出表 ：dwd.dwd_g2p_job_state_change_info_da
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-01 CREATE 
#-- 2 wangziming 2022-03-09 modify 重修初始化，任务状态表会有跨天的数据更新
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


insert overwrite table ${dwd_dbname}.dwd_g2p_job_state_change_info_da partition(d,pt)
select 
id,
warehouse_id,
zone_code,
job_id,
job_type,
agv_code,
agv_type,
state as job_state,
created_app as job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
project_code,
substr(created_date,1,10) as d,
project_code as pt
from 
${ods_dbname}.ods_qkt_g2p_job_state_change_da
where d>=date_sub('${pre1_date}',7) and substr(created_date,0,10)>=date_sub('${pre1_date}',7)
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



