#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  agv装载情况历史记录
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_rcs_agv_loading_history_di
#-- 输出表 ：dwd.dwd_rcs_agv_loading_history_info_di
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



init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;



insert overwrite table ${dwd_dbname}.dwd_rcs_agv_loading_history_info_di partition(d,pt)
select 
id,
create_time as loading_created_time,
agv_code,
is_virtual,
position as loading_position,
top_face,
type as loading_type,
value as loading_value,
cause as loading_cause,
project_code,
substr(create_time,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by create_time desc ) as rn 
from
${ods_dbname}.ods_qkt_rcs_agv_loading_history_di 
) t
where t.rn=1
;
"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_rcs_agv_loading_history_info_di partition(d,pt)
select 
id,
create_time as loading_created_time,
agv_code,
is_virtual,
position as loading_position,
top_face,
type as loading_type,
value as loading_value,
cause as loading_cause,
project_code,
substr(create_time,0,10) as d,
project_code as pt
from (
select 
*
,row_number() over(partition by id,project_code order by create_time desc) as rn
from ${ods_dbname}.ods_qkt_rcs_agv_loading_history_di
where d>=date_sub('${pre1_date}',30) and substr(create_time,0,10)>=date_sub('${pre1_date}',30)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


