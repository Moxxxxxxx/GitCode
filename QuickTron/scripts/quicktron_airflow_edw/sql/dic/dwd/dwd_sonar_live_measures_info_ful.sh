#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： sonar事件通知记录宽边
#-- 注意 ：  每日t-1增量分区
#-- 输入表 : ods.ods_qkt_sonar_live_measures_di
#-- 输出表 ：dwd.dwd_sonar_live_measures_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-29 CREATE 
#-- 2 wangziming 2021-12-01 modify 修改时间转换函数 from_uninxtime 到 to_utc_timestamp

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


insert overwrite table ${dwd_dbname}.dwd_sonar_live_measures_info_ful
select 
uuid,
project_uuid,
component_uuid,
metric_uuid,
measures_value,
measures_text_value,
created_time,
updated_time
from 
(
select 
uuid, 
project_uuid, 
component_uuid, 
metric_uuid, 
value as measures_value, 
text_value as measures_text_value, 
to_utc_timestamp(cast(created_at as bigint),'GMT-8') as created_time,
to_utc_timestamp(cast(updated_at as bigint),'GMT-8') as updated_time,
row_number() over(partition by project_uuid, metric_uuid order by updated_at desc) as rn
from 
${ods_dbname}.ods_qkt_sonar_live_measures_di
) t
where t.rn=1
"

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

insert overwrite table ${dwd_dbname}.dwd_sonar_live_measures_info_ful
select 
uuid,
project_uuid,
component_uuid,
metric_uuid,
measures_value,
measures_text_value,
created_time,
updated_time
from 
(
select 
*,row_number() over(partition by project_uuid, metric_uuid order by updated_time desc) as rn
from 
(
select 
uuid, 
project_uuid, 
component_uuid, 
metric_uuid, 
value as measures_value, 
text_value as measures_text_value, 
cast(to_utc_timestamp(cast(created_at as bigint),'GMT-8') as string) as created_time,
cast(to_utc_timestamp(cast(updated_at as bigint),'GMT-8') as string) as updated_time
from 
${ods_dbname}.ods_qkt_sonar_live_measures_di
where d='${pre1_date}'

union all
select 
uuid,
project_uuid,
component_uuid,
metric_uuid,
measures_value,
measures_text_value,
created_time,
updated_time
from 
${dwd_dbname}.dwd_sonar_live_measures_info_ful
) t
) rt 
where rt.rn=1

"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

