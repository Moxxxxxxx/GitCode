#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： ones bug消息通知数据记录，每天全量
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_ones_message_di
#-- 输出表 ：dwd.dwd_ones_task_message_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-04 CREATE 
#-- 2 wangziming 2021-11-10 modify 修改表名为 dwd_ones_task_message_info_ful
#-- 3 wangziming 2021-12-01 modify 修改时间转换函数 from_uninxtime 到 to_utc_timestamp
#-- 4 wangziming 2022-06-08 modify 修改表为增量表，每天分区即为每天的数据

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



insert overwrite table ${dwd_dbname}.dwd_ones_task_message_info_di partition(d)
select
uuid,
team_uuid,
reference_type,
reference_id,
from_uuid,
to_uuid,
to_utc_timestamp (floor(send_time/1000),'GMT-8') as send_time,
message,
type as message_type,
resource_uuid,
subject_type,
subject_id,
action,
object_type,
object_id,
object_name,
object_attr,
old_value,
new_value,
ext as ext_json,
'${pre1_date}' as d
from 
${ods_dbname}.ods_qkt_ones_message_di
where d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



