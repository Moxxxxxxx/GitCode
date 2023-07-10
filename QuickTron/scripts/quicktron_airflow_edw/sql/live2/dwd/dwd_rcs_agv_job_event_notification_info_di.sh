#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  rcs任务事件通告
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_rcs_agv_job_event_notification_di
#-- 输出表 ：dwd.dwd_rcs_agv_job_event_notification_info_di
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


insert overwrite table ${dwd_dbname}.dwd_rcs_agv_job_event_notification_info_di partition(d,pt)
select 
id,
create_time as event_created_time,
update_time as event_updated_time,
agv_code,
event_content,
event_name,
is_sent_success,
job_id,
job_type,
message_id,
project_code,
substr(create_time,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by update_time desc ) as rn 
from
${ods_dbname}.ods_qkt_rcs_agv_job_event_notification_di 
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

with tmp_agv_job_event_notification_str1 as (
select 
distinct substr(create_time,0,10) as d,project_code
from
${ods_dbname}.ods_qkt_rcs_agv_job_event_notification_di
where d='${pre1_date}' 
and substr(create_time,0,10)<>'${pre1_date}'
),
tmp_agv_job_event_notification_str2 as (
select 
b.*
from 
tmp_agv_job_event_notification_str1 a
inner join ${dwd_dbname}.dwd_rcs_agv_job_event_notification_info_di b on a.d=b.d and a.project_code=b.pt
)
insert overwrite table ${dwd_dbname}.dwd_rcs_agv_job_event_notification_info_di partition(d,pt)
select 
id,
event_created_time,
event_updated_time,
agv_code,
event_content,
event_name,
is_sent_success,
job_id,
job_type,
message_id,
project_code,
d,
project_code as pt
from 
(
select 
*,
row_number() over(partition by id,project_code order by event_updated_time desc) as rn
from 
(
select  
id,
create_time as event_created_time,
update_time as event_updated_time,
agv_code,
event_content,
event_name,
is_sent_success,
job_id,
job_type,
message_id,
project_code,
substr(create_time,0,10) as d
from 
${ods_dbname}.ods_qkt_rcs_agv_job_event_notification_di
where d='${pre1_date}'

union all
select 
id,
event_created_time,
event_updated_time,
agv_code,
event_content,
event_name,
is_sent_success,
job_id,
job_type,
message_id,
project_code,
d
from 
tmp_agv_job_event_notification_str2
) t
) rt 
where rt.rn=1
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


