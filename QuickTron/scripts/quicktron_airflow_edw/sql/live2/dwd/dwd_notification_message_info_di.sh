#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  接收通知信息表
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_notification_message_di,ods.ods_qkt_rcs_notification_message_di
#-- 输出表 ：dwd.dwd_notification_message_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-01 CREATE 
#-- 2 wangziming 2022-12-05 modify 增加evo_rcs中ods_qkt_rcs_notification_message_di与其进行合并进入dwd_notification_message_info_di
#-- 3 wangziming 2023-02-24 modify 回流状态七天数据

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



sql0="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


with tmp_notification_message_str1 as (
select 
distinct d,project_code
from 
(
select 
distinct substr(happen_at,0,10) as d,project_code
from
${ods_dbname}.ods_qkt_notification_message_di
where d='${pre1_date}' 
and substr(happen_at,0,10)<>'${pre1_date}'

union all
select 
distinct substr(happen_at,0,10) as d,project_code
from 
${ods_dbname}.ods_qkt_rcs_notification_message_di
where d='${pre1_date}' 
and substr(happen_at,0,10)<>'${pre1_date}'
) t
),
tmp_notification_message_str2 as (
select 
b.*
from 
tmp_notification_message_str1 a
inner join ${dwd_dbname}.dwd_notification_message_info_di b on a.d=b.d and a.project_code=b.pt
)
insert overwrite table ${dwd_dbname}.dwd_notification_message_info_di partition(d,pt)
select 
id,
agv_code, 
message_id,
unit_type,
warning_type,
message_title,
service_name,
read_status,
notify_status,
notify_event_type,
notify_level,
notify_start_time,
notify_close_time,
message_body,
compress_message_body,
warehouse_id,
notify_created_user,
notify_created_app,
notify_created_time,
notify_updated_user,
notify_updated_app,
notify_updated_time,
project_code,
d,
project_code as pt
from 
(
select 
*,
row_number() over(partition by id,project_code order by notify_updated_time desc) as rn
from 
(
select  
  id,
  unit_id as agv_code, 
  message_id,
  unit_type,
  warning_type,
  title as message_title,
  service_name,
  read_status,
  status as notify_status,
  event as notify_event_type,
  notify_level,
  happen_at as notify_start_time,
  close_at as notify_close_time,
  message_body,
  compress_message_body,
  warehouse_id,
  created_user as notify_created_user,
  created_app as notify_created_app,
  created_time as notify_created_time,
  last_updated_user as notify_updated_user,
  last_updated_app as notify_updated_app,
  last_updated_time as notify_updated_time,
  project_code,
substr(happen_at,0,10) as d
from 
${ods_dbname}.ods_qkt_notification_message_di
where d='${pre1_date}'


union all
select 
  id,
  unit_id as agv_code, 
  message_id,
  unit_type,
  warning_type,
  title as message_title,
  service_name,
  read_status,
  status as notify_status,
  event as notify_event_type,
  notify_level,
  happen_at as notify_start_time,
  close_at as notify_close_time,
  message_body,
  compress_message_body,
  warehouse_id,
  created_user as notify_created_user,
  created_app as notify_created_app,
  created_time as notify_created_time,
  last_updated_user as notify_updated_user,
  last_updated_app as notify_updated_app,
  last_updated_time as notify_updated_time,
  project_code,
substr(happen_at,0,10) as d
from
${ods_dbname}.ods_qkt_rcs_notification_message_di
where d='${pre1_date}'


union all
select 
id,
agv_code, 
message_id,
unit_type,
warning_type,
message_title,
service_name,
read_status,
notify_status,
notify_event_type,
notify_level,
notify_start_time,
notify_close_time,
message_body,
compress_message_body,
warehouse_id,
notify_created_user,
notify_created_app,
notify_created_time,
notify_updated_user,
notify_updated_app,
notify_updated_time,
project_code,
d
from 
tmp_notification_message_str2
) t
) rt 
where rt.rn=1
;
"


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.dwd_notification_message_info_di partition(d,pt)
select 
id,
agv_code, 
message_id,
unit_type,
warning_type,
message_title,
service_name,
read_status,
notify_status,
notify_event_type,
notify_level,
notify_start_time,
notify_close_time,
message_body,
compress_message_body,
warehouse_id,
notify_created_user,
notify_created_app,
notify_created_time,
notify_updated_user,
notify_updated_app,
notify_updated_time,
project_code,
d,
project_code as pt
from 
(
select  
  id,
  unit_id as agv_code, 
  message_id,
  unit_type,
  warning_type,
  title as message_title,
  service_name,
  read_status,
  status as notify_status,
  event as notify_event_type,
  notify_level,
  happen_at as notify_start_time,
  close_at as notify_close_time,
  message_body,
  compress_message_body,
  warehouse_id,
  created_user as notify_created_user,
  created_app as notify_created_app,
  created_time as notify_created_time,
  last_updated_user as notify_updated_user,
  last_updated_app as notify_updated_app,
  last_updated_time as notify_updated_time,
  project_code,
substr(happen_at,0,10) as d,
row_number() over(partition by id,project_code order by last_updated_time desc) as rn
from 
${ods_dbname}.ods_qkt_notification_message_di
where d>=date_sub('${pre1_date}',7) and substr(happen_at,0,10)>=date_sub('${pre1_date}',7)



union all
select 
  id,
  unit_id as agv_code, 
  message_id,
  unit_type,
  warning_type,
  title as message_title,
  service_name,
  read_status,
  status as notify_status,
  event as notify_event_type,
  notify_level,
  happen_at as notify_start_time,
  close_at as notify_close_time,
  message_body,
  compress_message_body,
  warehouse_id,
  created_user as notify_created_user,
  created_app as notify_created_app,
  created_time as notify_created_time,
  last_updated_user as notify_updated_user,
  last_updated_app as notify_updated_app,
  last_updated_time as notify_updated_time,
  project_code,
substr(happen_at,0,10) as d,
row_number() over(partition by id,project_code order by last_updated_time desc) as rn
from
${ods_dbname}.ods_qkt_rcs_notification_message_di
where d>=date_sub('${pre1_date}',7) and substr(happen_at,0,10)>=date_sub('${pre1_date}',7)
) t
where t.rn=1
;

"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

