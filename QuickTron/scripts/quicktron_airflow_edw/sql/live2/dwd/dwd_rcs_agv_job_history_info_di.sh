#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ：  rcs 小车任务历史记录信息表
#-- 注意 ： 每天增量T-1分区数据
#-- 输入表 : ods.ods_qkt_rcs_agv_job_history、ods.ods_qkt_rcs_agv_history_job
#-- 输出表 ：dwd.dwd_rcs_agv_job_history_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-02-15 CREATE 
#-- 2 wangziming 2023-02-24 modify 回流状态七天数据


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


with tmp_agv_job_history_str1 as (
select 
distinct d,project_code
from 
(
select 
distinct substr(create_time,0,10) as d,project_code
from
${ods_dbname}.ods_qkt_rcs_agv_job_history_di
where d='${pre1_date}' 
and substr(create_time,0,10)<>'${pre1_date}'

UNION all
select 
distinct substr(gmt_create,0,10) as d,project_code
from
${ods_dbname}.ods_qkt_rcs_agv_history_job_di
where d='${pre1_date}' 
and substr(gmt_create,0,10)<>'${pre1_date}'
) t
),
tmp_agv_job_history_str2 as (
select 
b.*
from 
tmp_agv_job_history_str1 a
inner join ${dwd_dbname}.dwd_rcs_agv_job_history_info_di b on a.d=b.d and a.project_code=b.pt
)
insert overwrite table ${dwd_dbname}.dwd_rcs_agv_job_history_info_di partition(d,pt)
select 
id,
agv_code,
bucket_id,
bucket_point_code,
dest_point_code,
job_created_time,
job_create_user,
job_updated_time,
job_updated_user,
job_id,
job_mark,
job_priority,
job_state,
job_type,
let_down,
own_job_type,
top_face,
top_face_list,
warehouse_id,
zone_code,
action_point_code,
action_state,
can_interrupt,
is_report_event,
job_context,
src_job_type,
project_code,
job_priority_type,
priority_create,
robot_job_id,
job_accept_time,
job_execute_time,
job_finish_time,
d,
project_code as pt
from 
(
select 
*,
row_number() over(partition by id,project_code order by job_updated_time desc) as rn
from 
(
select  
id,
agv_code,
bucket_id,
bucket_point_code,
dest_point_code,
create_time as job_created_time,
null as job_create_user,
update_time as job_updated_time,
null as job_updated_user,
job_id,
job_mark,
job_priority,
job_state,
job_type,
is_let_down as let_down,
own_job_type,
null as top_face,
top_face_list,
warehouse_id,
zone_code,
action_point_code,
action_state,
can_interrupt,
is_report_event,
job_context,
src_job_type,
project_code,
null as job_priority_type,
null as priority_create,
robot_job_id,
job_accept_time,
job_execute_time,
job_finish_time,
substr(create_time,0,10) as d
from 
${ods_dbname}.ods_qkt_rcs_agv_job_history_di 
where d='${pre1_date}'

union all
select 
id,
agv_id as agv_code,
bucket_id,
bucket_pointcode as bucket_point_code,
dest_pointcode as dest_point_code,
gmt_create as job_created_time,
gmt_create_user as job_create_user,
gmt_modified as job_updated_time,
gmt_modified_user as job_updated_user,
job_id,
job_mark,
job_priority,
job_state,
job_type,
let_down,
own_job_type,
top_face,
null as top_face_list,
warehouse_id,
null as zone_code,
action_point_code,
action_state,
can_interrupt,
is_report_event,
job_context,
src_job_type,
project_code,
job_priority_type,
priority_create,
null as robot_job_id,
null as job_accept_time,
null as job_execute_time,
null as job_finish_time,
substr(gmt_create,0,10) as d
from 
${ods_dbname}.ods_qkt_rcs_agv_history_job_di 
where d='${pre1_date}'

union all
select 
id,
agv_code,
bucket_id,
bucket_point_code,
dest_point_code,
job_created_time,
job_create_user,
job_updated_time,
job_updated_user,
job_id,
job_mark,
job_priority,
job_state,
job_type,
let_down,
own_job_type,
top_face,
top_face_list,
warehouse_id,
zone_code,
action_point_code,
action_state,
can_interrupt,
is_report_event,
job_context,
src_job_type,
project_code,
job_priority_type,
priority_create,
robot_job_id,
job_accept_time,
job_execute_time,
job_finish_time,
d
from 
tmp_agv_job_history_str2
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



insert overwrite table ${dwd_dbname}.dwd_rcs_agv_job_history_info_di partition(d,pt)
select 
id,
agv_code,
bucket_id,
bucket_point_code,
dest_point_code,
job_created_time,
job_create_user,
job_updated_time,
job_updated_user,
job_id,
job_mark,
job_priority,
job_state,
job_type,
let_down,
own_job_type,
top_face,
top_face_list,
warehouse_id,
zone_code,
action_point_code,
action_state,
can_interrupt,
is_report_event,
job_context,
src_job_type,
project_code,
job_priority_type,
priority_create,
robot_job_id,
job_accept_time,
job_execute_time,
job_finish_time,
d,
project_code as pt
from 
(
select  
id,
agv_code,
bucket_id,
bucket_point_code,
dest_point_code,
create_time as job_created_time,
null as job_create_user,
update_time as job_updated_time,
null as job_updated_user,
job_id,
job_mark,
job_priority,
job_state,
job_type,
is_let_down as let_down,
own_job_type,
null as top_face,
top_face_list,
warehouse_id,
zone_code,
action_point_code,
action_state,
can_interrupt,
is_report_event,
job_context,
src_job_type,
project_code,
null as job_priority_type,
null as priority_create,
robot_job_id,
job_accept_time,
job_execute_time,
job_finish_time,
substr(create_time,0,10) as d,
row_number() over(partition by id,project_code order by update_time desc) as rn

from 
${ods_dbname}.ods_qkt_rcs_agv_job_history_di 
where d>=date_sub('${pre1_date}',7) and substr(create_time,0,10)>=date_sub('${pre1_date}',7)

union all
select 
id,
agv_id as agv_code,
bucket_id,
bucket_pointcode as bucket_point_code,
dest_pointcode as dest_point_code,
gmt_create as job_created_time,
gmt_create_user as job_create_user,
gmt_modified as job_updated_time,
gmt_modified_user as job_updated_user,
job_id,
job_mark,
job_priority,
job_state,
job_type,
let_down,
own_job_type,
top_face,
null as top_face_list,
warehouse_id,
null as zone_code,
action_point_code,
action_state,
can_interrupt,
is_report_event,
job_context,
src_job_type,
project_code,
job_priority_type,
priority_create,
null as robot_job_id,
null as job_accept_time,
null as job_execute_time,
null as job_finish_time,
substr(gmt_create,0,10) as d,
row_number() over(partition by id,project_code order by gmt_modified desc) as rn

from 
${ods_dbname}.ods_qkt_rcs_agv_history_job_di 
where d>=date_sub('${pre1_date}',7) and substr(gmt_create,0,10)>=date_sub('${pre1_date}',7)

) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



