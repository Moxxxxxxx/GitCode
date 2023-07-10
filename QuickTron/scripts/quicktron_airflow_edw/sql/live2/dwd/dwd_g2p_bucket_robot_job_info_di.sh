#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  标准搬运任务
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_g2p_bucket_robot_job_di
#-- 输出表 ：dwd.dwd_g2p_bucket_robot_job_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-01 CREATE
#-- 2 wangziming 2022-03-09 modify 新增字段
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


with tmp_bucket_robot_job_str1 as (
select 
distinct substr(created_date,0,10) as d,project_code
from
${ods_dbname}.ods_qkt_g2p_bucket_robot_job_di
where d='${pre1_date}' 
and substr(created_date,0,10)<>'${pre1_date}'
),
tmp_bucket_robot_job_str2 as (
select 
b.*
from 
tmp_bucket_robot_job_str1 a
inner join ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di b on a.d=b.d and a.project_code=b.pt
)
insert overwrite table ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di partition(d,pt)
select 
id,
warehouse_id,
zone_code,
job_id,
robot_job_id,
priority_type,
priority,
bucket_code,
job_state,
job_source,
work_mode,
push_flag,
bucket_slot_code,
target_bucket_slot_code,
start_point,
start_point_name,
work_face,
work_faces,
end_area,
target_point,
target_point_name,
agv_end_point,
put_down,
need_operation,
need_reset,
lock_flag,
bucket_type_code,
need_out,
check_code,
stand_by_flag,
job_type,
up_job_type,
dispatch_state,
remark,
agv_code,
agv_type,
business_type,
device_code,
cancel_strategy,
deadline,
busi_group_id,
robot_job_group_id,
job_sequence,
flag,
job_created_app,
job_created_time,
job_updated_app,
job_updated_time,
hds_group_id,
bucket_type,
start_area,
project_code,
speed,
task_count_down,
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
warehouse_id,
zone_code,
job_id,
robot_job_id,
priority_type,
priority,
bucket_code,
state as job_state,
source as job_source,
work_mode,
push_flag,
bucket_slot_code,
target_bucket_slot_code,
start_point,
start_point_name,
work_face,
work_faces,
end_area,
target_point,
target_point_name,
agv_end_point,
put_down,
need_operation,
need_reset,
lock_flag,
bucket_type_code,
need_out,
check_code,
stand_by_flag,
job_type,
up_job_type,
dispatch_state,
remark,
agv_code,
agv_type,
business_type,
device_code,
cancel_strategy,
deadline,
busi_group_id,
robot_job_group_id,
sequence as job_sequence,
flag,
created_app as job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
hds_group_id,
bucket_type,
start_area,
project_code,
speed,
task_count_down,
substr(created_date,0,10) as d
from 
${ods_dbname}.ods_qkt_g2p_bucket_robot_job_di
where d='${pre1_date}'

union all
select 
id,
warehouse_id,
zone_code,
job_id,
robot_job_id,
priority_type,
priority,
bucket_code,
job_state,
job_source,
work_mode,
push_flag,
bucket_slot_code,
target_bucket_slot_code,
start_point,
start_point_name,
work_face,
work_faces,
end_area,
target_point,
target_point_name,
agv_end_point,
put_down,
need_operation,
need_reset,
lock_flag,
bucket_type_code,
need_out,
check_code,
stand_by_flag,
job_type,
up_job_type,
dispatch_state,
remark,
agv_code,
agv_type,
business_type,
device_code,
cancel_strategy,
deadline,
busi_group_id,
robot_job_group_id,
job_sequence,
flag,
job_created_app,
job_created_time,
job_updated_app,
job_updated_time,
hds_group_id,
bucket_type,
start_area,
project_code,
speed,
task_count_down,
d
from 
tmp_bucket_robot_job_str2
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


insert overwrite table ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di partition(d,pt)
select 
id,
warehouse_id,
zone_code,
job_id,
robot_job_id,
priority_type,
priority,
bucket_code,
state as job_state,
source as job_source,
work_mode,
push_flag,
bucket_slot_code,
target_bucket_slot_code,
start_point,
start_point_name,
work_face,
work_faces,
end_area,
target_point,
target_point_name,
agv_end_point,
put_down,
need_operation,
need_reset,
lock_flag,
bucket_type_code,
need_out,
check_code,
stand_by_flag,
job_type,
up_job_type,
dispatch_state,
remark,
agv_code,
agv_type,
business_type,
device_code,
cancel_strategy,
deadline,
busi_group_id,
robot_job_group_id,
sequence as job_sequence,
flag,
created_app as job_created_app,
created_date as job_created_time,
updated_app as job_updated_app,
updated_date as job_updated_time,
hds_group_id,
bucket_type,
start_area,
project_code,
speed,
task_count_down,
substr(created_date,0,10) as d,
project_code as pt
from 
(
select 
*,
row_number() over(partition by id,project_code order by updated_date desc) as rn
from 
${ods_dbname}.ods_qkt_g2p_bucket_robot_job_di
where d>=date_sub('${pre1_date}',7) and substr(created_date,0,10)>=date_sub('${pre1_date}',7)
) t
where t.rn=1
;
"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


