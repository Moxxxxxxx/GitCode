#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  agv任务
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_rcs_agv_job_di
#-- 输出表 ：dwd.dwd_rcs_agv_job_info_di
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


insert overwrite table ${dwd_dbname}.dwd_rcs_agv_job_info_di partition(d,pt)
select 
id,
action_point_code,
action_state,
coalesce(agv_id,agv_code) as agv_code,
bucket_id,
bucket_point_code,
can_interrupt,
dest_point_code,
is_let_down,
is_report_event,
job_context,
job_id,
job_mark,
job_priority,
job_state,
job_type,
own_job_type,
src_job_type,
top_face_list,
top_face,
warehouse_id,
zone_code,
gmt_create_user as job_created_user,
gmt_modified_user as job_updated_user,
coalesce(create_time,gmt_create) as job_created_time,
coalesce(update_time,gmt_modified) as job_updated_time,
let_down_flag,
mark_canceling,
project_code,
substr(coalesce(create_time,gmt_create),0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by if(nvl(gmt_modified,'')<>'',gmt_modified,update_time) desc ) as rn 
from
${ods_dbname}.ods_qkt_rcs_agv_job_di 
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


with tmp_agv_job_str1 as (
select 
distinct substr(coalesce(create_time,gmt_create),0,10) as d,project_code
from
${ods_dbname}.ods_qkt_rcs_agv_job_di
where d='${pre1_date}' 
and substr(coalesce(create_time,gmt_create),0,10)<>'${pre1_date}'
),
tmp_agv_job_str2 as (
select 
b.*
from 
tmp_agv_job_str1 a
inner join ${dwd_dbname}.dwd_rcs_agv_job_info_di b on a.d=b.d and a.project_code=b.pt
)
insert overwrite table ${dwd_dbname}.dwd_rcs_agv_job_info_di partition(d,pt)
select 
id,
action_point_code,
action_state,
agv_code,
bucket_id,
bucket_point_code,
can_interrupt,
dest_point_code,
is_let_down,
is_report_event,
job_context,
job_id,
job_mark,
job_priority,
job_state,
job_type,
own_job_type,
src_job_type,
top_face_list,
top_face,
warehouse_id,
zone_code,
job_created_user,
job_updated_user,
job_created_time,
job_updated_time,
let_down_flag,
mark_canceling,
project_code,
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
action_point_code,
action_state,
coalesce(agv_id,agv_code) as agv_code,
bucket_id,
bucket_point_code,
can_interrupt,
dest_point_code,
is_let_down,
is_report_event,
job_context,
job_id,
job_mark,
job_priority,
job_state,
job_type,
own_job_type,
src_job_type,
top_face_list,
top_face,
warehouse_id,
zone_code,
gmt_create_user as job_created_user,
gmt_modified_user as job_updated_user,
coalesce(create_time,gmt_create) as job_created_time,
coalesce(update_time,gmt_modified) as job_updated_time,
let_down_flag,
mark_canceling,
project_code,
substr(coalesce(create_time,gmt_create),0,10) as d
from 
${ods_dbname}.ods_qkt_rcs_agv_job_di
where d='${pre1_date}'

union all
select 
id,
action_point_code,
action_state,
agv_code,
bucket_id,
bucket_point_code,
can_interrupt,
dest_point_code,
is_let_down,
is_report_event,
job_context,
job_id,
job_mark,
job_priority,
job_state,
job_type,
own_job_type,
src_job_type,
top_face_list,
top_face,
warehouse_id,
zone_code,
job_created_user,
job_updated_user,
job_created_time,
job_updated_time,
let_down_flag,
mark_canceling,
project_code,
d
from 
tmp_agv_job_str2
) t
) rt 
where rt.rn=1

;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


