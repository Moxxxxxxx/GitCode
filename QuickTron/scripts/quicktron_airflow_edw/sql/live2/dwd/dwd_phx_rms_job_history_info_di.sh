#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 3.x项目机器人任务历史表
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_phx_rms_job_history_di
#-- 输出表 : dwd.dwd_phx_rms_job_history_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-06 CREATE 
#-- 2 wangziming 2023-02-23 modify 增加去重

# ------------------------------------------------------------------------------------------------

ods_dbname=ods
dim_dbname=dim
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


insert overwrite table ${dwd_dbname}.dwd_phx_rms_job_history_info_di partition(d,pt)
select 
id,
job_accept_time,
job_execute_time,
job_finish_time,
job_duration,
robot_code,
job_template_id,
job_template_code, 
job_sn, 
job_state, 
job_type, 
priority,
is_lockable,
is_interruptible, 
is_report_event,
is_enforce, 
pass_through,
transport_object_code,
transport_object_type,
transport_object_type_code,
order_no,
job_timeout,
remark,
zone_code,
warehouse_id,
project_code,
d,
pt
from 
(
select 
id,
accept_time as job_accept_time,
execute_time as job_execute_time,
finish_time as job_finish_time,
duration as job_duration,
robot_code,
job_template_id,
job_template_code, 
job_sn, 
job_state, 
job_type, 
priority,
is_lockable,
is_interruptible, 
is_report_event,
is_enforce, 
pass_through,
transport_object_code,
transport_object_type,
transport_object_type_code,
order_no,
timeout as job_timeout,
remark,
zone_code,
warehouse_id,
project_code,
substr(accept_time,1,10) as d,
project_code as pt,
row_number() over(partition by id,project_code order by create_time desc) as rn
from 
${ods_dbname}.ods_qkt_phx_rms_job_history_di
where d>=date_sub('${pre1_date}',10)
and substr(accept_time,1,10)>=date_sub('${pre1_date}',10)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



