#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 3.x项目任务移动操作记录
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_phx_rms_job_action_operation_di，ods.ods_qkt_phx_basic_robot_type_df
#-- 输出表 : dwd.dwd_phx_rms_job_action_operation_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-14 CREATE 
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


insert overwrite table ${dwd_dbname}.dwd_phx_rms_job_action_operation_info_di partition(d,pt)
select 
id,
action_start_time,
action_end_time,
job_sn,
action_uid,
operation_name,
robot_code,
robot_type_code,
robot_type_name,
first_classification,
project_code,
d,
pt
from 
(
select 
a.id,
a.start_time as action_start_time,
a.end_time as action_end_time,
a.job_sn,
a.action_uid,
a.operation_name,
a.robot_code,
a.robot_type_code,
b.robot_type_name,
b.first_classification,
a.project_code,
substr(a.start_time,1,10) as d,
a.project_code as pt,
row_number() over(partition by a.id,a.project_code order by a.update_time desc) as rn 
from 
${ods_dbname}.ods_qkt_phx_rms_job_action_operation_di a
left join ${ods_dbname}.ods_qkt_phx_basic_robot_type_df b on a.project_code=b.project_code and a.robot_type_code=b.robot_type_code and b.d='${pre1_date}'
where a.d>=date_sub('${pre1_date}',10) 
and substr(a.start_time,1,10)>=date_sub('${pre1_date}',10)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



