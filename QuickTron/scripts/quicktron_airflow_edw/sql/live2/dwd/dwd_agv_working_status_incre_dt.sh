#!/bin/bash


######### 设置表的变量
ods_dbname=ods
dwd_dbname=dwd
table=dwd_agv_working_status_incre_dt
hive=/opt/module/hive-3.1.2/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天

if [ -n "$1" ] ;then
    pre1_date=$1
    pre2_date=`date -d "-1 day $1" +%F`
    pre3_date=`date -d "-2 day $1" +%F`
else 
    pre1_date=`date -d "-1 day" +%F`
    pre2_date=`date -d "-2 day" +%F`
    pre3_date=`date -d "-3 day" +%F`
fi


#################################################################dwd###############################################################
echo "##############################################hive:{start executor dwd}####################################################################"



init_sql="
set hive.compute.query.using.stats=false;
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dwd_dbname}.${table} partition(d,pt)
select 
    agv_code,
    log_time as status_log_time,
    collect_time as status_collect_time,
    working_status,
    online_status,
    warehouse_id,
    project_code,
    project_name,
    substr(log_time,0,10) as d,
    project_code as pt
from (select 
*,row_number() over(partition by log_time,project_code,agv_code,warehouse_id order by update_time desc ) as rn 
from 
${ods_dbname}.ods_agv_working_status_dt
) t
where t.rn=1 and substr(log_time,0,10)<='${pre1_date}'
;
"
sql="
set hive.compute.query.using.stats=false;
set hive.insert.into.multilevel.dirs=true;
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=5000;
set mapreduce.map.memory.mb=22048;
set mapreduce.reduce.memory.mb=22048; 




insert overwrite table ${dwd_dbname}.${table} partition(d,pt)
select 
    agv_code,
    log_time as status_log_time,
    collect_time as status_collect_time,
    working_status,
    online_status,
    warehouse_id,
    project_code,
    project_name,
    substr(log_time,0,10) as d,
    project_code as pt
from (select 
*,row_number() over(partition by log_time,project_code,agv_code,warehouse_id order by update_time desc ) as rn 
from 
${ods_dbname}.ods_agv_working_status_dt where d>='${pre3_date}' and substr(log_time,0,10) between '${pre3_date}' and '${pre1_date}' and project_code not rlike '[\u4e00-\u9fa5]'
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"
