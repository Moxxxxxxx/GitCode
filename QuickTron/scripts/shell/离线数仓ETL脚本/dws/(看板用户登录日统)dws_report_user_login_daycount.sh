#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset可视化看板的用户登录的日统计
#-- 注意 ： 每日t-1分区
#-- 输入表 : dwd.dwd_report_action_log_info_da、
#-- 输出表 ：dws.dws_report_user_login_daycount
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-12 CREATE 

# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
dwd_dbname=dwd
dws_dbname=dws
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


insert overwrite table ${dws_dbname}.dws_report_user_login_daycount partition(d)
select
user_id,
user_cname,
user_action_name,
substr(record_create_time,1,10) as start_date,
count(1),
substr(record_create_time,1,10) as d
from 
${dwd_dbname}.dwd_report_action_log_info_da
where user_action_name='登录操作' and nvl(user_id,'')<>''
group by user_id,
user_cname,
user_action_name,
substr(record_create_time,1,10)
;
"

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


insert overwrite table ${dws_dbname}.dws_report_user_login_daycount partition(d='${pre1_date}')
select
user_id,
user_cname,
user_action_name,
substr(record_create_time,1,10) as start_date,
count(1)
from 
${dwd_dbname}.dwd_report_action_log_info_da
where d='${pre1_date}' and user_action_name='登录操作' and nvl(user_id,'')<>''
group by user_id,
user_cname,
user_action_name,
substr(record_create_time,1,10)
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"




