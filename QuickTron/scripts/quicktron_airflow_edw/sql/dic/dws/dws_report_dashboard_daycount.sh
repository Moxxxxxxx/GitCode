#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset可视化看板的看板的日统计
#-- 注意 ： 每日t-1分区
#-- 输入表 : dim.dim_report_dashboard_slices_info、dwd.dwd_report_action_log_info_da、
#-- 输出表 ：dws.dws_report_dashboard_daycount
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


with report_logs as (
select
*
from 
${dwd_dbname}.dwd_report_action_log_info_da
where  user_action_name='看板操作'
),
dashboard_dim as (
select 
distinct dashboard_id,dashboard_name
from 
${dim_dbname}.dim_report_dashboard_slices_info
)
insert overwrite table ${dws_dbname}.dws_report_dashboard_daycount partition(d)
select
dashboard_id,
dashboard_name,
user_id,
user_cname,
user_action_name,
start_date,
sum(num) as operation_count,
start_date as d
from 
(
select 
if(t.dashboard_id is null,'UNKNOWN',t.dashboard_id) as dashboard_id,
case when t.dashboard_id is null then 'UNKNOWN'
	 when t.dashboard_id is not null and di.dashboard_id is null then '已删除'
	 else di.dashboard_name end as dashboard_name,
t.user_id,
t.user_cname,
t.user_action_name,
substr(t.record_create_time,1,10) as start_date,
1 as num
from 
(
select 
max(dashboard_id) as dashboard_id,
user_id,
user_cname,
user_action_name,
date_format(record_create_time,'yyyy-MM-dd HH:mm') as record_create_time
from 
report_logs 
where user_action<>'log' and nvl(user_id,'')<>''
group by user_id,user_cname,date_format(record_create_time,'yyyy-MM-dd HH:mm'),user_action_name
) t
left join dashboard_dim di on t.dashboard_id=di.dashboard_id
) rt
group by dashboard_id,
dashboard_name,
user_id,
user_cname,
user_action_name,
start_date
;
"

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


with report_logs as (
select
*
from 
${dwd_dbname}.dwd_report_action_log_info_da
where d='${pre1_date}' and user_action_name='看板操作'
),
dashboard_dim as (
select 
distinct dashboard_id,dashboard_name
from 
${dim_dbname}.dim_report_dashboard_slices_info
)
insert overwrite table ${dws_dbname}.dws_report_dashboard_daycount partition(d='${pre1_date}')
select
dashboard_id,
dashboard_name,
user_id,
user_cname,
user_action_name,
start_date,
sum(num) as operation_count
from 
(
select 
if(t.dashboard_id is null,'UNKNOWN',t.dashboard_id) as dashboard_id,
case when t.dashboard_id is null then 'UNKNOWN'
	 when t.dashboard_id is not null and di.dashboard_id is null then '已删除'
	 else di.dashboard_name end as dashboard_name,
t.user_id,
t.user_cname,
t.user_action_name,
substr(t.record_create_time,1,10) as start_date,
1 as num
from 
(
select 
max(dashboard_id) as dashboard_id,
user_id,
user_cname,
user_action_name,
date_format(record_create_time,'yyyy-MM-dd HH:mm') as record_create_time
from 
report_logs 
where user_action<>'log' and nvl(user_id,'')<>''
group by user_id,user_cname,date_format(record_create_time,'yyyy-MM-dd HH:mm'),user_action_name
) t
left join dashboard_dim di on t.dashboard_id=di.dashboard_id
) rt
group by dashboard_id,
dashboard_name,
user_id,
user_cname,
user_action_name,
start_date
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

