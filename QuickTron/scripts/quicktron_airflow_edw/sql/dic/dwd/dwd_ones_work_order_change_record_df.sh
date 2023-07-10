#!/bin/bash


# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： one工单变更记录
#-- 注意 ： 每日t-1分区
#-- 输入表 : ods.ods_qkt_ones_work_order_change_record_df
#-- 输出表 ：dwd.dwd_ones_work_order_change_record_df、
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-10-12 CREATE 
#-- 2 wangziming 2022-01-14 midify 修改字段解析逻辑

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


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;



insert overwrite table ${dwd_dbname}.dwd_ones_work_order_change_record_df partition(d='${pre1_date}')
select 
order_change_record_uuid,
order_uuid,
ticket_id,
concat('{','\"','old_change_value','\"',':','\"',old_change_value,'\"',',','\"','new_change_value','\"',':','\"',new_change_value,'\"',',','\"','order_change_type','\"',':','\"',order_change_type,'\"','}') as modify_content_json,
modify_user,
updated_time,
old_change_value,
new_change_value,
order_change_type
from 
(
select 
order_change_record_uuid,
order_uuid,
ticket_id,
modify_user,
updated_time,
if(nvl(old_change_value,'')<>'',old_change_value,'UNKNOWN') as old_change_value, 
if(nvl(new_change_value,'')<>'',new_change_value,'UNKNOWN') as new_change_value,
if(nvl(order_change_type,'')<>'',order_change_type,'UNKNOWN') as order_change_type
from 
(
select 
ones_work_order_change_record_uuid as order_change_record_uuid,
ones_work_order_uuid as order_uuid,
ticket_id,
modify_user,
updated_time,
regexp_extract(split(modify_content,'，')[0],'(?<=旧值：).*',0) as old_change_value,
regexp_extract(split(modify_content,'，')[1],'(?<=新值：).*',0) as new_change_value,
if(modify_content rlike '.*负责人.*' ,'负责人','案列状态') as order_change_type
from 
${ods_dbname}.ods_qkt_ones_work_order_change_record_df
where d='${pre1_date}' 
) t
) t1 ;

"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


