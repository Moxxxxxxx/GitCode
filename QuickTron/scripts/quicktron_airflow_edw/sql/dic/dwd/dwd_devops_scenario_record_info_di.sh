#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： devops事件通知记录信息
#-- 注意 ：  每日t-1增量分区
#-- 输入表 : ods.ods_qkt_devops_scenario_record_di
#-- 输出表 ：dwd.dwd_devops_scenario_record_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-12-15 CREATE 
#-- 2 wangziming 2021-12-19 modify 根据用户ip，操作大类和操作小类进行 每分钟内去重
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

insert overwrite table ${dwd_dbname}.dwd_devops_scenario_record_info_di partition(d)
select 
user_id,
operation_type,
operation_sub_type,
submit_time,
data_time,
d
from
(
select 
  user_id ,
  operation_class1 as operation_type,
  operation_class2 as operation_sub_type,
  submit_time,
  data_time,
  to_date(data_time) as d,
  row_number() over(partition by user_id,operation_class1,operation_class2,substr(data_time,1,16) order by data_time) as rn 
from 
${ods_dbname}.ods_qkt_devops_scenario_record_di
where d='2021-12-14'
) t
;
"

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

insert overwrite table ${dwd_dbname}.dwd_devops_scenario_record_info_di partition(d='${pre1_date}')
select 
user_id,
operation_type,
operation_sub_type,
submit_time,
data_time
from 
(
select 
  user_id ,
  operation_class1 as operation_type,
  operation_class2 as operation_sub_type,
  submit_time,
  data_time,
  row_number() over(partition by user_id,operation_class1,operation_class2,substr(data_time,1,16) order by data_time) as rn 
from 
${ods_dbname}.ods_qkt_devops_scenario_record_di
where d='${pre1_date}'
) t
where t.rn=1
;
"



printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


