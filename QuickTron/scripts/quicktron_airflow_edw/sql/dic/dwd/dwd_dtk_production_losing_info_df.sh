#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉宝仓生产损失工时信息
#-- 注意 ： 每日全量
#-- 输入表 : dwd.dwd_dtk_daily_production_report_info_df
#-- 输出表 ：dwd.dwd_dtk_production_losing_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-12-29 CREATE 
#-- 2 wangziming 2022-12-30 modify 修改逻辑

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
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;

with tmp_production_losing_str1 as (
select 
org_name,
process_instance_id,
business_id,
production_date,
work_order_number,
product_part_number,
all_losing_hours_minutes,
loss_ategory,
accountability_unit,
losing_hours,
losing_desc,
product_process,
model_code,
product_name,
project_code
from 
(
select 
org_name,
process_instance_id,
business_id,
production_date,
work_order_number,
product_part_number,
all_losing_hours_minutes,
loss_ategory,
accountability_unit,
losing_hours,
losing_desc,
product_process,
model_code,
product_name,
project_code,
row_number()over(partition by business_id order by production_date desc) as rn
from ${dwd_dbname}.dwd_dtk_daily_production_report_info_df 
where d='${pre1_date}' 
and is_valid ='1' 
and approval_result='agree' 
and approval_status='COMPLETED'
) t
where t.rn=1 and nvl(loss_ategory,'')<>''
)
insert overwrite table ${dwd_dbname}.dwd_dtk_production_losing_info_df partition(d='${pre1_date}')
select 
a.org_name,
a.process_instance_id,
a.business_id,
a.production_date,
a.work_order_number,
a.product_part_number,
a.all_losing_hours_minutes,
b.value as loss_ategory,
e.value as accountability_unit,
c.value as losing_hours,
d.value as losing_desc,
a.product_process,
a.model_code,
a.product_name,
a.project_code
from 
tmp_production_losing_str1 a
lateral view posexplode(split(a.loss_ategory,';')) b as index,value
lateral view posexplode(split(a.losing_hours,';')) c as index,value
lateral view posexplode(split(a.losing_desc,';')) d as index,value
lateral view posexplode(split(a.accountability_unit,';')) e as index,value
where b.index=c.index and b.index=d.index and b.index=e.index and nvl(b.value,'')<>'' and upper(b.value)<>'NONE'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


