#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  作业单
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_picking_work_di
#-- 输出表 ：dwd.dwd_picking_work_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-02 CREATE 
#-- 2 wangziming 2022-03-09 modify 新增字段兼容2.9.1

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



insert overwrite table ${dwd_dbname}.dwd_picking_work_info_di partition(d,pt)
select 
id,
picking_work_number,
tenant_id,
owner_code,
picking_order_group_id,
work_type,
state as work_state,
out_of_stock_flag,
picking_order_id,
splittable,
station_id,
station_code,
station_slot_id,
station_slot_code,
cross_zone_flag,
priority_type,
priority_value,
udf1,
udf2,
udf3,
udf4,
udf5,
remark,
version,
zone_id,
zone_code,
warehouse_id,
delete_flag,
created_date as work_created_tiem,
created_user as work_created_user,
created_app as work_created_app,
last_updated_date as work_updated_time,
last_updated_user as work_updated_user,
last_updated_app as work_updated_app,
ship_deadline,
project_code,
scheduling_state,
substr(created_date,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc ) as rn 
from
${ods_dbname}.ods_qkt_picking_work_di 
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


with tmp_work_str1 as (
select 
distinct substr(created_date,0,10) as d,project_code
from
${ods_dbname}.ods_qkt_picking_work_di
where d='${pre1_date}' 
and substr(created_date,0,10)<>'${pre1_date}'
),
tmp_work_str2 as (
select 
b.*
from 
tmp_work_str1 a
inner join ${dwd_dbname}.dwd_picking_work_info_di b on a.d=b.d and a.project_code=b.pt
)
insert overwrite table ${dwd_dbname}.dwd_picking_work_info_di partition(d,pt)
select 
id,
picking_work_number,
tenant_id,
owner_code,
picking_order_group_id,
work_type,
work_state,
out_of_stock_flag,
picking_order_id,
splittable,
station_id,
station_code,
station_slot_id,
station_slot_code,
cross_zone_flag,
priority_type,
priority_value,
udf1,
udf2,
udf3,
udf4,
udf5,
remark,
version,
zone_id,
zone_code,
warehouse_id,
delete_flag,
work_created_tiem,
work_created_user,
work_created_app,
work_updated_time,
work_updated_user,
work_updated_app,
ship_deadline,
project_code,
scheduling_state,
d,
project_code as pt
from 
(
select 
*,
row_number() over(partition by id,project_code order by work_updated_time desc) as rn
from 
(
select  
id,
picking_work_number,
tenant_id,
owner_code,
picking_order_group_id,
work_type,
state as work_state,
out_of_stock_flag,
picking_order_id,
splittable,
station_id,
station_code,
station_slot_id,
station_slot_code,
cross_zone_flag,
priority_type,
priority_value,
udf1,
udf2,
udf3,
udf4,
udf5,
remark,
version,
zone_id,
zone_code,
warehouse_id,
delete_flag,
created_date as work_created_tiem,
created_user as work_created_user,
created_app as work_created_app,
last_updated_date as work_updated_time,
last_updated_user as work_updated_user,
last_updated_app as work_updated_app,
ship_deadline,
project_code,
scheduling_state,
substr(created_date,0,10) as d
from 
${ods_dbname}.ods_qkt_picking_work_di
where d='${pre1_date}'

union all
select 
id,
picking_work_number,
tenant_id,
owner_code,
picking_order_group_id,
work_type,
work_state,
out_of_stock_flag,
picking_order_id,
splittable,
station_id,
station_code,
station_slot_id,
station_slot_code,
cross_zone_flag,
priority_type,
priority_value,
udf1,
udf2,
udf3,
udf4,
udf5,
remark,
version,
zone_id,
zone_code,
warehouse_id,
delete_flag,
work_created_tiem,
work_created_user,
work_created_app,
work_updated_time,
work_updated_user,
work_updated_app,
ship_deadline,
project_code,
scheduling_state,
d
from 
tmp_work_str2
) t
) rt 
where rt.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


