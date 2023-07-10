#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  库存调整单明细
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_inventory_adjustment_detail_di
#-- 输出表 ：dwd.dwd_inventory_adjustment_detail_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-01 CREATE 

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


insert overwrite table ${dwd_dbname}.dwd_inventory_adjustment_detail_info_di partition(d,pt)
select 
id,
warehouse_id,
inventory_adjustment_id,
from_owner_code,
from_sku_id,
from_lot_id,
from_pack_id,
from_frozen_flag,
from_bucket_slot_code,
from_level1_container_code,
from_level2_container_code,
from_quantity,
from_feedback_quantity,
to_owner_code,
to_sku_id,
to_lot_id,
to_pack_id,
to_frozen_flag,
to_bucket_slot_code,
to_level1_container_code,
to_level2_container_code,
to_level3_inventory_id,
to_inventory_profit_version,
to_quantity,
to_feedback_quantity,
version,
remark,
delete_flag,
created_date as adjustment_created_time,
created_user as adjustment_created_user,
created_app as adjustment_created_app,
last_updated_date as adjustment_updated_time,
last_updated_user as adjustment_updated_user,
last_updated_app as adjustment_updated_app,
to_zone_code,
from_zone_code,
from_bucket_code,
to_bucket_code,
state as adjustment_state,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc ) as rn 
from
${ods_dbname}.ods_qkt_inventory_adjustment_detail_di
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

with tmp_adjustment_detail_str1 as (
select 
distinct substr(created_date,0,10) as d,project_code
from
${ods_dbname}.ods_qkt_inventory_adjustment_detail_di
where d='${pre1_date}' 
and substr(created_date,0,10)<>'${pre1_date}'
),
tmp_adjustment_detail_str2 as (
select 
b.*
from 
tmp_adjustment_detail_str1 a
inner join ${dwd_dbname}.dwd_inventory_adjustment_detail_info_di b on a.d=b.d and a.project_code=b.pt
)
insert overwrite table ${dwd_dbname}.dwd_inventory_adjustment_detail_info_di partition(d,pt)
select 
id,
warehouse_id,
inventory_adjustment_id,
from_owner_code,
from_sku_id,
from_lot_id,
from_pack_id,
from_frozen_flag,
from_bucket_slot_code,
from_level1_container_code,
from_level2_container_code,
from_quantity,
from_feedback_quantity,
to_owner_code,
to_sku_id,
to_lot_id,
to_pack_id,
to_frozen_flag,
to_bucket_slot_code,
to_level1_container_code,
to_level2_container_code,
to_level3_inventory_id,
to_inventory_profit_version,
to_quantity,
to_feedback_quantity,
version,
remark,
delete_flag,
adjustment_created_time,
adjustment_created_user,
adjustment_created_app,
adjustment_updated_time,
adjustment_updated_user,
adjustment_updated_app,
to_zone_code,
from_zone_code,
from_bucket_code,
to_bucket_code,
adjustment_state,
project_code,
d,
project_code as pt
from 
(
select 
*,
row_number() over(partition by id,project_code order by adjustment_updated_time desc) as rn
from 
(
select  
id,
warehouse_id,
inventory_adjustment_id,
from_owner_code,
from_sku_id,
from_lot_id,
from_pack_id,
from_frozen_flag,
from_bucket_slot_code,
from_level1_container_code,
from_level2_container_code,
from_quantity,
from_feedback_quantity,
to_owner_code,
to_sku_id,
to_lot_id,
to_pack_id,
to_frozen_flag,
to_bucket_slot_code,
to_level1_container_code,
to_level2_container_code,
to_level3_inventory_id,
to_inventory_profit_version,
to_quantity,
to_feedback_quantity,
version,
remark,
delete_flag,
created_date as adjustment_created_time,
created_user as adjustment_created_user,
created_app as adjustment_created_app,
last_updated_date as adjustment_updated_time,
last_updated_user as adjustment_updated_user,
last_updated_app as adjustment_updated_app,
to_zone_code,
from_zone_code,
from_bucket_code,
to_bucket_code,
state as adjustment_state,
project_code,
substr(created_date,0,10) as d
from 
${ods_dbname}.ods_qkt_inventory_adjustment_detail_di
where d='${pre1_date}'

union all
select 
id,
warehouse_id,
inventory_adjustment_id,
from_owner_code,
from_sku_id,
from_lot_id,
from_pack_id,
from_frozen_flag,
from_bucket_slot_code,
from_level1_container_code,
from_level2_container_code,
from_quantity,
from_feedback_quantity,
to_owner_code,
to_sku_id,
to_lot_id,
to_pack_id,
to_frozen_flag,
to_bucket_slot_code,
to_level1_container_code,
to_level2_container_code,
to_level3_inventory_id,
to_inventory_profit_version,
to_quantity,
to_feedback_quantity,
version,
remark,
delete_flag,
adjustment_created_time,
adjustment_created_user,
adjustment_created_app,
adjustment_updated_time,
adjustment_updated_user,
adjustment_updated_app,
to_zone_code,
from_zone_code,
from_bucket_code,
to_bucket_code,
adjustment_state,
project_code,
d
from 
tmp_adjustment_detail_str2
) t
) rt 
where rt.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



