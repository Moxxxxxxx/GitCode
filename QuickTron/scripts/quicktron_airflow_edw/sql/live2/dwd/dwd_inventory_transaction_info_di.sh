#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  库存事务
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_inventory_transaction_di
#-- 输出表 ：dwd.dwd_inventory_transaction_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-01 CREATE
#-- 2 wangziming 2022-03-09 modify 新增字段兼容2.9.1
#-- 3 wangziming 2023-02-24 modify 回流状态七天数据

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



insert overwrite table ${dwd_dbname}.dwd_inventory_transaction_info_di partition(d,pt)
select 
id,
warehouse_id,
inventory_level,
inventory_id,
biz_type,
biz_type_group,
inventory_action_type,
biz_idempotent_id,
biz_bill_id,
biz_bill_number,
biz_bill_detail_id,
zone_code,
bucket_code,
bucket_slot_code,
level1_container_code,
level2_container_code,
owner_code,
sku_id,
sn_enabled,
lot_id,
pack_id,
frozen_flag,
quantity,
out_locked_quantity,
in_locked_quantity,
post_quantity,
post_out_locked_quantity,
post_in_locked_quantity,
transaction_time,
state as transaction_state,
correlation_id,
created_date as transaction_created_time,
created_user as transaction_created_user,
created_app as transaction_created_app,
last_updated_date as transaction_updated_time,
last_updated_user as transaction_updated_user,
last_updated_app as transaction_updated_app,
project_code,
frozen_locked_quantity,
post_frozen_locked_quantity,
substr(created_date,0,10) as d,
project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc) as rn
from 
${ods_dbname}.ods_qkt_inventory_transaction_di
where d>=date_sub('${pre1_date}',7) and substr(created_date,0,10)>=date_sub('${pre1_date}',7)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



