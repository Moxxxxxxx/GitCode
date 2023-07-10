#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  商品基本信息
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_wes_basic_sku_di
#-- 输出表 ：dwd.dwd_wes_basic_sku_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-02 CREATE 
#-- 2 wangziming 2022-03-09 modify 新增字段兼容2.9.1
#-- 3 wangziming 2022-08-05 modify 修改逻辑为全量，修改表名

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

insert overwrite table ${dwd_dbname}.dwd_wes_basic_sku_info_df partition(d='${pre1_date}',pt)
select  
id,
owner_id,
sku_code,
sku_name,
batch_enabled,
sn_enabled,
lot_barcode_enabled,
over_weight_flag,
upper_limit_quantity,
lower_limit_quantity,
image_url,
expiration_date,
near_expiration_date,
spec,
supplier,
abc_category,
major_category,
medium_category,
minor_category,
mutex_category,
state as sku_state,
udf1,
udf2,
udf3,
udf4,
udf5,
created_user as sku_created_user,
created_app as sku_created_app,
created_time as sku_created_time,
last_updated_user as sku_updated_user,
last_updated_app as sku_updated_app,
last_updated_time as sku_updated_time,
extended_field,
project_code,
pick_enabled as is_pick_enabled,
replenish_enabled as is_replenish_enabled,
cycle_count_enabled as is_cycle_count_enabled,
expiry_date_enabled as is_expiry_date_enabled,
warehouse_id,
project_code as pt
from 
${ods_dbname}.ods_qkt_wes_basic_sku_df
where d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

