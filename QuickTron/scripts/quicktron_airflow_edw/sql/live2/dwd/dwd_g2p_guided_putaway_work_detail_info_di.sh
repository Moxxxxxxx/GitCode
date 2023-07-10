#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  推荐上架作业单主信息
#-- 注意 ： 每日按天增量分区
#-- 输入表 : ods.ods_qkt_g2p_guided_putaway_work_detail_di、ods.ods_qkt_g2p_w2p_guided_putaway_work_detail_di
#-- 输出表 ：dwd.dwd_g2p_guided_putaway_work_detail_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-01 CREATE 

# ------------------------------------------------------------------------------------------------

ods_dbname=ods
dwd_dbname=dwd
dim_dbname=dim
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

with tmp_guided_putaway_work_detail_str1 as (
select 
distinct d,project_code
from 
(
select 
distinct substr(created_date,0,10) as d,project_code
from
${ods_dbname}.ods_qkt_g2p_guided_putaway_work_detail_di
where d='${pre1_date}' 
and substr(created_date,0,10)<>'${pre1_date}'

UNION all
select 
distinct substr(created_date,0,10) as d,project_code
from
${ods_dbname}.ods_qkt_g2p_w2p_guided_putaway_work_detail_di
where d='${pre1_date}' 
and substr(created_date,0,10)<>'${pre1_date}'
) t
),
tmp_guided_putaway_work_detail_str2 as (
select 
b.*
from 
tmp_guided_putaway_work_detail_str1 a
inner join ${dwd_dbname}.dwd_g2p_guided_putaway_work_detail_info_di b on a.d=b.d and a.project_code=b.pt
)
insert overwrite table ${dwd_dbname}.dwd_g2p_guided_putaway_work_detail_info_di partition(d,pt)
select 
id,
warehouse_id,
zone_code,
work_id,
detail_id,
sku_id,
owner_code,
lot_id,
pack_id,
original_quantity,
quantity,
fulfill_quantity,
frozen,
level3_inventory_id,
work_created_app,
work_created_time,
work_updated_app,
work_updated_time,
project_code,
product_type,
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
warehouse_id,
zone_code,
work_id,
detail_id,
sku_id,
owner_code,
lot_id,
pack_id,
original_quantity,
quantity,
fulfill_quantity,
frozen,
level3_inventory_id,
created_app as work_created_app,
created_date as work_created_time,
updated_app as work_updated_app,
updated_date as work_updated_time,
a.project_code,
product_type,
substr(created_date,0,10) as d
from 
${ods_dbname}.ods_qkt_g2p_guided_putaway_work_detail_di a
left join ${dim_dbname}.dim_project_product_type b on a.project_code=b.project_code
where a.d='${pre1_date}'

union all
select 
id,
warehouse_id,
zone_code,
work_id,
detail_id,
sku_id,
owner_code,
lot_id,
pack_id,
original_quantity,
quantity,
fulfill_quantity,
frozen,
level3_inventory_id,
created_app as work_created_app,
created_date as work_created_time,
updated_app as work_updated_app,
updated_date as work_updated_time,
a.project_code,
product_type,
substr(created_date,0,10) as d
from 
${ods_dbname}.ods_qkt_g2p_w2p_guided_putaway_work_detail_di a
left join ${dim_dbname}.dim_project_product_type b on a.project_code=b.project_code
where a.d='${pre1_date}'

union all
select 
id,
warehouse_id,
zone_code,
work_id,
detail_id,
sku_id,
owner_code,
lot_id,
pack_id,
original_quantity,
quantity,
fulfill_quantity,
frozen,
level3_inventory_id,
work_created_app,
work_created_time,
work_updated_app,
work_updated_time,
project_code,
product_type,
d
from 
tmp_guided_putaway_work_detail_str2
) t
) rt 
where rt.rn=1
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"



