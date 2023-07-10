#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目现场货架基础信息记录表
#-- 注意 ： 每日按天全量分区
#-- 输入表 : ods.ods_qkt_basic_bucket_df,ods.ods_qkt_basic_bucket_type_df
#-- 输出表 ：dwd.dwd_basic_bucket_base_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-08-01 CREATE 
#-- 2 wangziming 2022-08-02 modify 增加项目编码分区

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


insert overwrite table ${dwd_dbname}.dwd_basic_bucket_base_info_df partition(d='${pre1_date}',pt)
select 
a.id,
a.warehouse_id,
a.zone_id,
a.bucket_code,
a.bucket_type_id,
a.enabled as is_bucket_enabled,
a.point_code as bucket_point_code,
a.top_face as bucket_to_face,
a.digital_code,
a.sku_mix_limit,
a.state as bucket_state,
a.validate_state,
a.project_code,
b.bucket_type_code,
b.virtual_type as bucket_virtual_type,
b.move_type as bucket_move_type,
b.walk_through as is_walk_through,
b.apply_type as bucket_apply_type,
b.length as bucket_length,
b.width as bucket_width,
b.height as bucket_height,
b.weight_limit as bucket_weight_limit,
b.available_length as bucket_available_length,
b.available_width as bucket_available_width,
b.available_height as bucket_available_height,
b.layer_layout,
b.layer_color,
b.work_face,
b.face_count,
b.layer_count,
b.leg_diameter,
b.state as bucket_type_state,
b.discern_bucket_code,
b.fork_height,
b.fork_base_height,
a.project_code as pt
from 
${ods_dbname}.ods_qkt_basic_bucket_df a
left join ${ods_dbname}.ods_qkt_basic_bucket_type_df b on a.bucket_type_id=b.id and b.d='${pre1_date}'
where a.d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

