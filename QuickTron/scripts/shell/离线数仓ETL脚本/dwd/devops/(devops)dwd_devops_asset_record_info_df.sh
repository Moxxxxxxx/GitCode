#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： devops 的资产信息记录
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : ods.ods_qkt_devops_user_df，ods_qkt_devops_asset_record_df
#-- 输出表 : dwd.dwd_devops_asset_record_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-06-01 CREATE 
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
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


insert overwrite table ${dwd_dbname}.dwd_devops_asset_record_info_df partition(d='${pre1_date}')
select 
a.id, 
a.create_time, 
a.update_time, 
a.remark,
a.name as asset_name, 
a.ip, 
a.cpu, 
a.mem, 
a.disk, 
a.status as asset_status,
a.type as asset_type, 
a.tag as asset_tag, 
a.username as user_name, 
a.password,
a.owner_id,
b.first_name as owner_name
from 
${ods_dbname}.ods_qkt_devops_asset_record_df a
left join ${ods_dbname}.ods_qkt_devops_user_df b on a.owner_id=b.id and b.d='${pre1_date}'
where a.d='${pre1_date}'
;
"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
