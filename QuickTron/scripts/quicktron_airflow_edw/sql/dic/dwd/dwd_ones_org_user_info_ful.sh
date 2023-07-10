#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ：  onse 用户表，每天全量
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_ones_org_user_df
#-- 输出表 ：dwd.dwd_ones_org_user_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-01-14 CREATE 
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


insert overwrite table ${dwd_dbname}.dwd_ones_org_user_info_ful
select
org_uuid,
uuid,
name as user_name,
name_pinyin as  user_name_pinyin,
email as user_email,
avatar as user_avatar,
phone as user_phone,
title as user_position,
company as user_company,
password as user_password,
channel,
inviter_uuid,
hash,
access_time,
status as user_status,
verify_status,
to_utc_timestamp(floor(create_time/1000),'GMT-8') as create_time,
to_utc_timestamp(floor(modify_time/1000),'GMT-8') as create_time,
id_number as user_id_number
from 
${ods_dbname}.ods_qkt_ones_org_user_df 
where d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

