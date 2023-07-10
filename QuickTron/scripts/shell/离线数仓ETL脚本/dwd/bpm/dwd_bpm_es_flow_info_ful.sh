#!/bin/bash

dbname=ods
#hive=/opt/module/hive-3.1.2/scripts/hive
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


use $dbname;
insert overwrite table dwd.dwd_bpm_es_flow_info_ful
select 
flowid as flow_id,
flowmodelid as flow_model_id,
appid as app_id,
name as flow_name,
subject as flow_subject,
premessageid as pre_message_id,
preflowid as pre_message_id,
jointype as join_type,
starttime as start_time,
endtime as end_time,
starterid as starter_id,
expectendtime as expect_end_time,
status as flow_status,
attachment as is_have_attachment,
k3msgid as k3_msg_id,
curmessageid as cur_message_id,
curnodeid as cur_node_id,
curuserid as cur_user_id,
curnodename as cur_node_name,
curusername as cur_user_name,
isread as is_read,
isred as is_red
from 
ods_qkt_bpm_es_flow_df
where d='$pre1_date'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

