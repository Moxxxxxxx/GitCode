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
insert overwrite table dwd.dwd_bpm_es_message_info_ful
select 
MessageID as message_id,
FlowID as flow_id,
NodeID as node_id,
MessageType as message_type,
FolderID as folder_id,
ReceiverID as receiver_id,
RecDeptID as rec_dept_ud,
RecOrgID as rec_org_id,
OriginID as origin_id,
SenderID as sender_id,
SenderDeptID as sen_dept_id,
SenderOrgID as Sen_org_id,
ReceiveTime as receive_time,
ReadTime as read_time,
ReceiveType as receive_type,
Expected as expected_time,
WarnTime as warn_time,
RecentProcessTime as recent_process_time,
FActors as factors,
TActors as tactors,
ActorType as actor_type,
ActionID as action_id,
ReceiverType as receiver_type,
OrgRecID as org_rec_id,
Opinion as opinion,
IsRead as is_read,
Important as important_level,
Deleted as is_deleted,
Status as status,
PrePauseStatus as pre_pause_status,
HasBuff as is_has_buff,
limitType as limit_type,
limitValue as limit_value,
serialOrConcurrentType as serial_Or_Concurrent_type,
OpinionTemp as opinion_temp
from 
ods_qkt_bpm_es_message_df
where d='$pre1_date'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
