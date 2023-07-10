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
insert overwrite table dwd.dwd_bpm_es_node_model_info_ful
select 
FlowModelID as flow_model_id,
NodeModelID as node_model_id,
NodeBusID as node_bus_id,
DisFlowModelID as disflow_model_id,
WaitType as wait_type,
DisperseType as disperse_type,
NodeName as node_name,
OpID as op_id,
Type as node_type,
PathID as path_id,
Counter as finsh_node_count,
MasterNode as master_node,
MasterPath as master_path,
Remark as node_model_desc,
TotalHours as total_hours,
ViewAttach as view_attach,
WarningHours as warning_hours,
CanAutoPass as is_auto_pass,
CanCustLimit as is_cust_limit,
TakeOver as takeover,
StopFlow as stop_flow,
CanJump as is_jump,
CanBack as is_back,
CanTakeBack as is_takeback,
TimeUnit as time_unit,
CanAttemper as is_attemper,
CanTransmit as is_transmit,
CanAutoTransmit as is_auto_transmit,
CanCommunic as is_communic,
MustCommunic as must_communic,
CanBackHasDone as is_backhasdone,
CanAssist as is_assist,
CanFreeTakeOver as is_free_takeover,
NodeSort as node_sort,
hList,
lList,
nList,
FentryAdd as fentry_add,
FentryDel as fentry_del,
FentryEdit as fentry_edit,
autopasstime as auto_pass_time,
MFList,
RFList,
AppShowPc,
FentryBL as fentry_bl,
isFentryAdd as is_fentry_add,
isFentryDel as is_fentry_del,
isFentryEdit as is_fentry_edit,
passRate,
reviewMode as review_mode,
LinkOpinionRecorded as link_opinion_recorded,
FlowPerson as flow_person,
FlowRisk as flow_risk,
Requirement
from 
ods_qkt_bpm_es_nodemodel_df
where d='$pre1_date'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
