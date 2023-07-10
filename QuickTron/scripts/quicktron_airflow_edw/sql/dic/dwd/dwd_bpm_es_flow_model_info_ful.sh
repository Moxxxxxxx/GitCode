#!/bin/bash
# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： bpm dwd层  流程模型版本
#-- 注意 ： 每日按天全量分区
#-- 输入表 : ods.ods_qkt_bpm_es_flowmodel_df
#-- 输出表 ：dwd.dwd_bpm_es_flow_model_info_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-02-28 CREATE 

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



insert overwrite table ${dwd_dbname}.dwd_bpm_es_flow_model_info_ful
select 
FlowModelID as flow_mode_lid,
FlowName as flow_name,
FlowType as flow_type,
AppID as app_id,
OFlowModelID as oflow_model_id,
Status,
Remark as flow_model_desc,
TotalHours as total_hours,
MonitList as monit_list,
Deleted as is_deleted,
FlowChart as  flow_chart,
OrgID as org_id,
CreatorID as creator_id,
UpdateID as modifier_id,
CreateDate as create_time,
UpdateDate as update_time,
FlowValidata as flow_validata,
FlowBusID as flow_bus_id,
EndTransfer as end_transfer,
StopFlow as stop_flow,
DelFlow as del_flow,
StartType as start_type,
endMsg as end_msg,
FlowNameFormat as flow_name_format,
NoAttr,
PauseFlow as pause_flow,
InvalidFlow as invalid_flow,
CopyFlow as copy_flow,
OpinionMustEnter as opinion_mustenter,
UserOwn,
FlwoVerName as flow_over_name,
usernames as user_name
from 
${ods_dbname}.ods_qkt_bpm_es_flowmodel_df
where d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

