#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉审批流记录表
#-- 注意 ： 每天增量数据追加到昨日分区，得到最新的数据
#-- 输入表 : dwd.dwd_dtk_emp_info_df、ods.ods_qkt_dtk_process_maintenance_log_di、ods.ods_qkt_dtk_process_business_travel_di、ods.ods_qkt_dtk_process_leave_di、ods.ods_qkt_dtk_implementers_attendamce_di、ods.ods_qkt_dtk_process_pe_log_di、ods.ods_qkt_dtk_process_work_overtime_di、ods.ods_qkt_dtk_process_special_labor_approval_df
#-- 输出表 ：dwd.dwd_dtk_process_operation_record_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-06-16 create


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


init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


  insert overwrite table dwd.dwd_dtk_process_operation_record_info_df partition(d='${pre1_date}')
  select 
  a.process_instance_id, 
  a.userid as user_id, 
  b.emp_name,
  a.operation_type,
  case upper(a.operation_type) when 'EXECUTE_TASK_NORMAL' then '正常执行任务'
                               when 'EXECUTE_TASK_AGENT' then '代理人执行任务'
                               when 'APPEND_TASK_BEFORE' then '前加签任务'
                               when 'APPEND_TASK_AFTER' then '后加签任务'
                               when 'REDIRECT_TASK' then '转交任务'
                               when 'START_PROCESS_INSTANCE' then '发起流程实例'
                               when 'TERMINATE_PROCESS_INSTANCE' then '终止(撤销)流程实例'
                               when 'FINISH_PROCESS_INSTANCE' then '结束流程实例'
                               when 'ADD_REMARK' then '添加评论'
                               when 'REDIRECT_PROCESS' then '审批退回'
                               when 'PROCESS_CC' then '抄送'
                               else null end as operation_type_name,
  a.operation_date as operation_time,
  a.operation_result,

case upper(a.operation_result) when 'AGREE' then '同意'
                               when 'REFUSE' then '拒绝'
                               when 'NONE' then '无'
                               else null end as operation_result_name,
  a.attachments,
  case when c.process_instance_id is not null then '维保'
       when e.process_instance_id is not null then '出差'
       when f.process_instance_id is not null then '请假'
       when g.process_instance_id is not null then '快仓实施运维考勤'
       when h.process_instance_id is not null then 'PE日志'
       when i.process_instance_id is not null then '加班'
       when j.process_instance_id is not null then '运维劳务特批'
       else null end operation_category
  from 
  ods.ods_qkt_dtk_process_operation_record_di a
  left join dwd.dwd_dtk_emp_info_df b on a.userid=b.emp_id and b.d='${pre1_date}'
  left join dwd.dwd_dtk_process_maintenance_log_info_df c on a.process_instance_id=c.process_instance_id and c.d='${pre1_date}'
  left join dwd.dwd_dtk_process_business_travel_df e on a.process_instance_id=e.process_instance_id and e.d='${pre1_date}'
  left join dwd.dwd_dtk_process_leave_info_df f on a.process_instance_id=f.process_instance_id and f.d='${pre1_date}'
  left join dwd.dwd_dtk_implementers_attendamce_di g on a.process_instance_id=g.process_instance_id
  left join dwd.dwd_dtk_process_pe_log_info_df h on a.process_instance_id=h.process_instance_id and h.d='${pre1_date}'
  left join dwd.dwd_dtk_process_work_overtime_info_df i on a.process_instance_id=i.process_instance_id and i.d='${pre1_date}'
  left join dwd.dwd_dtk_special_labor_approval_process_info_ful j on a.process_instance_id=j.process_instance_id
  ;
"




sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

with  tmp_dtk_process_operation_record_str1 as (
select 
a.*
from 
${dwd_dbname}.dwd_dtk_process_operation_record_info_df a
left join ${ods_dbname}.ods_qkt_dtk_process_operation_record_di b on a.process_instance_id=b.process_instance_id and a.operation_time=b.operation_date and b.d='${pre1_date}'
where a.d='${pre1_date}' and b.process_instance_id is null

)

  insert overwrite table ${dwd_dbname}.dwd_dtk_process_operation_record_info_df partition(d='${pre1_date}')
  select 
  a.process_instance_id, 
  a.userid as user_id, 
  b.emp_name,
  a.operation_type,
  case upper(a.operation_type) when 'EXECUTE_TASK_NORMAL' then '正常执行任务'
                               when 'EXECUTE_TASK_AGENT' then '代理人执行任务'
                               when 'APPEND_TASK_BEFORE' then '前加签任务'
                               when 'APPEND_TASK_AFTER' then '后加签任务'
                               when 'REDIRECT_TASK' then '转交任务'
                               when 'START_PROCESS_INSTANCE' then '发起流程实例'
                               when 'TERMINATE_PROCESS_INSTANCE' then '终止(撤销)流程实例'
                               when 'FINISH_PROCESS_INSTANCE' then '结束流程实例'
                               when 'ADD_REMARK' then '添加评论'
                               when 'REDIRECT_PROCESS' then '审批退回'
                               when 'PROCESS_CC' then '抄送'
                               else null end as operation_type_name,
  a.operation_date as operation_time,
  a.operation_result,

case upper(a.operation_result) when 'AGREE' then '同意'
                               when 'REFUSE' then '拒绝'
                               when 'NONE' then '无'
                               else null end as operation_result_name,
  a.attachments,
  case when c.process_instance_id is not null then '维保'
       when e.process_instance_id is not null then '出差'
       when f.process_instance_id is not null then '请假'
       when g.process_instance_id is not null then '快仓实施运维考勤'
       when h.process_instance_id is not null then 'PE日志'
       when i.process_instance_id is not null then '加班'
       when j.process_instance_id is not null then '运维劳务特批'
       else null end operation_category
  from 
  ${ods_dbname}.ods_qkt_dtk_process_operation_record_di a
  left join ${dwd_dbname}.dwd_dtk_emp_info_df b on a.userid=b.emp_id and b.d='${pre1_date}'
  left join ${ods_dbname}.ods_qkt_dtk_process_maintenance_log_di c on a.process_instance_id=c.process_instance_id and c.d='${pre1_date}'
  left join ${ods_dbname}.ods_qkt_dtk_process_business_travel_di e on a.process_instance_id=e.process_instance_id and e.d='${pre1_date}'
  left join ${ods_dbname}.ods_qkt_dtk_process_leave_di f on a.process_instance_id=f.process_instance_id and f.d='${pre1_date}'
  left join ${ods_dbname}.ods_qkt_dtk_implementers_attendamce_di g on a.process_instance_id=g.process_instance_id
  left join ${ods_dbname}.ods_qkt_dtk_process_pe_log_di h on a.process_instance_id=h.process_instance_id and h.d='${pre1_date}'
  left join ${ods_dbname}.ods_qkt_dtk_process_work_overtime_di i on a.process_instance_id=i.process_instance_id and i.d='${pre1_date}'
  left join ${ods_dbname}.ods_qkt_dtk_process_special_labor_approval_df j on a.process_instance_id=j.process_instance_id
  where a.d='${pre2_date}'


  union all
  select 
  process_instance_id, 
  user_id, user_name, 
  operation_type, 
  operation_type_name, 
  operation_time, 
  operation_result, 
  operation_result_name, 
  attachments, 
  operation_category
  from 
  tmp_dtk_process_operation_record_str1
  ;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
