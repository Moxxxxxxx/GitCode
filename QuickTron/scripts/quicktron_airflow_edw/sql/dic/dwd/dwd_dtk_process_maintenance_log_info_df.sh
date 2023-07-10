#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目现场pe日志记录信息
#-- 注意 ： 每日增量更新到昨日的分区内，每天的分区为最新的数据
#-- 输入表 : ods.ods_qkt_dtk_process_maintenance_log_di.dwd_dtk_emp_info_df
#-- 输出表 ：dwd.dwd_dtk_process_maintenance_log_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-05-30 CREATE 
#-- 2 wangziming 2022-05-31 modify 修改逻辑
#-- 3 wangziming 2022-06-02 modify 增加时长字段
#-- 4 wangziming 2022-07-06 modify 修改project的清洗规则

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


init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;



insert overwrite table ${dwd_dbname}.dwd_dtk_process_maintenance_log_info_df partition(d)
select 
a.org_name,
a.process_instance_id, 
a.attached_process_instance_ids, 
a.biz_action, 
a.business_id, 
a.cc_userids as approver_user_ids, 
a.create_time, 
a.finish_time,
a.originator_dept_id, 
a.originator_dept_name, 
a.originator_userid as originator_user_id, 
if(c.emp_name is null,split(a.title,'提交')[0],c.emp_name) as originator_user_name,
a.\`result\` as approval_result, 
a.status as approval_status, 
a.title as approval_title, 
a.attendance_status,
regexp_replace(a.job_content,'[\\\\]\\\\[\\\"]','') as job_content,
a.log_date,
upper(a.project_code) as project_code,
regexp_replace(a.project_name,'\t|\n|\r','') as project_name,
a.log_report,
a.internal_work_content,
a.remarks,
a.working_hours,
'${pre1_date}' as d
from 
${ods_dbname}.ods_qkt_dtk_process_maintenance_log_di a
left join ${dwd_dbname}.dwd_dtk_emp_info_df c on a.originator_userid=c.emp_id and c.d='${pre1_date}'
where a.create_time is not null
;
"

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

insert overwrite table ${dwd_dbname}.dwd_dtk_process_maintenance_log_info_df partition(d)
select 
a.org_name,
a.process_instance_id, 
a.attached_process_instance_ids, 
a.biz_action, 
a.business_id, 
a.cc_userids as approver_user_ids, 
a.create_time, 
a.finish_time,
a.originator_dept_id, 
a.originator_dept_name, 
a.originator_userid as originator_user_id, 
if(c.emp_name is null,split(a.title,'提交')[0],c.emp_name) as originator_user_name,
a.\`result\` as approval_result, 
a.status as approval_status, 
a.title as approval_title, 
a.attendance_status,
regexp_replace(a.job_content,'[\\\\]\\\\[\\\"]','') as job_content,
a.log_date,
regexp_replace(upper(a.project_code),'[^A-Z-0-9]','') as project_code,
regexp_replace(a.project_name,'\t|\n|\r','') as project_name,
a.log_report,
a.internal_work_content,
a.remarks,
a.working_hours,
'${pre1_date}' as d
from 
${ods_dbname}.ods_qkt_dtk_process_maintenance_log_di a
left join ${dwd_dbname}.dwd_dtk_emp_info_df c on a.originator_userid=c.emp_id and c.d='${pre1_date}'
where a.create_time is not null and a.d='${pre1_date}'

union all
select 
a.org_name,
a.process_instance_id, 
a.attached_process_instance_ids, 
a.biz_action, 
a.business_id, 
a.approver_user_ids, 
a.create_time, 
a.finish_time,
a.originator_dept_id, 
a.originator_dept_name, 
a.originator_user_id, 
a.originator_user_name,
a.approval_result, 
a.approval_status, 
a.approval_title, 
a.attendance_status,
a.job_content,
a.log_date,
regexp_replace(a.project_code,'[^A-Z-0-9]','') as project_code,
a.project_name,
a.log_report,
a.internal_work_content,
a.remarks,
a.working_hours,
'${pre1_date}' as d
from 
${dwd_dbname}.dwd_dtk_process_maintenance_log_info_df a
left join ${ods_dbname}.ods_qkt_dtk_process_maintenance_log_di b on a.org_name=b.org_name and a.process_instance_id=b.process_instance_id and b.d='${pre1_date}'
where a.d='${pre2_date}' and b.process_instance_id is null
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


