#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目现场pe日志记录信息
#-- 注意 ： 每日增量更新到昨日的分区内，每天的分区为最新的数据
#-- 输入表 : ods.ods_qkt_dtk_process_pe_log_di、dim.dim_dtk_org_level_info、dwd.dwd_dtk_emp_info_df
#-- 输出表 ：dwd.dwd_dtk_process_pe_log_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-08 CREATE 


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

insert overwrite table ${dwd_dbname}.dwd_dtk_process_pe_log_info_df partition(d)
select 
a.org_name,
a.process_instance_id, 
a.attached_process_instance_ids, 
a.biz_action, 
a.business_id, 
a.cc_userids as approver_user_ids, 
a.create_time, 
b.org_id_2 as originator_2_dept_id,
b.org_name_2 as originator_2_dept_name,
a.originator_dept_id, 
a.originator_dept_name, 
a.originator_userid as originator_user_id, 
if(c.emp_name is null,split(a.title,'提交')[0],c.emp_name) as originator_user_name,
c.emp_position as originator_user_position,
a.\`result\` as approval_result, 
a.status as approval_status, 
a.title as approval_title, 
regexp_replace(upper(regexp_replace(a.process_project_code,'[\u4e00-\u9fa5]','')),'[^A-Z0-9-]',',') as project_code, 
a.process_project_name as project_name, 
a.project_manage, 
a.work_status, 
a.start_work_time, 
a.end_work_time, 
a.work_go_out as work_go_out_content, 
a.job_content, 
a.log_date, 
a.site_team_members, 
a.task_statis, 
a.company_job_content, 
a.tomorrow_schedule, 
a.finish_today, 
a.over_time as is_over_time_or_content, 
a.enclosure, 
a.fault_statis, 
a.fault_num_statis,
case when a.work_7_24 ='是' then '1'
     when a.work_7_24 ='否' then '0'
     else '-1' end is_7_24_work, 
a.carry_task_num, 
a.remarks,
'${pre1_date}' as d
from 
${ods_dbname}.ods_qkt_dtk_process_pe_log_di a
left join ${dim_dbname}.dim_dtk_org_level_info b on a.org_name=b.org_company_name and a.originator_dept_id=b.org_id
left join ${dwd_dbname}.dwd_dtk_emp_info_df c on a.originator_userid=c.emp_id and a.org_name=c.org_company_name and c.d='${pre1_date}'
where a.create_time is not null;


"

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

insert overwrite table ${dwd_dbname}.dwd_dtk_process_pe_log_info_df partition(d)
select 
a.org_name,
a.process_instance_id, 
a.attached_process_instance_ids, 
a.biz_action, 
a.business_id, 
a.cc_userids as approver_user_ids, 
a.create_time, 
b.org_id_2 as originator_2_dept_id,
b.org_name_2 as originator_2_dept_name,
a.originator_dept_id, 
a.originator_dept_name, 
a.originator_userid as originator_user_id, 
if(c.emp_name is null,split(a.title,'提交')[0],c.emp_name) as originator_user_name,
c.emp_position as originator_user_position,
a.\`result\` as approval_result, 
a.status as approval_status, 
a.title as approval_title, 
regexp_replace(upper(regexp_replace(a.process_project_code,'[\u4e00-\u9fa5]','')),'[^A-Z0-9-]',',') as project_code, 
a.process_project_name as project_name, 
a.project_manage, 
a.work_status, 
a.start_work_time, 
a.end_work_time, 
a.work_go_out as work_go_out_content, 
a.job_content, 
a.log_date, 
a.site_team_members, 
a.task_statis, 
a.company_job_content, 
a.tomorrow_schedule, 
a.finish_today, 
a.over_time as is_over_time_or_content, 
a.enclosure, 
a.fault_statis, 
a.fault_num_statis,
case when a.work_7_24 ='是' then '1'
     when a.work_7_24 ='否' then '0'
     else '-1' end is_7_24_work, 
a.carry_task_num, 
a.remarks,
${pre1_date} as d
from 
${ods_dbname}.ods_qkt_dtk_process_pe_log_di a
left join ${dim_dbname}.dim_dtk_org_level_info b on a.org_name=b.org_company_name and a.originator_dept_id=b.org_id
left join ${dwd_dbname}.dwd_dtk_emp_info_df c on a.originator_userid=c.emp_id and a.org_name=c.org_company_name and c.d='${pre1_date}'
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
a.originator_2_dept_id, 
a.originator_2_dept_name, 
a.originator_dept_id, 
a.originator_dept_name, 
a.originator_user_id, 
a.originator_user_name, 
a.originator_user_position, 
a.approval_result, 
a.approval_status, 
a.approval_title, 
a.project_code, 
a.project_name, 
a.project_manage, 
a.work_status, 
a.start_work_time, 
a.end_work_time, 
a.work_go_out_content, 
a.job_content, 
a.log_date, 
a.site_team_members, 
a.task_statis, 
a.company_job_content, 
a.tomorrow_schedule, 
a.finish_today, 
a.is_over_time_or_content, 
a.enclosure, 
a.fault_statis, 
a.fault_num_statis, 
a.is_7_24_work, 
a.carry_task_num, 
a.remarks,
${pre1_date} as d
from 
${dwd_dbname}.dwd_dtk_process_pe_log_info_df a
left join ${ods_dbname}.ods_qkt_dtk_process_pe_log_di b on a.org_name=b.org_name and a.process_instance_id=b.process_instance_id and b.d='${pre1_date}'
where a.d='${pre2_date}' and b.process_instance_id is null and b.create_time is not null
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

