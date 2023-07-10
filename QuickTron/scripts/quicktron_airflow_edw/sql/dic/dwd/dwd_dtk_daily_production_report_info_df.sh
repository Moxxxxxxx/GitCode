#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉宝仓生产人员日报信息
#-- 注意 ： 每日增量更新到昨日的分区内，每天的分区为最新的数据
#-- 输入表 : ods_qkt_dtk_daily_production_report_di、dim_dtk_emp_job_number_mapping_info、ods_qkt_kde_production_order_df、ods_qkt_kde_bd_project_df
#-- 输出表 ：dwd.dwd_dtk_daily_production_report_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-12-12 CREATE 
#-- 2 wangziming 2022-12-17 modify 增加项目编码
#-- 3 wangziming 2022-12-26 modify 增加操作员字段列加入去重
#-- 4 wangziming 2023-01-31 modify work_order_number 字段存在脏数据进行逻辑清洗
#-- 5 wangziming 2023-02-16 modify 增加as frame_numbers 车架号并进行清洗规则逻辑开发
#-- 6 wangziming 2023-02-28 modify 修改 计划数量以及生产数量的字段类型
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
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;


with tmp_dtk_emp_job_number_mapping as (
select 
a.org_company_name,
a.emp_id as bc_emp_id,
b.emp_id,
b.emp_name
from 
(
select 
*
from 
dim.dim_dtk_emp_job_number_mapping_info
where d='${pre1_date}' and org_company_name ='上海快仓智能科技有限公司'
) a
left join 
(
select 
*
from 
dim.dim_dtk_emp_job_number_mapping_info
where d='${pre1_date}' and org_company_name ='宝仓'
) b on a.job_number=b.job_number
where nvl(a.job_number,'')<>''
),
tmp_dtk_daily_production_repor_str2 as (
select 
org_name,
process_instance_id,
operator,
concat_ws(',',collect_list(user_id)) as approval_user_ids,
concat_ws(',',collect_list(user_name)) as  approval_user_names
from 
(
select 
t1.org_name,
t1.process_instance_id,
t1.operator,
if(t1.user_id='UNKNOWN',null,t1.user_id) as user_id,
case when t1.user_id='UNKNOWN' then null
         when nvl(t2.emp_name,'')<>'' then t2.emp_name
         when nvl(t3.emp_name,'')<>'' then t3.emp_name
         else 'UNKNOWN' end as user_name
from 
(
select 
a.org_name,
a.process_instance_id,
a.operator,
b.user_id
from 
${ods_dbname}.ods_qkt_dtk_daily_production_report_di a
lateral view explode(split(regexp_replace(if(nvl(a.cc_userids,'')='','UNKNOWN',a.cc_userids),'[\\\\[\\\\]\'\\\\s+]',''),',')) b as user_id
where d='${pre1_date}'
) t1
left join tmp_dtk_emp_job_number_mapping t2 on t1.user_id=t2.bc_emp_id
left join tmp_dtk_emp_job_number_mapping t3 on t1.user_id=t3.emp_id
) b
group by 
org_name,
process_instance_id,
operator
),
tmp_dtk_daily_production_repor_str3 as (
select 
org_name,
process_instance_id,
concat_ws(',',collect_set(frame_number)) as frame_numbers
from 
(
select 
a.org_name,
a.process_instance_id,
get_json_object(b.frame_number_json,'$.rowValue.value') as frame_number
from 
${ods_dbname}.ods_qkt_dtk_daily_production_report_di a
lateral view explode(split(regexp_replace(regexp_replace(if(nvl(a.frame_number,'')='','UNKNOWN',a.frame_number) , '\\\\[|\\\\]',''),'\\\\}\\\\,\\\\{','\\\\}\\\\;\\\\{'),'\\\\;')) b as frame_number_json
where d='${pre1_date}'
) t
group by org_name,
process_instance_id
)
insert overwrite table ${dwd_dbname}.dwd_dtk_daily_production_report_info_df partition(d='${pre1_date}')
select 
org_name, 
process_instance_id, 
attached_process_instance_ids, 
biz_action, 
business_id, 
approval_user_ids, 
approval_user_names, 
process_start_time, 
process_end_time, 
applicant_dept_id, 
applicant_dept_name, 
applicant_userid,
applicant_user_name, 
approval_result, 
approval_status, 
approval_title,
production_date, 
regexp_replace(work_order_number,'\\\\s+','') as work_order_number, 
product_process, 
product_part_number, 
model_code, 
product_name, 
agv_standard_time, 
harness_or_parts_standard_time, 
standard_time_minutes, 
plan_number, 
production_number, 
all_working_hours_minutes, 
all_losing_hours_minutes, 
semi_finished_attendance_efficiency, 
semi_finished_prodction_efficiency, 
finished_attendance_efficiency, 
finished_prodction_efficiency, 
inspection_number, 
prodction_efficiency, 
attendance_efficiency, 
loss_rate, 
operator_name, 
working_hours, 
individual_output_quantity, 
individual_output_hours, 
loss_ategory, 
accountability_unit, 
losing_hours, 
losing_desc,
is_valid,
upper(rt2.fnumber) as project_code,
regexp_replace(frame_numbers,'\\\\s+','') as frame_numbers
from 
(
select 
*,
row_number() over(partition by org_name,business_id,operator_name order by process_end_time desc) as rn
from 
(
select 
a.org_name, 
a.process_instance_id, 
a.attached_process_instance_ids, 
a.biz_action, 
a.business_id, 
a.approval_user_ids, 
a.approval_user_names, 
a.process_start_time, 
a.process_end_time, 
a.applicant_dept_id, 
a.applicant_dept_name, 
a.applicant_userid,
a.applicant_user_name, 
a.approval_result, 
a.approval_status, 
a.approval_title,
a.production_date, 
a.work_order_number, 
a.product_process, 
a.product_part_number, 
a.model_code, 
a.product_name, 
a.agv_standard_time, 
a.harness_or_parts_standard_time, 
a.standard_time_minutes, 
a.plan_number, 
a.production_number, 
a.all_working_hours_minutes, 
a.all_losing_hours_minutes, 
a.semi_finished_attendance_efficiency, 
a.semi_finished_prodction_efficiency, 
a.finished_attendance_efficiency, 
a.finished_prodction_efficiency, 
a.inspection_number, 
a.prodction_efficiency, 
a.attendance_efficiency, 
a.loss_rate, 
a.operator_name, 
a.working_hours, 
a.individual_output_quantity, 
a.individual_output_hours, 
a.loss_ategory, 
a.accountability_unit, 
a.losing_hours, 
a.losing_desc,
a.is_valid,
a.frame_numbers
from
${dwd_dbname}.dwd_dtk_daily_production_report_info_df a
left join tmp_dtk_daily_production_repor_str2 b on a.org_name=b.org_name and a.process_instance_id=b.process_instance_id and a.operator_name = b.operator
where a.d='${pre2_date}' and b.process_instance_id is null


union all
select 
a.org_name,
a.process_instance_id,
if(a.attached_process_instance_ids='[]',null,a.attached_process_instance_ids) as attached_process_instance_ids,
a.biz_action,
a.business_id,
b.approval_user_ids,
b.approval_user_names,
a.create_time as process_start_time,
a.finish_time as process_end_time,
a.originator_dept_id as applicant_dept_id,
a.originator_dept_name as applicant_dept_name,
case when d.emp_id is not null then d.emp_id
     when c.bc_emp_id is not null then c.emp_id
     else a.originator_userid end applicant_userid,
case when d.emp_id is not null then d.emp_name
     when c.bc_emp_id is not null then c.emp_name
     else split(a.title,'提交')[0] end applicant_user_name,
a.result as approval_result,
a.status as approval_status,
a.title as approval_title,
a.production_date,
a.work_order_number,
a.process as product_process,
a.product_part_number,
a.model_code,
regexp_replace(a.product_name,'\t|\r|\n','') as product_name,
cast(a.agv_standard_time as decimal(10,2))as agv_standard_time,
cast(a.harness_or_parts_standard_time as decimal(10,2))as harness_or_parts_standard_time,
cast(a.standard_time_minutes as decimal(10,2))as standard_time_minutes,
cast(a.plan_number as decimal(10,3))as plan_number,
cast(a.production_number as decimal(10,3))as production_number,
cast(a.all_working_hours_minutes as decimal(10,2))as all_working_hours_minutes,
cast(a.all_losing_hours_minutes as decimal(10,2))as all_losing_hours_minutes,
cast(a.semi_finished_attendance_efficiency_production_report as decimal(10,3))as semi_finished_attendance_efficiency,
cast(a.semi_finished_prodction_efficiency_production_report as decimal(10,3))as semi_finished_prodction_efficiency,
cast(a.finished_attendance_efficiency_production_report as decimal(10,3))as finished_attendance_efficiency,
cast(a.finished_prodction_efficiency_production_report as decimal(10,3))as finished_prodction_efficiency,
cast(a.inspection_number as int)as inspection_number,
cast(a.prodction_efficiency_qualify as decimal(10,3))as prodction_efficiency,
cast(a.attendance_efficiency_qualify as decimal(10,3))as attendance_efficiency,
cast(a.loss_rate as decimal(10,3))as loss_rate,
a.operator as operator_name,
cast(a.working_hours as decimal(10,2))as working_hours,
cast(a.individual_output_quantity as decimal(10,3))as individual_output_quantity,
cast(a.individual_output_hours as decimal(10,2))as individual_output_hours,
a.loss_ategory,
a.accountability_unit,
-- cast(a.losing_hours as decimal(10,2))as losing_hours,
losing_hours,
regexp_replace(a.losing_description,'\t|\r|\n','') as losing_desc,
if(a.biz_action='REVOKE' or a.status='RUNNING','0','1') as is_valid,
if(e.frame_numbers='' or e.frame_numbers='无',null,e.frame_numbers)  as frame_numbers
from 
${ods_dbname}.ods_qkt_dtk_daily_production_report_di a 
left join tmp_dtk_daily_production_repor_str2 b on a.org_name=b.org_name and a.process_instance_id=b.process_instance_id
left join tmp_dtk_emp_job_number_mapping c on a.originator_userid=c.bc_emp_id
left join tmp_dtk_emp_job_number_mapping d on a.originator_userid=d.emp_id
left join tmp_dtk_daily_production_repor_str3 e on a.org_name=e.org_name and a.process_instance_id=e.process_instance_id
where a.d='${pre1_date}'
) t
) rt
left join ${ods_dbname}.ods_qkt_kde_production_order_df rt1 on rt.work_order_number=rt1.fbillno and rt1.d='${pre1_date}'
left join ${ods_dbname}.ods_qkt_kde_bd_project_df rt2 on rt1.f_abc_base =rt2.fid and rt2.d='${pre1_date}'
where rt.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


