#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉宝仓产线举手单
#-- 注意 ： 每日增量更新到昨日的分区内，每天的分区为最新的数据
#-- 输入表 : ods_qkt_dtk_production_hands_show_di、dim_dtk_emp_job_number_mapping_info
#-- 输出表 ：dwd.dwd_dtk_production_hands_show_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-11-18 CREATE 
#-- 2 wangziming 2022-12-12 modify 进行 org_name,business_id 排序去重处理
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
tmp_dtk_production_hands_show_str2 as (
select 
org_name,
process_instance_id,
concat_ws(',',collect_list(user_id)) as approval_user_ids,
concat_ws(',',collect_list(user_name)) as  approval_user_names
from 
(
select 
t1.org_name,
t1.process_instance_id,
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
b.user_id
from 
${ods_dbname}.ods_qkt_dtk_production_hands_show_di a
lateral view explode(split(regexp_replace(if(nvl(a.cc_userids,'')='','UNKNOWN',a.cc_userids),'[\\\\[\\\\]\'\\\\s+]',''),',')) b as user_id
where d='${pre1_date}'
) t1
left join tmp_dtk_emp_job_number_mapping t2 on t1.user_id=t2.bc_emp_id
left join tmp_dtk_emp_job_number_mapping t3 on t1.user_id=t3.emp_id
) b
group by 
org_name,
process_instance_id
)
insert overwrite table ${dwd_dbname}.dwd_dtk_production_hands_show_info_df partition(d='${pre1_date}')
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
work_order_number,
work_order_type,
product_agv_type,
production_procedure,
influence_people_number,
problem_desc,
exception_picture_desc,
confirmation_response,
liability_judgment,
judgment_basis_description,
problem_cause,
interim_measures,
question_type,
is_valid
from 
(
select 
*,
row_number() over(partition by org_name,business_id order by process_end_time desc) as rn
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
a.work_order_number,
a.work_order_type,
a.product_agv_type,
a.production_procedure,
a.influence_people_number,
a.problem_desc,
a.exception_picture_desc,
a.confirmation_response,
a.liability_judgment,
a.judgment_basis_description,
a.problem_cause,
a.interim_measures,
a.question_type,
a.is_valid
from
${dwd_dbname}.dwd_dtk_production_hands_show_info_df a
left join tmp_dtk_production_hands_show_str2 b on a.org_name=b.org_name and a.process_instance_id=b.process_instance_id
where a.d='${pre2_date}' and b.process_instance_id is null

-- 宝仓加班申请单
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
a.work_order_number,
a.work_order_type,
regexp_replace(a.product_type,'\r|\n|\t| ','') as product_agv_type,
a.production_procedure,
cast(a.influence_people_number as int) as influence_people_number,
regexp_replace(a.symptom_description,'\r|\n|\t| ','')  as problem_desc,
a.exception_description_picture as exception_picture_desc,
a.confirmation_response,
regexp_replace(a.liability_judgment,'\r|\n|\t| ','') as liability_judgment,
regexp_replace(a.judgment_basis_description,'\r|\n|\t| ','') as judgment_basis_description,
regexp_replace(a.problem_cause_analysis,'\r|\n|\t| ','') as problem_cause,
regexp_replace(a.interim_measures,'\r|\n|\t| ','') as interim_measures,
a.question_type,
if(a.biz_action='REVOKE','0','1') as is_valid
from 
${ods_dbname}.ods_qkt_dtk_production_hands_show_di a 
left join tmp_dtk_production_hands_show_str2 b on a.org_name=b.org_name and a.process_instance_id=b.process_instance_id
left join tmp_dtk_emp_job_number_mapping c on a.originator_userid=c.bc_emp_id
left join tmp_dtk_emp_job_number_mapping d on a.originator_userid=d.emp_id
where a.d='${pre1_date}'
) t
) rt
where rt.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


