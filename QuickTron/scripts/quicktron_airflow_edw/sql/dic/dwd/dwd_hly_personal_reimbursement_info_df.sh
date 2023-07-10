#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 汇联易的个人报销费用数据
#-- 注意 ： 每日全量分区
#-- 输入表 : ods_qkt_hly_personal_reimbursement_df,dwd_bpm_personal_expense_account_info_ful,dwd_dtk_emp_info_df,dim_dtk_org_level_info
#-- 输出表 ：dwd.dwd_hly_personal_reimbursement_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-12-14 create 
# ------------------------------------------------------------------------------------------------


ods_dbname=ods
dim_dbname=dim
dwd_dbname=dwd
tmp_dbname=tmp
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
set hive.vectorized.execution.enabled = false; -- 解决Output column number expected to be 0 when isRepeating




with tmp_project_str1 as (
select
integrationId,
concat_ws(',',cci2code,cci3code,cci4code,cci5code) as reimburse_project_codes,
concat_ws(',',cci2,cci3,cci4,cci5) as reimburse_project_names
from 
${ods_dbname}.ods_qkt_hly_personal_reimbursement_df
where d='${pre1_date}'
)
--,
--tmp_bpm_str1 as (
--select 
--flow_id,
--sum(amount) as amount
--from 
--(
--select 
--a.flow_id,
--b.amount
--from 
--${dwd_dbname}.dwd_bpm_personal_expense_account_info_ful a
--lateral view explode(split(row_reimburse_amounts,',')) b as amount
--) t
--group by flow_id
--)
insert overwrite table ${dwd_dbname}.dwd_hly_personal_reimbursement_info_df partition(d='${pre1_date}')
select 
a.businessCode as reimburse_code,
a.formTypeDesc as reimburse_form_type,
a.applicantName as applicant_name,
b.emp_id as applicant_code,
a.applicantComName as applicant_company_name,
c.org_id as applicant_dept_id,
split(a.applicantDeptPath,'\\\\|')[1] as applicant_dept_name,
a.applicantCustDeptNumber as applicant_org_id,
a.applicantDeptName as applicant_org_name,
a.applicantDeptPath as applicant_org_path_name,
a.submittedByName as submitter_name,
a.companyName as reimburse_company_name,
regexp_replace(a.title,'\t|\r|\n','') as title,
a.submittedDate as submit_time,
a.reimbStatusDesc as reimburse_status,
a.reimbLastModifiedDate as reimburse_last_update_time,
a.reimbLastApprover as reimburse_last_approver_name,
substr(a.reimbAuditApprovalDate,1,10) as reimburse_audit_approval_date,
a.reimbAuditApprover as reimburse_audit_approver_name,
a.currencyCode as form_currency_code,
cast(a.totalAmount as decimal(18,2)) as reimburse_total_amount,
a.functionalCurrencyCode as functional_currency_code,
a.exchageRate as exchage_rate,
cast(a.baseCurrencyAmount as decimal(18,2)) as functional_currency_amount,
cast(a.baseReimbPaymentAmount as decimal(18,2)) as functional_currency_loan_amount,
cast(a.realPaymentBaseAmount as decimal(18,2)) as real_payment_amount,
a.formName as reimburse_form_name,
a.integrationId as record_id,
substr(a.receiveDate,1,10) as receive_date,
substr(a.reimbRealPaymentDate,1,10) as real_payment_date,
if(nvl(e.reimburse_project_codes,'')='',null,e.reimburse_project_codes) as reimburse_project_codes,
if(nvl(e.reimburse_project_names,'')='',null,e.reimburse_project_names) as reimburse_project_names,
'HLY' as data_source
from 
${ods_dbname}.ods_qkt_hly_personal_reimbursement_df a
left join ${dwd_dbname}.dwd_dtk_emp_info_df b on a.applicantEmpId=b.job_number and b.d='${pre1_date}'
left join (select * from ${dim_dbname}.dim_dtk_org_level_info where org_level_num =2) c on split(a.applicantDeptPath,'\\\\|')[1]=c.org_name
left join tmp_project_str1 e on a.integrationId=e.integrationId
where a.d='${pre1_date}'

--union all
--select 
--a.flow_id as reimburse_code,
--reimburse_categories as reimburse_form_type,
--apply_user_name as applicant_name,
--null as applicant_code,
--cost_attr_org as applicant_company_name,
--null as applicant_dept_id,
--org_name_2 as applicant_dept_name,
--null as applicant_org_id,
--dept_name as applicant_org_name,
--null as applicant_org_path_name,
--reimburse_user_name as submitter_name,
--cost_attr_org as reimburse_company_name,
--regexp_replace(title,'\t|\r|\n','') as title,
--substr(start_time,1,10) as submit_date,
--approve_status as reimburse_status,
--end_time as reimburse_last_update_time,
--null as reimburse_last_approver_name,
--null as reimburse_audit_approval_date,
--null as reimburse_audit_approver_name,
--currency as form_currency_code,
--total_reimburse_amount as reimburse_total_amount,
--'人民币' as functional_currency_code,
--exchange_rate,
--amount * exchange_rate as functional_currency_amount,
--null as functional_currency_loan_amount,
--amount as real_payment_amount,
--reimburse_categories as reimburse_form_name,
--reimburse_flow_number as record_id,
--professional_work_date as receive_date,
--reimburse_date as real_payment_date,
--row_project_codes as reimburse_project_codes,
--null as reimburse_project_names,
--'BPM' as data_source
--from
--${dwd_dbname}.dwd_bpm_personal_expense_account_info_ful a
--left join tmp_bpm_str1 b on a.flow_id =b.flow_id



"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"
