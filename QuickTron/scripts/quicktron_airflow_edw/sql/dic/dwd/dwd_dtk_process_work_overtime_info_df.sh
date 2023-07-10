#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉加班申请记录单
#-- 注意 ： 每日增量更新到昨日的分区内，每天的分区为最新的数据
#-- 输入表 : ods.ods_qkt_dtk_process_work_overtime_di、ods.ods_qkt_dtk_user_info_df、ods_qkt_dtk_process_overtime_form_df、dim_dtk_emp_job_number_mapping_info
#-- 输出表 ：dwd.dwd_dtk_process_work_overtime_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-05-10 CREATE 
#-- 2 wangziming 2022-05-12 modify 增加加班拆分字段
#-- 3 wangziming 2022-06-16 modify 历史数据追朔会导致数据重复，修改取值逻辑（lateral view explode 方法对null的列会取不出来）
#-- 4 wangziming 2022-07-05 modify 由于撤销的值会新生成一条数据，要把撤销和原来的那个删除
#-- 5 wangziming 2022-08-22 modify 根据business_id分组，按照process_end_time进行去重
#-- 6 wangziming 2022-11-17 modify 增加宝仓的加班申请单数据融合
#-- 7 wangziming 2022-11-23 modify 宝仓的进行去重
#-- 8 wangziming 2022-11-25 modify 进行 business_id is not null 逻辑删除
#-- 9 wangziming 2023-02-02 modify 进行加班原因的增加清洗，去除空白符
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

with tmp_dtk_process_work_overtime_info_df_str1 as (
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
   when nvl(t2.name,'')='' then 'UNKNOWN'
   else t2.name end as user_name
from 
(
select 
a.org_name,
a.process_instance_id,
b.user_id
from 
${ods_dbname}.ods_qkt_dtk_process_work_overtime_di a
lateral view explode(split(regexp_replace(if(nvl(a.cc_userids,'')='','UNKNOWN',a.cc_userids),'[\\\\[\\\\]\']',''),',')) b as user_id
where d='${pre1_date}' 
) t1
left join ${ods_dbname}.ods_qkt_dtk_user_info_df t2 on t1.user_id=t2.user_id and t2.d='${pre1_date}'
) rt
group by 
org_name,
process_instance_id
),
-- 宝仓的钉钉id反转到钉钉
tmp_dtk_emp_job_number_mapping as (
select 
a.org_company_name,
a.emp_id,
b.emp_id as bc_emp_id,
a.emp_name
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
-- 宝仓
tmp_dtk_overtime_form_str2 as (
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
${ods_dbname}.ods_qkt_dtk_process_overtime_form_di a
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
insert overwrite table ${dwd_dbname}.dwd_dtk_process_work_overtime_info_df partition(d='${pre1_date}')

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
  approval_result,
  approval_status,
  approval_title,
  regexp_replace(work_overtime_reason,'\\\\s+','') as work_overtime_reason,
  is_legal_holiday,
  work_overtime_accounting_method,
  if(nvl(overtime_person,'')='',split(approval_title,'提交|\'s')[0],overtime_person) as overtime_person,
  overtime_start_time,
  overtime_end_time,
  overtime_duration,
  overtime_detail,
  start_date,
  end_date,
  start_time_period,
  end_time_period,
  if(biz_action='REVOKE','0','1') as is_valid
from 
(
select 
*,
 row_number() over(partition by org_name, business_id  order by process_end_time desc) as rn
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
  a.approval_result,
  a.approval_status,
  a.approval_title,
  a.work_overtime_reason,
  a.is_legal_holiday,
  a.work_overtime_accounting_method,
  a.overtime_person,
  a.overtime_start_time,
  a.overtime_end_time,
  a.overtime_duration,
  a.overtime_detail,
  a.start_date,
  a.end_date,
  a.start_time_period,
  a.end_time_period,
  a.is_valid
from
${dwd_dbname}.dwd_dtk_process_work_overtime_info_df a
left join tmp_dtk_process_work_overtime_info_df_str1 b on a.org_name=b.org_name and a.process_instance_id=b.process_instance_id
where a.d='${pre2_date}' and b.process_instance_id is null and a.org_name='上海快仓智能科技有限公司'


union all 
select 
  a.org_name,
  a.process_instance_id,
  a.attached_process_instance_ids,
  a.biz_action,
  a.business_id,
  b.approval_user_ids, 
  b.approval_user_names, 
  a.create_time as process_start_time, 
  a.finish_time as process_end_time,
  a.originator_dept_id as applicant_dept_id, 
  a.originator_dept_name as applicant_dept_name, 
  a.originator_userid as applicant_userid,
  a.result as approval_result,
  a.status as approval_status,
  a.title as approval_title,
  a.work_overtime_reason,
  case when legal_holiday='是' then '1'
  	   when legal_holiday ='否' then '0'
  	else '-1' end as is_legal_holiday,
  a.work_overtime_accounting_method,
  a.overtime_person,
  a.overtime_start_time,
  a.overtime_end_time,
  a.duration as overtime_duration,
  a.overtime_detail,
  split(a.overtime_start_time,' ')[0] as start_date, 
  split(a.overtime_end_time,' ')[0] as end_date,
 case upper(nvl(split(a.overtime_start_time,' ')[1],'')) when '上午' then '上午'
                when 'AM' then '上午'
                when '下午' then '下午'
                when 'PM' then '下午'
                when '' then '上午'
                else null end as start_time_period,
 case upper(nvl(split(a.overtime_end_time,' ')[1],'')) when '下午' then '下午'
                when 'PM' then '下午'
                when '上午' then '上午'
                when 'AM' then '上午'
                when '' then '下午'
                else null end as end_time_period,
  '1' as is_valid
from 
${ods_dbname}.ods_qkt_dtk_process_work_overtime_di a
left join tmp_dtk_process_work_overtime_info_df_str1 b on a.org_name=b.org_name and a.process_instance_id=b.process_instance_id
where a.d='${pre1_date}'
) t
) rt
where rn=1 and business_id is not null



union all
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
  approval_result,
  approval_status,
  approval_title,
  regexp_replace(work_overtime_reason,'\\\\s+','') as work_overtime_reason,
  is_legal_holiday,
  work_overtime_accounting_method,
  if(nvl(overtime_person,'')='',split(approval_title,'提交|\'s')[0],overtime_person) as overtime_person,
  overtime_start_time,
  overtime_end_time,
  overtime_duration,
  overtime_detail,
  start_date,
  end_date,
  start_time_period,
  end_time_period,
  if(biz_action='REVOKE','0','1') as is_valid
from 
(
select 
*,
 row_number() over(partition by org_name, business_id  order by process_end_time desc) as rn

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
  a.approval_result,
  a.approval_status,
  a.approval_title,
  a.work_overtime_reason,
  a.is_legal_holiday,
  a.work_overtime_accounting_method,
  a.overtime_person,
  a.overtime_start_time,
  a.overtime_end_time,
  a.overtime_duration,
  a.overtime_detail,
  a.start_date,
  a.end_date,
  a.start_time_period,
  a.end_time_period,
  a.is_valid
from
${dwd_dbname}.dwd_dtk_process_work_overtime_info_df a
left join tmp_dtk_overtime_form_str2 b on a.org_name=b.org_name and a.process_instance_id=b.process_instance_id
where a.d='${pre2_date}' and b.process_instance_id is null and a.org_name='宝仓'

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
a.result as approval_result,
a.status as approval_status,
a.title as approval_title,
a.work_overtime_reason,
case when a.legal_holiday='是' then '1'
     when a.legal_holiday ='否' then '0'
     else '-1' end as is_legal_holiday,
a.accounting_method as work_overtime_accounting_method,
if(nvl(a.overtime_person,'')<>'',a.overtime_person,split(a.title,'提交|\'s')[0]) as overtime_person,
a.overtime_start_time,
a.overtime_end_time,
a.duration as overtime_duration,
a.overtime_detail,
substr(a.overtime_start_time,1,10) as start_date,
substr(a.overtime_end_time,1,10) as end_date,
substr(a.overtime_start_time,11) as start_time_period,
substr(a.overtime_end_time,11) as end_time_period,
-- if(a.biz_action='REVOKE','0','1') as is_valid
'1' as is_valid
from 
${ods_dbname}.ods_qkt_dtk_process_overtime_form_di a 
left join tmp_dtk_overtime_form_str2 b on a.org_name=b.org_name and a.process_instance_id=b.process_instance_id
left join tmp_dtk_emp_job_number_mapping c on a.originator_userid=c.bc_emp_id
left join tmp_dtk_emp_job_number_mapping d on a.originator_userid=d.emp_id
where a.d='${pre1_date}'
) t
) rt 
where rt.rn=1 and business_id is not null
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"
