#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： pms相关的pe日志数据
#-- 注意 ： 每日增量更新到昨日的分区内，每天的分区为最新的数据
#-- 输入表 : ods_qkt_pms_implementer_log_di、ods_qkt_pms_project_info_df、ods_qkt_pms_user_info_df、dwd_dtk_emp_info_df、dwd_dtk_process_pe_log_info_df、dim_bpm_ud_spm_mapping_info_ful、dwd_dtk_emp_org_history_mapping_info_df
#-- 输出表 ：dwd.dwd_pms_pe_log_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-10-17 CREATE 
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


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;




with tmp_dtk_pe_log_str as (
select 
business_id as id,
project_code,
project_name,
b.spm_name,
originator_user_id as applicant_user_id,
originator_user_name as applicant_user_name,
originator_2_dept_id as dept_id,
originator_2_dept_name as dept_name,
originator_dept_id as org_id,
originator_dept_name  as org_path_name,
originator_user_position as applicant_user_position,
project_manage,
work_status,
log_date,
working_hours,
job_content,
site_team_members,
'dtk' as pe_source
from 
${dwd_dbname}.dwd_dtk_process_pe_log_info_df a
left join ${dim_dbname}.dim_bpm_ud_spm_mapping_info_ful b on a.project_manage=b.pm_name
where d='${pre1_date}' and approval_result ='agree' and approval_status ='COMPLETED'
),
tmp_pms_user_str as (
select 
id,
lastname,
email,
jobtitlename
from 
${ods_dbname}.ods_qkt_pms_user_info_df
where d='${pre1_date}'
),
tmp_org_user_str as (
select 
id,
emp_id as applicant_user_id,
emp_name as applicant_user_name,
emp_position as applicant_user_position,
email,
org_id,
org_name,
dept_id,
dept_name,
regexp_replace(regexp_replace(org_path_name,'上海快仓智能科技有限公司/',''),'/','-') as org_path_name
from 
(
select 
*,
row_number() over(partition by id order by org_sort asc) as rn 
from 
(
select 
a.id,
a.xmmc,
if(c.emp_name is not null,c.emp_name,b.lastname) as emp_name,
if(c.emp_position is not null ,c.emp_position,b.jobtitlename) as emp_position,
c.emp_id,
b.email,
c.is_job,
a.rzrq,
e.org_id,
e.org_name,
e.dept_id,
e.dept_name,
e.org_path_name,
e.org_start_date,
e.org_end_date,
case when e.dept_id ='71737917' then 1
     when e.dept_id ='479976580' then 2
     when e.dept_id='414097036' then 3
     when e.dept_id='101018245' then 4
     else 5 end as org_sort
from 
${ods_dbname}.ods_qkt_pms_implementer_log_di a
left join tmp_pms_user_str b on a.sqr=b.id
left join ${dwd_dbname}.dwd_dtk_emp_info_df c on b.email=c.email and c.d='${pre1_date}'
left join ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df e on b.email=e.emp_email  and  e.d='${pre1_date}'
where upper(a.js) ='PE' and a.d='${pre1_date}' and e.org_start_date <=a.rzrq and e.org_end_date>=a.rzrq
) t
) rt 
where rt.rn=1
),
tmp_pms_pe_user_str as (
select 
t.id,
concat_ws(',',collect_list(c.lastname)) as site_team_members
from 
(
select 
a.id,
b.pes
from 
${ods_dbname}.ods_qkt_pms_implementer_log_di a
lateral view explode(split(if(nvl(a.pe,'')='','UNKNOWN',a.pe),',')) b as pes
where upper(a.js) ='PE' and a.d='${pre1_date}'
) t
left join tmp_pms_user_str c on t.pes=c.id
group by t.id
)
insert overwrite table ${dwd_dbname}.dwd_pms_pe_log_info_df partition(d='${pre1_date}')
select 
cast(t.id as string) as id,
upper(t1.xmbm) as project_code,
t1.xmmc as project_name,
t2.lastname as spm_name,
t4.applicant_user_id,
t4.applicant_user_name,
t4.dept_id,
t4.dept_name,
t4.org_id,
t4.org_path_name,
t4.applicant_user_position,
t3.lastname as project_manage,
t.cqzt as work_status,
t.rzrq as log_date,
t.gszj as working_hours,
null as job_content,
t5.site_team_members,
'pms' as pe_source
from ${ods_dbname}.ods_qkt_pms_implementer_log_di t
left join ${ods_dbname}.ods_qkt_pms_project_info_df t1 on t.xmbm =t1.id and t1.d='${pre1_date}'
left join tmp_pms_user_str t2 on t.spm=t2.id
left join tmp_pms_user_str t3 on t.xmjl=t3.id
left join tmp_org_user_str t4 on t.id=t4.id 
left join tmp_pms_pe_user_str t5 on t.id=t5.id
where t.d='${pre1_date}' and upper(t.js)='PE'

union all
select 
a.id,
project_code,
project_name,
spm_name,
applicant_user_id,
applicant_user_name,
dept_id,
dept_name,
org_id,
org_path_name,
applicant_user_position,
project_manager,
work_status,
log_date,
working_hours,
job_content,
site_team_members,
pe_source
from 
${dwd_dbname}.dwd_pms_pe_log_info_df a
where a.d='${pre2_date}' and a.pe_source='pms' and a.id not in (select distinct id from ${ods_dbname}.ods_qkt_pms_implementer_log_di where d='${pre1_date}')


union all
select 
id,
project_code,
project_name,
spm_name,
applicant_user_id,
applicant_user_name,
dept_id,
dept_name,
org_id,
org_path_name,
applicant_user_position,
project_manage,
work_status,
log_date,
working_hours,
job_content,
site_team_members,
pe_source
from 
tmp_dtk_pe_log_str
;

"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

