#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉员工历史组织信息记录表
#-- 注意 ： 每天存最新数据到昨天的分区内
#-- 输入表 : dim.dim_dtk_emp_org_mapping_info、ods.ods_qkt_dtk_user_info_df、dim.dim_dtk_org_history_info_df
#-- 输出表 ：dwd.dwd_dtk_emp_org_history_mapping_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-05-19 CREATE 
#-- 2 wangziming 2022-06-14 modify 增加 小组id和name 组织字段map集合，并重刷历史数据
#-- 3 wangziming 2022-07-29 modify 修改变更或未变更的最新邮箱取值
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


with tmp_org_dtk_emp_str2 as (
select 
a.*,
b.parent_org_id,
b.parent_org_name,
b.org_path_id,
b.org_path_name,
'1' as is_job,
'1900-01-01' as org_start_date,
'9999-01-01' as org_end_date	
from 
${dim_dbname}.dim_dtk_emp_org_mapping_info a
left join ${dim_dbname}.dim_dtk_org_history_info_df b on a.org_id=b.org_id and b.d='${pre1_date}'
)
insert overwrite table ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df partition(d='${pre1_date}')
select 
emp_id,
emp_name,
email as  emp_email,
org_id,
org_name,
parent_org_id,
parent_org_name,
dept_id,
dept_name,
org_path_id,
org_path_name,
nvl(a.org_company_name,'上海快仓智能科技有限公司') as org_company_name,
org_role_type,
is_valid,
is_need_fill_manhour,
is_job,
if(nvl(b.hire_date,'')='','2021-12-01',b.hire_date) as org_start_date,
org_end_date
from 
tmp_org_dtk_emp_str2 a 
left join 
(
select user_id,
substr(hired_date,1,10) as hire_date,
nvl(org_name,'上海快仓智能科技有限公司') as org_company_name  
from ${ods_dbname}.ods_qkt_dtk_user_info_df where d='${pre1_date}') b on a.emp_id=b.user_id 
and if(nvl(a.org_company_name,'')='','上海快仓智能科技有限公司',a.org_company_name)=b.org_company_name
;
"


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;



-- 员工历史组织表
with tmp_dtk_emp_history_str0 as (  --前日的员工历史组织(只有组织未变动)
select 
emp_id, 
emp_name, 
emp_email, 
org_id, 
org_name, 
parent_org_id, 
parent_org_name, 
dept_id, 
dept_name, 
org_path_id, 
org_path_name, 
org_company_name, 
org_role_type, 
is_valid, 
is_need_fill_manhour, 
is_job, 
org_start_date, 
org_end_date,
team_org_id_map,
team_org_name_map
from 
${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df 
where d='${pre2_date}' and org_end_date='9999-01-01'
),
tmp_dtk_emp_history_str1 as( -- 前日的员工历史组织（已经结束的组织区间）
select 
emp_id, 
emp_name, 
emp_email, 
org_id, 
org_name, 
parent_org_id, 
parent_org_name, 
dept_id, 
dept_name, 
org_path_id, 
org_path_name, 
org_company_name, 
org_role_type, 
is_valid, 
is_need_fill_manhour, 
is_job, 
org_start_date, 
org_end_date,
team_org_id_map,
team_org_name_map,
'm4' as flag
from 
${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df 
where d='${pre2_date}' and org_end_date<>'9999-01-01'
), 
tmp_quit_emp_str1 as ( -- 昨日离职人员
select 
a.user_id as emp_id,
a.email as emp_email,
a.name as emp_name,
if(nvl(a.org_name,'')='','上海快仓智能科技有限公司',a.org_name) as org_company_name,
'0' as is_job,
'${pre1_date}' as quit_date
from ${ods_dbname}.ods_qkt_dtk_user_info_df a
left join ${ods_dbname}.ods_qkt_dtk_user_info_df b on a.user_id=b.user_id 
and b.d='${pre1_date}'
where a.d='${pre2_date}' and b.user_id is null and if(nvl(a.org_name,'')='','上海快仓智能科技有限公司', a.org_name)='上海快仓智能科技有限公司'
),
tmp_org_dtk_emp_str2 as ( --获取昨日的员工组织
select 
a.*,
b.parent_org_id,
b.parent_org_name,
b.org_path_id,
b.org_path_name,
'1' as is_job
from 
${dim_dbname}.dim_dtk_emp_org_mapping_info a
left join ${dim_dbname}.dim_dtk_org_history_info_df b on a.org_id=b.org_id and b.d='${pre1_date}'
where if(nvl(a.org_company_name,'')='','上海快仓智能科技有限公司', a.org_company_name)='上海快仓智能科技有限公司'
),
tmp_hire_emp_str3 as ( -- 获取昨日入职的员工组织
select 
c.*,
'${pre1_date}' as org_start_date,
'9999-01-01' as org_end_date,
'm1' as flag
from ${ods_dbname}.ods_qkt_dtk_user_info_df a
left join ${ods_dbname}.ods_qkt_dtk_user_info_df b on a.user_id=b.user_id 
and b.d='${pre2_date}'
left join tmp_org_dtk_emp_str2 c on a.user_id=c.emp_id
where a.d='${pre1_date}' and b.user_id is null and if(nvl(a.org_name,'')='','上海快仓智能科技有限公司', a.org_name)='上海快仓智能科技有限公司'
),
tmp_quit_emp_org_str3 as (  -- 获取昨日离职员工的数据
select 
a.emp_id, 
a.emp_name, 
a.emp_email, 
a.org_id, 
a.org_name, 
a.parent_org_id, 
a.parent_org_name, 
a.dept_id, 
a.dept_name, 
a.org_path_id, 
a.org_path_name, 
a.org_company_name, 
a.org_role_type, 
a.is_valid, 
a.is_need_fill_manhour, 
b.is_job, 
a.org_start_date, 
if(a.org_end_date<>'9999-01-01',a.org_end_date,b.quit_date) as org_end_date,
a.team_org_id_map,
a.team_org_name_map,
'm2' as flag
from 
tmp_dtk_emp_history_str0 a
left join tmp_quit_emp_str1 b on a.emp_id=b.emp_id and a.org_company_name=b.org_company_name
where b.emp_id is not null
),
tmp_change_emp_org_str4 as ( --获取变更或者未变更人员   
select 
t1.emp_id, 
t1.emp_name, 
if(nvl(t2.email,'')<>'',t2.email,t1.emp_email) as emp_email,  --变更
t1.org_id, 
t1.org_name, 
t1.parent_org_id, 
t1.parent_org_name, 
t1.dept_id, 
t1.dept_name, 
t1.org_path_id, 
t1.org_path_name, 
t1.org_company_name, 
if(t2.emp_id is not null,t2.org_role_type,t1.org_role_type)  as org_role_type, 
if(t2.emp_id is not null,t2.is_valid,t1.is_valid) as is_valid, 
if(t2.emp_id is not null,t2.is_need_fill_manhour,t1.is_need_fill_manhour) as is_need_fill_manhour, 
if(t2.emp_id is not null,t2.is_job,t1.is_job) as is_job, 
t1.org_start_date, 
if(t2.emp_id is not null,t1.org_end_date,'${pre1_date}') as org_end_date,
t1.team_org_id_map,
t1.team_org_name_map,
'm3' as flag
from 
(
select 
a.*
from 
tmp_dtk_emp_history_str0 a
left join tmp_quit_emp_str1 b on a.emp_id=b.emp_id and a.org_company_name=b.org_company_name
where b.emp_id is null
) t1
left join tmp_org_dtk_emp_str2 t2 on t1.emp_id=t2.emp_id 
and t1.org_path_id=t2.org_path_id
and t1.org_path_name=t2.org_path_name
-- where t2.org_path_id is not null


union all
select
t1.emp_id, 
t1.emp_name, 
t1.email as emp_email,   --变更
t1.org_id, 
t1.org_name, 
t1.parent_org_id, 
t1.parent_org_name, 
t1.dept_id, 
t1.dept_name, 
t1.org_path_id, 
t1.org_path_name, 
t1.org_company_name, 
t1.org_role_type, 
string(t1.is_valid) as is_valid, 
t1.is_need_fill_manhour, 
t1.is_job, 
'${pre1_date}' as org_start_date, 
'9999-01-01' as org_end_date,
t1.team_org_id_map,
t1.team_org_name_map,
'm3' as flag
from 
tmp_org_dtk_emp_str2 t1
left join 
(
select 
a.*
from 
tmp_dtk_emp_history_str0 a
left join tmp_quit_emp_str1 b on a.emp_id=b.emp_id
where b.emp_id is null
) t2 on t1.emp_id=t2.emp_id and t1.org_path_id=t2.org_path_id and t1.org_path_name=t2.org_path_name
left join tmp_hire_emp_str3 t3 on t1.emp_id=t3.emp_id
where t3.emp_id is null and t2.emp_id is null
)
insert overwrite table ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df partition(d='${pre1_date}')
select 
a.emp_id, 
a.emp_name, 
a.emp_email, 
a.org_id, 
a.org_name, 
a.parent_org_id, 
a.parent_org_name, 
a.dept_id, 
a.dept_name, 
a.org_path_id, 
a.org_path_name, 
a.org_company_name, 
a.org_role_type, 
a.is_valid, 
a.is_need_fill_manhour, 
a.is_job, 
a.org_start_date, 
a.org_end_date,
-- case when b1.emp_id is not null then b1.team_org_id_map
-- 	 when b2.emp_id is not null then b2.team_org_id_map
--	 when b3.emp_id is not null then b3.team_org_id_map
-- 	 when b4.emp_id is not null then b4.team_org_id_map
-- 	else null end team_org_id_map,
-- case when b1.emp_id is not null then b1.team_org_name_map
-- 	 when b2.emp_id is not null then b2.team_org_name_map
-- 	 when b3.emp_id is not null then b3.team_org_name_map
-- 	 when b4.emp_id is not null then b4.team_org_name_map
-- 	else null end team_org_name_map
coalesce(b1.team_org_id_map,b2.team_org_id_map,b3.team_org_id_map,b4.team_org_id_map) as team_org_id_map,
coalesce(b1.team_org_name_map,b2.team_org_name_map,b3.team_org_name_map,b4.team_org_name_map) as team_org_name_map
from 
(
select 
emp_id, 
emp_name, 
email as emp_email, 
org_id, 
org_name, 
parent_org_id, 
parent_org_name, 
dept_id, 
dept_name, 
org_path_id, 
org_path_name, 
org_company_name, 
org_role_type, 
string(is_valid) as is_valid, 
is_need_fill_manhour, 
is_job, 
org_start_date, 
org_end_date,
null,
null,
'm1' as flag
from 
tmp_hire_emp_str3

union all
select
emp_id, 
emp_name, 
emp_email, 
org_id, 
org_name, 
parent_org_id, 
parent_org_name, 
dept_id, 
dept_name, 
org_path_id, 
org_path_name, 
org_company_name, 
org_role_type, 
string(is_valid) as is_valid, 
is_need_fill_manhour, 
is_job, 
org_start_date, 
org_end_date,
null,
null,
'm2' as flag
from 
tmp_quit_emp_org_str3

union all
select 
emp_id, 
emp_name, 
emp_email, 
org_id, 
org_name, 
parent_org_id, 
parent_org_name, 
dept_id, 
dept_name, 
org_path_id, 
org_path_name, 
org_company_name, 
org_role_type, 
string(is_valid) as is_valid, 
is_need_fill_manhour, 
is_job, 
org_start_date, 
org_end_date,
null,
null,
'm3' as flag
from 
tmp_change_emp_org_str4

union all
select 
emp_id, 
emp_name, 
emp_email, 
org_id, 
org_name, 
parent_org_id, 
parent_org_name, 
dept_id, 
dept_name, 
org_path_id, 
org_path_name, 
org_company_name, 
org_role_type, 
string(is_valid) as is_valid, 
is_need_fill_manhour, 
is_job, 
org_start_date, 
org_end_date,
null,
null,
'm4' as flag
from 
tmp_dtk_emp_history_str1
) a 
left join tmp_hire_emp_str3 b1 on a.emp_id=b1.emp_id and a.org_path_id=b1.org_path_id and a.org_start_date=b1.org_start_date and a.org_end_date=b1.org_end_date and a.flag=b1.flag
left join tmp_quit_emp_org_str3 b2 on a.emp_id=b2.emp_id and a.org_path_id=b2.org_path_id and a.org_start_date=b2.org_start_date and a.org_end_date=b2.org_end_date and a.flag=b2.flag
left join tmp_change_emp_org_str4 b3 on a.emp_id=b3.emp_id and a.org_path_id=b3.org_path_id and a.org_start_date=b3.org_start_date and a.org_end_date=b3.org_end_date and a.flag=b3.flag
left join tmp_dtk_emp_history_str1 b4 on a.emp_id=b4.emp_id and a.org_path_id=b4.org_path_id and a.org_start_date=b4.org_start_date and a.org_end_date=b4.org_end_date and a.flag=b4.flag
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

