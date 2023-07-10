#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉员工信息表
#-- 注意 ： 每天T-1全量分区
#-- 输入表 : ods.ods_qkt_dtk_user_info_df、dim.dim_dtk_org_info、dim.dim_dtk_org_level_info、ods.ods_qkt_dtk_user_roster_df
#-- 输出表 ：dim.dwd_dtk_emp_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-17 CREATE 
#-- 2 wangziming 2021-12-30 modify 新增org_company_name 字段
#-- 3 wangziming 2022-03-16 modify 追加相应的离职人员到员工表（若员工二次入职保留入职信息，二次离职保留最新的一次离职信息）
#-- 4 wangziming 2022-05-13 modify 增加员工视图，并回流数据到mysql
#-- 5 wangziming 2022-05-14 modify 修改入职日期的字符清洗逻辑
#-- 6 wangziming 2022-05-27 modify 修补emp_id 不同，但是同一个人的数据
#-- 7 wangziming 2022-06-13 modify 增加员工花名册信息字段，修正逻辑
#-- 8 wangziming 2022-06-21 modify 增加员工dept_id,dept_name以及主dept_id,主dept_name
#-- 9 wangziming 2022-08-16 modify 增加离职人员回流数据到mysql
#-- 10 wangyingying 2022-09-27 modify 修改员工视图username字段
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

add jar /data/hive/jar/hie-udf-1.0-SNAPSHOT.jar;
create temporary function udf_concat_str as 'com.quicktron.controll.ConcatStrUDF';

with tmp_dtk_emp_roster_str1 as  (
select 
split(report_manager,'##')[1] as superior_leader,
split(employee_type,'##')[0] as emp_type,
split(contract_period_type,'##')[0] as contract_period_type,
split(contract_period_type,'##')[1] as contract_period,
split(contract_renew_count,'##')[1] as contract_renewl_count,
split(project_role,'##')[0] as emp_function_role,
split(contract_type,'##')[0] as contract_type,
split(main_dept_id,'##')[1] as principal_org_id,
split(main_dept,'##')[1] as principal_org_name,
b.org_path_id as principal_org_path_id,
b.org_path_name as principal_org_path_name,
b.org_id_2 as principal_dept_id,
b.org_name_2 as principal_dept_name,
split(first_contract_start_time,'##')[1] as first_contract_start_date,
split(first_contract_end_time,'##')[1] as first_contract_end_date,
split(now_contract_start_time,'##')[1] as current_contract_start_date,
split(now_contract_end_time,'##')[1] as current_contract_end_date,
split(cost_center,'##')[1] as cost_center_org,
split(regular_time,'##')[1] as regular_date,
split(sex_type,'##')[0] as gender,
split(split(entry_age,'##')[1],'年|月')[0]*12 + split(split(entry_age,'##')[1],'年|月')[1] as company_working_years,
split(probation_period_type,'##')[1] as probation_type,
split(contract_company_name,'##')[1] as contract_company_name,
split(plan_regular_time,'##')[1] as plan_regular_date,
split(employee_status,'##')[0] as emp_status,
a.org_name,
a.user_id as emp_id
from 
${ods_dbname}.ods_qkt_dtk_user_roster_df a
left join ${dim_dbname}.dim_dtk_org_level_info b on split(a.main_dept_id,'##')[1]=b.org_id
where a.d='${pre1_date}'
)
insert overwrite table ${dwd_dbname}.dwd_dtk_emp_info_df partition(d='${pre1_date}')
select
union_id,
open_id, 
remark,
t1.user_id as emp_id, 
if(is_boss='False','0','1') as is_boss, 
to_date(hired_date) as hired_date, 
tel as tel_number, 
department as org_ids, 
t2.org_cnames,
t2.org_path_id,
t2.org_path_name,
work_place, 
email,
order_code, 
if(is_leader='False','0','1') as is_leader, 
mobile as mobile_number, 
active as is_active, 
if(is_admin='False','0','1') as is_admin, 
avatar as avatar_url, 
if(is_hide='False','0','1') as is_hide, 
job_number, 
name as emp_name, 
extattr, 
state_code, 
\`position\` as emp_position,
t1.org_name as org_company_name,
1 as is_job,
null as quit_date,
superior_leader,
emp_type,
contract_period_type,
contract_period,
contract_renewl_count,
emp_function_role,
contract_type,
principal_org_id,
principal_org_name,
principal_org_path_id,
principal_org_path_name,
first_contract_start_date,
first_contract_end_date,
current_contract_start_date,
current_contract_end_date,
cost_center_org,
regular_date,
gender,
company_working_years,
probation_type,
contract_company_name,
plan_regular_date,
emp_status,
dept_id,
dept_name,
principal_dept_id,
principal_dept_name
from 
${ods_dbname}.ods_qkt_dtk_user_info_df t1 
left join 
(
select 
p1.user_id,
p1.org_company_name,
concat_ws(',',collect_list(p1.org_id)) as org_ids,
concat_ws(',',collect_list(p2.org_name)) as org_cnames,
concat_ws(',',collect_list(regexp_replace(udf_concat_str('/','id',p3.org_id_1,p3.org_id_2,p3.org_id_3,p3.org_id_4,p3.org_id_5,p3.org_id_6,p3.org_id_7,p3.org_id_8,p3.org_id_9,p3.org_id_10),'/id',''))) as org_path_id,
concat_ws(',',collect_list(regexp_replace(udf_concat_str('/','name',p3.org_name_1,p3.org_name_2,p3.org_name_3,p3.org_name_4,p3.org_name_5,p3.org_name_6,p3.org_name_7,p3.org_name_8,p3.org_name_9,p3.org_name_10),'/name',''))) as org_path_name,
concat_ws(',',collect_list(p3.org_id_2)) as dept_id,
concat_ws(',',collect_list(p3.org_name_2)) as dept_name
from 
(
select 
a.user_id,
a.org_name as org_company_name,
b.org_id
from 
${ods_dbname}.ods_qkt_dtk_user_info_df a
lateral view explode(split(a.department,',')) b as org_id
where a.d='${pre1_date}'
) p1
left join ${dim_dbname}.dim_dtk_org_info p2 on p1.org_id=p2.org_id and p1.org_company_name=p2.org_company_name
left join ${dim_dbname}.dim_dtk_org_level_info p3 on p1.org_id=p3.org_id and p1.org_company_name=p3.org_company_name
group by p1.user_id,p1.org_company_name
) t2 on t1.user_id=t2.user_id and t1.org_name=t2.org_company_name
left join tmp_dtk_emp_roster_str1 t3 on t1.user_id=t3.emp_id and t1.org_name=t3.org_name
where t1.d='${pre1_date}'
;

-- 插入临时表
with tmp_dingtalk_user_str1 as (
select 
a.user_id,
a.org_name as org_company_name,
'${pre1_date}' as quit_date
from 
${ods_dbname}.ods_qkt_dtk_user_info_df a
left join ${ods_dbname}.ods_qkt_dtk_user_info_df b on a.user_id =b.user_id and b.d='${pre1_date}'
where a.d='${pre2_date}' and b.user_id is null
),
tmp_dingtalk_user_str2 as (
select 
b.union_id,
b.open_id,
b.remark,
b.emp_id,
b.is_boss,
b.hired_date,
b.tel_number,
b.org_ids,
b.org_cnames,
b.org_path_id,
b.prg_path_name,
b.work_place,
b.email,
b.order_code,
b.is_leader,
b.mobile_number,
b.is_active,
b.is_admin,
b.avatar_url,
b.is_hide,
b.job_number,
b.emp_name,
b.extattr,
b.state_code,
b.emp_position,
b.org_company_name,
0 as is_job,
a.quit_date,
superior_leader,
emp_type,
contract_period_type,
contract_period,
contract_renewl_count,
emp_function_role,
contract_type,
principal_org_id,
principal_org_name,
principal_org_path_id,
principal_org_path_name,
first_contract_start_date,
first_contract_end_date,
current_contract_start_date,
current_contract_end_date,
cost_center_org,
regular_date,
gender,
company_working_years,
probation_type,
contract_company_name,
plan_regular_date,
emp_status,
dept_id,
dept_name,
principal_dept_id,
principal_dept_name
from 
tmp_dingtalk_user_str1 a
left join ${dwd_dbname}.dwd_dtk_emp_info_df b on a.user_id=b.emp_id and a.org_company_name=b.org_company_name and b.d='${pre2_date}'
)
insert overwrite table ${tmp_dbname}.tmp_dtk_emp_info_df partition (d='${pre1_date}')
select 
union_id,
open_id,
remark,
emp_id,
is_boss,
hired_date,
tel_number,
org_ids,
org_cnames,
org_path_id,
prg_path_name,
work_place,
email,
order_code,
is_leader,
mobile_number,
is_active,
is_admin,
avatar_url,
is_hide,
job_number,
emp_name,
extattr,
state_code,
emp_position,
org_company_name,
is_job,
quit_date,
superior_leader,
emp_type,
contract_period_type,
contract_period,
contract_renewl_count,
emp_function_role,
contract_type,
principal_org_id,
principal_org_name,
principal_org_path_id,
principal_org_path_name,
first_contract_start_date,
first_contract_end_date,
current_contract_start_date,
current_contract_end_date,
cost_center_org,
regular_date,
gender,
company_working_years,
probation_type,
contract_company_name,
plan_regular_date,
emp_status,
dept_id,
dept_name,
principal_dept_id,
principal_dept_name
from 
tmp_dingtalk_user_str2

union all
select 
union_id,
open_id,
remark,
emp_id,
is_boss,
hired_date,
tel_number,
org_ids,
org_cnames,
org_path_id,
prg_path_name,
work_place,
email,
order_code,
is_leader,
mobile_number,
is_active,
is_admin,
avatar_url,
is_hide,
job_number,
emp_name,
extattr,
state_code,
emp_position,
org_company_name,
is_job,
quit_date,
superior_leader,
emp_type,
contract_period_type,
contract_period,
contract_renewl_count,
emp_function_role,
contract_type,
principal_org_id,
principal_org_name,
principal_org_path_id,
principal_org_path_name,
first_contract_start_date,
first_contract_end_date,
current_contract_start_date,
current_contract_end_date,
cost_center_org,
regular_date,
gender,
company_working_years,
probation_type,
contract_company_name,
plan_regular_date,
emp_status,
dept_id,
dept_name,
principal_dept_id,
principal_dept_name
from 
${tmp_dbname}.tmp_dtk_emp_info_df
where d='${pre2_date}'
;

-- 合并离职人员数据
insert overwrite table ${dwd_dbname}.dwd_dtk_emp_info_df partition(d='${pre1_date}')
select 
union_id,
open_id,
remark,
emp_id,
is_boss,
substr(hired_date,1,10) as  hired_date,
tel_number,
org_ids,
org_cnames,
org_path_id,
prg_path_name,
work_place,
email,
order_code,
is_leader,
mobile_number,
is_active,
is_admin,
avatar_url,
is_hide,
job_number,
emp_name,
extattr,
state_code,
emp_position,
org_company_name,
is_job,
quit_date,
superior_leader,
emp_type,
contract_period_type,
contract_period,
contract_renewl_count,
emp_function_role,
contract_type,
principal_org_id,
principal_org_name,
principal_org_path_id,
principal_org_path_name,
first_contract_start_date,
first_contract_end_date,
current_contract_start_date,
current_contract_end_date,
cost_center_org,
regular_date,
gender,
company_working_years,
probation_type,
contract_company_name,
plan_regular_date,
emp_status,
dept_id,
dept_name,
principal_dept_id,
principal_dept_name
from
(
select
*,row_number() over(partition by emp_id order by is_job desc,hired_date desc) as rn
from 
(
select 
union_id,
open_id,
remark,
emp_id,
is_boss,
hired_date,
tel_number,
org_ids,
org_cnames,
org_path_id,
prg_path_name,
work_place,
email,
order_code,
is_leader,
mobile_number,
is_active,
is_admin,
avatar_url,
is_hide,
job_number,
emp_name,
extattr,
state_code,
emp_position,
org_company_name,
is_job,
quit_date,
superior_leader,
emp_type,
contract_period_type,
contract_period,
contract_renewl_count,
emp_function_role,
contract_type,
principal_org_id,
principal_org_name,
principal_org_path_id,
principal_org_path_name,
first_contract_start_date,
first_contract_end_date,
current_contract_start_date,
current_contract_end_date,
cost_center_org,
regular_date,
gender,
company_working_years,
probation_type,
contract_company_name,
plan_regular_date,
emp_status,
dept_id,
dept_name,
principal_dept_id,
principal_dept_name
from 
${dwd_dbname}.dwd_dtk_emp_info_df
where d='${pre1_date}'

union all
select 
union_id,
open_id,
remark,
emp_id,
is_boss,
hired_date,
tel_number,
org_ids,
org_cnames,
org_path_id,
prg_path_name,
work_place,
email,
order_code,
is_leader,
mobile_number,
is_active,
is_admin,
avatar_url,
is_hide,
job_number,
emp_name,
extattr,
state_code,
emp_position,
org_company_name,
is_job,
quit_date,
superior_leader,
emp_type,
contract_period_type,
contract_period,
contract_renewl_count,
emp_function_role,
contract_type,
principal_org_id,
principal_org_name,
principal_org_path_id,
principal_org_path_name,
first_contract_start_date,
first_contract_end_date,
current_contract_start_date,
current_contract_end_date,
cost_center_org,
regular_date,
gender,
company_working_years,
probation_type,
contract_company_name,
plan_regular_date,
emp_status,
dept_id,
dept_name,
principal_dept_id,
principal_dept_name
from
${tmp_dbname}.tmp_dtk_emp_info_df
where d='${pre1_date}' and emp_id not in('01254853540036625928','01445962556723204472','301644355023192940','01442428235029528938')
) t
) rt 
where rt.rn=1
;



-- 员工视图
insert overwrite table tmp.tmp_dtk_emp_info
select 
split(username,'@')[0] as username,
-- username,
nickname,
concat_ws(',',collect_list(department_id)) as department_id,
concat_ws(',',collect_list(department_name)) as department_name,
is_leader,
is_job as delete_status,
1 as is_inner_user
from 
(
select 
email as username,
emp_name as nickname,
split(b.dept_ids,'/')[1] as department_id,
split(c.dept_names,'/')[1] as department_name,
is_leader,
is_job
from 
dwd.dwd_dtk_emp_info_df a
lateral view explode(split(a.org_path_id,',')) b as dept_ids
lateral view explode(split(a.prg_path_name,',')) c as dept_names
where a.d='${pre1_date}' and nvl(a.email,'')<>''
) t
group by split(username,'@')[0],nickname,is_leader,is_job
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


