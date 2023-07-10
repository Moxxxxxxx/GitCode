#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉员工考勤天信息记录
#-- 注意 ： 每天全量分区
#-- 输入表 : ods.ods_qkt_dtk_attendance_di,dwd.dwd_dtk_emp_info_df
#-- 输出表 ：dwd.dwd_dtk_emp_attendance_day_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-07-14 CREATE 
#-- 2 wangziming 2022-08-03 modify 修改考勤实际登记天，根据钉钉登记工作日为实际考勤日（重新初始化）
#-- 3 wangziming 2022-08-04 modify 修改考勤逻辑（凌晨七点之前都属于前一天的打卡记录，考勤日也属于前一天的分区内），故回流两天数据，并重新初始化数据
#-- 4 wangziming 2022-08-13 modify 跨天考勤逻辑存在重复数据，修改相应逻辑
#-- 5 wangziming 2022-10-11 modify 增加 宝仓公司打卡记录，并根据工号将宝仓人员的user_id替换成emp_id,并重新进行初始化
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
where d='${pre1_date}' and org_company_name ='宝仓'
) a
left join 
(
select 
*
from 
dim.dim_dtk_emp_job_number_mapping_info
where d='${pre1_date}' and org_company_name ='上海快仓智能科技有限公司'
) b on a.job_number=b.job_number
where nvl(a.job_number,'')<>''
),
tmp_dtk_emp_attendance_day_str0 as (
select 
gmt_modified, 
base_mac_addr, 
base_check_time, 
group_id,
time_result, 
work_date, 
biz_id, 
plan_id, 
id, 
check_type, 
plan_check_time, 
corp_id, 
location_result, 
is_legal, 
gmt_create, 
if(a.org_name='宝仓',b.emp_id,a.user_id) as user_id, 
device_sn, 
user_address, 
source_type, 
user_check_time, 
location_method, 
create_time, 
update_time, 
org_name
from 
${ods_dbname}.ods_qkt_dtk_attendance_di a
left join tmp_dtk_emp_job_number_mapping b on a.user_id=b.bc_emp_id
where a.d='${pre1_date}'  and (a.org_name='上海快仓智能科技有限公司' or (a.org_name='宝仓' and b.bc_emp_id is not null))
),
tmp_dtk_emp_attendance_day_str1 as (
select 
user_id,
attendance_work_date,
collect_list(user_check_time) as user_check_times,
collect_list(time_result) as time_results,
collect_list(user_address) as user_addresss,
collect_list(source_type) as source_types,
max(rn) as days_attendance_number
from 
(
select 
*
from
(
select 
user_id,
if(hour(user_check_time)<7 or (hour(user_check_time)=7 and minute(user_check_time)=0 and second(user_check_time)=0) ,date_sub(substr(user_check_time,1,10),1),substr(user_check_time,1,10)) as attendance_work_date,
user_check_time,
case time_result when 'Normal' then '正常'
				   when 'Early' then '早退'
				   when 'Late' then '迟到'
				   when 'SeriousLate' then '严重迟到'
				   when 'Absenteeism' then '旷工迟到'
				   when 'NotSigned' then '未打卡'
				else null end as time_result,
user_address,
case source_type when 'ATM' then '考勤机打卡（指纹/人脸打卡）'
				   when 'BEACON' then 'IBeacon'
				   when 'DING_ATM' then '钉钉考勤机（考勤机蓝牙打卡）'
				   when 'USER' then '用户打卡'
				   when 'BOSS' then '老板改签'
				   when 'APPROVE' then '审批系统'
				   when 'SYSTEM' then '考勤系统'
				   when 'AUTO_CHECK' then '自动打卡'
				else null end as source_type,
row_number() over(partition by user_id,if(hour(user_check_time)<7 or (hour(user_check_time)=7 and minute(user_check_time)=0 and second(user_check_time)=0) ,date_sub(substr(user_check_time,1,10),1),substr(user_check_time,1,10)) order by user_check_time asc) as rn,
row_number() over(partition by user_id,if(hour(user_check_time)<7 or (hour(user_check_time)=7 and minute(user_check_time)=0 and second(user_check_time)=0) ,date_sub(substr(user_check_time,1,10),1),substr(user_check_time,1,10)) order by user_check_time desc) as rn1
from 
tmp_dtk_emp_attendance_day_str0
-- ${ods_dbname}.ods_qkt_dtk_attendance_di
-- where d='${pre1_date}'
order by user_id,user_check_time asc
) t
where rn=1 or rn1=1
) t1
group by user_id,attendance_work_date
),
tmp_dtk_emp_attendance_day_str2 as (
select 
*
from 
${dwd_dbname}.dwd_dtk_emp_attendance_day_info_di
where d in (select distinct if(hour(user_check_time)<7 or (hour(user_check_time)=7 and minute(user_check_time)=0 and second(user_check_time)=0) ,date_sub(substr(user_check_time,1,10),1),substr(user_check_time,1,10)) from tmp_dtk_emp_attendance_day_str0
--${ods_dbname}.ods_qkt_dtk_attendance_di where d='${pre1_date}' 
)
and d<>'${pre1_date}'
),
tmp_dtk_emp_attendance_day_str3 as (
select 
*
from 
tmp_dtk_emp_attendance_day_str1
where attendance_work_date<>substr(user_check_times[0],1,10) or attendance_work_date<'${pre1_date}'
)
insert overwrite table ${dwd_dbname}.dwd_dtk_emp_attendance_day_info_di partition(d)
select 
emp_id,
emp_name,
attendance_work_date,
attendance_working_time,
attendance_working_result,
attendance_working_address,
attendance_working_source,
attendance_off_time,
attendance_off_result,
attendance_off_address,
attendance_off_source,
days_attendance_number,
d
from 
(
select 
*,
row_number() over(partition by emp_id,attendance_work_date,attendance_working_time,attendance_off_time order by days_attendance_number desc) as rn
from 
(
select 
a.user_id as emp_id,
b.emp_name,
a.attendance_work_date,
a.user_check_times[0] as attendance_working_time,
a.time_results[0] as attendance_working_result,
a.user_addresss[0] as attendance_working_address,
a.source_types[0] as attendance_working_source,
a.user_check_times[1] as attendance_off_time,
a.time_results[1] as attendance_off_result,
a.user_addresss[1] as attendance_off_address,
a.source_types[1] as attendance_off_source,
a.days_attendance_number,
a.attendance_work_date as d
from 
tmp_dtk_emp_attendance_day_str1 a 
left join ${dwd_dbname}.dwd_dtk_emp_info_df b on a.user_id=b.emp_id and b.d='${pre1_date}'
where a.attendance_work_date=substr(a.user_check_times[0],1,10) and a.attendance_work_date>='${pre1_date}'

union all
select 
a.emp_id,
a.emp_name,
a.attendance_work_date,
--a.attendance_working_time,
least(a.attendance_working_time,nvl(b.user_check_times[0],'9999-99-99'),nvl(b.user_check_times[1],'9999-99-99')) as attendance_working_time,
a.attendance_working_result,
a.attendance_working_address,
a.attendance_working_source,
if(greatest(nvl(a.attendance_off_time,'1111-11-11'),nvl(b.user_check_times[1],'1111-11-11'),nvl(b.user_check_times[0],'1111-11-11'))='1111-11-11',null,greatest(nvl(a.attendance_off_time,'1111-11-11'),nvl(b.user_check_times[1],'1111-11-11'),nvl(b.user_check_times[0],'1111-11-11'))) as attendance_off_time,
a.attendance_off_result,
a.attendance_off_address,
a.attendance_off_source,
a.days_attendance_number,
a.d
from 
tmp_dtk_emp_attendance_day_str2 a
left join tmp_dtk_emp_attendance_day_str3 b on a.emp_id=b.user_id and a.attendance_work_date=b.attendance_work_date

union all
select 
a.user_id as emp_id,
c.emp_name,
a.attendance_work_date,
a.user_check_times[0] as attendance_working_time,
a.time_results[0] as attendance_working_result,
a.user_addresss[0] as attendance_working_address,
a.source_types[0] as attendance_working_source,
a.user_check_times[1] as attendance_off_time,
a.time_results[1] as attendance_off_result,
a.user_addresss[1] as attendance_off_address,
a.source_types[1] as attendance_off_source,
a.days_attendance_number,
a.attendance_work_date as d
from 
tmp_dtk_emp_attendance_day_str3 a
left join tmp_dtk_emp_attendance_day_str2 b on a.user_id=b.emp_id and a.attendance_work_date=b.attendance_work_date
left join ${dwd_dbname}.dwd_dtk_emp_info_df c on a.user_id=c.emp_id and c.d='${pre1_date}'
where b.emp_id is null
) t
) rt
where rt.rn=1
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

