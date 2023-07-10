#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉群签到信息表
#-- 注意 ： 每天全量分区
#-- 输入表 : ods.ods_qkt_dtk_attendance_di,dwd.dwd_dtk_emp_info_df
#-- 输出表 ：dwd.dwd_dtk_attendance_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-07-14 CREATE 
#-- 2 wangziming 2022-08-03 modify 修改考勤实际登记天，根据钉钉登记工作日为实际考勤日（重新初始化）
#-- 3 wangziming 2022-08-04 modify 修改考勤逻辑（凌晨七点之前都属于前一天的打卡记录，考勤日也属于前一天的分区内），故回流两天数据，并重新初始化数据
#-- 4 wangziming 2022-08-13 modify 重新初始化
#-- 5 wangziming 2022-10-11 modify 重新初始化
#-- 6 wangziming 2022-11-16 modify 根据工号将宝仓人员的user_id替换成emp_id
#-- 7 wangziming 2023-01-11 modify 根据 user_id 和 实际打卡时间进行去重
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
tmp_dtk_attendance_str1 as (
select 
*
from 
${dwd_dbname}.dwd_dtk_attendance_info_di
where d in (select distinct if(hour(user_check_time)<7 or (hour(user_check_time)=7 and minute(user_check_time)=0 and second(user_check_time)=0) ,date_sub(substr(user_check_time,1,10),1),substr(user_check_time,1,10))
from ${ods_dbname}.ods_qkt_dtk_attendance_di where d='${pre1_date}')
and d<>'${pre1_date}'
)
insert overwrite table ${dwd_dbname}.dwd_dtk_attendance_info_di partition(d)
select 
id,
emp_id, 
emp_name,
gmt_create_time,
gmt_modified_time,
base_mac_address, 
base_check_time,
group_id,
attendance_result,
attendance_work_date, 
attendance_type,
plan_check_time,
attendance_location_result,
is_legal,
attendance_device_sn,
attendance_actual_time,
attendance_address,	
attendance_source,
attendance_location_method,
d
from 
(
select 
*,
row_number() over(partition by emp_id,attendance_actual_time order by gmt_modified_time desc) as rn
from 
(
select 
id,
t.emp_id, 
b.emp_name,
gmt_create_time,
gmt_modified_time,
base_mac_address, 
base_check_time,
group_id,
attendance_result,
attendance_work_date, 
attendance_type,
plan_check_time,
attendance_location_result,
is_legal,
attendance_device_sn,
attendance_actual_time,
attendance_address,	
attendance_source,
attendance_location_method,
t.d
from 
(
select 
cast(a.id as bigint) as id,
if(a.org_name='宝仓',c.emp_id,a.user_id) as emp_id, 
a.gmt_create as gmt_create_time,
a.gmt_modified as gmt_modified_time,
a.base_mac_addr as base_mac_address, 
a.base_check_time,
a.group_id,
case a.time_result when 'Normal' then '正常'
				   when 'Early' then '早退'
				   when 'Late' then '迟到'
				   when 'SeriousLate' then '严重迟到'
				   when 'Absenteeism' then '旷工迟到'
				   when 'NotSigned' then '未打卡'
				else null end as attendance_result,
if(hour(a.user_check_time)<7 or (hour(a.user_check_time)=7 and minute(a.user_check_time)=0 and second(a.user_check_time)=0) ,date_sub(substr(a.user_check_time,1,10),1),substr(a.user_check_time,1,10)) as attendance_work_date, 
if(a.check_type='OnDuty','上班','下班') as attendance_type,
a.plan_check_time,
case a.location_result when 'Normal' then '范围内'
					   when 'Outside' then '范围外'
					   when 'NotSigned' then '未打卡'
					   else null end  as attendance_location_result,
if(a.is_legal='Y','1','0') as is_legal,
a.device_sn as attendance_device_sn,
a.user_check_time as attendance_actual_time,
a.user_address as attendance_address,	
case a.source_type when 'ATM' then '考勤机打卡（指纹/人脸打卡）'
				   when 'BEACON' then 'IBeacon'
				   when 'DING_ATM' then '钉钉考勤机（考勤机蓝牙打卡）'
				   when 'USER' then '用户打卡'
				   when 'BOSS' then '老板改签'
				   when 'APPROVE' then '审批系统'
				   when 'SYSTEM' then '考勤系统'
				   when 'AUTO_CHECK' then '自动打卡'
				else null end as attendance_source,
a.location_method as attendance_location_method,
if(hour(a.user_check_time)<7 or (hour(a.user_check_time)=7 and minute(a.user_check_time)=0 and second(a.user_check_time)=0) ,date_sub(substr(a.user_check_time,1,10),1),substr(a.user_check_time,1,10)) as d
from 
${ods_dbname}.ods_qkt_dtk_attendance_di a

left join tmp_dtk_emp_job_number_mapping c on a.user_id=c.bc_emp_id
where a.d='${pre1_date}'  and (a.org_name='上海快仓智能科技有限公司' or (a.org_name='宝仓' and c.bc_emp_id is not null))
) t
left join ${dwd_dbname}.dwd_dtk_emp_info_df b on t.emp_id=b.emp_id and b.d='${pre1_date}'

union all 
select 
id,
emp_id,
emp_name,
gmt_create_time,
gmt_modified_time,
base_mac_address, 
base_check_time,
group_id,
attendance_result,
attendance_work_date, 
attendance_type,
plan_check_time,
attendance_location_result,
is_legal,
attendance_device_sn,
attendance_actual_time,
attendance_address,	
attendance_source,
attendance_location_method,
d
from 
tmp_dtk_attendance_str1
) pt
) p
where p.rn=1
;
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

