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


with tmp_dtk_emp_attendance_day_str1 as (
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
substr(work_date,1,10) as attendance_work_date,
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
row_number() over(partition by user_id,substr(work_date,1,10) order by user_check_time asc) as rn,
row_number() over(partition by user_id,substr(work_date,1,10) order by user_check_time desc) as rn1
from 
${ods_dbname}.ods_qkt_dtk_attendance_di
where d='${pre1_date}'
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
where d in (select distinct substr(user_check_time,1,10) from ${ods_dbname}.ods_qkt_dtk_attendance_di where d='${pre1_date}')
and d<>'${pre1_date}'
)
insert overwrite table ${dwd_dbname}.dwd_dtk_emp_attendance_day_info_di partition(d)
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

union all
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
tmp_dtk_emp_attendance_day_str2
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
