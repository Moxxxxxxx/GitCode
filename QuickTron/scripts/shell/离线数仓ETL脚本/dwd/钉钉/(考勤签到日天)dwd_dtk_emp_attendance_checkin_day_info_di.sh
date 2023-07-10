#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉员工考勤签到日天信息记录
#-- 注意 ： 每天全量分区
#-- 输入表 : dwd.dwd_dtk_emp_attendance_day_info_di,dwd.dwd_dtk_group_day_checkin_info_di
#-- 输出表 ：dwd.dwd_dtk_emp_attendance_checkin_day_info_di
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-08-31 CREATE 

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

with tmp_emp_attendance_checkin_day_str1 as (
select 
emp_id,
emp_name,
checkin_date,
first_checkin_time,
first_checkin_detail_place,
last_checkin_time,
last_checkin_detail_place,
days_checkin_number,
d as d2
from 
${dwd_dbname}.dwd_dtk_group_day_checkin_info_di
where d>='${pre2_date}'
),
tmp_emp_attendance_checkin_day_str2 as (
select 
emp_id,
emp_name,
attendance_work_date,
attendance_working_time,
attendance_working_address,
attendance_off_time,
attendance_off_address,
days_attendance_number,
d as d1
from 
${dwd_dbname}.dwd_dtk_emp_attendance_day_info_di
where d>='${pre2_date}'
)

insert overwrite table ${dwd_dbname}.dwd_dtk_emp_attendance_checkin_day_info_di partition(d)
select 
coalesce(emp_id_1,emp_id_2) as emp_id,
coalesce(emp_name_1,emp_name_2) as emp_name ,
coalesce(attendance_work_date,checkin_date) as att_checkin_work_date ,
att_checkin_start_time,
place_map[att_checkin_start_time] as att_checkin_start_place,
att_checkin_end_time ,
place_map[att_checkin_end_time] as att_checkin_end_place,
days_attendance_number+days_checkin_number as att_checkin_days_number,
coalesce(attendance_work_date,checkin_date) as d
from 
(
select 
a.emp_id as emp_id_1,
a.emp_name as emp_name_1 ,
a.attendance_work_date,
a.attendance_working_time,
a.attendance_off_time,
nvl(a.days_attendance_number,0) as days_attendance_number,
a.d1,
b.emp_id as emp_id_2,
b.emp_name as emp_name_2,
b.checkin_date,
b.first_checkin_time,
b.last_checkin_time,
nvl(b.days_checkin_number,0) as days_checkin_number,
b.d2,
least(nvl(a.attendance_working_time,'9999-99-99'),nvl(a.attendance_off_time,'9999-99-99'),nvl(b.first_checkin_time,'9999-99-99'),nvl(b.last_checkin_time,'9999-99-99')) as att_checkin_start_time,
greatest(nvl(a.attendance_working_time,'11'),nvl(a.attendance_off_time,'12'),nvl(b.first_checkin_time,'13'),nvl(b.last_checkin_time,'14')) as att_checkin_end_time,
map(nvl(a.attendance_working_time,'11'),attendance_working_address,nvl(a.attendance_off_time,'12'),attendance_off_address,nvl(b.first_checkin_time,'13'),first_checkin_detail_place,nvl(b.last_checkin_time,'14'),last_checkin_detail_place) as place_map
from 
tmp_emp_attendance_checkin_day_str2 a
full join tmp_emp_attendance_checkin_day_str1 b on a.attendance_work_date =b.checkin_date and a.emp_id=b.emp_id and a.d1=b.d2
) t
"




printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
