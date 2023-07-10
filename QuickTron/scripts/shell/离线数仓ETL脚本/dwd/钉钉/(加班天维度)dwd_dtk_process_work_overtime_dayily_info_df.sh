#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉人员加班每天信息记录信息维度表
#-- 注意 ： 每天数据为所有的数据记录
#-- 输入表 : dwd.dwd_dtk_process_work_overtime_info_df
#-- 输出表 ：dwd.dwd_dtk_process_work_overtime_dayily_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-08-18 CREATE 
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



-- 回流请假数据到指定表
with tmp_dtk_process_overtime_str1 as (
select 
*,
datediff(end_date,start_date) as array_size,
case when start_time_period='上午' and end_time_period='上午' then datediff(end_date,start_date)+0.5
     when start_time_period='上午' and end_time_period='下午' then datediff(end_date,start_date)+1
     when start_time_period='下午' and end_time_period='上午' then datediff(end_date,start_date)
     when start_time_period='下午' and end_time_period='下午' then datediff(end_date,start_date)+0.5
 else overtime_duration end as total_overtime_days
from 
${dwd_dbname}.dwd_dtk_process_work_overtime_info_df 
where d='${pre1_date}'
)
insert overwrite table ${dwd_dbname}.dwd_dtk_process_work_overtime_dayily_info_df partition(d='${pre1_date}')
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
work_overtime_reason,
is_legal_holiday,
work_overtime_accounting_method,
overtime_person,
start_date,
end_date,
start_time_period,
end_time_period,
total_overtime_days,
overtime_date,
every_days,
case when every_days=0.5 then ( case when start_time_period='上午' and end_time_period='上午'  then '上午'
                                     when start_time_period='下午' and end_time_period='下午'  then '下午'
                                     when start_time_period='下午' and overtime_date=start_date then '下午'
                                     when start_time_period='下午' and overtime_date=end_date then '上午'
                                     else '其它' end)
     when every_days=1 then '全天'
     else '其它' end period_type,
is_valid
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
a.start_date,
a.end_date,
a.start_time_period,
a.end_time_period,
a.total_overtime_days,
date_add(a.start_date,b.pos) as overtime_date,
case when a.total_overtime_days like '%.5%' then (
		case when a.start_time_period ='上午' and a.total_overtime_days>1 then if(b.pos=a.array_size,0.5,1)
			 when a.start_time_period in('上午','下午') and a.total_overtime_days<1 then 0.5
			 when a.start_time_period='下午'  and a.total_overtime_days>1 then if(b.pos=0,0.5,1)
			 else 999 end )
     when a.total_overtime_days not like '%.5%'  then (
     	case when a.start_time_period ='上午' then 1
     	     when a.start_time_period='下午' then if(b.pos=0 or b.pos=a.array_size,0.5,1)
     	     else 888 end )
     
	 
     else 777 end as every_days,
a.is_valid
from 
tmp_dtk_process_overtime_str1 a
lateral view posexplode(split(repeat('o',datediff(a.end_date,a.start_date)),'o')) b
) t
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

