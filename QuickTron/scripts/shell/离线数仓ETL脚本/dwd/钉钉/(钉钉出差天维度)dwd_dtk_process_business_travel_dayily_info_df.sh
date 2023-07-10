#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉人员出差每天信息记录信息维度表
#-- 注意 ： 每天数据为所有的数据记录
#-- 输入表 : dwd.dwd_dtk_process_business_travel_df
#-- 输出表 ：dwd.dwd_dtk_process_business_travel_dayily_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-08-15 CREATE 
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
with tmp_dtk_process_leave_str1 as (
select 
*,
datediff(end_date,start_date) as array_size,
case when start_am_or_pm='AM' and end_am_or_pm='AM' then datediff(end_date,start_date)+0.5
     when start_am_or_pm='AM' and end_am_or_pm='PM' then datediff(end_date,start_date)+1
     when start_am_or_pm='PM' and end_am_or_pm='AM' then datediff(end_date,start_date)
     when start_am_or_pm='PM' and end_am_or_pm='PM' then datediff(end_date,start_date)+0.5
 else business_travel_days end as total_travel_days
from 
${dwd_dbname}.dwd_dtk_process_business_travel_df 
where d='${pre1_date}'  and business_travel_days>0
)
insert overwrite table ${dwd_dbname}.dwd_dtk_process_business_travel_dayily_info_df partition(d='${pre1_date}')
select
org_name, 
process_instance_id, 
attached_process_instance_ids,
biz_action, 
business_id, 
cc_userids, 
create_time, 
finish_time, 
originator_dept_id, 
originator_dept_name, 
originator_user_id, 
originator_user_name,
approval_result, 
approval_status, 
approval_title,
old_project_code,
project_code,
project_name,
business_trip,
traffic_tool,
single_or_return,
departure_city,
arrival_city,
start_date,
end_date,
start_time_period,
end_time_period,
total_travel_days,
business_travel_memo,
cost_center,
invoice_title,
is_project_matching,
travel_date,
every_days,
case when every_days=0.5 then ( case when start_time_period='AM' and end_time_period='AM'  then '上午'
                                     when start_time_period='PM' and end_time_period='PM'  then '下午'
                                     when start_time_period='PM' and travel_date=start_date then '下午'
                                     when start_time_period='PM' and travel_date=end_date then '上午'
                                     else '其它' end)
     when every_days=1 then '全天'
     else '其它' end period_type
from 
(
select
a.org_name, 
a.process_instance_id, 
a.attached_process_instance_ids,
a.biz_action, 
a.business_id, 
a.cc_userids, 
a.create_time, 
a.finish_time, 
a.originator_dept_id, 
a.originator_dept_name, 
a.originator_user_id, 
a.originator_user_name,
a.approval_result, 
a.approval_status, 
a.approval_title,
a.old_project_code,
a.project_code,
a.project_name,
a.business_trip,
a.traffic_tool,
a.single_or_return,
a.departure_city,
a.arrival_city,
a.start_date,
a.end_date,
a.start_am_or_pm as start_time_period,
a.end_am_or_pm as end_time_period,
a.total_travel_days,
a.business_travel_memo,
a.cost_center,
a.invoice_title,
a.is_project_matching,
date_add(a.start_date,b.pos) as travel_date,
case when a.total_travel_days like '%.5%' then (
		case when a.start_am_or_pm ='AM' and a.total_travel_days>1 then if(b.pos=a.array_size,0.5,1)
			 when a.start_am_or_pm in('AM','PM') and a.total_travel_days<1 then 0.5
			 when a.start_am_or_pm='PM'  and a.total_travel_days>1 then if(b.pos=0,0.5,1)
			 else 999 end )
     when a.total_travel_days not like '%.5%'  then (
     	case when a.start_am_or_pm ='AM' then 1
     	     when a.start_am_or_pm='PM' then if(b.pos=0 or b.pos=a.array_size,0.5,1)
     	     else 888 end )
     else 777 end as every_days
from 
tmp_dtk_process_leave_str1 a
lateral view posexplode(split(repeat('o',datediff(a.end_date,a.start_date)),'o')) b
) t
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
