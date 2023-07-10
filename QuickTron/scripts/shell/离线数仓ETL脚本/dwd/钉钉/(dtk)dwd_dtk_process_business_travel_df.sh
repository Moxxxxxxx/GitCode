#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 钉钉的出差审批信息记录
#-- 注意 ：  每日t-1全量分区
#-- 输入表 : ods.ods_qkt_dtk_process_business_travel_di、dwd.dwd_share_project_base_info_df、dwd.dwd_dtk_emp_info_df
#-- 输出表 ：dwd.dwd_dtk_process_business_travel_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-12-30 CREATE 
#-- 2 wangziming 2022-01-07 modify project_code(-、大写字母、数字) 实际出差时间按照昨天的当前时间进行计算
#-- 3 wangziming 2022-01-26 modify business_id 一致的，取finish_time最新的数据
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

insert overwrite table ${dwd_dbname}.dwd_dtk_process_business_travel_df partition(d='${pre1_date}')
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
trip_type, 
business_trip, 
traffic_tool, 
single_or_return, 
departure_city, 
arrival_city, 
start_date, 
end_date, 
start_am_or_pm,
end_am_or_pm,
case when start_am_or_pm='PM' then (case when end_am_or_pm='PM' then datediff(end_date,start_date)+0.5
										 when end_am_or_pm='AM' then datediff(end_date,start_date)
										else 0 end)
	 when start_am_or_pm=end_am_or_pm and start_am_or_pm<>'UNKNOWN' and end_am_or_pm<>'UNKNOWN'  then datediff(end_date,start_date)+0.5
	 when start_am_or_pm<>end_am_or_pm and start_am_or_pm<>'UNKNOWN' and end_am_or_pm<>'UNKNOWN' then datediff(end_date,start_date)+1
	 else 0 end as apply_days, 
case when cdate<start_date then 0
	 when cdate>=start_date and cdate<=end_date then
	 (case when start_am_or_pm='AM' and end_am_or_pm ='AM' and cdate=end_date then datediff(cdate,start_date)+0.5
	 	   when start_am_or_pm='AM' then datediff(cdate,start_date)+1
	 	   when start_am_or_pm='PM' and end_date>=start_date and end_am_or_pm='PM' then datediff(cdate,start_date)+0.5
	 	   when start_am_or_pm='PM' and end_am_or_pm='AM' and end_date>start_date then datediff(cdate,start_date)
	 	   else 0 end )
	 when cdate>end_date then
	 	(case when start_am_or_pm='PM' then (case when end_am_or_pm='PM' then datediff(end_date,start_date)+0.5
										 when end_am_or_pm='AM' then datediff(end_date,start_date)
										else 0 end)
	 when start_am_or_pm=end_am_or_pm and start_am_or_pm<>'UNKNOWN' and end_am_or_pm<>'UNKNOWN'  then datediff(end_date,start_date)+0.5
	 when start_am_or_pm<>end_am_or_pm and start_am_or_pm<>'UNKNOWN' and end_am_or_pm<>'UNKNOWN' then datediff(end_date,start_date)+1
	 else 0 end)
	 else 0 end as business_travel_days,
business_travel_memo,
cost_center, 
invoice_title, 
partner, 
enclosure, 
is_project_matching
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
a.originator_userid as originator_user_id, 
c.emp_name as originator_user_name, 
a.result as approval_result, 
a.status as approval_status, 
a.title as approval_title, 
a.process_project_code as old_project_code,
a.project_code as project_code, 
a.process_project_name as project_name, 
a.trip_type, 
a.business_trip, 
a.traffic_tool, 
a.single_or_return, 
a.departure as departure_city,
a.arrival as arrival_city, 
split(a.start_time,' ')[0] as start_date, 
split(a.end_time,' ')[0] as end_date, 
case split(a.start_time,' ')[1]  
	 when '上午' then 'AM'
	 when 'AM' then 'AM'
	 when '下午' then 'PM'
	 when 'PM' then 'PM'
	 else 'UNKNOWN' end as start_am_or_pm,
case split(a.end_time,' ')[1]  
	 when '上午' then 'AM'
	 when 'AM' then 'AM'
	 when '下午' then 'PM'
	 when 'PM' then 'PM'
	 else 'UNKNOWN' end as end_am_or_pm,
0 as apply_days, 
0 as business_travel_days, 
a.business_travel_memo, 
a.cost_center, 
a.invoice_title, 
a.partner, 
a.enclosure, 
if(b.project_code is not null,'1','0') as is_project_matching,
date_sub(current_date(),1) as cdate,
row_number() over(partition by a.business_id order by a.finish_time desc ) as rn
from 
(select *,regexp_replace(regexp_replace(upper(process_project_code),'_','-'),'[^A-Z-0-9-]','') as project_code from ${ods_dbname}.ods_qkt_dtk_process_business_travel_di) a
left join ${dwd_dbname}.dwd_share_project_base_info_df b on a.project_code=b.project_code and b.d=date_sub(current_date(),1)
left join (select * from ${dwd_dbname}.dwd_dtk_emp_info_df where d='${pre1_date}' and org_company_name='上海快仓智能科技有限公司') c on a.originator_userid=c.emp_id
) t
where t.rn=1
;
"



printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
