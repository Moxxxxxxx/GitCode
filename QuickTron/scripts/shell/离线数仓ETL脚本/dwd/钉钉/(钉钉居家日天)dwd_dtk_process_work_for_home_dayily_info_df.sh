#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉居家办公日天审批记录信息
#-- 注意 ： 每天数据为所有的数据记录
#-- 输入表 : dwd.dwd_dtk_process_work_for_home_info_df
#-- 输出表 ：dwd.dwd_dtk_process_work_for_home_dayily_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-09-07 CREATE 
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
with tmp_dtk_process_work_for_home_str1 as (
select 
*,
datediff(end_date,start_date) as array_size,
case when start_time_period='上午' and end_time_period='上午' then datediff(end_date,start_date)+0.5
     when start_time_period='上午' and end_time_period='下午' then datediff(end_date,start_date)+1
     when start_time_period='下午' and end_time_period='上午' then datediff(end_date,start_date)
     when start_time_period='下午' and end_time_period='下午' then datediff(end_date,start_date)+0.5
 else duration end as total_work_home_days
from 
${dwd_dbname}.dwd_dtk_process_work_for_home_info_df 
where d='${pre1_date}'
)
insert overwrite table ${dwd_dbname}.dwd_dtk_process_work_for_home_dayily_info_df partition(d='${pre1_date}')
select
org_name, 
process_instance_id, 
cc_userids, 
attached_process_instance_ids,
biz_action, 
business_id, 
create_time, 
finish_time, 
reason,
attachment,
originator_dept_id, 
originator_dept_name, 
originator_user_id, 
originator_user_name,
approval_result, 
approval_status, 
approval_title, 
start_date,
end_date,
start_time_period,
end_time_period,
total_work_home_days,
work_home_date,
every_days,
case when every_days=0.5 then ( case when start_time_period='上午' and end_time_period='上午'  then '上午'
                                     when start_time_period='下午' and end_time_period='下午'  then '下午'
                                     when start_time_period='下午' and work_home_date=start_date then '下午'
                                     when start_time_period='下午' and work_home_date=end_date then '上午'
                                     else '其它' end)
     when every_days=1 then '全天'
     else '其它' end period_type,
is_valid
from 
(
select
a.org_name, 
a.process_instance_id, 
a.cc_userids, 
a.attached_process_instance_ids,
a.biz_action, 
a.business_id, 
a.create_time, 
a.finish_time, 
a.reason,
a.attachment,
a.originator_dept_id, 
a.originator_dept_name, 
a.originator_user_id, 
a.originator_user_name,
a.approval_result, 
a.approval_status, 
a.approval_title, 
a.start_date,
a.end_date,
a.start_time_period,
a.end_time_period,
a.total_work_home_days,
date_add(a.start_date,b.pos) as work_home_date,
case when a.total_work_home_days like '%.5%' then (
        case when a.start_time_period ='上午' and a.total_work_home_days>1 then if(b.pos=a.array_size,0.5,1)
             when a.start_time_period in('上午','下午') and a.total_work_home_days<1 then 0.5
             when a.start_time_period='下午'  and a.total_work_home_days>1 then if(b.pos=0,0.5,1)
             else 999 end )
     when a.total_work_home_days not like '%.5%'  then (
        case when a.start_time_period ='上午' then 1
             when a.start_time_period='下午' then if(b.pos=0 or b.pos=a.array_size,0.5,1)
             else 888 end )
     
     
     else 777 end as every_days,
a.is_valid
from 
tmp_dtk_process_work_for_home_str1 a
lateral view posexplode(split(repeat('o',datediff(a.end_date,a.start_date)),'o')) b
) t
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

