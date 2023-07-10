#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉公出人员审批记录信息
#-- 注意 ： 每天全量分区
#-- 输入表 : ods.ods_qkt_dtk_process_attendance_business_df，dwd.dwd_dtk_emp_info_df
#-- 输出表 ：dwd.dwd_dtk_process_attendance_business_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-09-06 CREATE 
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


insert overwrite table ${dwd_dbname}.dwd_dtk_process_attendance_business_info_df partition(d='${pre1_date}')
select 
org_name,
process_instance_id,
cc_userids,
attached_process_instance_ids,
biz_action,
business_id,
city,
create_time,
finish_time,
reason,
attachment,
source_start_time,
source_end_time,
duration,
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
if(biz_action='REVOKE','0','1') as is_valid
from 
(
select 
a.org_name,
a.process_instance_id,
if(a.cc_userids ='null',null,a.cc_userids) as cc_userids,
a.attached_process_instance_ids,
a.biz_action,
a.business_id,
a.dd_select_field as city,
a.create_time,
a.finish_time,
regexp_replace(a.textarea_field,'\r|\n|\t','') as reason,
a.dd_attachment as attachment,
a.dd_goout_field_start_time as source_start_time,
a.dd_goout_field_end_time as source_end_time,
a.dd_goout_field_duration as duration,
a.originator_dept_id,
a.originator_dept_name,
a.originator_userid as originator_user_id,
b.emp_name as originator_user_name,
a.result as approval_result,
a.status as approval_status,
a.title as approval_title,
split(a.dd_goout_field_start_time,' ')[0] as start_date,
split(a.dd_goout_field_end_time,' ')[0] as end_date,
case upper(nvl(split(a.dd_goout_field_start_time,' ')[1],'')) when '上午' then '上午'
                when 'AM' then '上午'
                when '下午' then '下午'
                when 'PM' then '下午'
                when '' then '上午'
                else null end as start_time_period,
case upper(nvl(split(a.dd_goout_field_end_time,' ')[1],'')) when '下午' then '下午'
                when 'PM' then '下午'
                when '上午' then '上午'
                when 'AM' then '上午'
                when '' then '下午'
                else null end as end_time_period,
row_number() over(partition by business_id order by finish_time desc) as rn
from 
${ods_dbname}.ods_qkt_dtk_process_attendance_business_df a
left join ${dwd_dbname}.dwd_dtk_emp_info_df b on a.originator_userid=b.emp_id and b.d='${pre1_date}'
where a.d='${pre1_date}'
) t
where rn=1
;
"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
