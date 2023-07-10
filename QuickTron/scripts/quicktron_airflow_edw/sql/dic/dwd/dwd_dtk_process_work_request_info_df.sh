#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉工作请示申请记录信息
#-- 注意 ： 每日增量更新到昨日的分区内，每天的分区为最新的数据
#-- 输入表 : ods.ods_qkt_dtk_process_work_request_df、dwd.dwd_dtk_emp_info_df
#-- 输出表 ：dwd.dwd_dtk_process_work_request_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-06-30 CREATE 

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

with tmp_dtk_process_work_request_str1 as (
select 
org_name,
process_instance_id,
concat_ws(',',collect_list(user_id)) as approval_user_ids,
concat_ws(',',collect_list(user_name)) as  approval_user_names
from 
(
select 
t1.org_name,
t1.process_instance_id,
if(t1.user_id='UNKNOWN',null,t1.user_id) as user_id,
case when t1.user_id='UNKNOWN' then null
   when nvl(t2.emp_name,'')='' then 'UNKNOWN'
   else t2.emp_name end as user_name
from 
(
select 
a.org_name,
a.process_instance_id,
b.user_id
from 
${ods_dbname}.ods_qkt_dtk_process_work_request_df a
lateral view explode(split(regexp_replace(if(nvl(a.cc_userids,'')='','UNKNOWN',a.cc_userids),'[\\\\[\\\\]\']',''),',')) b as user_id
where d='${pre1_date}' 
) t1
left join ${dwd_dbname}.dwd_dtk_emp_info_df t2 on trim(t1.user_id)=t2.emp_id and t2.d='${pre1_date}'
) rt
group by 
org_name,
process_instance_id
)
insert overwrite table ${dwd_dbname}.dwd_dtk_process_work_request_info_df partition(d='${pre1_date}')
select 
a.org_name, 
a.process_instance_id, 
a.attached_process_instance_ids, 
a.biz_action, 
a.business_id, 
b.approval_user_ids, 
b.approval_user_names,
a.create_time as process_start_time, 
a.finish_time as process_end_time, 
a.originator_dept_id, 
a.originator_dept_name, 
a.originator_userid as originator_user_id, 
c.emp_name as originator_user_name,
a.\`result\` as approval_result, 
a.status as approval_status, 
a.title as approval_title, 
regexp_replace(a.cause,'\r|\n|\t','') as request_cause, 
a.urgency as urgency_degree,
a.cause_date as request_cause_date, 
regexp_replace(a.content,'\r|\n|\t','')  as request_content, 
a.enclosure,
if(substr(a.cause_date,1,10)>='2022-02-01' and (a.cause rlike '(远程|办公|封闭|封控|小区|解封|居家|阳性|确诊|管控|隔离|疫情|防控|核酸|筛查|密接|排查|感染)'
 or a.content rlike '(远程|办公|封闭|封控|小区|解封|居家|阳性|确诊|管控|隔离|疫情|防控|核酸|筛查|密接|排查|感染)'),'1','0') as is_homeworking
from
${ods_dbname}.ods_qkt_dtk_process_work_request_df a
left join tmp_dtk_process_work_request_str1 b on a.org_name=b.org_name and a.process_instance_id=b.process_instance_id
left join ${dwd_dbname}.dwd_dtk_emp_info_df c on a.originator_userid=c.emp_id and c.d='${pre1_date}'
where a.d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"


