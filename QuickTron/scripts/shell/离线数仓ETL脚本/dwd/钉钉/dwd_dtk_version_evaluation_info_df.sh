#!/bin/bash

dbname=ods
#hive=/opt/module/hive-3.1.2/scripts/hive
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


use $dbname;
insert overwrite table dwd.dwd_dtk_version_evaluation_info_df partition(d='$pre1_date')
select 
  id,
  process_instance_id,
  project_code,
  project_name,
  product_name,
  project_type,
  project_stage,
  upgrade_res,
  case  when version= '2.81' then '2.8.1'
          when version= '2.2.0' then '其它'
         else version end as upgrade_version,
  upgrade_module,
  upgrade_date,
  applicant,
  approver,
  result as apply_result,
  status as apply_status,
  case when nvl(score,'')='' and datediff(current_date(),create_time)>120 then '3.5'
       else  if(nvl(score,'')<>'',split(score,'-')[0],score) end  as score,
  case when nvl(score,'')='' and datediff(current_date(),create_time)>120 and nvl(score_remark,'')='' then '其它'
        else regexp_replace(score_remark,'\n|\r|\t','') end as score_remark ,

  regexp_replace(evaluate,'\n|\r|\t','') as evaluate_desc,
  first_examiner_user as first_approver_id,
  if(nvl(score,'')='' and datediff(current_date(),create_time)>120 and nvl(first_examiner,'')='','system',first_examiner) as first_approver_name,
  if(nvl(score,'')='' and datediff(current_date(),create_time)>120 and nvl(first_time,'')='',current_timestamp(),first_time) as first_approval_time,
  second_examiner_user as second_approver_id,
  if(nvl(score,'')='' and datediff(current_date(),create_time)>120 and nvl(second_examiner,'')='','system',second_examiner) as second_approver_name,
  if(nvl(score,'')='' and datediff(current_date(),create_time)>120 and nvl(second_time,'')='',current_timestamp(),second_time) as second_approval_time,
  third_examiner_user as third_approver_id,
  if(nvl(score,'')='' and datediff(current_date(),create_time)>120 and nvl(third_examiner,'')='','system',third_examiner) as third_approver_name,
  if(nvl(score,'')='' and datediff(current_date(),create_time)>120 and nvl(third_time,'')='',current_timestamp(),third_time) as third_approval_time,
  remark as apply_remark,
  create_time as apply_create_time,
  finish_time as apply_finish_time,
 regexp_replace(bug_describe,'\n|\r|\t','') as bug_desc,
 regexp_replace(demand_describe,'\n|\r|\t','') as demand_desc,
  case when project_first='否' then 0
       when project_first='是' then 1
       else '-1' end as is_first_project,
  version_desc,
  version_tag
from 
ods_qkt_dtk_version_evaluation_df
where d='$pre1_date' 
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
