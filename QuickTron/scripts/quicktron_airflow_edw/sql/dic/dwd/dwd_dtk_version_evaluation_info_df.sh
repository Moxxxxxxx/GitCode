#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 钉钉的项目版本信息记录
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_dtk_version_evaluation_df
#-- 输出表 ：dwd.dwd_dtk_version_evaluation_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-10-16 CREATE 
#-- 2 wangziming 2021-12-10 modify 新增project_code、upgrade_version的清洗逻辑，去掉空白符,product_name字段清洗
#-- 3 wangziming 2021-12-20 modify project_code 为中文，或者小写或者 project_code 与project_name 写反的情况进行规则调整


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



insert overwrite table ${dwd_dbname}.dwd_dtk_version_evaluation_info_df partition(d='${pre1_date}')
select 
id,
process_instance_id,
upper(
case when nvl(project_code,'')='' and regexp_replace(project_name,'[a-zA-Z0-9]','')='' then project_name
     when project_code='SVT' and regexp_replace(project_name,'[a-zA-Z0-9]','')='' then project_name
     when project_code rlike '-$' then regexp_replace(project_code,'-','') 
     else project_code end) as project_code,
project_name,
product_name,
project_type,
project_stage,
upgrade_res,
upgrade_version,
upgrade_module,
upgrade_date,
applicant,
approver,
apply_result,
apply_status,
score,
score_remark,
evaluate_desc,
first_approver_id,
first_approver_name,
first_approval_time,
second_approver_id,
second_approver_name,
second_approval_time,
third_approver_id,
third_approver_name,
third_approval_time,
apply_remark,
apply_create_time,
apply_finish_time,
bug_desc,
demand_desc,
is_first_project,
version_desc,
version_tag
from 
(
select 
  id,
  process_instance_id,
  regexp_replace(project_code,'\n|\r|\t|\\\s+|[\u4e00-\u9fa5]','') as project_code,
  project_name,
  regexp_replace(product_name,'\\\"|\\\\[|\\\\]','') as product_name,
  project_type,
  project_stage,
  upgrade_res,
  regexp_replace(case  when version= '2.81' then '2.8.1'
          when version= '2.2.0' then '其它'
         else version end,'\n|\r|\t','') as upgrade_version,
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
${ods_dbname}.ods_qkt_dtk_version_evaluation_df
where d='${pre1_date}'  
and project_code!='上汽大众项目超市2.0项目测试'
) t
where  nvl(project_code,'')<>'' or regexp_replace(project_name,'[a-zA-Z0-9]','')=''
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "${sql}"

