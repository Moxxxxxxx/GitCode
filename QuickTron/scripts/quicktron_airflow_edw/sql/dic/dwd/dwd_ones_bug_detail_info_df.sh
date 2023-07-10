#!/bin/bash


# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     d 
#-- 功能描述 ： 项目 dwd层  one bug明细
#-- 注意 ： 每日按天全量分区
#-- 输入表 : ods.ods_qkt_ones_bug_detail_df
#-- 输出表 ：dwd.dwd_ones_bug_detail_info_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-03-01 CREATE 

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



insert overwrite table ${dwd_dbname}.dwd_ones_bug_detail_info_df partition(d='$pre1_date')
select 
  bug_uuid,
  bug_id,
  regexp_replace(bug_title,'\n|\r','') as bug_title,
  regexp_extract(regexp_replace(bug_version,'-','.'),'[0-9]{1}\\.{1}[0-9]{1}\\.{1}[0-9]{1}',0) as bug_version,
  project_code,
 CASE when product_name like '%货架到人%' THEN '1'
      when product_name like '%智能搬运%' THEN '2'
      when product_name like '%料箱到人%' THEN '3'
      when product_name like '%料箱搬运%' THEN '4'
      ELSE '-1' END as product_id,
  belonging_ft,
  product_name,
  bug_status,
  severity_level,
  person_liable,
  creator as bug_created_user,
  bug_create_time,
  bug_finish_duration,
  bug_finish_time,
  bug_update_time,
  overdue_duration,
  deadline as deadline_time
from 
${ods_dbname}.ods_qkt_ones_bug_detail_df
where d='${pre1_date}' 
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

