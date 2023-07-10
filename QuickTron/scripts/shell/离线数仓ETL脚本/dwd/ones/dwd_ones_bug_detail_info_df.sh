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
insert overwrite table dwd.dwd_ones_bug_detail_info_df partition(d='$pre1_date')
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
ods_qkt_ones_bug_detail_df
where d='$pre1_date' 
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

