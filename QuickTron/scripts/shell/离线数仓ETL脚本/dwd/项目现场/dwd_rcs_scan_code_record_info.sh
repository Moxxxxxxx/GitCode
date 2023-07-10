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



init_sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;

use $dbname;
insert overwrite table dwd.dwd_rcs_scan_code_record_info partition(d,pt)
select 
id,
agv_code,
barcode_decoded,
barcode_in_map,
bias_type,
date_created as scan_created_time,
direction,
job_id,
point_code,
point_x,
point_y,
project_code,
substr(date_created,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by date_created desc ) as rn 
from
ods_qkt_rcs_scan_code_record 
) t
where t.rn=1

;
"


sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=500;


use $dbname;
insert overwrite table dwd.dwd_rcs_scan_code_record_info partition(d,pt)
select 
id,
agv_code,
barcode_decoded,
barcode_in_map,
bias_type,
date_created as scan_created_time,
direction,
job_id,
point_code,
point_x,
point_y,
project_code,
substr(date_created,0,10) as d,
project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by date_created desc) as rn
from ods_qkt_rcs_scan_code_record
where d>=date_sub('$pre1_date',7) and substr(date_created,0,10)>=date_sub('$pre1_date',7)
) t
where t.rn=1
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"


echo "##############################################hive:{end executor dwd}####################################################################"
