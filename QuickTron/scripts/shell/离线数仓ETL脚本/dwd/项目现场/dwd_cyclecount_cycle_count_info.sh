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
insert overwrite table dwd.dwd_cyclecount_cycle_count_info partition(d,pt)
select 
id,
warehouse_id,
cycle_count_number,
cycle_count_type,
tenant_id,
external_id,
owner_code,
state as cyclecount_state,
version,
zone_code,
include_empty_bucket_slot,
include_empty_container,
operating_mode,
redo_cycle_count_time,
adjustment_generated,
manual,
done_date,
done_user,
delete_flag,
remark,
udf1,
udf2,
udf3,
udf4,
udf5,
created_date as cyclecount_created_time,
created_user as cyclecount_created_user,
created_app as cyclecount_created_app,
last_updated_date as cyclecount_updated_time,
last_updated_user as cyclecount_updated_user,
last_updated_app as cyclecount_updated_app,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc ) as rn 
from
ods_qkt_cyclecount_cycle_count 
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
insert overwrite table dwd.dwd_cyclecount_cycle_count_info partition(d,pt)
select 
id,
warehouse_id,
cycle_count_number,
cycle_count_type,
tenant_id,
external_id,
owner_code,
state as cyclecount_state,
version,
zone_code,
include_empty_bucket_slot,
include_empty_container,
operating_mode,
redo_cycle_count_time,
adjustment_generated,
manual,
done_date,
done_user,
delete_flag,
remark,
udf1,
udf2,
udf3,
udf4,
udf5,
created_date as cyclecount_created_time,
created_user as cyclecount_created_user,
created_app as cyclecount_created_app,
last_updated_date as cyclecount_updated_time,
last_updated_user as cyclecount_updated_user,
last_updated_app as cyclecount_updated_app,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc) as rn
from ods_qkt_cyclecount_cycle_count
where d>=date_sub('$pre1_date',7) and substr(created_date,0,10)>=date_sub('$pre1_date',7)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

