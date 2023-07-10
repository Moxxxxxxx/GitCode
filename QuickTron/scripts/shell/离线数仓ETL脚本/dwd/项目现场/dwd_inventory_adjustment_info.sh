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
insert overwrite table dwd.dwd_inventory_adjustment_info partition(d,pt)
select 
id,
warehouse_id,
adjustment_number,
external_id,
adjustment_type,
source_bill_no,
state as adjustment_state,
remark,
version,
delete_flag,
created_date as adjustment_created_time,
created_user as adjustment_created_user,
created_app as adjustment_created_app,
last_updated_date as adjustment_updated_time,
last_updated_user as adjustment_updated_user,
last_updated_app as adjustment_updated_app,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc ) as rn 
from
ods_qkt_inventory_adjustment 
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
insert overwrite table dwd.dwd_inventory_adjustment_info partition(d,pt)
select 
id,
warehouse_id,
adjustment_number,
external_id,
adjustment_type,
source_bill_no,
state as adjustment_state,
remark,
version,
delete_flag,
created_date as adjustment_created_time,
created_user as adjustment_created_user,
created_app as adjustment_created_app,
last_updated_date as adjustment_updated_time,
last_updated_user as adjustment_updated_user,
last_updated_app as adjustment_updated_app,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc) as rn
from ods_qkt_inventory_adjustment
where d>=date_sub('$pre1_date',7) and substr(created_date,0,10)>=date_sub('$pre1_date',7)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"


