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
insert overwrite table dwd.dwd_replenish_work_info partition(d,pt)
select 
id,
replenish_work_number,
tenant_id,
owner_code,
work_type,
state as work_state,
priority_type,
priority_value,
station_id,
station_code,
done_date,
done_user,
source_order_type,
source_order_id,
replenish_mode,
opened,
submit_times,
remark,
version,
zone_id,
zone_code,
warehouse_id,
delete_flag,
udf1,
udf2,
udf3,
udf4,
udf5,
created_date as work_created_date,
created_user as work_created_user,
created_app as work_created_app,
last_updated_date as work_updated_date,
last_updated_user as work_updated_user,
last_updated_app as work_updated_app,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc ) as rn 
from
ods_qkt_replenish_work 
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
insert overwrite table dwd.dwd_replenish_work_info partition(d,pt)
select 
id,
replenish_work_number,
tenant_id,
owner_code,
work_type,
state as work_state,
priority_type,
priority_value,
station_id,
station_code,
done_date,
done_user,
source_order_type,
source_order_id,
replenish_mode,
opened,
submit_times,
remark,
version,
zone_id,
zone_code,
warehouse_id,
delete_flag,
udf1,
udf2,
udf3,
udf4,
udf5,
created_date as work_created_date,
created_user as work_created_user,
created_app as work_created_app,
last_updated_date as work_updated_date,
last_updated_user as work_updated_user,
last_updated_app as work_updated_app,
project_code,
substr(created_date,0,10) as d,
project_code as pt
from (
select 
*
,row_number() over(partition by id,project_code order by last_updated_date desc) as rn
from ods_qkt_replenish_work
where d>=date_sub('$pre1_date',7) and substr(created_date,0,10)>=date_sub('$pre1_date',7)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"


