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
insert overwrite table dwd.dwd_wes_basic_sku_info partition(d,pt)
select 
id,
owner_id,
sku_code,
sku_name,
batch_enabled,
sn_enabled,
lot_barcode_enabled,
over_weight_flag,
upper_limit_quantity,
lower_limit_quantity,
image_url,
expiration_date,
near_expiration_date,
spec,
supplier,
abc_category,
major_category,
medium_category,
minor_category,
mutex_category,
state as sku_state,
udf1,
udf2,
udf3,
udf4,
udf5,
created_user as sku_created_user,
created_app as sku_created_app,
created_time as sku_created_time,
last_updated_user as sku_updated_user,
last_updated_app as sku_updated_app,
last_updated_time as sku_updated_time,
extended_field,
project_code,
substr(created_time,0,10) as d,
project_code as pt
from  ( 
select 
*
,row_number() over(partition by id,project_code order by last_updated_time desc ) as rn 
from
ods_qkt_wes_basic_sku 
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
insert overwrite table dwd.dwd_wes_basic_sku_info partition(d,pt)
select 
id,
owner_id,
sku_code,
sku_name,
batch_enabled,
sn_enabled,
lot_barcode_enabled,
over_weight_flag,
upper_limit_quantity,
lower_limit_quantity,
image_url,
expiration_date,
near_expiration_date,
spec,
supplier,
abc_category,
major_category,
medium_category,
minor_category,
mutex_category,
state as sku_state,
udf1,
udf2,
udf3,
udf4,
udf5,
created_user as sku_created_user,
created_app as sku_created_app,
created_time as sku_created_time,
last_updated_user as sku_updated_user,
last_updated_app as sku_updated_app,
last_updated_time as sku_updated_time,
extended_field,
project_code,
substr(created_time,0,10) as d,
project_code as pt
from  (
select 
*
,row_number() over(partition by id,project_code order by last_updated_time desc) as rn
from ods_qkt_wes_basic_sku
where d>=date_sub('$pre1_date',7) and substr(created_time,0,10)>=date_sub('$pre1_date',7)
) t
where t.rn=1
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
