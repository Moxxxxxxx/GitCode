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
insert overwrite table dwd.dwd_rcs_basic_roller_park_info partition(d,pt)
select 
id,
created_app as roller_created_app,
created_time as roller_created_time,
created_user as roller_created_user,
delta_x,
delta_y,
install_angle,
last_updated_app as roller_updated_app,
last_updated_time as roller_updated_time,
last_updated_user as roller_updated_user,
roller_layer,
roller_no,
roller_team,
work_direction,
project_code,
'$pre1_date' as d,
project_code as pt
from 
ods_qkt_rcs_basic_roller_park
where d='$pre1_date' 
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"
