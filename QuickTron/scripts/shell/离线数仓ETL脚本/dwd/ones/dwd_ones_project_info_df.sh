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
insert overwrite table dwd.dwd_ones_project_info_df partition(d='$pre1_date')
select 
ones_project_uuid as project_uuid,
project_code,
project_name,
if(nvl(project_status,'')='','UNKNOWN',project_status) as project_status,
if(nvl(project_system,'') in ('','/','其他','无'),'UNKNOWN',project_system) as project_system,
if(nvl(agv_type,'')='','UNKNOWN',agv_type) as agv_type,
created_time as project_created_time,
if(nvl(owner_name,'')='','UNKNOWN',owner_name) as project_owner_name,
IF(nvl(ft_group,'')='','UNKNOWN',ft_group) as ft_group
from 
ods_qkt_ones_project_base_info_df
where d='$pre1_date' 
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

#$hive $hive_username $hive_passwd -e "$sql"
$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

