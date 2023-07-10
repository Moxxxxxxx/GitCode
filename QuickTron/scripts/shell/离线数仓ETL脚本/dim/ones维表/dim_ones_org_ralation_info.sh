#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：      
#-- 功能描述 ： ones上组织关联维度数据
#-- 注意 ： 
#-- 输入表 : ods.ods_qkt_ones_department_ralation_df
#-- 输出表 : dim.dim_ones_org_ralation_info
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2021-11-04 CREATE 

# ------------------------------------------------------------------------------------------------


dbname=ods
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
insert overwrite table dim.dim_ones_org_ralation_info
select
d_uuid as org_uuid,
uuid_1 as org_uuid_1,  
name_1 as org_name_1, 
uuid_2 as org_uuid_2, 
name_2 as org_name_2, 
uuid_3 as org_uuid_3,
name_3 as org_name_3,
uuid_4 as org_uuid_4,
name_4 as org_name_4,
uuid_5 as org_uuid_5,
name_5 as org_name_5
from 
ods_qkt_ones_department_ralation_df 
where d='${pre1_date}'
;
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "##############################################hive:{end executor dwd}####################################################################"

