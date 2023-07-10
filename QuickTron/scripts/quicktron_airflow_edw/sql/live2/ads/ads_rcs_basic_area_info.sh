#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-2 day" +%F`
fi

    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
-- 现场地图基础信息表 ads_rcs_basic_area_info 

INSERT overwrite table ${ads_dbname}.ads_rcs_basic_area_info
SELECT '' as id,
       a.d as cur_date,
       a.project_code,
       a.area_code,
       a.warehouse_id,
       a.point_code,
       a.zone_id,
       a.area_name, 
       a.area_type, 
       a.super_area_id, 
       a.json_data, 
       a.area_state, 
       a.area_created_user, 
       a.area_created_app, 
       a.area_created_time, 
       a.area_updated_user, 
       a.area_updated_app, 
       a.area_updated_time, 
       a.agv_type_code, 
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${dwd_dbname}.dwd_rcs_basic_area_info_df a
WHERE a.d = '${pre1_date}';
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"