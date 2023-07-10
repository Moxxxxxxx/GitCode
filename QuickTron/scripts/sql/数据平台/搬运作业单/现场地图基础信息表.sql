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
FROM ${dwd_dbname}.dwd_rcs_basic_area_info a
WHERE a.d = '${pre1_date}';
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "


#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="evo_wds_base"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_rcs_basic_area_info;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：现场地图基础信息表 ads_rcs_basic_area_info
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/evo_wds_base?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_rcs_basic_area_info \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_rcs_basic_area_info \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,cur_date,project_code,area_code,warehouse_id,point_code,zone_id,area_name,area_type,super_area_id,json_data,area_state,area_created_user,area_created_app,area_created_time,area_updated_user,area_updated_app,area_updated_time,agv_type_code,create_time,update_time"




echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "