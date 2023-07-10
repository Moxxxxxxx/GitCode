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