#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     


ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_dtk_process_business_travel;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ads_dtk_process_business_travel    --出差审批信息记录表
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_dtk_process_business_travel \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_dtk_process_business_travel \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,process_instance_id,business_id,business_create_time,business_finish_time,originator_dept_name,originator_user_id,originator_user_name,original_project_code,project_code,project_name,project_operation_state,project_area,project_ft,project_priority,project_progress_stage,project_area_group,business_trip,travel_date,travel_days,travel_type,data_source,is_valid,create_time,update_time"