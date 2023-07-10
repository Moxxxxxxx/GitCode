#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     


ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_pms_process_service_log_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ads_pms_process_service_log_detail    --pms 外包劳务日志
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_pms_process_service_log_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_pms_process_service_log_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,cur_date,business_id,project_code,project_name,project_operation_state,project_area,project_ft,project_priority,project_progress_stage,project_area_group,is_active,team_name,member_name,service_type,member_function,check_duration_hour,check_sort,check_duration_day_hour,check_duration_day,service_cost,data_source,is_valid,create_time,update_time"