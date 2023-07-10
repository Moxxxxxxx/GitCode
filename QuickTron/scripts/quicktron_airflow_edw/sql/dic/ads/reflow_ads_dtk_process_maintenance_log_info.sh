#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     


ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_dtk_process_maintenance_log_info;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ads_dtk_process_maintenance_log_info    --维保日志明细表
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_dtk_process_maintenance_log_info \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_dtk_process_maintenance_log_info \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,business_id,business_create_time,originator_user_name,log_date,log_month,project_code,project_name,attendance_status,job_content,internal_work_content,working_hours,create_time,update_time"