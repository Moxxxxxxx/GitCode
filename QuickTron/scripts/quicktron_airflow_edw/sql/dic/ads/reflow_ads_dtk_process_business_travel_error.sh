#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     


ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_dtk_process_business_travel_error;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ads_dtk_process_business_travel_error    --出差审批异常信息记录表
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_dtk_process_business_travel_error \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_dtk_process_business_travel_error \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,cur_date,org_name,business_id,project_code,project_name,project_ft,project_operation_state,team_name,member_name,member_function,error_type,trip_duration,start_time,end_time,create_time,update_time"