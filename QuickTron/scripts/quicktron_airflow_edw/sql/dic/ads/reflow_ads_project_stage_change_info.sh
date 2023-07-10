#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     


ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_project_stage_change_info;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ads_project_stage_change_info    --项目阶段变化表
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_project_stage_change_info \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_project_stage_change_info \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,project_code_class,project_area,project_priority,project_ft,month_scope,pre_num,handover_num,total_amount,online_num,online_amount,no_online_num,no_online_amount,final_inspection_num,final_inspection_amount,no_final_inspection_num,no_final_inspection_amount,create_time,update_time"