#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     


ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_project_member_effcive;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ads_project_member_effcive    --项目进展人员效率
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_project_member_effcive \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_project_member_effcive \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,month_scope,month_scope_first_day,project_area,project_priority,project_ft,project_stage,project_area_group,is_active,no_online_num_total,no_online_amount_total,online_num_month,online_amount_month,no_final_inspection_num_total,no_final_inspection_amount_total,final_inspection_num_month,final_inspection_amount_month,suspend_num_month,suspend_amount_month,pe_num,create_time,update_time"