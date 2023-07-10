#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads.ads_superset_project_used_detail; 

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：项目看板成员使用明细 ads_superset_project_used_detail
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_superset_project_used_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_superset_project_used_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,log_id,days,record_start_time,duration_ms,user_id,user_cname,role_id,role_cname,user_email,user_action,user_action_name,dashboard_id,dashboard_name,is_dashboard_publish,url_address,team_ft,emp_position,create_time,update_time"