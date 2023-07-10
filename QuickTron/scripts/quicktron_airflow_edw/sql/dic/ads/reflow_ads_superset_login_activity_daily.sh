#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"




ssh -tt 001.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_superset_login_activity_daily;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：各看板使用用户分布 ads_superset_login_activity_daily （superset看板）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_superset_login_activity_daily \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_superset_login_activity_daily \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "user_id,user_name,date_node,current_date_login_count,total_login_count"