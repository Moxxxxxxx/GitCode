#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"




ssh -tt 001.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_monitor_platform_error_perday;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：各看板使用用户分布 ads_monitor_platform_error_perday （superset看板）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_monitor_platform_error_perday \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_monitor_platform_error_perday \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "date_node,evo_notification_error_count,slave_mysql_error_count,evo_interface_error_count,evo_rcs_error_count,master_ram_error_count,evo_wcs_g2p_error_count,master_redis_error_count,rcs_log_error_count,master_mysql_error_count,evo_basic_error_count,evo_station_error_count"