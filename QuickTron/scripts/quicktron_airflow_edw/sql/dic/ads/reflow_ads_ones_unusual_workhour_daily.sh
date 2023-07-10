#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

delete from ads.ads_ones_unusual_workhour_daily where date(create_time) = date(sysdate());

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ones异常工时日统计 ads_ones_unusual_workhour_daily
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_ones_unusual_workhour_daily \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_ones_unusual_workhour_daily \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,cur_date,team_ft,team_group,team_sub_group,unusual_work_hour,create_time,update_time"