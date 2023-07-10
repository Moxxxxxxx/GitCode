#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="evo_wds_base"                                     




ssh -tt 008.bg.qkt <<effo

mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table avg_type_proportion;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------向接口表插入数据----------------------------------------------------------------------------------------------- "


##表：机器人类型比例 ads_avg_type_proportion （项目概览）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/evo_wds_base?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table avg_type_proportion \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_avg_type_proportion \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,project_code,avg_type,avg_type_name,fenzi,fenmu,avg_proportion,create_time,update_time"