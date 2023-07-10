#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

delete from ads.ads_sku_abc_bucket_rate where date(create_time) = date(sysdate());

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：动销SKU分布货架在货架总量占比 ads_sku_abc_bucket_rate
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_sku_abc_bucket_rate \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_sku_abc_bucket_rate \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,cur_date,project_code,abc_type,abc_bucket_num,total_bucket_num,abc_bucket_rate,create_time,update_time"