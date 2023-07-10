#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

delete from ads.ads_sku_ABC_checkout_distribution where date(create_time) = date(sysdate());

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：根据出库数量区分ABC分类：货品数量及货架分布 ads_sku_ABC_checkout_distribution
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_sku_ABC_checkout_distribution \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_sku_ABC_checkout_distribution \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,cur_date,project_code,sku_id,bucket_num,sku_num,class_level,create_time,update_time"