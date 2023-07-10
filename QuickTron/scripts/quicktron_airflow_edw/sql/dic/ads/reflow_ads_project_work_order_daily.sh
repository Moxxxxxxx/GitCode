#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

delete from ads.ads_project_work_order_daily where date(create_time) = date(sysdate());

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：项目工单日趋势指标统计表 ads_project_work_order_daily
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_project_work_order_daily \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_project_work_order_daily \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,cur_date,ft_name,project_code,project_name,project_operation_state,product_name,current_version,work_order_type,noclose_over_tendays_inthirtydays,noclose_over_tendays_inninetydays,noclose_over_tendays_inthirtydays_rate,noclose_over_tendays_inninetydays_rate,closed_sevendays_num,total_sevendays,closed_sevendays_rate,closed_fourteendays_num,total_fourteendays,closed_fourteendays_rate,closed_thirtydays_num,total_thirtydays,closed_thirtydays_rate,closed_ninetydays_num,total_ninetydays,closed_ninetydays_rate,create_time,update_time"