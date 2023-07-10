#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_project_workorder_type_count;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ads_project_workorder_type_count 项目工单类型数量统计 
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_project_workorder_type_count \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_project_workorder_type_count \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,date_scope,date_scope_fisrt_day,run_type,project_code,project_sale_code,project_name,work_order_type,new_workorder_num,solve_workorder_num,solve_duration,close_workorder_num,close_duration,create_time,update_time"