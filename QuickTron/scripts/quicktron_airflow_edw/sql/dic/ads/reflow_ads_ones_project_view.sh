#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_ones_project_view;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ads_ones_project_view ones项目概览表 
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_ones_project_view \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_ones_project_view \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,ones_project_uuid,ones_project_name,project_type_name,project_bpm_code,project_bpm_name,person_incharge,sprint_qty,sprint_classify_name,sprint_create_time,sprint_end_time,is_newset,test_success_qty,test_intime_qty,total_test_qty,total_demand_qty,effective_bug_qty,solved_bug_qty,total_bug_qty,create_time,update_time"