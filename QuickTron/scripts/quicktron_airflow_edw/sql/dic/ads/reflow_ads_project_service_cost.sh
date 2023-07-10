#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     


ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_project_service_cost;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ads_project_service_cost    --项目劳务成本统计
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_project_service_cost \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_project_service_cost \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,project_code,project_name,fh_project_code,fh_project_name,project_status,labour_type_id,labour_type,area,pm_name,spm_name,labour_budget_contract,labour_budget_incremental,actual_labour,oneweek_work_num,avg_oneweek_work_num,twoweek_work_num,avg_twoweek_work_num,threeweek_work_num,avg_threeweek_work_num,fourweek_work_num,avg_fourweek_work_num,create_time,update_time"