#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"




ssh -tt 001.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_ones_manhour_dismemberment_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：各看板使用用户分布 ads_ones_manhour_dismemberment_detail （superset看板）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_ones_manhour_dismemberment_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_ones_manhour_dismemberment_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,team_ft,team_group,team_sub_group,team_member,emp_position,is_job,hired_date,quit_date,is_need_fill_manhour,org_role_type,virtual_role_type,module_branch,virtual_org_name,project_org_name,project_classify_name,sprint_classify_name,external_project_code,external_project_name,project_bpm_code,project_bpm_name,project_type_name,work_create_date,work_id,work_summary,work_desc,work_type,work_status,work_check_date,day_type,work_hour,actual_date,error_type,work_hour_total,work_hour_rate,cost_amount,create_time,update_time"