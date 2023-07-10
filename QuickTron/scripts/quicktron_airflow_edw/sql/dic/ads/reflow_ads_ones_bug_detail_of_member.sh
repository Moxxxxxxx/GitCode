#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_ones_bug_detail_of_member;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ads_ones_bug_detail_of_member ones缺陷人员明细表
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_ones_bug_detail_of_member \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_ones_bug_detail_of_member \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,team_member,team_ft,team_group,team_sub_group,emp_position,is_job,hired_date,quit_date,ones_work_id,ones_project_uuid,ones_project_name,project_type_name,project_bpm_code,project_bpm_name,sprint_classify_name,ones_summary,ones_desc,work_priority,critical_level,ineffective_type,task_create_member,reopen_times,total_solve_duration,total_verify_duration,create_time,update_time"