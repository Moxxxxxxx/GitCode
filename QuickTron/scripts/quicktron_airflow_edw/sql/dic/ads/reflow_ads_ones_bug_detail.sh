#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_ones_bug_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ads_ones_bug_detail ones缺陷明细
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_ones_bug_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_ones_bug_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,ones_project_uuid,ones_project_name,project_type_name,project_bpm_code,project_bpm_name,person_incharge,sprint_classify_name,sprint_create_time,sprint_end_time,work_type,ones_work_id,work_status,work_summary,ones_create_date,ones_update_date,ones_close_time,work_priority,critical_level,is_remove,reopen_time,reopen_type,reopen_type_count,is_ineffective_bug,ineffective_type,bug_first_category,bug_second_category,bug_third_category,task_create_member,task_create_member_ft,task_assign_member,task_assign_member_ft,last_bug_solver,last_bug_solver_ft,total_solve_duration,total_verify_duration,bug_solve_intime_s,bug_verify_intime_s,bug_solve_intime_p,bug_verify_intime_p,create_time,update_time"