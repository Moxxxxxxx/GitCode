#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     


ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads.ads_team_ft_rd_member_quality_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##研发成员质量明细表 ads_team_ft_rd_member_quality_detail
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_team_ft_rd_member_quality_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_team_ft_rd_member_quality_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,work_id,work_summary,task_create_time,task_update_time,task_status,severity_level,defect_validity,sprint_classify_name,project_classify_name,bug_type,defect_discovery_stage,defect_best_discovery_stage,assign_team_ft,assign_team_group,assign_team_sub_group,assign_team_last_group,assign_team_member,repair_team_ft,repair_team_group,repair_team_sub_group,repair_team_last_group,repair_team_member,create_time,update_time"