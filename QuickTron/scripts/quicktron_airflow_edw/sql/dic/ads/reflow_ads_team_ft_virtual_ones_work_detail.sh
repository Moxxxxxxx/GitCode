#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_team_ft_virtual_ones_work_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：缺陷任务需求明细 ads_team_ft_virtual_ones_work_detail （研发团队能效）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_team_ft_virtual_ones_work_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_team_ft_virtual_ones_work_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,team_ft,team_group,team_sub_group,team_member,is_job,org_role_type,virtual_role_type,module_branch,virtual_org_name,project_classify_name,sprint_classify_name,ones_work_id,ones_create_date,ones_open_date,ones_update_date,work_type,work_status,work_summary,work_desc,create_time,update_time"