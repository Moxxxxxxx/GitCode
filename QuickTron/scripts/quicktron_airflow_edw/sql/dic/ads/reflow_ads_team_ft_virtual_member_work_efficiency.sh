#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads.ads_team_ft_virtual_member_work_efficiency; 

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：团队小组成员效能 ads_team_ft_virtual_member_work_efficiency （研发团队能效）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_team_ft_virtual_member_work_efficiency \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_team_ft_virtual_member_work_efficiency \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,team_ft,team_group,team_sub_group,team_member,is_job,role_type,module_branch,virtual_org_name,project_classify_name,work_date,day_type,code_quantity,work_hour,newly_increased_defect_num,solve_defect_num,close_defect_num,newly_increased_task_num,solve_task_num,close_task_num,newly_increased_demand_num,solve_demand_num,close_demand_num,create_time,update_time"