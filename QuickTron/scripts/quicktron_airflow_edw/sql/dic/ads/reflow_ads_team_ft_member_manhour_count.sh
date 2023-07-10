#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads.ads_team_ft_member_manhour_count; 

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：团队小组成员工时日统计 ads_team_ft_member_manhour_count 
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_team_ft_member_manhour_count \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_team_ft_member_manhour_count \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,team_ft,team_group,team_sub_group,emp_position,team_member,is_job,is_need_fill_manhour,role_type,project_classify_name,work_date,day_type,issue_type_cname,check_num,work_hours,create_time,update_time"