#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_member_work_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：人员工作明细表 ads_member_work_detail
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_member_work_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_member_work_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,dtk_user_id,team_member,org_path,emp_position,work_date,ones_work_hour,clock_in_work_hour,workhour_deviation,is_deviation,is_business,is_leave,is_home_office,home_office_location,demand_ones_id,task_ones_id,bug_ones_id,work_order_ones_id,mgmt_work_hour,ineffective_work_hour,saturation,code_error_work_hour,wh_check_times,unusual_wh_check_times,internal_project_wh,internal_project_summary,external_project_wh,external_project_summary,ones_demand_detail,ones_task_detail,ones_bug_detail,ones_work_detail,create_time,update_time"