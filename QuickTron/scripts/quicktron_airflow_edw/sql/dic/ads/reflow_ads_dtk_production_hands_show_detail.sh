#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     


ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_dtk_production_hands_show_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##ads_dtk_production_hands_show_detail    --钉钉举手单明细表
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_dtk_production_hands_show_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_dtk_production_hands_show_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,org_name,business_id,approval_user_names,process_start_time,process_end_time,applicant_dept_name,team_member,approval_result,approval_status,approval_title,work_order_number,work_order_type,product_agv_type,production_procedure,influence_people_number,problem_desc,exception_picture_desc,confirmation_response,liability_judgment,judgment_basis_description,problem_cause,interim_measures,question_type,create_time,update_time"