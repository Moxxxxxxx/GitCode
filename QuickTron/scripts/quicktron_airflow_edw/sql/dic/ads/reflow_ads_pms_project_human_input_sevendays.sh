#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     


ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_pms_project_human_input_sevendays;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##ads_pms_project_human_input_sevendays    --pms项目近七天人力投入

/opt/module/sqoop-1.4.7/bin/sqoop export \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_pms_project_human_input_sevendays \
--columns "project_code,project_sale_code,project_name,project_ft,project_priority,project_area,data_source,project_area_group,pms_project_operation_state,pms_project_status,core_project_code,is_main_project,project_type_name,pms_core_project_status,pms_core_project_operation_state,pms_core_project_type_name,is_active,one_day_ago,two_day_ago,three_day_ago,four_day_ago,five_day_ago,six_day_ago,seven_day_ago,pe_total,te_total,se_total,project_total,create_time,update_time" \
--hcatalog-database ads \
--hcatalog-table ads_pms_project_human_input_sevendays \
--input-fields-terminated-by "\t" \
--num-mappers 1  