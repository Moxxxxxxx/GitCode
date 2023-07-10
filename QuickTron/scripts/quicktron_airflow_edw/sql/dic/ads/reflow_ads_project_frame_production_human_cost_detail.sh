#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     


ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_project_frame_production_human_cost_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##ads_project_frame_production_human_cost_detail    --项目车架生产人工费用明细表

/opt/module/sqoop-1.4.7/bin/sqoop export \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_project_frame_production_human_cost_detail \
--columns "frame_numbers,operator_name,person_working_hours,person_losing_hours,cost_rate,person_working_cost,person_losing_cost,create_time,update_time" \
--hcatalog-database ads \
--hcatalog-table ads_project_frame_production_human_cost_detail \
--input-fields-terminated-by "\t" \
--num-mappers 1  