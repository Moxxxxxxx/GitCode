#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     


ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_project_frame_production_material_cost_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##ads_project_frame_production_material_cost_detail    --项目车架生产材料费用明细表

/opt/module/sqoop-1.4.7/bin/sqoop export \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_project_frame_production_material_cost_detail \
--columns "order_bill_no,frame_no,start_date,end_date,duration_days,pick_bill_no,pick_bill_date,material_id,material_number,material_name,pick_quantity,consume_quantity,pick_price,create_time,update_time" \
--hcatalog-database ads \
--hcatalog-table ads_project_frame_production_material_cost_detail \
--input-fields-terminated-by "\t" \
--num-mappers 1  