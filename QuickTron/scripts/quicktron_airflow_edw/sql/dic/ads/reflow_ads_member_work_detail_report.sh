#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_member_work_detail_report;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：成员工时周报(excel版)明细表 ads_member_work_detail_report
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_member_work_detail_report \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_member_work_detail_report \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,org_name,team_member,dtk_emp_id,emp_position,work_date,day_type,work_hour,clock_in_hour,attendance_working_time,attendance_working_place,attendance_off_time,attendance_off_place,leave_days,travel_days,home_office_days,official_out_days,demand_qty,demand_ones_ids,bug_qty,bug_ones_ids,task_qty,task_ones_ids,work_order_qty,work_order_ones_ids,add_lines_count,internal_wh,internal_occ,external_wh,external_occ,mgmt_wh,mgmt_occ,create_time,update_time"