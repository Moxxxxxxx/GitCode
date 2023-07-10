#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads.ads_team_ft_engineer_member_work_detail; 

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##生产制造部生产数据统计明细表 ads_team_ft_engineer_member_work_detail
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_team_ft_engineer_member_work_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_team_ft_engineer_member_work_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,process_instance_id,business_id,process_start_time,applicant_user_name,production_date,work_order_number,project_code,project_name,project_ft,product_process,product_part_number,material_name,model_code,product_name,production_standard_working_hour,standard_time_minutes,agv_standard_time,harness_or_parts_standard_time,queue_number_month,queue_number_day,plan_num_month,plan_num_day,plan_num_rate,predict_production_hours_month,predict_production_hours_day,production_number,all_working_hours_minutes,operator_name,leave_type,free_time,working_hours,person_production_number,person_production_hours_minutes,all_losing_hours_minutes,person_losing_hours_minutes,production_efficiency,attendance_efficiency,scheduling_efficiency,plan_reach_rate,losing_rate,production_efficiency_targets,attendance_efficiency_targets,scheduling_efficiency_targets,plan_achievement_rate,working_hours_deviation,total_work_hour,normal_work_hour,overtime_work_hour,actual_work_hour,work_hour_scope,work_order_sort,product_part_sort,create_time,update_time"