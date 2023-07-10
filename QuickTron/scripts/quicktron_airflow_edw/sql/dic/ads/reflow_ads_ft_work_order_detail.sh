#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     




ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_ft_work_order_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ads_ft_work_order_detail    --工单明细表（项目工单质量）
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_ft_work_order_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_ft_work_order_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,project_ft,project_code,project_name,project_operation_state,product_name,current_version,project_area,project_priority,project_area_group,pms_project_operation_state,pms_project_status,ticket_id,summary,first_category,second_category,third_category,first_class,second_class,third_class,memo,case_status,is_closed,case_origin_code,duty_type,work_order_type,problem_type,created_time,te_user_name,te_user_position,respond_time,respond_user,respond_user_position,respond_duration,is_te_autonomous,to_rb_time,task_no,issue_type_cname,ones_create_time,ones_assign_user_dept,ones_assign_user,ones_assign_user_position,ones_solver_user_dept,ones_solver_user,ones_solver_user_position,ones_solve_time,ones_solve_duration,ones_close_user_dept,ones_close_user,ones_close_user_position,ones_close_time,ones_close_duration,workorder_close_type,workorder_close_user,workorder_close_user_position,workorder_close_time,workorder_close_duration,person_charge_num,is_repeat_activate,create_time,update_time"