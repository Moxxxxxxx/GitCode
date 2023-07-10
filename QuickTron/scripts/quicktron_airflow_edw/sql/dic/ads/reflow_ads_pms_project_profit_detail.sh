#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     


ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_pms_project_profit_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ads_pms_project_profit_detail    --pms项目利润表
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_pms_project_profit_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_pms_project_profit_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,project_code,project_sale_code,project_name,project_priority,project_dispaly_state_group,project_ft,pm_name,project_area,project_area_group,pms_project_operation_state,pms_project_status,core_project_code,is_main_project,pms_core_project_status,pms_core_project_operation_state,pms_core_project_type_name,online_process_date,final_inspection_process_date,post_project_date,online_date,final_inspection_date,is_active,project_income,agv_num,agv_cost,bucket_cost,charging_cost,project_other_matters_cost,export_packing_cost,transportation_cost,ectocyst_software_cost,ectocyst_hardware_cost,pe_cost,mt_service_cost,io_service_cost,op_service_cost,te_cost,ctrip_amount,reimburse_amount,cost_sum,project_gp,project_gp_rate,contract_code,sales_manager,project_type_name,contract_signed_date,contract_amount,currency,exchange_rate,rate_begin_date,rate_end_date,yf_collection_ratio,yf_collection_amount,yf_already_collection_amount,yf_overdays_amount,yf_pay_amount_date,yf_overdays_days,yf_entry_amount,dh_date,dh_collection_ratio,dh_collection_amount,dh_already_collection_amount,dh_overdays_amount,dh_pay_amount_date,dh_overdays_days,dh_entry_amount,ys_date,ys_collection_ratio,ys_collection_amount,ys_already_collection_amount,ys_overdays_amount,ys_pay_amount_date,ys_overdays_days,ys_entry_amount,zb_date,zb_collection_ratio,wk_collection_amount,wk_no_collection_amount,wk_already_collection_amount,wk_overdays_amount,wk_pay_amount_date,wk_overdays_days,wk_entry_amount,sx_date,sx_collection_ratio,sx_collection_amount,sx_already_collection_amount,sx_overdays_amount,sx_pay_amount_date,sx_overdays_days,sx_entry_amount,create_time,update_time"