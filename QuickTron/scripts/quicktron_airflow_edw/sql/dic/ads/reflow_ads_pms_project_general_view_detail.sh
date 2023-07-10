#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/
#json_name=(ads_pms_project_general_view_detail.json ads_project_view_project_info.json ads_project_view_project_process.json)

#ssh -tt hadoop@003.bg.qkt <<effo
#for json in ${json_name[@]}; do $datax  -p "-Dpre1_date='${pre1_date}'" $json_dir$json;done
#exit
#effo

start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select project_code_class,project_code,project_sale_code,project_name,project_info,project_product_name,project_type,project_dispaly_state,project_dispaly_state_group,project_ft,project_priority,project_current_version,sales_area_director,project_area,pm_name,spm_name,sap_counselor,sales_person,pre_sales_consultant,pe_members,amount,contract_signed_year,cast(contract_signed_date as date),deliver_goods_achieving_rate,deliver_goods_desc,cast(pre_project_approval_time as date),cast(project_handover_end_time as date),cast(expect_online_date as date),cast(online_date as date),cast(online_overdue_days as int),online_process_month,cast(online_process_month_begin as date),is_online,cast(sap_entry_date as date),cast(online_times as int),cast(no_online_times as int),cast(expect_final_inspection_date as date),cast(final_inspection_date as date),cast(final_inspection_overdue_days as int),final_inspection_process_month,cast(final_inspection_process_month_begin as date),is_final_inspection,final_inspection_times,no_final_inspection_times,post_project_date,project_stage,project_progress_stage,zeroweek_work_num,oneweek_work_num,twoweek_work_num,threeweek_work_num,fourweek_work_num,data_source,project_area_group,pms_project_operation_state,total_agv_num,open_package_agv_num,qualified_rate,pms_project_status,core_project_code,is_main_project,project_type_name,pms_core_project_status,pms_core_project_operation_state,equitment_arrival_date,equitment_arrival_plan_end_date,yf_collection_ratio,dh_collection_ratio,ys_collection_ratio,zb_collection_ratio,pms_core_project_type_name,is_active from ads.ads_pms_project_general_view_detail
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin mysqlwriter 
\--column project_code_class,project_code,project_sale_code,project_name,project_info,project_product_name,project_type,project_dispaly_state,project_dispaly_state_group,project_ft,project_priority,project_current_version,sales_area_director,project_area,pm_name,spm_name,sap_counselor,sales_person,pre_sales_consultant,pe_members,amount,contract_signed_year,contract_signed_date,deliver_goods_achieving_rate,deliver_goods_desc,pre_project_approval_time,project_handover_end_time,expect_online_date,online_date,online_overdue_days,online_process_month,online_process_month_begin,is_online,sap_entry_date,online_times,no_online_times,expect_final_inspection_date,final_inspection_date,final_inspection_overdue_days,final_inspection_process_month,final_inspection_process_month_begin,is_final_inspection,final_inspection_times,no_final_inspection_times,post_project_date,project_stage,project_progress_stage,zeroweek_work_num,oneweek_work_num,twoweek_work_num,threeweek_work_num,fourweek_work_num,data_source,project_area_group,pms_project_operation_state,total_agv_num,open_package_agv_num,qualified_rate,pms_project_status,core_project_code,is_main_project,project_type_name,pms_core_project_status,pms_core_project_operation_state,equitment_arrival_date,equitment_arrival_plan_end_date,yf_collection_ratio,dh_collection_ratio,ys_collection_ratio,zb_collection_ratio,pms_core_project_type_name,is_active
\--ipAddress 007.bg.qkt 
\--port 3306 
\--dataBase ads
\--table ads_pms_project_general_view_detail 
\--preSql truncate table ads_pms_project_general_view_detail 
\--passWord quicktron123456 
\--userName root 
\--channel 1" "ads_pms_project_general_view_detail"


start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select b.project_name,b.project_code,b.project_product_name,b.project_current_version,b.project_ft,b.project_priority,b.project_area,b.pe_members,b.project_progress_stage,b.deliver_goods_achieving_rate,b.pm_name from dim.dim_collection_project_record_ful a join ads.ads_pms_project_general_view_detail b on a.project_code = b.project_code
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin mysqlwriter 
\--column project_name,project_code,project_type,project_version,ft,project_level,to_distinct,pe,pro_stage,completion_rate,pm
\--ipAddress 007.bg.qkt 
\--port 3306 
\--dataBase evo_wds_base
\--table ads_project_view_project_info 
\--preSql truncate table ads_project_view_project_info 
\--passWord quicktron123456 
\--userName root 
\--channel 1" "ads_project_view_project_info"


start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select b.project_progress_stage,b.project_code,b.project_stage,b.deliver_goods_achieving_rate,date(b.pre_project_approval_time),b.contract_signed_date,date(b.project_handover_end_time),date(b.sap_entry_date),b.expect_online_date,b.online_date,b.online_overdue_days,b.expect_final_inspection_date,b.final_inspection_date,b.final_inspection_overdue_days,b.deliver_goods_desc from dim.dim_collection_project_record_ful a join ads.ads_pms_project_general_view_detail b on a.project_code = b.project_code
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin mysqlwriter 
\--column process_stage_group,project_code,process_stage,delivery_completed_rate,lead_time,contract_date,handover_time,entrance_time,planned_launch_time,actual_launch_time,launch_out_time,planned_accept_time,actual_accept_time,accept_out_time,delivery_status
\--ipAddress 007.bg.qkt 
\--port 3306 
\--dataBase evo_wds_base
\--table ads_project_view_project_process 
\--preSql truncate table ads_project_view_project_process 
\--passWord quicktron123456 
\--userName root 
\--channel 1" "ads_project_view_project_process"