#!/bin/bash


# --------------------------------------------------------------------------------------------------
# 项目运营大表

# ------------------------------------------------------------------------------------------------


/opt/module/datax/bin/start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select id, project_code, project_name, pms_project_operation_state, pms_project_status, project_ft, active_agv, period_front, period_back, period, error_list, error_num, carry_order_num, agv_num_total, carry_task_num, theory_time, error_duration, mttr_error_duration, mttr_error_num, add_mtbf, d from ads.ads_amr_breakdown_general where d >= date_sub('\${pre1_date}',9) and d <= '\${pre1_date}'
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/ 
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin mysqlwriter 
\--column id, project_code, project_name, pms_project_operation_state, pms_project_status, project_ft, active_agv, period_front, period_back, period, error_list, error_num, carry_order_num, agv_num_total, carry_task_num, theory_time, error_duration, mttr_error_duration, mttr_error_num, add_mtbf, d
\--ipAddress 007.bg.qkt 
\--port 3306 
\--dataBase ads 
\--table ads_amr_breakdown_general
\--preSql delete from ads_amr_breakdown_general where d >= DATE_SUB('\${pre1_date}',INTERVAL 9 DAY) and d <= '\${pre1_date}'
\--passWord quicktron123456 
\--userName root" "ads_amr_breakdown_general"