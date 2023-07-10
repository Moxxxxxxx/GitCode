#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/

start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select error_time,project_code,project_name,project_ft,is_active,system_version,upper_computer_version,low_computer_version,first_classification,agv_type_code,agv_type_name,agv_code,breakdown_id,error_level,error_code,error_name,error_display_name,end_time,error_duration,d,pt from ads.ads_project_operate_breakdown_detail where d >= date_sub('\${pre1_date}',7) and d <= '\${pre1_date}'
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=spark;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin mysqlwriter 
\--column error_time,project_code,project_name,project_ft,is_active,system_version,upper_computer_version,low_computer_version,first_classification,agv_type_code,agv_type_name,agv_code,breakdown_id,error_level,error_code,error_name,error_display_name,end_time,error_duration,d,pt
\--ipAddress 007.bg.qkt 
\--port 3306 
\--dataBase ads
\--table ads_project_operate_breakdown_detail 
\--preSql delete from ads_project_operate_breakdown_detail where d >= DATE_SUB('\${pre1_date}',INTERVAL 7 DAY) and d <= '\${pre1_date}'
\--passWord quicktron123456 
\--userName root 
\--channel 1" "ads_project_operate_breakdown_detail"




