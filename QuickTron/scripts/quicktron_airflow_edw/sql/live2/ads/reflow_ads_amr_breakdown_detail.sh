#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/

start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select data_time,project_code,happen_time,carr_type_des,amr_type,amr_type_des,amr_code,error_level,error_des,error_code,error_module,end_time,error_duration,d,pt from ads.ads_amr_breakdown_detail where pt = 'A51488' and d >= date_sub('\${pre1_date}',7) and d <= '\${pre1_date}'
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin mysqlwriter 
\--column data_time,project_code,happen_time,carr_type_des,amr_type,amr_type_des,amr_code,error_level,error_des,error_code,error_module,end_time,error_duration,d,pt
\--ipAddress 007.bg.qkt 
\--port 3306 
\--dataBase evo_wds_base
\--table ads_amr_breakdown_detail 
\--preSql delete from ads_amr_breakdown_detail where d >= DATE_SUB('\${pre1_date}',INTERVAL 7 DAY) and d <= '\${pre1_date}'
\--passWord quicktron123456 
\--userName root 
\--channel 1" "ads_amr_breakdown_detail"