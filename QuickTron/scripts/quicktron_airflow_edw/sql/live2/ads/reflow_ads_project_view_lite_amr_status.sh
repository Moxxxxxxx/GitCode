#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/

start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select project_code,amr_type,amr_total_num,amr_online_num,amr_offline_num,amr_execute_num,amr_off_execute_num,is_version,count_date from ads.ads_project_view_lite_amr_status where d >= date_sub('\${pre1_date}',7) and d <= '\${pre1_date}'
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin mysqlwriter 
\--column project_code,amr_type,amr_total_num,amr_online_num,amr_offline_num,amr_execute_num,amr_off_execute_num,is_version,count_date
\--ipAddress 007.bg.qkt 
\--port 3306 
\--dataBase evo_wds_base
\--table ads_project_view_lite_amr_status 
\--preSql delete from ads_project_view_lite_amr_status where date(count_date) >= DATE_SUB('\${pre1_date}',INTERVAL 7 DAY) and date(count_date) <= '\${pre1_date}'
\--passWord quicktron123456 
\--userName root 
\--channel 1" "ads_project_view_lite_amr_status"