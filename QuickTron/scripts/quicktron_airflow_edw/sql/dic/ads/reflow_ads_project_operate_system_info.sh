#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/

start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select project_code,project_name,project_ft,robot_type_code,robot_num,upper_computer_version,low_computer_version,system_version,d,pt from ads.ads_project_operate_system_info where d = '\${pre1_date}'
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=spark;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin mysqlwriter 
\--column project_code,project_name,project_ft,robot_type_code,robot_num,upper_computer_version,low_computer_version,system_version,d,pt
\--ipAddress 007.bg.qkt 
\--port 3306 
\--dataBase ads
\--table ads_project_operate_system_info 
\--preSql delete from ads_project_operate_system_info where d <= '\${pre1_date}'
\--passWord quicktron123456 
\--userName root 
\--channel 1" "ads_project_operate_system_info"




