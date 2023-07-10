#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/

start-datax.sh "\--readerPlugin hivereader 
\--hiveSql SELECT happen_time, amr_type, amr_code, error_level, error_des, error_code, error_module, end_time, error_duration, project_code from ads.ads_amr_breakdown_detail where d>=date_sub('\${pre1_date}',7)
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=spark;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin clickhousewriter 
\--column happen_time, amr_type, amr_code, error_level, error_des, error_code, error_module, end_time, error_duration, project_code
\--ipAddress 006.bg.qkt 
\--port 8123 
\--dataBase ads
\--table local_ads_amr_breakdown_detail 
\--passWord pPTqoeOVaWJ8pQ==
\--userName super_user 
\--preSql alter table ads.local_ads_amr_breakdown_detail delete where toDate(formatDateTime(happen_time,'%Y-%m-%d'))>=addDays(toDate('\${pre1_date}'),-7)
\--channel 1" "reflow_ads_amr_breakdown_detail_ck"
