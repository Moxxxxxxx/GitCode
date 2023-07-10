#!/bin/bash

start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select error_id,date_format(happen_time,'yyyy-MM-dd 00:00:00') as data_time,happen_time,amr_type,carr_type_des as carry_type_des,'' as carry_type,amr_type_des,amr_code,error_level,error_des,error_code,error_module,end_time,error_duration,project_code
 from ads.ads_phx_amr_breakdown_detail where d>=date_sub('\${pre1_date}',10)
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin clickhousewriter 
\--column error_id,data_time,happen_time,amr_type,carry_type_des,carry_type,amr_type_des,amr_code,error_level,error_des,error_code,error_module,end_time,error_duration,project_code
\--ipAddress 006.bg.qkt 
\--port 8123 
\--dataBase ads 
\--table local_ads_phx_amr_breakdown_detail 
\--passWord  pPTqoeOVaWJ8pQ==
\--userName  super_user
\--preSql alter table ads.local_ads_phx_amr_breakdown_detail delete where toDate(formatDateTime(toDate(substring(happen_time,1,10)),'%Y-%m-%d'))>=addDays(toDate('\${pre1_date}'),-10)
\--channel 1" "ads_phx_amr_breakdown_detail"
