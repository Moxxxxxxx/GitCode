#!/bin/bash

start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select data_time,COALESCE(breakdown_id,'') as breakdown_id,amr_code,amr_type,carry_order_num,right_order_num,amr_task,total_charge,COALESCE(error_duration,0) as error_duration,mttr_error_num,COALESCE(mttr_error_duration,0) as mttr_error_duration,COALESCE (start_time,'') as start_time,COALESCE(end_time,'') as end_time,COALESCE(actual_duration,0) as actual_duration,project_code,happen_time,COALESCE(add_breakdown_id,'') as add_breakdown_id from ads.ads_phx_lite_amr_breakdown where d>=date_sub('\${pre1_date}',10)
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin clickhousewriter 
\--column data_time,breakdown_id,amr_code,amr_type,carry_order_num,right_order_num,amr_task,total_charge,error_duration,mttr_error_num,mttr_error_duration,start_time,end_time,actual_duration,project_code,happen_time,add_breakdown_id
\--ipAddress 006.bg.qkt 
\--port 8123 
\--dataBase ads 
\--table local_ads_phx_lite_amr_breakdown 
\--passWord  pPTqoeOVaWJ8pQ==
\--userName  super_user
\--preSql alter table ads.local_ads_phx_lite_amr_breakdown delete where toDate(formatDateTime(toDate(substring(happen_time,1,10)),'%Y-%m-%d'))>=addDays(toDate('\${pre1_date}'),-10)
\--channel 1" "ads_phx_lite_amr_breakdown"
