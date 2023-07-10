#!/bin/bash

start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select data_time,COALESCE(breakdown_id,'') as breakdown_id,carry_order_num,carry_task_num,amr_type,amr_type_des,mttr_error_num,amr_code,COALESCE(theory_time,0) as theory_time,COALESCE(error_duration,0) as error_duration,COALESCE(mttr_error_duration,0) as mttr_error_duration,COALESCE(add_mtbf,0) as add_mtbf,type_class,project_code,happen_time,COALESCE(add_breakdown_id,'') as add_breakdown_id from ads.ads_phx_amr_breakdown  where d>=date_sub('\${pre1_date}',10)
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin clickhousewriter 
\--column data_time,breakdown_id,carry_order_num,carry_task_num,amr_type,amr_type_des,mttr_error_num,amr_code,theory_time,error_duration,mttr_error_duration,add_mtbf,type_class,project_code,happen_time,add_breakdown_id
\--ipAddress 006.bg.qkt 
\--port 8123 
\--dataBase ads 
\--table local_ads_phx_amr_breakdown 
\--passWord  pPTqoeOVaWJ8pQ==
\--userName  super_user
\--preSql alter table ads.local_ads_phx_amr_breakdown delete where toDate(formatDateTime(toDate(substring(happen_time,1,10)),'%Y-%m-%d'))>=addDays(toDate('\${pre1_date}'),-10)
\--channel 1" "ads_phx_amr_breakdown"