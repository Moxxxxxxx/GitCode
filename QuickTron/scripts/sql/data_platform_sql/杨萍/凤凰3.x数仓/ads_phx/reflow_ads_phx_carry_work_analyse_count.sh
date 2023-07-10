#!/bin/bash

start-datax.sh "\--readerPlugin hivereader 
\--hiveSql select DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as data_time,upstream_work_id,work_id,work_path,start_point,target_point,COALESCE(work_state,'') as work_state,COALESCE(first_classification,'') as first_classification,COALESCE(first_classification_desc,'') as first_classification_desc,COALESCE(agv_type_code,'') as agv_type_code,COALESCE(agv_code,'') as agv_code,robot_num,COALESCE(wotk_duration_total,0) as wotk_duration_total,COALESCE(robot_assign_duration,0) as robot_assign_duration,COALESCE(robot_move_duration,0) as robot_move_duration,COALESCE(station_executor_duration,0) as station_executor_duration,COALESCE(work_create_time,'') as work_create_time,COALESCE(work_complete_time,'') as work_complete_time,project_code from ads.ads_phx_carry_work_analyse_count where d>=date_sub('\${pre1_date}',10)
\--defaultFs hdfs://001.bg.qkt:8020 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--separator 
\--writerPlugin clickhousewriter 
\--column data_time,upstream_work_id,work_id,work_path,start_point,target_point,work_state,first_classification,first_classification_desc,agv_type_code,agv_code,robot_num,wotk_duration_total,robot_assign_duration,robot_move_duration,station_executor_duration,work_create_time,work_complete_time,project_code
\--ipAddress 006.bg.qkt 
\--port 8123 
\--dataBase ads 
\--table local_ads_phx_carry_work_analyse_count 
\--passWord  pPTqoeOVaWJ8pQ==
\--userName  super_user
\--preSql alter table ads.local_ads_phx_carry_work_analyse_count delete where toDate(formatDateTime(toDate(substring(work_complete_time,1,10)),'%Y-%m-%d'))>=addDays(toDate('\${pre1_date}'),-10)
\--channel 1" "ads_phx_carry_work_analyse_count"
