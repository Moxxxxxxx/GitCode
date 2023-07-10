#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ： d     
#-- 功能描述 ： 
#-- 注意 ：
#-- 输入表 : 
#-- 输出表 
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-03-15 CREATE 

# ------------------------------------------------------------------------------------------------

start-datax.sh "\--readerPlugin mysqlreader 
\--ipAddress 008.bg.qkt 
\--port 3306 
\--dataBase  ops_platform
\--userName root 
\--passWord quicktron123456 
\--querySql  select  id, version_record_id, app_name, app_version, create_time, current_status, project_code,'\${pre1_date}' as d from ops_version_app_record
\--separator 
\--writerPlugin hivewriter 
\--dataBase dwd 
\--table dwd_ops_version_app_record_info_df 
\--defaultFs hdfs://001.bg.qkt:8020 
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--writeMode overwrite 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/ 
\--partition d 
\--column id, version_record_id, app_name, app_version, create_time:2, current_status, project_code,d" "dwd_ops_version_app_record_info_df"