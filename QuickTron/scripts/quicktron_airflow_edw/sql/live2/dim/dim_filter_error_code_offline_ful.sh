#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：      
#-- 功能描述 ： 3.x项目现场故障过滤错误码信息记录
#-- 注意 ：每日全量
#-- 输入表 : datatron.dim_filter_error_code
#-- 输出表 ：dim.dim_filter_error_code_offline_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-14 CREATE 

# ------------------------------------------------------------------------------------------------



/opt/module/datax/bin/start-datax.sh "\--readerPlugin mysqlreader 
\--ipAddress 007.bg.qkt 
\--port 3306 
\--dataBase datatron 
\--userName root 
\--passWord quicktron123456 
\--querySql select id,dt_create_time,dt_update_time,project_code,error_code,auto_offline,error_level,error_describe from dim_filter_error_code
\--separator 
\--writerPlugin hivewriter 
\--dataBase dim 
\--table dim_filter_error_code_offline_ful 
\--defaultFs hdfs://001.bg.qkt:8020 
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--writeMode overwrite 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/ 
\--column id,dt_create_time,dt_update_time,project_code,error_code,auto_offline,error_level,error_describe" "dim_filter_error_code_offline_ful"