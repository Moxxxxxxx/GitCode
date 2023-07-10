#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：      
#-- 功能描述 ： 项目现场信息表
#-- 注意 ：每日T-1全量
#-- 输入表 : collection_offline.collection_project_record_info
#-- 输出表 ：dim.dim_collection_project_record_ful
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-08-29 CREATE 
#-- 2 wangziming 2023-01-04 modify 修正部分逻辑

# ------------------------------------------------------------------------------------------------



/opt/module/datax/bin/start-datax.sh "\--readerPlugin mysqlreader 
\--ipAddress 008.bg.qkt 
\--port 3306 
\--dataBase collection_offline 
\--userName root 
\--passWord quicktron123456 
\--querySql SELECT project_code,project_name,project_product_type,project_version,
case project_product_type when '货架到人' then 1
													when '料箱到人' then 2
												  when '料箱搬运' then 3
													when '智能搬运' then 4
													else -1 end as project_product_type_code,
if(project_code='A51488','1',is_nonetwork) as is_nonetwork
FROM collection_project_record_info 
\--separator 
\--writerPlugin hivewriter 
\--dataBase dim 
\--table dim_collection_project_record_ful 
\--defaultFs hdfs://001.bg.qkt:8020 
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--writeMode overwrite 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/ 
\--column project_code,project_name,project_product_type,project_version,project_product_type_code,is_nonetwork" "dim_collection_project_record_ful"