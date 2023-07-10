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
\--querySql  select a.id, a.product_version, a.file_name, a.deploy_way, a.operation_user, a.operation_account, a.operation_time, a.operation_reason, a.code, a.remark, a.create_time, a.relation_id, a.apps, a.project_code,b.file_type,if(b.file_type is not null,substr(product_version,1,5),null) as product_big_version,'\${pre1_date}' as d
from
ops_version_record a
left join 
(select 
* 
from 
ops_deploy_record 
where file_type IN ( 'big_zip', 'class_zip' )
)
 b on a.relation_id=b.id
\--separator 
\--writerPlugin hivewriter 
\--dataBase dwd 
\--table dwd_ops_version_record_info_df 
\--defaultFs hdfs://001.bg.qkt:8020 
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--writeMode overwrite 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/ 
\--partition d 
\--column id, product_version, file_name, deploy_way, operation_user, operation_account, operation_time:2, operation_reason, code, remark, create_time:2, relation_id, apps, project_code,file_type,product_big_version,d" "dwd_ops_version_record_info_df"