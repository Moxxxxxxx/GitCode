#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：      
#-- 功能描述 ： 金蝶物料多语言表
#-- 注意 ：每日T-1全量
#-- 输入表 : kingdee.t_bd_material_l
#-- 输出表 ：ods.ods_qkt_kde_bd_material_lang_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2023-02-22 CREATE 

# ------------------------------------------------------------------------------------------------



/opt/module/datax/bin/start-datax.sh "\--readerPlugin mysqlreader 
\--ipAddress 008.bg.qkt 
\--port 3306 
\--dataBase kingdee 
\--userName root 
\--passWord quicktron123456 
\--querySql select FPKID,FMATERIALID,FLOCALEID,FNAME,FSPECIFICATION,FDESCRIPTION,'\${pre1_date}' as d from t_bd_material_l
\--separator 
\--writerPlugin hivewriter 
\--dataBase ods 
\--table ods_qkt_kde_bd_material_lang_df 
\--defaultFs hdfs://001.bg.qkt:8020 
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--writeMode overwrite 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/ 
\--partition d
\--column id,material_id,locale_id,material_name,specification,material_desc,d" "ods_qkt_kde_bd_material_lang_df"