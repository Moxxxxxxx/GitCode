#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：      
#-- 功能描述 ： datatron-生产计划表
#-- 注意 ：每日全量
#-- 输入表 :  datatron. production_plan
#-- 输出表 ： ods.ods_qkt_datatron_production_plan_df
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-12-19 CREATE 

# ------------------------------------------------------------------------------------------------



/opt/module/datax/bin/start-datax.sh "\--readerPlugin mysqlreader 
\--ipAddress 007.bg.qkt 
\--port 3306 
\--dataBase datatron 
\--userName root 
\--passWord quicktron123456 
\--querySql select *, date_add(current_date(),interval -1 day) as d from production_plan
\--separator 
\--writerPlugin hivewriter 
\--dataBase ods 
\--table ods_qkt_datatron_production_plan_df 
\--defaultFs hdfs://001.bg.qkt:8020 
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--writeMode overwrite 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/ 
\--partition d
\--column id, project_code, project_name, work_order_number, material_number, machine_type, \`group\`, queue_number, start_month, \`first\`, \`second\`, third, fourth, fifth, sixth, seventh, eighth, ninth, tenth, eleventh, twelfth, thirteenth, fourteenth, fifteenth, sixteenth, seventeenth, eighteenth, nineteenth, twentieth, twenty_first, twenty_second, twenty_third, twenty_fourth, twenty_fifth, twenty_sixth, twenty_seventh, twenty_eighth, twenty_ninth, thirtieth, thirtieth_first, name,d:1" "ods_qkt_datatron_production_plan_df"