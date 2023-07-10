#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：      
#-- 功能描述 ： datatron-生产计划效率线下表
#-- 注意 ：每日全量
#-- 输入表 :  datatron.production_attendance_scheduling_plan
#-- 输出表 ：dim.dim_product_plan_achievement_rate_offline
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangziming 2022-12-19 CREATE 
#-- 2 wangziming 2022-12-28 modify 增加字段的判断逻辑

# ------------------------------------------------------------------------------------------------

#\--querySql select id,process,replace(production_efficiency_targets,'%','')/100 as production_efficiency_targets,replace(attendance_efficiency_targets,'%','')/100 as attendance_efficiency_targets,replace(scheduling_efficiency_targets,'%','')/100 as scheduling_efficiency_targets,replace(plan_achievement_rate,'%','')/100 as plan_achievement_rate from  production_attendance_scheduling_plan



/opt/module/datax/bin/start-datax.sh "\--readerPlugin mysqlreader 
\--ipAddress 007.bg.qkt 
\--port 3306 
\--dataBase datatron 
\--userName root 
\--passWord quicktron123456 
\--querySql select id,process,if(production_efficiency_targets>1,production_efficiency_targets/100,production_efficiency_targets) as production_efficiency_targets, 
if(attendance_efficiency_targets>1,attendance_efficiency_targets/100,attendance_efficiency_targets) as attendance_efficiency_targets,
if(scheduling_efficiency_targets>1,scheduling_efficiency_targets/100,scheduling_efficiency_targets) as scheduling_efficiency_targets,
if(plan_achievement_rate>1,plan_achievement_rate/100,plan_achievement_rate) as plan_achievement_rate
from (select id,process,replace(production_efficiency_targets,'%','') as production_efficiency_targets,replace(attendance_efficiency_targets,'%','') as attendance_efficiency_targets,replace(scheduling_efficiency_targets,'%','') as scheduling_efficiency_targets,replace(plan_achievement_rate,'%','') as plan_achievement_rate from  production_attendance_scheduling_plan) a
\--separator 
\--writerPlugin hivewriter 
\--dataBase dim 
\--table dim_product_plan_achievement_rate_offline 
\--defaultFs hdfs://001.bg.qkt:8020 
\--hiveSetSql set hive.execution.engine=mr;set mapreduce.job.queuename=hive;
\--writeMode overwrite 
\--tmpDataBase tmp 
\--tmpPath /user/hive/warehouse/tmp.db/ 
\--column id,process,production_efficiency_targets,attendance_efficiency_targets,scheduling_efficiency_targets,plan_achievement_rate" "dim_product_plan_achievement_rate_offline"