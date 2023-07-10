#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset各看板总使用数量
#-- 注意 ： 
#-- 输入表 : dwd.dwd_report_action_log_info_da
#-- 输出表 ：ads.ads_superset_sql_lab_activity_detail
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-11-15 CREATE
#-- 2 查博文 2021-11-15 user_name应为user_cname
# ------------------------------------------------------------------------------------------------
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dws_dbname=dws
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-------------------------------------------------------------------------------------------------------------00
--sqllab详表 ads_superset_sql_lab_activity_detail （superset sqllab详表）

INSERT overwrite table ${ads_dbname}.ads_superset_sql_lab_activity_detail
SELECT 
       user_cname                                         as user_name,
       record_create_time                                 as record_create_time,
       duration_ms                                        as duration_ms
FROM(
		SELECT 
        	user_cname,
            record_create_time,
            duration_ms 
      	FROM 
        	${dwd_dbname}.dwd_report_action_log_info_da 
      	WHERE 
        	user_action = 'sql_json'
      	ORDER BY 
        	record_create_time DESC
    ) t
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"