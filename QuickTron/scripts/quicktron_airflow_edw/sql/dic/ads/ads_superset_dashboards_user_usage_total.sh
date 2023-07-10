#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset各看板总使用数量
#-- 注意 ： 
#-- 输入表 : dws.dws_report_dashboard_daycount
#-- 输出表 ：ads.ads_superset_dashboards_usage_total
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-11-15 CREATE
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
--各看板使用用户分布 ads_superset_dashboards_user_usage_total （superset看板）

INSERT overwrite table ${ads_dbname}.ads_superset_dashboards_user_usage_total
SELECT 
       	dashboard_id                                            as dashboard_id,
       	dashboard_name                                          as dashboard_name,
       	user_name                                               as user_name,
       	personal_usage_count                                    as personal_usage_count
FROM 
		(SELECT 
        		dashboard_id,
                dashboard_name,
                user_cname as user_name, 
                SUM(operation_count) as personal_usage_count
      	FROM 
        		${dws_dbname}.dws_report_dashboard_daycount 
      	WHERE 
        		dashboard_id <> 'UNKNOWN'
      	GROUP BY 
        		dashboard_id,
                dashboard_name,
                user_cname) t
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"