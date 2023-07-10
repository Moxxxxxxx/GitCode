#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset各看板总使用数量
#-- 注意 ： 
#-- 输入表 : dws.dws_report_dashboard_daycount
#-- 输出表 ：ads.ads_superset_dashboards_usage_daily
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-11-16 CREATE
#-- 2 查博文 2021-11-16 导入到接口表字段修改
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
--逐天各看板总使用数量 ads_superset_dashboards_usage_daily （superset看板）

INSERT overwrite table ${ads_dbname}.ads_superset_dashboards_usage_daily
SELECT 
        dashboard_id                                            as dashboard_id,
        dashboard_name                                          as dashboard_name,
        user_name                                               as user_name,
        personal_usage_count                                    as usage_count,
        start_date                                              as date_node
FROM 
    (SELECT 
            dashboard_id,
                dashboard_name,
                user_cname as user_name, 
                SUM(operation_count) as personal_usage_count,
                start_date
        FROM 
            ${dws_dbname}.dws_report_dashboard_daycount 
        WHERE 
            dashboard_id <> 'UNKNOWN'
        GROUP BY 
            dashboard_id,
                dashboard_name,
                user_cname,
                start_date
        ORDER BY 
            user_name DESC
                ) t
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"