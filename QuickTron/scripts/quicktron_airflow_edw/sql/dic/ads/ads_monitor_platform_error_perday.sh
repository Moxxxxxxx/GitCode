#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 监控平台钉钉自动工单表
#-- 注意 ： 
#-- 输入表 : dws.dws_monitor_platform_auto_work_order
#-- 输出表 ：ads.ads_monitor_platform_error_perday
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-12-21 CREATE
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
--监控平台钉钉自动工单表 ads_monitor_platform_error_perday 

INSERT overwrite table ${ads_dbname}.ads_monitor_platform_error_perday
SELECT 
    aa.date_node                                as date_node,
    SUM(evo_notification_error)                 as evo_notification_error_count,
    SUM(slave_mysql_error)                      as slave_mysql_error_count,
    SUM(evo_interface_error)                    as evo_interface_error_count,
    SUM(evo_rcs_error)                          as evo_rcs_error_count,
    SUM(master_ram_error)                       as master_ram_error_count,
    SUM(evo_wcs_g2p_error)                      as evo_wcs_g2p_error_count,
    SUM(master_redis_error)                     as master_redis_error_count,
    SUM(rcs_log_error)                          as rcs_log_error_count,
    SUM(master_mysql_error)                     as master_mysql_error_count,
    SUM(evo_basic_error)                        as evo_basic_error_count,
    SUM(evo_station_error)                      as evo_station_error_count
FROM
(
    SELECT
        TO_DATE(work_order_create_time) as date_node,
        COUNT(CASE WHEN error_detail = 'evo-notification 容器状态异常 ' THEN 1 ELSE NULL END) as evo_notification_error,
        COUNT(CASE WHEN error_detail = 'slave-node mysql进程异常' THEN 1 ELSE NULL END) as slave_mysql_error,
        COUNT(CASE WHEN error_detail = 'evo-interface 容器状态异常 ' THEN 1 ELSE NULL END) as evo_interface_error,
        COUNT(CASE WHEN error_detail = 'evo-rcs 容器状态异常 ' THEN 1 ELSE NULL END) as evo_rcs_error,
        COUNT(CASE WHEN error_detail = 'master-node 内存可用小于10% ' THEN 1 ELSE NULL END) as master_ram_error,
        COUNT(CASE WHEN error_detail = 'evo-wcs-g2p 容器状态异常 ' THEN 1 ELSE NULL END) as evo_wcs_g2p_error,
        COUNT(CASE WHEN error_detail = 'redis-on-master redis进程异常' THEN 1 ELSE NULL END) as master_redis_error,
        COUNT(CASE WHEN error_detail = 'evo-rcs-log 容器状态异常 ' THEN 1 ELSE NULL END) as rcs_log_error,
        COUNT(CASE WHEN error_detail = 'master-node mysql进程异常' THEN 1 ELSE NULL END) as master_mysql_error,
        COUNT(CASE WHEN error_detail = 'evo-basic 容器状态异常 ' THEN 1 ELSE NULL END) as evo_basic_error,
        COUNT(CASE WHEN error_detail = 'evo-station 容器状态异常 ' THEN 1 ELSE NULL END) as evo_station_error
    FROM 
        ${dws_dbname}.dws_monitor_platform_auto_work_order 
    GROUP BY 
        TO_DATE(work_order_create_time)

    UNION ALL

    SELECT 
        to_date(days) as date_node,
        0 as evo_notification_error,
        0 as slave_mysql_error,
        0 as evo_interface_error,
        0 as evo_rcs_error,
        0 as master_ram_error,
        0 as evo_wcs_g2p_error,
        0 as master_redis_error,
        0 as rcs_log_error,
        0 as master_mysql_error,
        0 as evo_basic_error,
        0 as evo_station_error
    FROM 
        ${dim_dbname}.dim_day_date 
    WHERE 
        days BETWEEN (SELECT TO_DATE(MIN(work_order_create_time)) FROM ${dws_dbname}.dws_monitor_platform_auto_work_order  ) AND current_date() 
) aa
GROUP BY 
    aa.date_node;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"