#!/bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： 运维监控平台钉钉自动工单表
#-- 注意 ： 每日t-1分区
#-- 输入表 ：dwd.dwd_ones_work_order_info_df
#-- 输出表 ：dws.dws_monitor_platform_auto_work_order
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-12-19 CREATE 
#-- 1 查博文 2021-12-21 将'工单恢复','工单解决'并入处理中状态
#-- 2 查博文 2021-12-21 将'工单恢复','工单解决'并入已关闭状态
# ------------------------------------------------------------------------------------------------
hive=/opt/module/hive-3.1.2/bin/hive
ods_dbname=ods
dim_dbname=dim
dwd_dbname=dwd
dws_dbname=dws

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else 
    pre1_date=`date -d "-1 day" +%F`
fi

if [ -n "$1" ] ;then
    pre2_date=`date -d "-1 day $1" +%F`
else
    pre2_date=`date -d "-2 day" +%F`
fi

echo "##############################################hive:{start executor dwd}####################################################################"

sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
-------------------------------------------------------------------------------------------------------------00
--监控平台钉钉自动工单表 dws_monitor_platform_auto_work_order 

INSERT overwrite table ${dws_dbname}.dws_monitor_platform_auto_work_order
SELECT
    ones_work_order_uuid  as  ones_work_order_uuid,
    ticket_id             as  ticket_id,
    project_code          as  project_code,
	CASE 
    WHEN work_order_status IN ('工单：恢复','工单：解决') THEN '已关闭' 
    ELSE work_order_status 
    END                   as  work_order_status,
    created_time          as  work_order_create_time,
    status_time           as  work_order_status_update_time,
    CASE 
    WHEN memo IN ('evo-rcs 容器状态异常 ','evo-rcs 容器运行状态异常 ') THEN 'evo-rcs 容器状态异常 ' 
    WHEN memo IN ('evo-wcs-g2p 容器状态异常 ','evo-wcs-g2p 容器运行状态异常 ') THEN 'evo-wcs-g2p 容器状态异常 ' 
    ELSE memo 
    END                   as  error_detail
FROM(
    SELECT 
      a.ones_work_order_uuid,
      a.ticket_id,project_code,
      a.work_order_status,
      a.created_time,
      a.status_time,
      regexp_replace(LTRIM(split(b.memo_array,':')[0]),'[{\"]','') as memo
    FROM 
      ${dwd_dbname}.dwd_ones_work_order_info_df a
      lateral view explode(split(a.memo,',')) b as memo_array 
    WHERE 
      a.created_user IN ('普勇军','monitor_robot','monitor-reboot')
      AND a.project_code NOT IN ('TEST001','TE-tese2','TE-test','A66666','TEST3')
      AND d = '${pre1_date}'
      AND a.created_time > '2021-11-01 05:00:02'
      AND memo NOT LIKE '%组件状态异常%'
      AND memo NOT LIKE '%exporter%'
  	) t  
-----------------------------------------------------------------------------------------------------------------------------00
"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"

$hive -e "$sql"

