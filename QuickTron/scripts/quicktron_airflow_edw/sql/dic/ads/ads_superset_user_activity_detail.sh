#! /bin/bash

# --------------------------------------------------------------------------------------------------
#-- 运行类型 ： 日跑
#-- 参数 ：     
#-- 功能描述 ： superset各看板总使用数量
#-- 注意 ： 
#-- 输入表 : dwd.dwd_report_action_log_info_da
#-- 输出表 ：ads.ads_superset_user_activity_detail
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 查博文 2021-11-15 CREATE
#-- 2 查博文 2021-11-15 删除多余括号
#-- 3 查博文 2021-11-15 美化
#-- 5 查博文 2021-11-15 时间字段增加ss
#-- 6 王莹莹 2021-12-16 重新编辑逻辑和表设计
#-- 7 王梓明 2021-12-16 优化逻辑
#-- 8 王莹莹 2021-12-17 增加一分钟收敛
#-- 9 查博文 2022-06-01 操作记录时间增加8小时
#-- 10 王莹莹 2022-06-01 按照人员和看板进行一分钟收敛
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
--用户活动详表 ads_superset_user_activity_detail （superset用户明细表）

INSERT overwrite table ${ads_dbname}.ads_superset_user_activity_detail
SELECT 
tmp.id,
DATE_FORMAT(tmp.record_start_time,'yyyy-MM-dd') as d,
tmp.record_start_time,
tmp.duration_ms,
tmp.user_id,
tmp.user_cname,
tmp.role_id,
tmp.role_cname,
tmp.user_action,
tmp.user_action_name,
tmp.dashboard_id,
tmp.dashboard_name,
tmp.url_address,
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
SELECT 
row_number() over(partition by ali.user_id,ali.dashboard_id,DATE_FORMAT(ali.record_start_time,'yyyy-MM-dd HH:mm:00')  order by ali.record_start_time desc) row_number1,
ali.id,
ali.d,
from_unixtime(unix_timestamp(ali.record_start_time)+28800,'yyyy-MM-dd HH:mm:ss') as record_start_time,
ali.duration_ms,
ali.user_id,
ali.user_cname,
ui.role_id,
ui.role_cname,
ali.user_action,
ali.user_action_name,
ali.dashboard_id,
dsi.dashboard_name,
ali.url_address
FROM ${dwd_dbname}.dwd_report_action_log_info_da ali
JOIN (select distinct dashboard_id,dashboard_name from ${dim_dbname}.dim_report_dashboard_slices_info) dsi ON ali.dashboard_id  = dsi.dashboard_id
JOIN ${dim_dbname}.dim_report_dashboard_user_info ui ON ali.user_id = ui.id
)tmp
WHERE tmp.row_number1 = 1 
order by record_start_time desc
-----------------------------------------------------------------------------------------------------------------------------00
"

$hive -e "$sql"