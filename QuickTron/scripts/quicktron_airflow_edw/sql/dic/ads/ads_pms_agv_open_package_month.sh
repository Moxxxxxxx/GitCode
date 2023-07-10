#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-11-26 创建
#-- 2 wangyingying 2022-11-29 增加大区、大区组字段
#-- 3 wangyingying 2023-02-13 增加是否活跃字段
# ------------------------------------------------------------------------------------------------


hive=/opt/module/hive-3.1.2/bin/hive
dwd_dbname=dwd
ads_dbname=ads
dim_dbname=dim
tmp_dbname=tmp


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    pre1_date=$1
else
    pre1_date=`date -d "-1 day" +%F`
fi
    

echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--ads_pms_agv_open_package_month    --pms小车开箱月趋势

INSERT overwrite table ${ads_dbname}.ads_pms_agv_open_package_month
SELECT '' AS id, -- 主键
       td.cur_month, -- 统计月份
       nvl(v.project_ft,'未知') AS project_ft, -- 所属FT
       nvl(v.project_area,'未知') AS project_area, -- 大区
       nvl(v.project_area_group,'未知') AS project_area_group, -- 大区组
	   nvl(v.is_active,'未知') AS is_active, -- 是否活跃
       c.agv_type, -- 机器人类型
       SUM(nvl(t1.fenzi,0)) AS qualified_rate_fenzi, -- 开箱合格率分子
       SUM(nvl(t1.fenmu,0)) AS qualified_rate_fenmu, -- 开箱合格率分母
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM ${tmp_dbname}.tmp_pms_project_general_view_detail v -- 项目大表 => 取所属FT
LEFT JOIN 
(
  SELECT c.agv_type -- 机器人类型
  FROM ${dwd_dbname}.dwd_pms_open_package_check_info_df c -- 小车开箱明细表 => 取机器人类型
  GROUP BY c.agv_type
)c
LEFT JOIN 
-- 获取月份
(
  SELECT CAST(days AS DATE) AS cur_month -- 统计月份
  FROM ${dim_dbname}.dim_day_date
  WHERE is_month_begin = 1 AND days >= '2022-01-01' AND days <= '${pre1_date}' -- 取2022.01之后的月初日期
)td
-- 小车开箱情况
LEFT JOIN 
(
  SELECT c.project_code, -- 项目编码
         CAST(CONCAT(SUBSTR(c.check_date,1,7),'-01') AS DATE) AS check_date, -- 开箱填单日期
         c.agv_type, -- 机器人类型
         SUM(IF(c.is_open_package_pass = 1,1,0)) AS fenzi, -- 开箱合格率分子
         COUNT(c.erp_agv_uuid) AS fenmu -- 开箱合格率分母
  FROM ${dwd_dbname}.dwd_pms_open_package_check_info_df c
  WHERE c.d = '${pre1_date}'
  GROUP BY c.project_code,CAST(CONCAT(SUBSTR(c.check_date,1,7),'-01') AS DATE),c.agv_type
)t1
ON v.project_code = t1.project_code AND c.agv_type = t1.agv_type AND td.cur_month = t1.check_date
GROUP BY td.cur_month,nvl(v.project_ft,'未知'),nvl(v.project_area,'未知'),nvl(v.project_area_group,'未知'),nvl(v.is_active,'未知'),c.agv_type;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"