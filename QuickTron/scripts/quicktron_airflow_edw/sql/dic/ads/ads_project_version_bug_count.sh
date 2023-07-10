#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-11-28 创建
#-- 2 wangyingying 2022-11-29 增加大区、大区组字段
#-- 3 wangyingying 2023-02-13 增加是否活跃字段
# ------------------------------------------------------------------------------------------------


hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
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
--项目版本P0级bug统计 ads_project_version_bug_count

INSERT overwrite table ${ads_dbname}.ads_project_version_bug_count
SELECT '' AS id, -- 主键
       v.project_code, -- 项目编码
       b.project_area, -- 大区
       b.project_area_group, -- 大区组
	   b.is_active, -- 是否活跃
       v.new_version, -- 新版本
       v.start_date, -- 开始时间
       v.end_date, -- 结束时间
       COUNT(DISTINCT t.uuid) AS bug_num, -- P0缺陷数量
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM 
(
  SELECT v1.project_code, -- 项目编码
         v1.new_version, -- 新版本
         v1.create_date AS start_date, -- 开始时间
         nvl(v2.create_date,'${pre1_date}') AS end_date -- 结束时间 => 为空用当前时间补
  FROM 
  (
    SELECT CAST(v.create_time AS DATE) AS create_date, -- 开始时间
           v.project_code, -- 项目编码
           v.new_version, -- 新版本
           ROW_NUMBER()OVER(PARTITION BY v.project_code ORDER BY CAST(v.create_time AS DATE) ASC) rn -- 按创建时间排序
    FROM ${dwd_dbname}.dwd_devops_project_deploy_version_info_df v
    WHERE v.d = '${pre1_date}'
    GROUP BY CAST(v.create_time AS DATE),v.project_code,v.new_version -- 去重相同时间内的版本
  )v1
  LEFT JOIN 
  (
    SELECT CAST(v.create_time AS DATE) AS create_date, -- 开始时间
           v.project_code, -- 项目编码
           v.new_version, -- 新版本
           ROW_NUMBER()OVER(PARTITION BY v.project_code ORDER BY CAST(v.create_time AS DATE) ASC) rn  -- 按创建时间排序
    FROM ${dwd_dbname}.dwd_devops_project_deploy_version_info_df v
    WHERE v.d = '${pre1_date}'
    GROUP BY CAST(v.create_time AS DATE),v.project_code,v.new_version -- 去重相同时间内的版本
  )v2
  ON v1.project_code = v2.project_code AND v1.rn = v2.rn - 1 
)v
LEFT JOIN 
(
  SELECT *
  FROM ${dwd_dbname}.dwd_ones_task_info_ful t
  WHERE t.project_classify_name = '工单问题汇总' AND (t.severity_level = '1' OR t.task_priority_value = 'P0') AND t.issue_type_cname = '缺陷' -- 工单问题汇总类别下<任务优先级为P0或者严重等级为1>的缺陷
)t
ON v.project_code = t.external_project_code AND t.task_create_time >= v.start_date AND t.task_create_time < v.end_date -- 拿到版本范围内的ones数据
LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
ON v.project_code = b.project_code
WHERE v.new_version IS NOT NULL
GROUP BY v.project_code,b.project_area,b.project_area_group,b.is_active,v.new_version,v.start_date,v.end_date
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"      