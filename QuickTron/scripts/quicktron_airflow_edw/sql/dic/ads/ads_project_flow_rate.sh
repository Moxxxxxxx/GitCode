#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-11-16 创建
#-- 2 wangyingying 2022-11-22 修改流速字段计算逻辑
#-- 3 wangyingying 2022-11-29 增加大区、大区组字段
#-- 4 wangyingying 2023-02-13 增加是否活跃字段
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
--ads_project_flow_rate    --项目流动速率月趋势

INSERT overwrite table ${ads_dbname}.ads_project_flow_rate
SELECT '' AS id, -- 主键
       d.days AS cur_month, -- 统计月份
       ft.project_ft, -- 所属ft
       area.project_area, -- 大区
       area.project_area_group, -- 大区组
	   act.is_active, -- 是否活跃
       COUNT(DISTINCT b.project_code) AS new_project_num, -- 新增项目数量
       COUNT(DISTINCT b1.project_code) AS final_inspection_num, -- 验收项目数量
       CASE WHEN COUNT(b.project_code) = 0 AND COUNT(DISTINCT b1.project_code) = 0 THEN 0
            WHEN COUNT(b.project_code) = 0 AND COUNT(DISTINCT b1.project_code) != 0 THEN 1
            WHEN COUNT(b.project_code) != 0 AND COUNT(DISTINCT b1.project_code) = 0 THEN 0
            WHEN COUNT(b.project_code) != 0 AND COUNT(DISTINCT b1.project_code) != 0 THEN CAST(COUNT(DISTINCT b1.project_code) / COUNT(b.project_code) AS DECIMAL(10,2))
       END AS project_flow_rate, -- 项目流速
       DATE_FORMAT(current_timestamp(),'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(),'yyyy-MM-dd HH:mm:ss') AS update_time
FROM 
(
  SELECT d.days
  FROM ${dim_dbname}.dim_day_date d
  WHERE d.is_month_begin = 1 AND d.days >= '2018-01-01' AND d.days <= '${pre1_date}'
)d
LEFT JOIN 
(
  SELECT nvl(b.project_ft,'未知') AS project_ft
  FROM ${tmp_dbname}.tmp_pms_project_general_view_detail b
  GROUP BY nvl(b.project_ft,'未知')
)ft
LEFT JOIN 
(
  SELECT nvl(b.project_area,'未知') AS project_area,
         nvl(b.project_area_group,'未知') AS project_area_group
  FROM ${tmp_dbname}.tmp_pms_project_general_view_detail b
  GROUP BY nvl(b.project_area,'未知'),nvl(b.project_area_group,'未知')
)area
LEFT JOIN 
(
  SELECT nvl(b.is_active,'未知') AS is_active
  FROM ${tmp_dbname}.tmp_pms_project_general_view_detail b
  GROUP BY nvl(b.is_active,'未知')
)act
-- 新增项目数量
LEFT JOIN 
(
  SELECT nvl(b.project_ft,'未知') AS project_ft,
         nvl(b.project_area,'未知') AS project_area,
         nvl(b.project_area_group,'未知') AS project_area_group,
		 nvl(b.is_active,'未知') AS is_active,
         TO_DATE(CONCAT(SUBSTR(b.project_handover_end_time,1,7),'-','01')) AS cur_month,
         b.project_code
  FROM ${tmp_dbname}.tmp_pms_project_general_view_detail b
  WHERE b.project_handover_end_time IS NOT NULL
)b
ON d.days = b.cur_month AND ft.project_ft = b.project_ft AND area.project_area = b.project_area AND area.project_area_group = b.project_area_group AND act.is_active = b.is_active
-- 验收项目数量
LEFT JOIN 
(
  SELECT nvl(b.project_ft,'未知') AS project_ft,
         nvl(b.project_area,'未知') AS project_area,
         nvl(b.project_area_group,'未知') AS project_area_group,
		 nvl(b.is_active,'未知') AS is_active,
         TO_DATE(b.final_inspection_process_month_begin) AS cur_month,
         b.project_code
  FROM ${tmp_dbname}.tmp_pms_project_general_view_detail b
  WHERE b.final_inspection_process_month_begin IS NOT NULL
)b1
ON d.days = b1.cur_month AND ft.project_ft = b1.project_ft AND area.project_area = b1.project_area AND area.project_area_group = b1.project_area_group AND act.is_active = b1.is_active
GROUP BY d.days,ft.project_ft,area.project_area,area.project_area_group,act.is_active;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"