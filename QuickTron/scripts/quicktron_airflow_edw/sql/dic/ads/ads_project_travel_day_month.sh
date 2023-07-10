#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2023-01-10 创建
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
--ads_project_travel_day_month    --项目投入出差人天月统计

WITH travel_detail AS 
(
  SELECT t.process_instance_id,
         t.business_id,
         t.create_time AS business_create_time,
         t.finish_time AS business_finish_time,
         t.originator_dept_name,
         t.originator_user_id,
         t.originator_user_name,
         t.project_code AS original_project_code,
         regexp_replace(t.business_trip, '\\\\s+', ' ') AS business_trip,
         t.travel_date,
         t.every_days AS travel_days,
         t.period_type,
         CASE WHEN t.period_type = '全天' THEN '全天出差'
              WHEN t.period_type = '下午' THEN '下半天出差'
              WHEN t.period_type = '上午' THEN '上半天出差' END travel_type,
         t.data_source
  FROM ${dwd_dbname}.dwd_dtk_process_business_travel_dayily_info_df t
  WHERE t.d = '${pre1_date}' AND IF(t.data_source = 'DTK',t.is_valid = 1 AND t.approval_result = 'agree' AND t.approval_status = 'COMPLETED',t.approval_status = '审批通过')
),
efficiency AS 
(
  SELECT t.process_instance_id, -- 审批编码id
         t.business_id, -- 审批编码
         t.business_create_time, -- 创建时间
         t.business_finish_time, -- 结束时间
         t.originator_dept_name, -- 发起部门
         t.originator_user_id,-- 发起人编码
         t.originator_user_name, -- 发起人姓名
         t.original_project_code, -- 原本项目编码
         b.project_code, -- 项目编码
         b.project_name, -- 项目名称
         b.project_dispaly_state AS project_operation_state, -- 项目运营状态
         b.project_area, -- 项目区域
         b.project_ft, -- 项目所属ft
         b.project_priority, -- 项目等级
         b.project_progress_stage, -- 项目进展阶段
         b.project_area_group, -- 项目区域（国内|国外）
         t.business_trip, -- 出差事由
         t.travel_date, -- 初查日期
         t.travel_days, -- 出差天数
         t.travel_type, -- 出差日期类型
         t.data_source, -- 数据来源
         IF(t.rn1 = 1 AND t.rn2 = 1,1,0) AS is_valid -- 是否有效
  FROM 
  (
    SELECT *,
           DENSE_RANK() OVER(partition by originator_user_id,travel_date order by travel_days DESC) as rn1,
           ROW_NUMBER() OVER(PARTITION BY originator_user_id,travel_date,period_type ORDER BY period_type) as rn2
    FROM travel_detail
  )t
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON t.original_project_code = b.project_code OR t.original_project_code = b.project_sale_code
)

INSERT overwrite table ${ads_dbname}.ads_project_travel_day_month
SELECT '' AS id, -- 主键
       d.days AS cur_month, --统计月份
       area.project_area, -- 区域-PM
       ft.project_ft, -- 大区/FT => <技术方案评审>ft
       area.project_area_group, -- 大区组
       l.project_code, -- 项目编码
       SUM(nvl(l.travel_days,0)) AS travel_day, -- pe人天
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM 
(
  SELECT days -- 日期
  FROM ${dim_dbname}.dim_day_date -- 日期维表
  WHERE is_month_begin = 1 AND days >= '2021-01-01' AND days <= '${pre1_date}' -- 取2021年1月1日之后的日期补零
  GROUP BY days
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
  SELECT project_code,
         project_area,
         project_ft,
         project_area_group,
         TO_DATE(CONCAT(SUBSTR(travel_date,1,7),'-','01')) AS cur_month,
         travel_days
  FROM efficiency
  WHERE (originator_dept_name LIKE '%箱式FT%' OR originator_dept_name LIKE '%智能搬运FT%' OR originator_dept_name LIKE '%系统中台%' OR originator_dept_name LIKE '%硬件自动化%')
    AND project_code is NOT NULL AND is_valid = 1
)l
ON d.days = l.cur_month AND ft.project_ft = nvl(l.project_ft,'未知') AND area.project_area = nvl(l.project_area,'未知') AND area.project_area_group = nvl(l.project_area_group,'未知')
GROUP BY d.days,ft.project_ft,area.project_area,area.project_area_group,l.project_code;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"