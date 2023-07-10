#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-11-18 创建
#-- 2 wangyingying 2022-11-29 增加大区、大区组字段
#-- 3 wangyingying 2022-12-01 增加项目交接时间字段
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
--ads_project_plan_stage    --项目计划阶段统计

INSERT overwrite table ${ads_dbname}.ads_project_plan_stage
SELECT '' as id,
       '设备到货签收' as data_type, -- 数据类型
       b.project_code, -- 项目编码
       b.project_name, -- 项目名称
       b.project_area, -- 大区
       b.project_area_group, -- 大区组
	   b.is_active, -- 是否活跃
       b.contract_signed_date, -- 合同日期
       b.project_handover_end_time, -- 交接日期
       s.equitment_arrival_plan_end_date as plan_date, -- 计划到货时间
       s.equitment_arrival_approval_date as actual_date, -- 实际到货时间
       datediff(nvl(s.equitment_arrival_approval_date,current_date()),s.equitment_arrival_plan_end_date) as overdue_days, -- 逾期时长（天）
       nvl(c2.change_times,0) as change_times, -- 计划时间变更次数
       c1.soure_plan_end_date as fisrt_plan_date, -- 首版计划到货时间
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${tmp_dbname}.tmp_pms_project_general_view_detail b
LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s 
ON b.project_code = s.project_code AND s.d = '${pre1_date}'
-- 首版计划到货时间
LEFT JOIN 
(
  SELECT *,row_number()over(PARTITION by tmp.project_code order by tmp.is_change asc,tmp.soure_plan_end_date asc)rn
  FROM 
  (
    SELECT c.project_code,
           c.soure_plan_end_date,
           c.is_change
    FROM ${dwd_dbname}.dwd_pms_wbs_change_detail_info_df c
    WHERE c.wbs_name ='设备到货签收' AND c.d = '${pre1_date}'
    GROUP BY c.project_code,c.soure_plan_end_date,c.is_change
  )tmp
)c1
ON c1.rn = 1 AND (b.project_code = c1.project_code OR b.project_sale_code = c1.project_code)
-- 计划到货时间变更次数
LEFT JOIN 
(
  SELECT c.project_code,
         SUM(IF(c.soure_plan_end_date = c.current_plan_end_date,0,1)) as change_times
  FROM ${dwd_dbname}.dwd_pms_wbs_change_detail_info_df c
  WHERE c.wbs_name ='设备到货签收' AND c.d = '${pre1_date}' AND c.is_change = 1
  GROUP BY c.project_code
)c2
ON b.project_code = c2.project_code OR b.project_sale_code = c2.project_code

UNION ALL 

SELECT '' as id,
       '业务场景联调' as data_type, -- 数据类型
       b.project_code, -- 项目编码
       b.project_name, -- 项目名称
       b.project_area, -- 大区
       b.project_area_group, -- 大区组
	   b.is_active, -- 是否活跃
       b.contract_signed_date, -- 合同日期
       b.project_handover_end_time, -- 交接日期
       s.uat_plan_end_date as plan_date, -- 计划UAT时间
       s.uat_actual_close_date as actual_date, -- 实际UAT时间
       datediff(nvl(s.uat_actual_close_date,current_date()),s.uat_plan_end_date) as overdue_days, -- 逾期时长（天）
       nvl(c2.change_times,0) as change_times, -- 计划时间变更次数
       c1.soure_plan_end_date as fisrt_plan_date, -- 首版计划UAT时间
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${tmp_dbname}.tmp_pms_project_general_view_detail b
LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s 
ON b.project_code = s.project_code AND s.d = '${pre1_date}'
-- 首版计划UAT时间
LEFT JOIN 
(
  SELECT *,row_number()over(PARTITION by tmp.project_code order by tmp.is_change asc,tmp.soure_plan_end_date asc)rn
  FROM 
  (
    SELECT c.project_code,
           c.soure_plan_end_date,
           c.is_change
    FROM ${dwd_dbname}.dwd_pms_wbs_change_detail_info_df c
    WHERE c.wbs_name ='业务场景联调' AND c.d = '${pre1_date}'
    GROUP BY c.project_code,c.soure_plan_end_date,c.is_change
  )tmp
)c1
ON c1.rn = 1 AND (b.project_code = c1.project_code OR b.project_sale_code = c1.project_code)
-- 计划UAT时间变更次数
LEFT JOIN 
(
  SELECT c.project_code,
         SUM(IF(c.soure_plan_end_date = c.current_plan_end_date,0,1)) as change_times
  FROM ${dwd_dbname}.dwd_pms_wbs_change_detail_info_df c
  WHERE c.wbs_name ='业务场景联调' AND c.d = '${pre1_date}' AND c.is_change = 1
  GROUP BY c.project_code
)c2
ON b.project_code = c2.project_code OR b.project_sale_code = c2.project_code

UNION ALL 

SELECT '' as id,
       '第一次实施入场' as data_type, -- 数据类型
       b.project_code, -- 项目编码
       b.project_name, -- 项目名称
       b.project_area, -- 大区
       b.project_area_group, -- 大区组
	   b.is_active, -- 是否活跃
       b.contract_signed_date, -- 合同日期
       b.project_handover_end_time, -- 交接日期
       s.sap_entry_plan_end_date as plan_date, -- 计划入场时间
       s.sap_entry_actual_close_date as actual_date, -- 实际入场时间
       datediff(nvl(s.sap_entry_actual_close_date,current_date()),s.sap_entry_plan_end_date) as overdue_days, -- 逾期时长（天）
       nvl(c2.change_times,0) as change_times, -- 计划时间变更次数
       c1.soure_plan_end_date as fisrt_plan_date, -- 首版计划入场时间
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${tmp_dbname}.tmp_pms_project_general_view_detail b
LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s 
ON b.project_code = s.project_code AND s.d = '${pre1_date}'
-- 首版计划入场时间
LEFT JOIN 
(
  SELECT *,row_number()over(PARTITION by tmp.project_code order by tmp.is_change asc,tmp.soure_plan_end_date asc)rn
  FROM 
  (
    SELECT c.project_code,
           c.soure_plan_end_date,
           c.is_change
    FROM ${dwd_dbname}.dwd_pms_wbs_change_detail_info_df c
    WHERE c.wbs_name ='第一次实施入场' AND c.d = '${pre1_date}'
    GROUP BY c.project_code,c.soure_plan_end_date,c.is_change
  )tmp
)c1
ON c1.rn = 1 AND (b.project_code = c1.project_code OR b.project_sale_code = c1.project_code)
-- 计划入场时间变更次数
LEFT JOIN 
(
  SELECT c.project_code,
         SUM(IF(c.soure_plan_end_date = c.current_plan_end_date,0,1)) as change_times
  FROM ${dwd_dbname}.dwd_pms_wbs_change_detail_info_df c
  WHERE c.wbs_name ='第一次实施入场' AND c.d = '${pre1_date}' AND c.is_change = 1
  GROUP BY c.project_code
)c2
ON b.project_code = c2.project_code OR b.project_sale_code = c2.project_code

UNION ALL 

SELECT '' as id,
       '上线验收' as data_type, -- 数据类型
       b.project_code, -- 项目编码
       b.project_name, -- 项目名称
       b.project_area, -- 大区
       b.project_area_group, -- 大区组
	   b.is_active, -- 是否活跃
       b.contract_signed_date, -- 合同日期
       b.project_handover_end_time, -- 交接日期
       s.inspection_plan_end_date as plan_date, -- 计划上线时间
       s.inspection_actual_close_date as actual_date, -- 实际上线时间
       datediff(nvl(s.inspection_actual_close_date,current_date()),s.inspection_plan_end_date) as overdue_days, -- 逾期时长（天）
       nvl(c2.change_times,0) as change_times, -- 计划时间变更次数
       c1.soure_plan_end_date as fisrt_plan_date, -- 首版计划发货时间
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM ${tmp_dbname}.tmp_pms_project_general_view_detail b
LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df s 
ON b.project_code = s.project_code AND s.d = '${pre1_date}'
-- 首版计划上线时间
LEFT JOIN 
(
  SELECT *,row_number()over(PARTITION by tmp.project_code order by tmp.is_change asc,tmp.soure_plan_end_date asc)rn
  FROM 
  (
    SELECT c.project_code,
           c.soure_plan_end_date,
           c.is_change
    FROM ${dwd_dbname}.dwd_pms_wbs_change_detail_info_df c
    WHERE c.wbs_name ='上线验收' AND c.d = '${pre1_date}'
    GROUP BY c.project_code,c.soure_plan_end_date,c.is_change
  )tmp
)c1
ON c1.rn = 1 AND (b.project_code = c1.project_code OR b.project_sale_code = c1.project_code)
-- 计划上线时间变更次数
LEFT JOIN 
(
  SELECT c.project_code,
         SUM(IF(c.soure_plan_end_date = c.current_plan_end_date,0,1)) as change_times
  FROM ${dwd_dbname}.dwd_pms_wbs_change_detail_info_df c
  WHERE c.wbs_name ='上线验收' AND c.d = '${pre1_date}' AND c.is_change = 1
  GROUP BY c.project_code
)c2
ON b.project_code = c2.project_code OR b.project_sale_code = c2.project_code;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"