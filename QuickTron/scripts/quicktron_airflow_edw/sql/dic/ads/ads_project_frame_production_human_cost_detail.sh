#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2023-02-21 创建
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
--项目车架生产人工费用明细表 ads_project_frame_production_human_cost_detail

INSERT overwrite table ${ads_dbname}.ads_project_frame_production_human_cost_detail
SELECT '' AS id, -- 主键
       r1.frame_numbers, -- 车架号
       r1.operator_name, -- 操作员
       -- SUM(r1.working_hours_minutes) AS working_hours_minutes, -- 投入工时(分钟)
       -- SUM(CAST(r1.losing_hours_minutes / r1.frame_num / r2.person_num AS DECIMAL(10,4))) AS losing_hours_minutes, -- 损失工时(分钟)
       SUM(r1.person_working_hours) AS person_working_hours, -- 分摊车架投入工时(小时)
       SUM(CAST(CAST(r1.losing_hours_minutes / r1.frame_num / r2.person_num AS DECIMAL(10,4)) / 60 AS DECIMAL(10,4))) AS person_losing_hours, -- 分摊车架损失工时(小时)
       37 AS cost_rate, -- 固定工时费率
       SUM(r1.person_working_cost) AS person_working_cost, -- 分摊车架投入成本
       SUM(CAST(CAST(r1.losing_hours_minutes / r1.frame_num / r2.person_num AS DECIMAL(10,4)) / 60 AS DECIMAL(10,4)) * 37) AS person_losing_cost, -- 分摊车架损失成本
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM
(
  SELECT r.process_instance_id,
         n.frame_numbers,
         r.operator_name,
         SUM(nvl(r.working_hours,0)) AS working_hours_minutes,
         AVG(nvl(r.all_losing_hours_minutes,0)) AS losing_hours_minutes,
         AVG(SIZE(split(r.frame_numbers,','))) AS frame_num,
         CAST(SUM(nvl(r.working_hours,0)) / AVG(SIZE(split(r.frame_numbers,','))) / 60 AS DECIMAL(10,4)) AS person_working_hours,
         CAST(SUM(nvl(r.working_hours,0)) / AVG(SIZE(split(r.frame_numbers,','))) / 60 AS DECIMAL(10,4)) * 37 AS person_working_cost
  FROM ${dwd_dbname}.dwd_dtk_daily_production_report_info_df r
  lateral view explode(split(r.frame_numbers,',')) n AS frame_numbers
  WHERE r.d = '${pre1_date}' AND r.approval_result = 'agree' AND r.approval_status = 'COMPLETED' AND r.is_valid = 1 -- 审批状态:已结束 且 审批结果:审批通过 且 有效记录
    AND n.frame_numbers IS NOT NULL
  GROUP BY r.process_instance_id,n.frame_numbers,r.operator_name
)r1
LEFT JOIN 
(
  SELECT r.process_instance_id,
         COUNT(DISTINCT r.operator_name) AS person_num
  FROM ${dwd_dbname}.dwd_dtk_daily_production_report_info_df r
  WHERE r.d = '${pre1_date}' AND r.approval_result = 'agree' AND r.approval_status = 'COMPLETED' AND r.is_valid = 1 -- 审批状态:已结束 且 审批结果:审批通过 且 有效记录
  GROUP BY r.process_instance_id
)r2
ON r1.process_instance_id = r2.process_instance_id
GROUP BY r1.frame_numbers,r1.operator_name;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"      