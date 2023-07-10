#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp
pre11_date=`date -d "-10 day" +%F`

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
#if [ -n "$1" ] ;then
#    pre11_date=$1
#else
#    pre11_date=`date -d "-10 day" +%F`
#fi

    
echo "------------------------------------------------------------------------------#######开始执行###########--------------------------------------------------------------"
sql="
-- set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=5000;
-------------------------------------------------------------------------------------------------------------00
-- 项目概览简易版本机器人状态表 ads_project_view_lite_amr_status 

INSERT overwrite table ${ads_dbname}.ads_project_view_lite_amr_status partition(d,pt)
-- 最近agv运行状态
SELECT '' AS id, -- 主键
       c.project_code, -- 项目编码
       bat.agv_type_code AS amr_type, -- 机器人类型
       COUNT(DISTINCT ba.agv_code) AS amr_total_num, -- 机器人总数
       NULL AS amr_online_num, -- 机器人在线数量
       NULL AS amr_offline_num, -- 机器人离线数量
       COUNT(DISTINCT h.agv_code) AS amr_execute_num, -- 机器人执行数量
       COUNT(DISTINCT ba.agv_code) - COUNT(DISTINCT h.agv_code) AS amr_off_execute_num, -- 机器人未执行数量
       0 AS is_version, -- 是否3X版本
       ba.d AS count_date, -- 统计日期
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time,
       ba.d,
       c.project_code AS pt
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful c
  WHERE c.is_nonetwork = 1 and project_version LIKE '2.%'
)c
LEFT JOIN 
-- 机器人基础信息
(
  SELECT *
  FROM ${dwd_dbname}.dwd_rcs_basic_agv_info_df ba
  WHERE ba.d >= '${pre11_date}' AND ba.state = 'effective' 
)ba
ON c.project_code = ba.project_code
LEFT JOIN 
-- 机器人类型基础信息
(
  SELECT *
  FROM ${dwd_dbname}.dwd_rcs_basic_agv_type_info_df bat
  WHERE bat.d >= '${pre11_date}'
)bat
ON bat.d = ba.d AND bat.project_code = ba.project_code AND bat.id = ba.agv_type_id
LEFT JOIN 
-- 机器人任务
(
  SELECT h.project_code,
         h.agv_code,
         h.d
  FROM ${dwd_dbname}.dwd_rcs_agv_job_history_info_di h
  WHERE h.d >= '${pre11_date}'
  GROUP BY h.project_code,h.agv_code,h.d
)h
ON ba.d = h.d AND ba.project_code = h.project_code AND ba.agv_code = h.agv_code
WHERE bat.agv_type_code IS NOT NULL -- 剔除无机器人类型数据
GROUP BY c.project_code,ba.d,bat.agv_type_code

-- 凤凰3.X CARRIER逻辑
union all 
SELECT '' AS id, -- 主键
       c.project_code, -- 项目编码
       ba.robot_type_code AS amr_type, -- 机器人类型
       COUNT(DISTINCT ba.robot_code) AS amr_total_num, -- 机器人总数
       NULL AS amr_online_num, -- 机器人在线数量
       NULL AS amr_offline_num, -- 机器人离线数量
       COUNT(DISTINCT h.robot_code) AS amr_execute_num, -- 机器人执行数量
       COUNT(DISTINCT ba.robot_code) - COUNT(DISTINCT h.robot_code) AS amr_off_execute_num, -- 机器人未执行数量
       1 AS is_version, -- 是否3X版本
       ba.d AS count_date, -- 统计日期
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time,
       ba.d,
       c.project_code AS pt
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful c
  WHERE  project_version LIKE '3.%'
)c
-- 机器人基础信息
left join 
(select pt,d,project_code ,robot_code,robot_type_code,robot_type_name  
from ${dwd_dbname}.dwd_phx_basic_robot_base_info_df
where d >= '${pre11_date}' and robot_usage_state ='using')ba ON c.project_code = ba.project_code
-- 机器人任务
LEFT JOIN 
(select project_code,robot_code,d 
from ${dwd_dbname}.dwd_phx_rms_robot_state_info_di
where d >= '${pre11_date}'
and online_state='REGISTERED' 
group by project_code,robot_code,d )h
ON ba.d = h.d AND ba.project_code = h.project_code AND ba.robot_code = h.robot_code
GROUP BY c.project_code,ba.d,ba.robot_type_code
;
-----------------------------------------------------------------------------------------------------------------------------00

"
$hive -e "$sql"