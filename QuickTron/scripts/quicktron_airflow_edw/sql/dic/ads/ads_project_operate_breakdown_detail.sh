#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp
pre1_date=`date -d "-8 day" +%F`

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
#if [ -n "$1" ] ;then
#    pre1_date=$1
#else
#    pre1_date=`date -d "-1 day" +%F`
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
-- 项目运营故障明细表 ads_project_operate_breakdown_detail 

INSERT overwrite table ${ads_dbname}.ads_project_operate_breakdown_detail partition(d,pt)
SELECT '' AS id, -- 主键
       bd.error_time, -- 故障触发时间
       pt.project_code, -- 项目编码	
       pt.project_name, -- 项目名称
       pt.project_ft, -- 所属ft
       pt.is_active, -- 是否活跃
       pt.system_version, -- 系统版本
       bd.upper_computer_version, -- 上位机版本
       bd.low_computer_version, -- 下位机版本
       bd.first_classification, -- 机器人大类
       bd.agv_type_code, -- 机器人类型编码
       bd.agv_type_name, -- 机器人类型名称
       bd.agv_code, -- 机器人编码
       bd.breakdown_id, -- 故障id
       bd.error_level, -- 故障等级
       bd.error_code, -- 故障编码
       bd.error_name, -- 故障名称
       bd.error_display_name, -- 故障描述
       bd.end_time, -- 故障结束时间
       bd.error_duration, -- 故障时长
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time,
       SUBSTR(bd.error_time,1,10) AS d,
       bd.project_code AS pt
FROM 
(
  SELECT d.project_code,
         d.project_name,
         v.project_ft,
         v.is_active,
         r.product_big_version AS system_version
  FROM ${dim_dbname}.dim_collection_project_record_ful d
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail v
  ON d.project_code = v.project_code OR split(d.project_code,'-')[0] = v.project_code
  LEFT JOIN 
  (
    SELECT r.*,ROW_NUMBER()OVER(PARTITION BY r.project_code ORDER BY r.create_time DESC) rn 
    FROM ${dwd_dbname}.dwd_ops_version_record_info_df r
    WHERE r.d = DATE_ADD(CURRENT_DATE(),-1) AND r.code = 'coreapp' AND r.file_type IS NOT NULL
  ) r
  ON d.project_code = r.project_code AND r.rn = 1
  WHERE d.project_version LIKE '2.%'
)pt
JOIN 
(
  SELECT b.project_code,
         CONCAT(b.breakdown_log_time,'.000') AS error_time,
         CONCAT(b.breakdown_end_time,'.000') AS end_time,
         b.agv_code,
         b.agv_type_code,
         b.agv_type_name,
         b.breakdown_id,
         CAST(b.error_code AS STRING) AS error_code,
         b.error_name,
         b.error_display_name,
         b.error_level,
         b.d,
         b.first_classification,
         unix_timestamp(b.breakdown_end_time) - unix_timestamp(b.breakdown_log_time) AS error_duration,
         r1.version_no AS upper_computer_version,
         r2.version_no AS low_computer_version
  FROM ${dwd_dbname}.dwd_agv_breakdown_astringe_v5_di b
  LEFT JOIN 
  (
    SELECT *,ROW_NUMBER()OVER(PARTITION BY r.project_code,r.robot_code,r.d ORDER BY r.create_time DESC) rn 
    FROM ${dwd_dbname}.dwd_ops_robot_version_record_info_df r 
    WHERE r.\`type\` = 'upper_computer' AND r.d >= '${pre1_date}'
  )r1
  ON b.project_code = r1.project_code AND b.agv_code = r1.robot_code AND b.d = r1.d AND r1.rn = 1
  LEFT JOIN 
  (
    SELECT *,ROW_NUMBER()OVER(PARTITION BY r.project_code,r.robot_code,r.d ORDER BY r.create_time DESC) rn 
    FROM ${dwd_dbname}.dwd_ops_robot_version_record_info_df r 
    WHERE r.\`type\` = 'low_computer' AND r.d >= '${pre1_date}'
  )r2
  ON b.project_code = r2.project_code AND b.agv_code = r2.robot_code AND b.d = r2.d AND r2.rn = 1
  WHERE b.d >= '${pre1_date}'
)bd
ON pt.project_code = bd.project_code

UNION ALL 

SELECT '' AS id, -- 主键
       bd.error_time, -- 故障触发时间
       pt.project_code, -- 项目编码	
       pt.project_name, -- 项目名称
       pt.project_ft, -- 所属ft
       pt.is_active, -- 是否活跃
       pt.system_version, -- 系统版本
       bd.upper_computer_version, -- 上位机版本
       bd.low_computer_version, -- 下位机版本
       bd.first_classification, -- 机器人大类
       bd.agv_type_code, -- 机器人类型编码
       bd.agv_type_name, -- 机器人类型名称
       bd.agv_code, -- 机器人编码
       bd.breakdown_id, -- 故障id
       bd.error_level, -- 故障等级
       bd.error_code, -- 故障编码
       bd.error_name, -- 故障名称
       bd.error_display_name, -- 故障描述
       bd.end_time, -- 故障结束时间
       bd.error_duration, -- 故障时长
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time,
       SUBSTR(bd.error_time,1,10) AS d,
       bd.project_code AS pt
FROM 
(
  SELECT d.project_code,
         d.project_name,
         v.project_ft,
         v.is_active,
         r.product_big_version AS system_version
  FROM ${dim_dbname}.dim_collection_project_record_ful d
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail v
  ON d.project_code = v.project_code OR split(d.project_code,'-')[0] = v.project_code
  LEFT JOIN 
  (
    SELECT r.*,ROW_NUMBER()OVER(PARTITION BY r.project_code ORDER BY r.create_time DESC) rn 
    FROM ${dwd_dbname}.dwd_ops_version_record_info_df r
    WHERE r.d = DATE_ADD(CURRENT_DATE(),-1) AND r.code = 'coreapp' AND r.file_type IS NOT NULL
  ) r
  ON d.project_code = r.project_code AND r.rn = 1
  WHERE d.project_version LIKE '3.%'
)pt
JOIN 
(
  SELECT b.project_code,
         b.error_start_time AS error_time,
         b.error_end_time AS end_time,
         b.robot_code AS agv_code,
         b.robot_type_code AS agv_type_code,
         b.robot_type_name AS agv_type_name,
         CAST(b.id AS STRING) AS breakdown_id,
         b.error_code,
         i.error_name,
         b.error_detail AS error_display_name,
         b.error_level,
         b.d,
         b.first_classification,
		 CASE WHEN b.error_end_time IS NOT NULL THEN (nvl(unix_timestamp(from_unixtime(unix_timestamp(b.error_end_time),'yyyy-MM-dd HH:mm:ss')) - unix_timestamp(from_unixtime(unix_timestamp(b.error_start_time),'yyyy-MM-dd HH:mm:ss')),0) * 1000 + nvl(CAST(SUBSTRING(rpad(b.error_end_time,23,'0'),21,3) AS INT),0) - nvl(CAST(SUBSTRING(rpad(b.error_start_time,23,'0'),21,3) AS INT),0))/1000 END AS error_duration,	 	 
         r1.version_no AS upper_computer_version,
         r2.version_no AS low_computer_version
  FROM ${dwd_dbname}.dwd_phx_robot_breakdown_astringe_v1_di b
  LEFT JOIN ${dim_dbname}.dim_phx_basic_error_info_ful i 
  ON i.project_code = b.project_code AND i.error_code = b.error_code
  LEFT JOIN 
  (
    SELECT *,ROW_NUMBER()OVER(PARTITION BY r.project_code,r.robot_code,r.d ORDER BY r.create_time DESC) rn 
    FROM ${dwd_dbname}.dwd_ops_robot_version_record_info_df r 
    WHERE r.\`type\` = 'upper_computer' AND r.d >= '${pre1_date}'
  )r1
  ON b.project_code = r1.project_code AND b.robot_code = r1.robot_code AND b.d = r1.d AND r1.rn = 1
  LEFT JOIN 
  (
    SELECT *,ROW_NUMBER()OVER(PARTITION BY r.project_code,r.robot_code,r.d ORDER BY r.create_time DESC) rn 
    FROM ${dwd_dbname}.dwd_ops_robot_version_record_info_df r 
    WHERE r.\`type\` = 'low_computer' AND r.d >= '${pre1_date}'
  )r2
  ON b.project_code = r2.project_code AND b.robot_code = r2.robot_code AND b.d = r2.d AND r2.rn = 1
  WHERE b.d >= '${pre1_date}' AND b.error_module = 'robot' AND b.error_level >= 3
)bd
ON pt.project_code = bd.project_code;
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql" && hive_concatenate ads ads_project_operate_breakdown_detail ${pre1_date}