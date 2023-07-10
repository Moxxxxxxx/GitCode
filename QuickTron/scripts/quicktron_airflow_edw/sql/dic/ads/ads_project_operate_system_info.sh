#!/bin/bash
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
-- set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=5000;
-------------------------------------------------------------------------------------------------------------00
-- 项目运营系统信息表 ads_project_operate_system_info 

INSERT overwrite table ${ads_dbname}.ads_project_operate_system_info partition(d,pt)
SELECT '' AS id, -- 主键
       p.project_code, -- 项目编码
       p.project_name, -- 项目名称
       v.project_ft, -- 所属ft
       CONCAT_WS(',',COLLECT_SET(b.agv_type_code)) AS robot_type_code, -- 部署机器人类型
       COUNT(DISTINCT b.agv_code) AS robot_num, -- 部署机器人数量
       CONCAT_WS(',',COLLECT_SET(r1.version_no)) AS upper_computer_version, -- 上位机版本
       CONCAT_WS(',',COLLECT_SET(r2.version_no)) AS low_computer_version, -- 下位机版本
       r.product_big_version AS system_version, -- 系统大版本
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time,
       '${pre1_date}' AS d,
       p.project_code AS pt
FROM ${dim_dbname}.dim_collection_project_record_ful p
LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail v
ON p.project_code = v.project_code OR split(p.project_code,'-')[0] = v.project_code
LEFT JOIN ${dwd_dbname}.dwd_rcs_agv_base_info_df b
ON b.project_code = p.project_code AND b.d = '${pre1_date}'
LEFT JOIN 
(
  SELECT r.*,ROW_NUMBER()OVER(PARTITION BY r.project_code ORDER BY r.create_time DESC) rn 
  FROM ${dwd_dbname}.dwd_ops_version_record_info_df r
  WHERE r.d = '${pre1_date}' AND r.code = 'coreapp' AND r.file_type IS NOT NULL
)r
ON p.project_code = r.project_code AND r.rn = 1
LEFT JOIN 
(
  SELECT *,ROW_NUMBER()OVER(PARTITION BY r.project_code,r.robot_code,r.d ORDER BY r.create_time DESC) rn 
  FROM ${dwd_dbname}.dwd_ops_robot_version_record_info_df r 
  WHERE r.\`type\` = 'upper_computer' AND r.d = '${pre1_date}'
)r1
ON b.project_code = r1.project_code AND b.agv_code = r1.robot_code AND r1.rn = 1
LEFT JOIN 
(
  SELECT *,ROW_NUMBER()OVER(PARTITION BY r.project_code,r.robot_code,r.d ORDER BY r.create_time DESC) rn 
  FROM ${dwd_dbname}.dwd_ops_robot_version_record_info_df r 
  WHERE r.\`type\` = 'low_computer' AND r.d = '${pre1_date}'
)r2
ON b.project_code = r2.project_code AND b.agv_code = r2.robot_code AND r2.rn = 1
WHERE p.project_version LIKE '2.%'
GROUP BY p.project_code,p.project_name,v.project_ft,r.product_big_version

UNION ALL 

SELECT '' AS id, -- 主键
       p.project_code, -- 项目编码
       p.project_name, -- 项目名称
       v.project_ft, -- 所属ft
       CONCAT_WS(',',COLLECT_SET(b.robot_type_code)) AS robot_type_code, -- 部署机器人类型
       COUNT(DISTINCT b.robot_code) AS robot_num, -- 部署机器人数量
       CONCAT_WS(',',COLLECT_SET(r1.version_no)) AS upper_computer_version, -- 上位机版本
       CONCAT_WS(',',COLLECT_SET(r2.version_no)) AS low_computer_version, -- 下位机版本
       r.product_big_version AS system_version, -- 系统大版本
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time,
       '${pre1_date}' AS d,
       p.project_code AS pt
FROM ${dim_dbname}.dim_collection_project_record_ful p
LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail v
ON p.project_code = v.project_code OR split(p.project_code,'-')[0] = v.project_code
LEFT JOIN ${dwd_dbname}.dwd_phx_basic_robot_base_info_df b
ON b.project_code = p.project_code AND b.d = '${pre1_date}'
LEFT JOIN 
(
  SELECT r.*,ROW_NUMBER()OVER(PARTITION BY r.project_code ORDER BY r.create_time DESC) rn 
  FROM ${dwd_dbname}.dwd_ops_version_record_info_df r
  WHERE r.d = '${pre1_date}' AND r.code = 'coreapp' AND r.file_type IS NOT NULL
)r
ON p.project_code = r.project_code AND r.rn = 1
LEFT JOIN 
(
  SELECT *,ROW_NUMBER()OVER(PARTITION BY r.project_code,r.robot_code,r.d ORDER BY r.create_time DESC) rn 
  FROM ${dwd_dbname}.dwd_ops_robot_version_record_info_df r 
  WHERE r.\`type\` = 'upper_computer' AND r.d = '${pre1_date}'
)r1
ON b.project_code = r1.project_code AND b.robot_code = r1.robot_code AND r1.rn = 1
LEFT JOIN 
(
  SELECT *,ROW_NUMBER()OVER(PARTITION BY r.project_code,r.robot_code,r.d ORDER BY r.create_time DESC) rn 
  FROM ${dwd_dbname}.dwd_ops_robot_version_record_info_df r 
  WHERE r.\`type\` = 'low_computer' AND r.d = '${pre1_date}'
)r2
ON b.project_code = r2.project_code AND b.robot_code = r2.robot_code AND r2.rn = 1
WHERE p.project_version LIKE '3.%'
GROUP BY p.project_code,p.project_name,v.project_ft,r.product_big_version;
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql" && hive_concatenate ads ads_project_operate_system_info ${pre1_date}