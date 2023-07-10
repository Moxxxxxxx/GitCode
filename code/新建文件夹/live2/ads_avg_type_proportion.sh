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
    
echo "------------------------------------------------------------------------------开始执行--------------------------------------------------------------"
sql="
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--机器人类型比例 ads_avg_type_proportion （项目概览）

INSERT overwrite table ${ads_dbname}.ads_avg_type_proportion
SELECT '' as id,
       t1.project_code,
       t2.agv_type_code as agv_type,
       t2.agv_type_name,
       nvl(t2.agv_num,0) as fenzi,
       nvl(t1.agv_num,0) as fenmu,
       CAST(nvl(t2.agv_num,0) / nvl(t1.agv_num,0) * 100 as decimal(10,2)) as avg_proportion,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT b.project_code,
         b.d,
         count(distinct b.agv_code) as agv_num
  FROM ${dwd_dbname}.dwd_rcs_agv_base_info_df b
  WHERE b.d = '${pre1_date}' AND b.agv_ip is not null AND b.agv_state = 'effective' AND b.project_code is not NULL 
  GROUP BY b.project_code,b.d
)t1
LEFT JOIN 
(
  SELECT b.project_code,
         b.d,
         coalesce(b.agv_type_code, -1) as agv_type_code,
         coalesce(b.agv_type_name, -1) as agv_type_name,
         count(distinct b.agv_code)    as agv_num
  FROM ${dwd_dbname}.dwd_rcs_agv_base_info_df b
  WHERE b.d = '${pre1_date}' AND b.agv_ip is not null AND b.agv_state = 'effective'
  GROUP BY b.project_code,b.d,coalesce(b.agv_type_code, -1), coalesce(b.agv_type_name, -1)
)t2
ON t1.project_code = t2.project_code AND t1.d = t2.d;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"