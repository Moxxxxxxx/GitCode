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
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=5000;
-------------------------------------------------------------------------------------------------------------00
--机器人故障表 ads_agv_breakdown （机器人故障分析）

INSERT overwrite table ${ads_dbname}.ads_agv_breakdown partition(d,pt)

-- 项目各机器人故障数(收敛V4)
SELECT '' as id,
       t.project_num,
       t.agv_code,
       t1.agv_type_code as agv_type,
       t.breakdown_level,
       t.breakdown_type,
       t.index_value,
       t.run_time,
       1 as time_type,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
        '' as index_english_name,
       SUBSTR(t.run_time,1,10) as d,
       t.project_num as pt
FROM 
(
  SELECT tmp.project_code as project_num,
         tmp.agv_code,
         concat(tmp.dt,' ',lpad(tmp.hourofday,2,'0'),':00:00') as run_time,
         tmp.error_level as breakdown_level,
         tmp.error_type as breakdown_type,
         tmp.value as index_value
  FROM 
  (
    SELECT b.project_code,
           '${pre1_date}' as dt,
           b.agv_code,
           hour(b.breakdown_log_time) as hourofday,
           nvl(b.error_level,'未知') as error_level,
           nvl(b.error_name,b.error_code) as error_type,
           count(distinct concat(b.agv_code,'-', breakdown_id,'-',error_code)) as value,
           count(distinct concat(b.agv_code,'-', breakdown_id)) as breakdown_time_num
    FROM ${dwd_dbname}.dwd_agv_breakdown_astringe_v4_di b
    WHERE b.d = '${pre1_date}'
    GROUP BY b.agv_code,b.project_code,hour(b.breakdown_log_time),nvl(b.error_level,'未知'),nvl(b.error_name,b.error_code) 
    GROUPING SETS ((b.agv_code,b.project_code,hour(b.breakdown_log_time),nvl(b.error_level,'未知'),nvl(b.error_name,b.error_code)),
                   (b.agv_code,b.project_code,hour(b.breakdown_log_time),nvl(b.error_level,'未知')),
                   (b.agv_code,b.project_code,hour(b.breakdown_log_time),nvl(b.error_name,b.error_code)),
                   (b.agv_code,b.project_code,nvl(b.error_level,'未知'), nvl(b.error_name,b.error_code)),
                   (b.agv_code,b.project_code,hour(b.breakdown_log_time)),
                   (b.agv_code,b.project_code,nvl(b.error_level,'未知')),
                   (b.agv_code,b.project_code,nvl(b.error_name,b.error_code)),
                   (b.agv_code,b.project_code))
  )tmp
  WHERE tmp.hourofday is not null AND tmp.error_type is not null AND tmp.error_level is not null
)t
LEFT JOIN ${dwd_dbname}.dwd_rcs_agv_base_info_df t1
ON t1.agv_code = t.agv_code AND t1.project_code = t.project_num AND t1.d = '${pre1_date}'
WHERE t.project_num is not null AND t.agv_code is not null
  AND t1.agv_type_code is not null
    
UNION ALL

-- 项目各机器人故障数(收敛V4)
SELECT '' as id,
       t.project_num,
       t.agv_code,
       t1.agv_type_code as agv_type,
       t.breakdown_level,
       t.breakdown_type,
       t.index_value,
       t.run_time,
       2 as time_type,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
        '' as index_english_name,
       SUBSTR(t.run_time,1,10) as d,
       t.project_num as pt
FROM 
(
  SELECT tmp.project_code as project_num,
         tmp.agv_code,
         tmp.dt as run_time,
         tmp.error_level as breakdown_level,
         tmp.error_type as breakdown_type,
         SUM(tmp.value) as index_value
  FROM 
  (
    SELECT b.project_code,
           '${pre1_date}' as dt,
           b.agv_code,
           hour(b.breakdown_log_time) as hourofday,
           nvl(b.error_level,'未知') as error_level,
           nvl(b.error_name,b.error_code) as error_type,
           count(distinct concat(b.agv_code,'-', breakdown_id,'-',error_code)) as value,
           count(distinct concat(b.agv_code,'-', breakdown_id)) as breakdown_time_num
    FROM ${dwd_dbname}.dwd_agv_breakdown_astringe_v4_di b
    WHERE b.d = '${pre1_date}'
    GROUP BY b.agv_code,b.project_code,hour(b.breakdown_log_time),nvl(b.error_level,'未知'),nvl(b.error_name,b.error_code) 
    GROUPING SETS ((b.agv_code,b.project_code,hour(b.breakdown_log_time),nvl(b.error_level,'未知'),nvl(b.error_name,b.error_code)),
                   (b.agv_code,b.project_code,hour(b.breakdown_log_time),nvl(b.error_level,'未知')),
                   (b.agv_code,b.project_code,hour(b.breakdown_log_time),nvl(b.error_name,b.error_code)),
                   (b.agv_code,b.project_code,nvl(b.error_level,'未知'), nvl(b.error_name,b.error_code)),
                   (b.agv_code,b.project_code,hour(b.breakdown_log_time)),
                   (b.agv_code,b.project_code,nvl(b.error_level,'未知')),
                   (b.agv_code,b.project_code,nvl(b.error_name,b.error_code)),
                   (b.agv_code,b.project_code))
  )tmp
  WHERE tmp.hourofday is not null AND tmp.error_type is not null AND tmp.error_level is not null
  GROUP BY tmp.project_code,tmp.agv_code,tmp.dt,tmp.error_level,tmp.error_type
) t
LEFT JOIN ${dwd_dbname}.dwd_rcs_agv_base_info_df t1
ON t1.agv_code = t.agv_code AND t1.project_code = t.project_num AND t1.d = '${pre1_date}'
WHERE t.project_num is not null AND t.agv_code is not null
  AND t1.agv_type_code is not null;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"