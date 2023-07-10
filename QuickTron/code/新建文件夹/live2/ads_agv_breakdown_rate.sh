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
--故障覆盖率 ads_agv_breakdown_rate （机器人故障分析）

INSERT overwrite table ${ads_dbname}.ads_agv_breakdown_rate partition(d,pt)

-- 小时故障覆盖率(收敛V4) 每小时故障覆盖率：该小时故障机器数(取故障等级>=3)/项目总机器人数(收敛V4)
SELECT '' as id,
       t.project_num,
       1 as time_type,
       t.agv_type_code,
       t.fenzi,
       t.fenmu,
       concat(t.dt,' ', lpad(t.hourofday,2,'0'),':00:00') as run_time,
       substr(t.value,1,length(t.value) - 1) as err_rate,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
       SUBSTR(t.dt,1,10) as d,
       t.project_num as pt
FROM
(
  SELECT t1.project_code as project_num,
         t1.agv_type_code,
         t1.hourofday,
         concat(round(t1.agv3 / t2.project_agv_num * 100,2),'%') as value,
         t1.agv3 as fenzi,
         t2.project_agv_num as fenmu,
         '${pre1_date}' as dt
  FROM
  (
    SELECT project_code,
           nvl(agv_type_code,-1) as agv_type_code,
           hour(breakdown_log_time) as hourofday,
           count(distinct case when error_level >= 3 then agv_code end) as agv3
    FROM ${dwd_dbname}.dwd_agv_breakdown_astringe_v4_di
    WHERE d = '${pre1_date}'
    GROUP BY project_code,nvl(agv_type_code, -1),hour(breakdown_log_time) 
    GROUPING SETS ((project_code,nvl(agv_type_code, -1),hour(breakdown_log_time)),
                   (project_code,hour(breakdown_log_time)))
  )t1
  LEFT JOIN 
  (
    SELECT t.project_code,
           nvl(t.agv_type_code,-1) as  agv_type_code,
           count(distinct t.agv_code) as project_agv_num
    FROM ${dwd_dbname}.dwd_rcs_agv_base_info_df t
    WHERE t.d = '${pre1_date}' AND t.agv_state = 'effective'
    GROUP BY t.project_code,nvl(t.agv_type_code,-1) 
    GROUPING SETS ((t.project_code,nvl(t.agv_type_code, -1)),
                   (t.project_code))
   )t2 
   ON t2.project_code = t1.project_code AND nvl(t2.agv_type_code, -99) = nvl(t1.agv_type_code, -99)
)t
WHERE t.agv_type_code is not null AND t.hourofday is not null AND t.fenmu is not null
  
UNION ALL

-- 当天故障覆盖率(收敛V4) 当天故障覆盖率：该天故障机器数(取故障等级>=3)/项目总机器人数(收敛V4)
SELECT '' as id,
       t.project_num,
       2 as time_type,
       t.agv_type_code,
       t.fenzi,
       t.fenmu,
       t.dt as run_time,
       substr(t.value,1,length(t.value) - 1) as err_rate,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
       SUBSTR(t.dt,1,10) as d,
       t.project_num as pt
FROM 
(
  SELECT t1.project_code as project_num,
         t1.agv_type_code,
         concat(round(t1.agv3 / t2.project_agv_num * 100,2),'%') as value,
         t1.agv3 as fenzi,
         t2.project_agv_num as fenmu,
         '${pre1_date}' as dt
  FROM
  (
    SELECT project_code,
           nvl(agv_type_code,-1) as agv_type_code,
           count(distinct case when error_level >= 3 then agv_code end) as agv3
    FROM ${dwd_dbname}.dwd_agv_breakdown_astringe_v4_di
    WHERE d = '${pre1_date}'
    GROUP BY project_code,nvl(agv_type_code, -1)
    GROUPING SETS ((project_code,nvl(agv_type_code, -1)),
                   (project_code))
  )t1
  LEFT JOIN 
  (
    SELECT t.project_code,
           nvl(t.agv_type_code,-1) as  agv_type_code,
           count(distinct t.agv_code) as project_agv_num
    FROM ${dwd_dbname}.dwd_rcs_agv_base_info_df t
    WHERE t.d = '${pre1_date}' AND t.agv_state = 'effective'
    GROUP BY t.project_code,nvl(t.agv_type_code,-1) 
    GROUPING SETS ((t.project_code,nvl(t.agv_type_code, -1)),
                   (t.project_code))
  )t2 
  ON t2.project_code = t1.project_code AND nvl(t2.agv_type_code, -99) = nvl(t1.agv_type_code, -99)
)t
WHERE t.agv_type_code is not null AND t.fenmu is not null;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"