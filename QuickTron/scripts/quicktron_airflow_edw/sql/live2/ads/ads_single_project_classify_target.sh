#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
project_code=A51118


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
-- 单项目分类指标统计 ads_single_project_classify_target 

with t1 as 
(
  SELECT TO_DATE(m.d) as cur_date,
         m.project_code,
         '机器人故障' as classify,
         m.agv_code as classify_value,
         COUNT(*) as num_of_times,
         row_number()over(PARTITION by TO_DATE(m.d),m.project_code order by COUNT(*) desc) as sort
  FROM ${dim_dbname}.dim_collection_project_record_ful c
  LEFT JOIN ${dwd_dbname}.dwd_agv_breakdown_astringe_v5_di m
  ON c.project_code = m.pt AND m.d = '${pre1_date}'
  WHERE m.pt is not null
  GROUP BY TO_DATE(m.d),m.project_code,m.agv_code
),
t2 as 
(
  SELECT TO_DATE(m.d) as cur_date,
         m.project_code,
         '机器人故障码' as classify,
         m.error_display_name as classify_value,
         COUNT(*) as num_of_times,
         row_number()over(PARTITION by TO_DATE(m.d),m.project_code order by COUNT(*) desc) as sort
  FROM ${dim_dbname}.dim_collection_project_record_ful c
  LEFT JOIN ${dwd_dbname}.dwd_agv_breakdown_astringe_v5_di m
  ON c.project_code = m.pt AND m.d = '${pre1_date}'
  WHERE m.pt is not null
  GROUP BY TO_DATE(m.d),m.project_code,m.error_display_name
),
t3 as 
(
  SELECT w.cur_date,
         w.project_code,
         '系统工单' as classify,
         IF(t.property_value_map['second_category'] is null OR t.property_value_map['third_category'] is null,CONCAT(w.second_category,' : ',w.third_category),CONCAT(t.property_value_map['second_category'],' : ',t.property_value_map['third_category'])) as classify_value,
         COUNT(DISTINCT w.ticket_id) as num_of_times,
         row_number()over(PARTITION by w.cur_date,w.project_code order by COUNT(DISTINCT w.ticket_id) desc) as sort
  FROM ${dim_dbname}.dim_collection_project_record_ful c
  JOIN 
  (
    SELECT TO_DATE(w.d) as cur_date,
           w.project_code,
           w.second_category,
           w.third_category, 
           w.ticket_id
    FROM ${dwd_dbname}.dwd_ones_work_order_info_df w
    WHERE w.d = '${pre1_date}' AND w.work_order_status != '已驳回' AND w.first_category = '系统'
  )w
  ON c.project_code = w.project_code
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_field_value_info_ful v 
  ON w.ticket_id = v.field_value
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t 
  ON v.task_uuid = t.uuid
  GROUP BY w.cur_date,w.project_code,IF(t.property_value_map['second_category'] is null OR t.property_value_map['third_category'] is null,CONCAT(w.second_category,' : ',w.third_category),CONCAT(t.property_value_map['second_category'],' : ',t.property_value_map['third_category']))
)/*,
-- mock 系统故障数据
t4 as 
(
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '系统故障' as classify,
         '服务器' as classify_value,
         18 as num_of_times,
         1 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '系统故障' as classify,
         '任务' as classify_value,
         14 as num_of_times,
         2 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '系统故障' as classify,
         '接口' as classify_value,
         10 as num_of_times,
         3 as sort
  LIMIT 5
),
-- mock 解死锁时长数据
t5 as 
(
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '解死锁时长' as classify,
         '< 5s' as classify_value,
         6 as num_of_times,
         1 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '解死锁时长' as classify,
         '5s - 1min' as classify_value,
         6 as num_of_times,
         2 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '解死锁时长' as classify,
         '1min - 5min' as classify_value,
         9 as num_of_times,
         3 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '解死锁时长' as classify,
         '> 5min' as classify_value,
         14 as num_of_times,
         4 as sort
  LIMIT 5
),
-- mock 拥堵时长数据
t6 as 
(
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '拥堵时长' as classify,
         '< 5s' as classify_value,
         6 as num_of_times,
         1 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '拥堵时长' as classify,
         '5s - 1min' as classify_value,
         6 as num_of_times,
         2 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '拥堵时长' as classify,
         '1min - 5min' as classify_value,
         9 as num_of_times,
         3 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '拥堵时长' as classify,
         '> 5min' as classify_value,
         14 as num_of_times,
         4 as sort
  LIMIT 5
),
-- mock 人工介入恢复方式分布数据
t7 as 
(
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '人工介入' as classify,
         '拍急停' as classify_value,
         7 as num_of_times,
         1 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '人工介入' as classify,
         'QS恢复' as classify_value,
         6 as num_of_times,
         2 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '人工介入' as classify,
         '重启机器人恢复' as classify_value,
         9 as num_of_times,
         3 as sort
  UNION ALL
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         '${project_code}' as project_code,
         '人工介入' as classify,
         'others' as classify_value,
         14 as num_of_times,
         4 as sort
  LIMIT 5
)
*/

INSERT overwrite table ${ads_dbname}.ads_single_project_classify_target
SELECT '' as id, -- 主键
       t1.*,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM t1
WHERE t1.sort <= 5
UNION ALL
SELECT '' as id, -- 主键
       t2.*,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM t2
WHERE t2.sort <= 5
UNION ALL
SELECT '' as id, -- 主键
       t3.*,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM t3
WHERE t3.sort <= 5
/*
UNION ALL
SELECT '' as id, -- 主键
       t4.*,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM t4
WHERE t4.sort <= 5
UNION ALL
SELECT '' as id, -- 主键
       t5.*,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM t5
WHERE t5.sort <= 5
UNION ALL
SELECT '' as id, -- 主键
       t6.*,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM t6
WHERE t6.sort <= 5
UNION ALL
SELECT '' as id, -- 主键
       t7.*,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM t7
WHERE t7.sort <= 5
*/;
-----------------------------------------------------------------------------------------------------------------------------00

"


printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"