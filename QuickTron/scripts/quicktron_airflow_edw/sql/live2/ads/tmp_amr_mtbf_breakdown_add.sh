#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp
pre1_date=`date -d "-10 day" +%F`

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
#if [ -n "$1" ] ;then
#    pre1_date=$1
#else
#    pre1_date=`date -d "-10 day" +%F`
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
-- 机器人故障统计临时表 tmp_amr_mtbf_breakdown_add 

/*
-- v4故障 
with breakdown as
(
  SELECT tt1.*,
         ROW_NUMBER() over (PARTITION by tt1.project_code,tt1.agv_code,tt1.breakdown_id,tt1.d order by tt2.status_change_time asc) as rk,
         sort_array(ARRAY(tt1.next_error_time, tt2.status_change_time)) as sort_time,
         tt2.status_change_time
  FROM 
  (
    SELECT b.project_code,
           b.breakdown_log_time as error_time,
           lead(b.breakdown_log_time, 1) over (PARTITION by b.project_code,b.agv_code,to_date(b.breakdown_log_time) order by b.breakdown_log_time asc) as next_error_time,
           b.agv_code,
           b.agv_type_code,
           b.breakdown_id,
           b.error_code,
           b.error_name,
           b.error_display_name,
           b.error_level,
           b.d
    FROM ${dwd_dbname}.dwd_agv_breakdown_astringe_v4_di b
    WHERE b.d >= '${pre1_date}' AND b.error_level >= '3'  
  )tt1
  LEFT JOIN 
  (
    SELECT w.project_code,
           w.agv_code,
           w.status_log_time as status_change_time,
           w.d
    FROM ${dwd_dbname}.dwd_agv_working_status_incre_dt w
    WHERE w.d >= '${pre1_date}' AND w.online_status = 'REGISTERED' AND w.working_status = 'BUSY' 
    
    UNION ALL 
    
    SELECT r.project_code,
           r.agv_code,
           r.job_accept_time as status_change_time,
           r.d
    FROM ${dwd_dbname}.dwd_rcs_agv_job_history_info_di r
    WHERE r.d >= '${pre1_date}' AND r.pt IN (SELECT project_code FROM ${dim_dbname}.dim_collection_project_record_ful WHERE is_nonetwork = 1)
  ) tt2 
  ON tt2.project_code = tt1.project_code AND tt2.agv_code = tt1.agv_code
  WHERE tt2.status_change_time > tt1.error_time
),
err_times as 
(
  SELECT TO_DATE(t.error_time) as cur_date,
         date_format(t.error_time,'yyyy-MM-dd HH:00:00') as cur_hour,
         t.error_time,
         coalesce(t.sort_time[0],t.sort_time[1]) as end_time,
         t.project_code,
         cast(coalesce(t.agv_type_code, -1) as string) as agv_type_code,
         cast(coalesce(t.agv_code, -1) as string) as agv_code,
         t.breakdown_id
  FROM breakdown t
  WHERE t.rk = 1
),
err_breakdown as 
(
  SELECT TO_DATE(tmp2.day_hour_start) as cur_date,
         tmp2.day_hour_start as cur_hour,
         IF(tmp1.cur_hour = tmp2.day_hour_start,tmp1.error_time,tmp2.day_hour_start) as error_time,
         IF(date_format(tmp1.end_time,'yyyy-MM-dd HH:00:00') = tmp2.day_hour_start,tmp1.end_time,tmp2.day_hour_end) as end_time,
         IF(unix_timestamp(IF(date_format(tmp1.end_time,'yyyy-MM-dd HH:00:00') = tmp2.day_hour_start,tmp1.end_time,tmp2.day_hour_end)) - unix_timestamp(IF(tmp1.cur_hour = tmp2.day_hour_start,tmp1.error_time,tmp2.day_hour_start)) = 3599,3600,unix_timestamp(IF(date_format(tmp1.end_time,'yyyy-MM-dd HH:00:00') = tmp2.day_hour_start,tmp1.end_time,tmp2.day_hour_end)) - unix_timestamp(IF(tmp1.cur_hour = tmp2.day_hour_start,tmp1.error_time,tmp2.day_hour_start))) as breakdown_duration,
         tmp1.project_code,
         tmp1.agv_type_code,
         tmp1.agv_code,
         tmp1.breakdown_id
  FROM err_times tmp1
  LEFT JOIN 
  (
    SELECT DATE_FORMAT(CONCAT(d.days,' ',h.startofhour),'yyyy-MM-dd HH:mm:ss') as day_hour_start,
           DATE_FORMAT(CONCAT(d.days,' ',h.endofhour),'yyyy-MM-dd HH:mm:ss') as day_hour_end
    FROM ${dim_dbname}.dim_day_date d
    LEFT JOIN ${dim_dbname}.dim_day_of_hour h
    WHERE days >= '${pre1_date}' AND days <= DATE_ADD(current_date(),-1)
  )tmp2
  ON unix_timestamp(date_format(tmp1.error_time,'yyyy-MM-dd HH:00:00')) <= unix_timestamp(tmp2.day_hour_start) AND unix_timestamp(date_format(tmp1.end_time,'yyyy-MM-dd HH:00:00')) >= unix_timestamp(tmp2.day_hour_start)
),
*/
-- v5故障
with breakdown as
(
  SELECT b.project_code,
         TO_DATE(b.breakdown_log_time) as cur_date,
         date_format(b.breakdown_log_time,'yyyy-MM-dd HH:00:00') as cur_hour,
         cast(coalesce(b.agv_code, -1) as string) as agv_code,
         cast(coalesce(b.agv_type_code, -1) as string) as agv_type_code,
         b.breakdown_id,
         b.breakdown_log_time as error_time,
         b.breakdown_end_time as end_time,
         unix_timestamp(b.breakdown_end_time) - unix_timestamp(b.breakdown_log_time) as breakdown_duration -- 故障时长
  FROM ${dwd_dbname}.dwd_agv_breakdown_astringe_v5_di b
  WHERE b.d >= '${pre1_date}'
),
err_times as 
(
  SELECT t.cur_date,
         t.cur_hour,
         t.error_time,
         t.end_time,
         t.project_code,
         t.agv_type_code,
         t.agv_code,
         t.breakdown_id
  FROM breakdown t
),
err_breakdown as 
(
  SELECT TO_DATE(tmp2.day_hour_start) as cur_date,
         tmp2.day_hour_start as cur_hour,
         IF(tmp1.cur_hour = tmp2.day_hour_start,tmp1.error_time,tmp2.day_hour_start) as error_time,
         IF(date_format(tmp1.end_time,'yyyy-MM-dd HH:00:00') = tmp2.day_hour_start,tmp1.end_time,tmp2.day_hour_end) as end_time,
         IF(unix_timestamp(IF(date_format(tmp1.end_time,'yyyy-MM-dd HH:00:00') = tmp2.day_hour_start,tmp1.end_time,tmp2.day_hour_end)) - unix_timestamp(IF(tmp1.cur_hour = tmp2.day_hour_start,tmp1.error_time,tmp2.day_hour_start)) = 3599,3600,unix_timestamp(IF(date_format(tmp1.end_time,'yyyy-MM-dd HH:00:00') = tmp2.day_hour_start,tmp1.end_time,tmp2.day_hour_end)) - unix_timestamp(IF(tmp1.cur_hour = tmp2.day_hour_start,tmp1.error_time,tmp2.day_hour_start))) as breakdown_duration,
         tmp1.project_code,
         tmp1.agv_type_code,
         tmp1.agv_code,
         tmp1.breakdown_id
  FROM err_times tmp1
  LEFT JOIN 
  (
    SELECT DATE_FORMAT(CONCAT(d.days,' ',h.startofhour),'yyyy-MM-dd HH:mm:ss') as day_hour_start,
           DATE_FORMAT(CONCAT(d.days,' ',h.endofhour),'yyyy-MM-dd HH:mm:ss') as day_hour_end
    FROM ${dim_dbname}.dim_day_date d
    LEFT JOIN ${dim_dbname}.dim_day_of_hour h
    WHERE days >= '${pre1_date}' AND days <= DATE_ADD(current_date(),-1)
  )tmp2
  ON unix_timestamp(date_format(tmp1.error_time,'yyyy-MM-dd HH:00:00')) <= unix_timestamp(tmp2.day_hour_start) AND unix_timestamp(date_format(tmp1.end_time,'yyyy-MM-dd HH:00:00')) >= unix_timestamp(tmp2.day_hour_start)
),
agv_num as 
(
  SELECT t.project_code,
         a.agv_type,
         t.agv_type_code,
         nvl(a.agv_type_name,t.agv_type_name) as agv_type_name,
         nvl(a.agv_code,t.agv_code) as agv_code
  FROM ${dwd_dbname}.dwd_rcs_agv_base_info_df t
  LEFT JOIN ${tmp_dbname}.tmp_basic_agv_data_offline_info a
  ON t.project_code = a.project_code AND a.agv_code = t.agv_code
  WHERE t.d = DATE_ADD(current_date(),-1) AND (a.project_code is null OR a.active_status = '运营中')
),
base as 
(
  SELECT t1.cur_week, -- 统计星期
         t1.cur_date, -- 统计日期
         t1.cur_hour, -- 统计小时
         t2.project_code, -- 项目编码
         t3.agv_type, -- 离线表机器人类型编码
         t3.agv_type_code, -- 机器人类型编码
         t3.agv_type_name, -- 机器人类型名称
         t3.agv_code, -- 机器人编码
         CASE WHEN t1.cur_hour = DATE_FORMAT(t4.start_actual_duration_day,'yyyy-MM-dd HH:00:00') THEN 3600 - (unix_timestamp(t4.start_actual_duration_day) - unix_timestamp(t1.cur_hour))
              WHEN t1.cur_hour != DATE_FORMAT(t4.start_actual_duration_day,'yyyy-MM-dd HH:00:00') AND t1.cur_hour != DATE_FORMAT(t4.end_actual_duration_day,'yyyy-MM-dd HH:00:00') THEN 3600
              WHEN t1.cur_hour = DATE_FORMAT(t4.end_actual_duration_day,'yyyy-MM-dd HH:00:00') THEN unix_timestamp(t4.end_actual_duration_day) - unix_timestamp(t1.cur_hour)
         ELSE 0 end as theory_time
  FROM 
  (
    SELECT WEEKOFYEAR(t1.days) as cur_week,
           t1.days as cur_date,
           date_format(concat(t1.days,' ',tt1.hourofday,':00:00'),'yyyy-MM-dd HH:00:00') as cur_hour
    FROM ${dim_dbname}.dim_day_date t1
    LEFT JOIN ${dim_dbname}.dim_day_of_hour tt1
    WHERE t1.days >= '${pre1_date}' AND t1.days <= DATE_ADD(current_date(),-1)
  )t1
  LEFT JOIN 
  (
    SELECT project_code
    FROM ${dim_dbname}.dim_collection_project_record_ful
  )t2
  LEFT JOIN agv_num t3
  ON t2.project_code = t3.project_code
  LEFT JOIN 
  (
    SELECT h.project_code,
           h.agv_code,
           h.d,
           case when h.d != TO_DATE(MIN(DATE_FORMAT(h.job_accept_time,'yyyy-MM-dd HH:mm:ss'))) AND h.d = TO_DATE(MAX(DATE_FORMAT(h.job_finish_time,'yyyy-MM-dd HH:mm:ss'))) then DATE_FORMAT(h.d,'yyyy-MM-dd HH:mm:ss') 
                else MIN(DATE_FORMAT(h.job_accept_time,'yyyy-MM-dd HH:mm:ss')) end AS start_actual_duration_day,
           MAX(DATE_FORMAT(h.job_finish_time,'yyyy-MM-dd HH:mm:ss')) AS end_actual_duration_day
    FROM ${dwd_dbname}.dwd_rcs_agv_job_history_info_di h
    WHERE h.d >= '${pre1_date}' AND TO_DATE(h.job_accept_time) >= '${pre1_date}'
    GROUP BY h.project_code,h.d,h.agv_code
  )t4
  ON t1.cur_date = t4.d AND t2.project_code = t4.project_code AND t3.agv_code = t4.agv_code AND t1.cur_hour >= DATE_FORMAT(t4.start_actual_duration_day,'yyyy-MM-dd HH:00:00') AND t1.cur_hour <= DATE_FORMAT(t4.end_actual_duration_day,'yyyy-MM-dd HH:00:00') 
)

INSERT overwrite table ${tmp_dbname}.tmp_amr_mtbf_breakdown_add partition(d,pt)
SELECT '' as id,
       t1.cur_week, -- 统计星期
       t1.cur_date, -- 统计日期
       t1.cur_hour, -- 统计小时
       t1.project_code, -- 项目编码
       t1.agv_type, -- 离线表机器人类型编码
       t1.agv_type_code, -- 机器人类型编码
       t1.agv_type_name, -- 机器人类型名称
       t1.agv_code, -- 机器人编码
       t1.mtbf_error_num, -- mtbf故障次数
       t1.mtbf_error_duration, -- mtbf故障次数
       t1.theory_time, -- mtbf故障次数
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
       SUBSTR(t1.cur_date,1,10) as d,
       t1.project_code as pt
FROM 
(
  SELECT t1.cur_week,
         t1.cur_date,
         t1.cur_hour,
         t1.project_code,
         t1.agv_type,
         t1.agv_type_code,
         t1.agv_type_name,
         t1.agv_code,
         cast(sum(nvl(t1.mtbf_error_num,0)) over(PARTITION BY t1.project_code,t1.agv_code order by t1.cur_date,t1.cur_hour) as int) as mtbf_error_num,
         cast(sum(nvl(t1.mtbf_error_duration,0)) over(PARTITION BY t1.project_code,t1.agv_code order by t1.cur_date,t1.cur_hour) as int) as mtbf_error_duration,
         cast(sum(nvl(t1.theory_time,0)) over(PARTITION BY t1.project_code,t1.agv_code order by t1.cur_date,t1.cur_hour) as int) as theory_time
  FROM 
  (
    SELECT t1.cur_week, -- 统计星期
           t1.cur_date, -- 统计日期
           t1.cur_hour, -- 统计小时
           t1.project_code, -- 项目编码
           t1.agv_type, -- 离线表机器人类型编码
           t1.agv_type_code, -- 机器人类型编码
           t1.agv_type_name, -- 机器人类型名称
           t1.agv_code, -- 机器人编码
           t2.mtbf_error_num, -- mtbf故障次数
           t1.mtbf_error_duration, -- mtbf故障时长
           t3.theory_time -- 实际运行时长
    FROM 
    (
      --每天新增mtbf故障时长 
      SELECT t1.cur_week, -- 统计星期
             t1.cur_date, -- 统计日期
             t1.cur_hour, -- 统计小时
             t1.project_code, -- 项目编码
             t1.agv_type, -- 离线表机器人类型编码
             t1.agv_type_code, -- 机器人类型编码
             t1.agv_type_name, -- 机器人类型名称
             t1.agv_code, -- 机器人编码
             sum(nvl(t2.breakdown_duration,0)) as mtbf_error_duration -- mtbf故障时长 
      FROM base t1
      LEFT JOIN err_breakdown t2
      ON t1.cur_date = t2.cur_date AND t1.cur_hour = t2.cur_hour AND t1.project_code = t2.project_code AND t1.agv_code = t2.agv_code
      GROUP BY t1.cur_week,t1.cur_date,t1.cur_hour,t1.project_code,t1.agv_type,t1.agv_type_code,t1.agv_type_name,t1.agv_code
    )t1
    LEFT JOIN 
    (
      --每天新增mtbf故障次数
      SELECT t1.cur_week, -- 统计星期
             t1.cur_date, -- 统计日期
             t1.cur_hour, -- 统计小时
             t1.project_code, -- 项目编码
             t1.agv_type, -- 离线表机器人类型编码
             t1.agv_type_code, -- 机器人类型编码
             t1.agv_type_name, -- 机器人类型名称
             t1.agv_code, -- 机器人编码
             COUNT(DISTINCT t2.breakdown_id) as mtbf_error_num -- mtbf故障次数
      FROM base t1
      LEFT JOIN err_times t2
      ON t1.cur_date = t2.cur_date AND t1.cur_hour = t2.cur_hour AND t1.project_code = t2.project_code AND t1.agv_code = t2.agv_code
      GROUP BY t1.cur_week,t1.cur_date,t1.cur_hour,t1.project_code,t1.agv_type,t1.agv_type_code,t1.agv_type_name,t1.agv_code
    )t2
    ON t1.cur_date = t2.cur_date AND t1.cur_hour = t2.cur_hour AND t1.project_code = t2.project_code AND t1.agv_code = t2.agv_code
    LEFT JOIN 
    (
      --每天新增实际运行时长
      SELECT t1.cur_week, -- 统计星期
             t1.cur_date, -- 统计日期
             t1.cur_hour, -- 统计小时
             t1.project_code, -- 项目编码
             t1.agv_type, -- 离线表机器人类型编码
             t1.agv_type_code, -- 机器人类型编码
             t1.agv_type_name, -- 机器人类型名称
             t1.agv_code, -- 机器人编码
             SUM(t1.theory_time) as theory_time -- 实际运行时长
      FROM base t1
      GROUP BY t1.cur_week,t1.cur_date,t1.cur_hour,t1.project_code,t1.agv_type,t1.agv_type_code,t1.agv_type_name,t1.agv_code
    )t3
    ON t1.cur_date = t3.cur_date AND t1.cur_hour = t3.cur_hour AND t1.project_code = t3.project_code AND t1.agv_code = t3.agv_code
  
    UNION ALL 
  
    SELECT cast(t1.cur_week as int) as cur_week, -- 统计星期
           t1.cur_date, -- 统计日期
           t1.cur_hour, -- 统计小时
           t1.project_code, -- 项目编码
           t1.agv_type, -- 离线表机器人类型编码
           t1.agv_type_code, -- 机器人类型编码
           t1.agv_type_name, -- 机器人类型名称
           t1.agv_code, -- 机器人编码
           cast(t1.mtbf_error_num as int) as mtbf_error_num, -- mtbf故障次数
           cast(t1.mtbf_error_duration as int) as mtbf_error_duration, -- mtbf故障时长
           cast(t1.theory_time as int) as theory_time -- 实际运行时长
    FROM 
    (
      SELECT *,ROW_NUMBER() over (PARTITION by cur_date,project_code,agv_code order by cur_hour desc) as rn
      FROM ${tmp_dbname}.tmp_amr_mtbf_breakdown_add
      WHERE d = DATE_ADD('${pre1_date}',-1)
    )t1
    WHERE t1.rn = 1 
  )t1
)t1
WHERE t1.cur_date >= '${pre1_date}';
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"