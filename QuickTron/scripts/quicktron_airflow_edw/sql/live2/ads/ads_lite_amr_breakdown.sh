#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp
pre11_date=`date -d "-8 day" +%F`

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
-- 项目概览简易机器人统计指标表 ads_lite_amr_breakdown 

WITH move_work AS
(
  -- 货到人场景
  SELECT j.project_code,
         j.agv_code,
         j.d,
         COUNT(DISTINCT j.job_id) AS move_work_num,
         SUM(IF(j.job_state = 'DONE',1,0)) AS complete_move_work_num
  FROM
  (
    SELECT *
    FROM ${dim_dbname}.dim_collection_project_record_ful c
    WHERE c.project_product_type_code IN (1,2)
  )c
  LEFT JOIN
  (
    SELECT *
    FROM ${dwd_dbname}.dwd_g2p_bucket_move_job_info_di j
    WHERE j.d >= '${pre11_date}'
  )j
  ON c.project_code = j.project_code
  WHERE LENGTH(j.agv_code) != 0
  GROUP BY j.project_code,j.agv_code,j.d
  
  UNION ALL 
  
  -- 辊筒车搬运场景|潜伏式标准搬运场景
  SELECT j.project_code,
         j.agv_code,
         j.d,
         COUNT(DISTINCT j.job_id) AS move_work_num,
         SUM(IF(j.job_state = 'DONE',1,0)) AS complete_move_work_num
  FROM
  (
    SELECT *
    FROM ${dim_dbname}.dim_collection_project_record_ful c
    WHERE c.project_product_type_code IN (3,4) OR c.project_product_type IN ('标准搬运')
  )c
  LEFT JOIN
  (
    SELECT *
    FROM ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di j
    WHERE j.d >= '${pre11_date}'
  )j
  ON c.project_code = j.project_code
  WHERE LENGTH(j.agv_code) != 0
  GROUP BY j.project_code,j.agv_code,j.d
  
  UNION ALL 
  
  -- QP场景
  SELECT qm.project_code,
         qm.agv_code,
         qm.d,
         COUNT(DISTINCT qm.job_id) AS move_work_num,
         SUM(IF(qm.job_state = 'DONE',1,0)) AS complete_move_work_num
  FROM
  (
    SELECT *
    FROM ${dim_dbname}.dim_collection_project_record_ful c
    WHERE c.project_product_type_code IN (3) OR c.project_product_type IN ('Quickpick','料箱搬运QP','QP')
  )c
  LEFT JOIN
  (
    SELECT *
    FROM ${dwd_dbname}.dwd_g2p_si_qp_move_job_info_di qm
    WHERE qm.d >= '${pre11_date}'
  )qm
  ON c.project_code = qm.project_code
  WHERE LENGTH(qm.agv_code) != 0
  GROUP BY qm.project_code,qm.agv_code,qm.d
    
  UNION ALL 
  
  -- QP场景 
  SELECT qt.project_code,
         qt.agv_code,
         qt.d,
         COUNT(DISTINCT qt.job_id) AS move_work_num,
         SUM(IF(qt.job_state = 'DONE',1,0)) AS complete_move_work_num
  FROM
  (
    SELECT *
    FROM ${dim_dbname}.dim_collection_project_record_ful c
    WHERE c.project_product_type_code IN (3) OR c.project_product_type IN ('Quickpick','料箱搬运QP','QP')
  )c
  LEFT JOIN
  (
    SELECT *
    FROM ${dwd_dbname}.dwd_g2p_si_qp_transfer_job_info_di qt
    WHERE qt.d >= '${pre11_date}'
  )qt
  ON c.project_code = qt.project_code
  WHERE LENGTH(qt.agv_code) != 0
  GROUP BY qt.project_code,qt.agv_code,qt.d
),
/*
-- v4故障
breakdown as
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
    WHERE b.d >= '${pre11_date}' AND b.error_level >= '3' 
  )tt1
  LEFT JOIN 
  (
    SELECT w.project_code,
           w.agv_code,
           w.status_log_time as status_change_time,
           w.d
    FROM ${dwd_dbname}.dwd_agv_working_status_incre_dt w
    WHERE w.d >= '${pre11_date}' AND w.online_status = 'REGISTERED' AND w.working_status = 'BUSY' 
    
    UNION ALL 
    
    SELECT r.project_code,
           r.agv_code,
           r.job_accept_time as status_change_time,
           r.d
    FROM ${dwd_dbname}.dwd_rcs_agv_job_history_info_di r
    WHERE r.d >= '${pre11_date}' AND r.pt IN (SELECT project_code FROM ${dim_dbname}.dim_collection_project_record_ful WHERE is_nonetwork = 1)
  ) tt2 
  ON tt2.project_code = tt1.project_code AND tt2.agv_code = tt1.agv_code
  WHERE tt2.status_change_time > tt1.error_time
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
  FROM 
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
  )tmp1
  LEFT JOIN 
  (
    SELECT DATE_FORMAT(CONCAT(d.days,' ',h.startofhour),'yyyy-MM-dd HH:mm:ss') as day_hour_start,
           DATE_FORMAT(CONCAT(d.days,' ',h.endofhour),'yyyy-MM-dd HH:mm:ss') as day_hour_end
    FROM ${dim_dbname}.dim_day_date d
    LEFT JOIN ${dim_dbname}.dim_day_of_hour h
    WHERE days >= '${pre11_date}' AND days <= DATE_ADD(current_date(),-1)
  )tmp2
  ON unix_timestamp(date_format(tmp1.error_time,'yyyy-MM-dd HH:00:00')) <= unix_timestamp(tmp2.day_hour_start) AND unix_timestamp(date_format(tmp1.end_time,'yyyy-MM-dd HH:00:00')) >= unix_timestamp(tmp2.day_hour_start)
),
end_breakdown as 
(
  SELECT TO_DATE(coalesce(t.sort_time[0],t.sort_time[1])) as cur_date,
         date_format(coalesce(t.sort_time[0],t.sort_time[1]),'yyyy-MM-dd HH:00:00') as cur_hour,
         t.error_time,
         coalesce(t.sort_time[0],t.sort_time[1]) as end_time,
         t.project_code,
         cast(coalesce(t.agv_type_code, -1) as string) as agv_type_code,
         cast(coalesce(t.agv_code, -1) as string) as agv_code,
         unix_timestamp(coalesce(t.sort_time[0],t.sort_time[1])) - unix_timestamp(t.error_time) as breakdown_duration -- 故障时长
  FROM breakdown t
  WHERE t.rk = 1
)
*/
-- v5故障
breakdown as
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
  WHERE b.d >= '${pre11_date}'
),
err_breakdown as 
(
  SELECT TO_DATE(tmp.day_hour_start) as cur_date,
         tmp.day_hour_start as cur_hour,
         IF(t.cur_hour = tmp.day_hour_start,t.error_time,tmp.day_hour_start) as error_time,
         IF(date_format(t.end_time,'yyyy-MM-dd HH:00:00') = tmp.day_hour_start,t.end_time,tmp.day_hour_end) as end_time,
         IF(unix_timestamp(IF(date_format(t.end_time,'yyyy-MM-dd HH:00:00') = tmp.day_hour_start,t.end_time,tmp.day_hour_end)) - unix_timestamp(IF(t.cur_hour = tmp.day_hour_start,t.error_time,tmp.day_hour_start)) = 3599,3600,unix_timestamp(IF(date_format(t.end_time,'yyyy-MM-dd HH:00:00') = tmp.day_hour_start,t.end_time,tmp.day_hour_end)) - unix_timestamp(IF(t.cur_hour = tmp.day_hour_start,t.error_time,tmp.day_hour_start))) as breakdown_duration,
         t.project_code,
         t.agv_type_code,
         t.agv_code,
         t.breakdown_id
  FROM breakdown t
  LEFT JOIN 
  (
    SELECT DATE_FORMAT(CONCAT(d.days,' ',h.startofhour),'yyyy-MM-dd HH:mm:ss') as day_hour_start,
           DATE_FORMAT(CONCAT(d.days,' ',h.endofhour),'yyyy-MM-dd HH:mm:ss') as day_hour_end
    FROM ${dim_dbname}.dim_day_date d
    LEFT JOIN ${dim_dbname}.dim_day_of_hour h
    WHERE days >= '${pre11_date}' AND days <= DATE_ADD(current_date(),-1)
  )tmp
  ON unix_timestamp(t.cur_hour) <= unix_timestamp(tmp.day_hour_start) AND unix_timestamp(date_format(t.end_time,'yyyy-MM-dd HH:00:00')) >= unix_timestamp(tmp.day_hour_start)
),
end_breakdown as 
(
  SELECT TO_DATE(t.end_time) as cur_date,
         date_format(t.end_time,'yyyy-MM-dd HH:00:00') as cur_hour,
         t.error_time,
         t.end_time,
         t.project_code,
         t.agv_type_code,
         t.agv_code,
         unix_timestamp(t.end_time) - unix_timestamp(t.error_time) as breakdown_duration -- 故障时长
  FROM breakdown t
)

INSERT overwrite table ${ads_dbname}.ads_lite_amr_breakdown partition(d,pt)
SELECT '' AS id, -- 主键
       NULL AS data_time, -- 数据产生时间（业务无关）
       bd.breakdown_id, -- 故障编码
       ba.agv_code AS amr_code, -- 机器人编码
       bat.agv_type_code AS amr_type, -- 机器人类型
       nvl(mw.move_work_num,0) AS carry_order_num, -- 搬运任务数量
       nvl(mw.complete_move_work_num,0) AS right_order_num, -- 正常完成的搬运作业单数量
       nvl(atn.agv_task_num,0) AS amr_task, -- 机器人任务数量
       nvl(ct.charger_times,0) AS total_charge, -- 充电次数
       nvl(ct.unusual_charger_times,0) AS exc_charge, -- 充电异常次数
       nvl(bd.breakdown_duration,0) AS error_duration, -- 机器人故障时长
       nvl(mttr.breakdown_num,0) AS mttr_error_num, -- mttr故障次数
       nvl(mttr.breakdown_duration,0) AS mttr_error_duration, -- mttr故障时长
       ad.start_actual_duration AS start_time, -- 开始运行时段
       ad.end_actual_duration AS end_time, -- 结束运行时段
       unix_timestamp(ad.end_actual_duration_day) - unix_timestamp(ad.start_actual_duration_day) AS actual_duration, -- 时间运行时间
       c.project_code, -- 项目编码
       ba.d AS happen_time, -- 统计日期
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time,
	   abi.breakdown_id as add_breakdown_id, -- 新增故障id
       ba.d,
       c.project_code AS pt
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful c
  WHERE c.project_version like '2.%'
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
-- 机器人故障次数
(
  SELECT bd.project_code,
         bd.agv_code,
         bd.cur_date,
         concat_ws(',',collect_set(bd.breakdown_id)) AS breakdown_id,
         SUM(nvl(bd.breakdown_duration,0)) AS breakdown_duration
  FROM err_breakdown bd
  GROUP BY bd.project_code,bd.agv_code,bd.cur_date
)bd
ON bd.cur_date = ba.d AND bd.project_code = ba.project_code AND bd.agv_code = ba.agv_code
LEFT JOIN 
-- 机器人搬运任务数量
(
  SELECT *
  FROM move_work
)mw
ON mw.d = ba.d AND mw.project_code = ba.project_code AND mw.agv_code = ba.agv_code
LEFT JOIN 
-- 机器人任务
(
  SELECT h.project_code,
         h.agv_code,
         h.d,
         COUNT(DISTINCT h.job_id) AS agv_task_num
  FROM ${dwd_dbname}.dwd_rcs_agv_job_history_info_di h
  WHERE h.d >= '${pre11_date}'
  GROUP BY h.project_code,h.agv_code,h.d
)atn
ON atn.d = ba.d AND atn.project_code = ba.project_code AND atn.agv_code = ba.agv_code
LEFT JOIN 
--机器人充电任务
(
  SELECT h.project_code,
         h.agv_code,
         h.d,
         COUNT(DISTINCT h.job_id) AS charger_times,
         SUM(IF(h.job_state != 'JOB_COMPLETED',1,0)) AS unusual_charger_times
  FROM ${dwd_dbname}.dwd_rcs_agv_job_history_info_di h
  WHERE h.d >= '${pre11_date}' AND h.job_type = 'CHARGE_JOB'
  GROUP BY h.project_code,h.agv_code,h.d
)ct
ON ct.d = ba.d AND ct.project_code = ba.project_code AND ct.agv_code = ba.agv_code
LEFT JOIN 
-- 机器人运行时间段
(
  SELECT h.project_code,
         h.agv_code,
         h.d,
         case when h.d != TO_DATE(MIN(DATE_FORMAT(h.job_accept_time,'yyyy-MM-dd HH:mm:ss'))) AND h.d = TO_DATE(MAX(DATE_FORMAT(h.job_finish_time,'yyyy-MM-dd HH:mm:ss'))) then DATE_FORMAT(h.d,'yyyy-MM-dd HH:mm:ss') 
              else MIN(DATE_FORMAT(h.job_accept_time,'yyyy-MM-dd HH:mm:ss')) end AS start_actual_duration_day,
         MAX(DATE_FORMAT(h.job_finish_time,'yyyy-MM-dd HH:mm:ss')) AS end_actual_duration_day,
         SUBSTR(case when h.d != TO_DATE(MIN(DATE_FORMAT(h.job_accept_time,'yyyy-MM-dd HH:mm:ss'))) AND h.d = TO_DATE(MAX(DATE_FORMAT(h.job_finish_time,'yyyy-MM-dd HH:mm:ss'))) then DATE_FORMAT(h.d,'yyyy-MM-dd HH:mm:ss') 
              else MIN(DATE_FORMAT(h.job_accept_time,'yyyy-MM-dd HH:mm:ss')) end,12,8) AS start_actual_duration,
         MAX(SUBSTR(DATE_FORMAT(h.job_finish_time,'yyyy-MM-dd HH:mm:ss'),12,8)) AS end_actual_duration
  FROM ${dwd_dbname}.dwd_rcs_agv_job_history_info_di h
  WHERE h.d >= '${pre11_date}' AND h.job_accept_time >= '${pre11_date}'
  GROUP BY h.project_code,h.agv_code,h.d
)ad
ON ad.d = ba.d AND ad.project_code = ba.project_code AND ad.agv_code = ba.agv_code
LEFT JOIN 
-- MTTR故障次数|故障时长
(
  SELECT bd.project_code,
         bd.agv_code,
         bd.cur_date,
         COUNT(*) AS breakdown_num,
         SUM(nvl(bd.breakdown_duration,0)) AS breakdown_duration
  FROM end_breakdown bd
  GROUP BY bd.project_code,bd.agv_code,bd.cur_date
)mttr
ON mttr.cur_date = ba.d AND mttr.project_code = ba.project_code AND mttr.agv_code = ba.agv_code
LEFT JOIN 
(
  SELECT TO_DATE(t.error_time) as cur_date,
         t.project_code,
         cast(coalesce(t.agv_code, -1) as string) as agv_code,
         concat_ws(',' , collect_set(t.breakdown_id)) as breakdown_id
  FROM breakdown t
  GROUP BY TO_DATE(t.error_time),t.project_code,cast(coalesce(t.agv_code, -1) as string)
)abi
ON abi.cur_date = ba.d AND abi.project_code = ba.project_code AND abi.agv_code = ba.agv_code
WHERE ba.agv_code IS NOT NULL -- 剔除无机器人的数据
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"