#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp
pre1_date=`date -d "-4 day" +%F`

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
-- set mapred.max.split.size=128000000;
-- set mapred.min.split.size.per.node=50000000;
-- set mapred.min.split.size.per.rack=50000000;
-- set hive.merge.sparkfiles=true;
-- set hive.merge.mapfiles=true;
-- set hive.merge.mapredfiles=true;
-- set hive.merge.size.per.task=128000000;
-- set hive.merge.smallfiles.avgsize=8000000;
-------------------------------------------------------------------------------------------------------------00
-- 机器人故障统计 ads_amr_breakdown 

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
    WHERE days >= '${pre1_date}' AND days <= DATE_ADD(current_date(),-1)
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
),
*/
-- v5 故障
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
    WHERE days >= '${pre1_date}' AND days <= DATE_ADD(current_date(),-1)
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
),
agv_num as 
(
  SELECT t.project_code,
         a.agv_type,
         t.agv_type_code,
         nvl(a.agv_type_name,t.agv_type_name) as agv_type_name,
         nvl(a.agv_code,t.agv_code) as agv_code,
         t.d as cur_date
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
	WHERE project_version LIKE '2.%'
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
),
order_id as 
(
  SELECT t1.cur_week, -- 统计星期
         t1.cur_date, -- 统计日期
         t1.cur_hour, -- 统计小时
         t2.project_code, -- 项目编码
         t3.agv_type, -- 离线表机器人类型编码
         t3.agv_type_code, -- 机器人类型编码
         t3.agv_type_name, -- 机器人类型名称
         t3.agv_code, -- 机器人编码
         c.cyclecount_num, -- 盘点单
         r1.guided_putaway_num, -- 指导上架单
         p.picking_num -- 拣选单
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
    WHERE project_version LIKE '2.%' AND project_product_type_code IN (1,2) 
  )t2
  LEFT JOIN agv_num t3
  ON t2.project_code = t3.project_code
  -- 盘点订单
  LEFT JOIN
  (
    SELECT cc.d as cur_date,
           date_format(cc.cyclecount_created_time,'yyyy-MM-dd HH:00:00') as cur_hour,
           cc.project_code,
           nvl(cj.agv_code,'UNKNOWN') as agv_code,
           cc.id as cyclecount_num
    FROM ${dwd_dbname}.dwd_cyclecount_cycle_count_info_di cc
    LEFT JOIN ${dwd_dbname}.dwd_cyclecount_cycle_count_work_info_di ck
    ON cc.id = ck.cycle_count_id AND ck.d = cc.d AND cc.pt = ck.pt
    LEFT JOIN ${dwd_dbname}.dwd_g2p_countcheck_job_info_di cj
    ON ck.id = cj.work_id AND cj.d = cc.d AND cc.pt = cj.pt
    WHERE cc.d >= '${pre1_date}'
  )c
  ON t1.cur_date = c.cur_date AND t1.cur_hour = c.cur_hour AND t2.project_code = c.project_code AND t3.agv_code = c.agv_code
  -- 指导上架订单
  LEFT JOIN
  ( 
    SELECT r.d as cur_date,
           date_format(r.order_created_time,'yyyy-MM-dd HH:00:00') as cur_hour,
           r.project_code,
           nvl(g.agv_code,'UNKNOWN') as agv_code,
           r.id as guided_putaway_num
    FROM ${dwd_dbname}.dwd_replenish_order_info_di r
    JOIN ${dwd_dbname}.dwd_g2p_guided_putaway_job_info_di g
    ON r.id = g.order_id AND g.d = r.d AND r.pt = g.pt
    WHERE r.d >= '${pre1_date}'
  )r1
  ON t1.cur_date = r1.cur_date AND t1.cur_hour = r1.cur_hour AND t2.project_code = r1.project_code AND t3.agv_code = r1.agv_code
  -- 直接上架订单
  -- 拣选订单
  LEFT JOIN
  ( 
    SELECT p.d as cur_date,
           date_format(p.order_created_time,'yyyy-MM-dd HH:00:00') as cur_hour,
           p.project_code,
           nvl(pj.agv_code,'UNKNOWN') as agv_code,
           p.id as picking_num
    FROM ${dwd_dbname}.dwd_picking_order_info_di p
    LEFT JOIN ${dwd_dbname}.dwd_g2p_picking_job_info_di pj
    ON p.id = pj.order_id AND pj.d = p.d AND p.pt = pj.pt
    WHERE p.d >= '${pre1_date}'
  )p
  ON t1.cur_date = p.cur_date AND t1.cur_hour = p.cur_hour AND t2.project_code = p.project_code AND t3.agv_code = p.agv_code
),
movejob_id as 
(
  SELECT aj.d as cur_date,
         date_format(aj.job_created_time,'yyyy-MM-dd HH:00:00') as cur_hour,
         aj.project_code,
         b.agv_type_code,
         aj.agv_code,
         aj.id as move_job_num
  FROM ${dwd_dbname}.dwd_rcs_agv_job_history_info_di aj
  LEFT JOIN ${dwd_dbname}.dwd_rcs_agv_base_info_df b
  ON aj.project_code = b.project_code AND nvl(aj.agv_code,'unknown') = b.agv_code AND aj.d = b.d
  WHERE b.agv_code is not null AND aj.d >= '${pre1_date}'
),
qpwork_id as 
(
  SELECT t1.cur_week, -- 统计星期
         t1.cur_date, -- 统计日期
         t1.cur_hour, -- 统计小时
         t2.project_code, -- 项目编码
         t3.agv_type, -- 离线表机器人类型编码
         t3.agv_type_code, -- 机器人类型编码
         t3.agv_type_name, -- 机器人类型名称
         t3.agv_code, -- 机器人编码
         qp.job_id -- 作业单编码
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
    WHERE project_version LIKE '2.%' AND (project_product_type IN ('Quickpick','料箱搬运QP','QP') OR project_code IN ('A51274'))
  )t2
  LEFT JOIN agv_num t3
  ON t2.project_code = t3.project_code
  -- 作业单
  LEFT JOIN
  (
    SELECT r.d as cur_date, 
           date_format(r.job_created_time,'yyyy-MM-dd HH:00:00') as cur_hour,
           r.project_code,
           r.job_id,
           m.agv_code,
           m.agv_type as agv_type_code
    FROM ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di r
    LEFT JOIN ${dwd_dbname}.dwd_g2p_si_qp_extend_info_df e 
    ON r.project_code = e.project_code AND r.robot_job_id = e.job_id AND e.d = r.d
    LEFT JOIN ${dwd_dbname}.dwd_g2p_si_qp_move_job_info_di m
    ON r.project_code = m.project_code AND e.move_job_id = m.id AND m.d = r.d
    WHERE r.d >= '${pre1_date}'
        
    UNION ALL 
        
    SELECT r.d as cur_date, 
           date_format(r.job_created_time,'yyyy-MM-dd HH:00:00') as cur_hour,
           r.project_code,
           r.job_id,
           t.agv_code,
           t.agv_type
    FROM ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di r
    LEFT JOIN ${dwd_dbname}.dwd_g2p_si_qp_extend_info_df e 
    ON r.project_code = e.project_code AND r.robot_job_id = e.job_id AND e.d = r.d
    LEFT JOIN ${dwd_dbname}.dwd_g2p_si_qp_transfer_job_info_di t
    ON r.project_code = t.project_code AND e.transfer_job_id = t.id AND t.d = r.d
    WHERE r.d >= '${pre1_date}'
  )qp
  ON t1.cur_date = qp.cur_date AND t1.cur_hour = qp.cur_hour AND t2.project_code = qp.project_code AND t3.agv_code = qp.agv_code
),
stwork_id as 
(
  SELECT t1.cur_week, -- 统计星期
         t1.cur_date, -- 统计日期
         t1.cur_hour, -- 统计小时
         t2.project_code, -- 项目编码
         t3.agv_type, -- 离线表机器人类型编码
         t3.agv_type_code, -- 机器人类型编码
         t3.agv_type_name, -- 机器人类型名称
         t3.agv_code, -- 机器人编码
         st.job_id -- 作业单编码
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
    WHERE project_version LIKE '2.%' AND (project_product_type_code = 4 OR project_product_type IN ('标准搬运','堆高车搬运') OR project_code = 'A51346')
  )t2
  LEFT JOIN agv_num t3
  ON t2.project_code = t3.project_code
  -- 作业单
  LEFT JOIN
  (
    SELECT r.d as cur_date, 
           date_format(r.job_created_time,'yyyy-MM-dd HH:00:00') as cur_hour,
           r.project_code,
           r.agv_code,
           r.job_id
    FROM ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di r
    WHERE r.d >= '${pre1_date}'
  )st
  ON t1.cur_date = st.cur_date AND t1.cur_hour = st.cur_hour AND t2.project_code = st.project_code AND t3.agv_code = st.agv_code
)



INSERT overwrite table ${ads_dbname}.ads_amr_breakdown partition(d,pt)
-- 分时分小车
SELECT '' as id, -- 主键
       NULL as data_time, -- 统计小时
       t1.project_code, -- 项目编码
       t1.cur_hour as happen_time, -- 统计小时
       'single' as type_class, -- 数据类型
       nvl(t1.agv_type,t1.agv_type_code) as amr_type, -- 机器人类型编码
       t1.agv_type_name as amr_type_des, -- 机器人类型名称
       t1.agv_code as amr_code, -- 机器人编码
       t2.breakdown_id, -- 故障次数
       t1.cyclecount_num + t1.guided_putaway_num + t1.picking_num + t1.send_workbin as carry_order_num, -- 订单量
       nvl(t3.move_job_num,0) as carry_task_num, -- 搬运任务数
       t5.theory_time, -- 理论运行时长
       nvl(t2.breakdown_duration,0) as error_duration, -- 故障时长
       nvl(t4.breakdown_duration,0) as mttr_error_duration, -- mttr故障时长
       nvl(t4.breakndown_num,0) as mttr_error_num, -- mttr错误次数
       cast(nvl((t6.theory_time - t6.mtbf_error_duration) / t6.mtbf_error_num,t6.theory_time) as decimal(10,2)) as add_mtbf, -- 累计mtbf
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
       t7.breakdown_id as add_breakdown_id, -- 新增故障id
       SUBSTR(t1.cur_hour,1,10) as d,
       t1.project_code as pt
FROM 
(
  SELECT cur_week, -- 统计星期
         cur_date, -- 统计日期
         cur_hour, -- 统计小时
         project_code, -- 项目编码
         agv_type, -- 离线表机器人类型编码
         agv_type_code, -- 机器人类型编码
         agv_type_name, -- 机器人类型名称
         agv_code, -- 机器人编码
         COUNT(DISTINCT cyclecount_num) as cyclecount_num, -- 盘点单
         COUNT(DISTINCT guided_putaway_num) as guided_putaway_num, -- 指导上架单
         COUNT(DISTINCT picking_num) as picking_num, -- 拣选单
         0 as send_workbin -- 作业单
  FROM order_id
  GROUP BY cur_week,cur_date,cur_hour,project_code,agv_type,agv_type_code,agv_type_name,agv_code
  
  UNION ALL 
  
  SELECT cur_week, -- 统计星期
         cur_date, -- 统计日期
         cur_hour, -- 统计小时
         project_code, -- 项目编码
         agv_type, -- 离线表机器人类型编码
         agv_type_code, -- 机器人类型编码
         agv_type_name, -- 机器人类型名称
         agv_code, -- 机器人编码
         0 as cyclecount_num, -- 盘点单
         0 as guided_putaway_num, -- 指导上架单
         0 as picking_num, -- 拣选单
         COUNT(DISTINCT job_id) as send_workbin -- 作业单
  FROM qpwork_id
  GROUP BY cur_week,cur_date,cur_hour,project_code,agv_type,agv_type_code,agv_type_name,agv_code
  
  UNION ALL 
  
  SELECT cur_week, -- 统计星期
         cur_date, -- 统计日期
         cur_hour, -- 统计小时
         project_code, -- 项目编码
         agv_type, -- 离线表机器人类型编码
         agv_type_code, -- 机器人类型编码
         agv_type_name, -- 机器人类型名称
         agv_code, -- 机器人编码
         0 as cyclecount_num, -- 盘点单
         0 as guided_putaway_num, -- 指导上架单
         0 as picking_num, -- 拣选单
         COUNT(DISTINCT job_id) as send_workbin -- 作业单
  FROM stwork_id
  GROUP BY cur_week,cur_date,cur_hour,project_code,agv_type,agv_type_code,agv_type_name,agv_code
)t1
LEFT JOIN 
(
  SELECT t.cur_date, -- 统计日期
         t.cur_hour, -- 统计小时
         t.project_code, -- 项目编码
         t.agv_type_code, -- 机器人类型编码
         t.agv_code, -- 机器人编码
         concat_ws(',' , collect_list(t.breakdown_id)) as breakdown_id, -- 故障编码
         COUNT(DISTINCT t.agv_code) as breakndown_agv_num, -- 故障小车数
         sum(t.breakdown_duration) as breakdown_duration -- 故障时长
  FROM err_breakdown t
  GROUP BY t.cur_date,t.cur_hour,t.project_code,t.agv_type_code,t.agv_code
)t2
ON t1.cur_date = t2.cur_date AND t1.cur_hour = t2.cur_hour AND t1.project_code = t2.project_code AND t1.agv_code = t2.agv_code
LEFT JOIN 
(
  SELECT cur_date, -- 统计日期
         cur_hour, -- 统计小时
         project_code, -- 项目编码
         agv_type_code, -- 机器人类型
         agv_code, -- 机器人编码
         COUNT(DISTINCT move_job_num) as move_job_num -- 搬运任务
  FROM movejob_id
  GROUP BY cur_date,cur_hour,project_code,agv_type_code,agv_code
)t3
ON t1.cur_date = t3.cur_date AND t1.cur_hour = t3.cur_hour AND t1.project_code = t3.project_code AND t1.agv_code = t3.agv_code
LEFT JOIN 
(
  SELECT t.cur_date, -- 统计日期
         t.cur_hour, -- 统计小时
         t.project_code, -- 项目编码
         t.agv_type_code, -- 机器人类型编码
         t.agv_code, -- 机器人编码
         COUNT(*) as breakndown_num, -- 故障次数
         COUNT(DISTINCT t.agv_code) as breakndown_agv_num, -- 故障小车数
         sum(t.breakdown_duration) as breakdown_duration -- 故障时长
  FROM end_breakdown t
  GROUP BY t.cur_date,t.cur_hour,t.project_code,t.agv_type_code,t.agv_code
)t4
ON t1.cur_date = t4.cur_date AND t1.cur_hour = t4.cur_hour AND t1.project_code = t4.project_code AND t1.agv_code = t4.agv_code
LEFT JOIN base t5
ON t1.cur_date = t5.cur_date AND t1.cur_hour = t5.cur_hour AND t1.project_code = t5.project_code AND t1.agv_code = t5.agv_code
LEFT JOIN 
(
  SELECT t.cur_date, -- 统计日期
         t.cur_hour, -- 统计小时
         t.project_code, -- 项目编码
         t.agv_type_code, -- 机器人类型编码
         t.agv_code, -- 机器人编码
         t.mtbf_error_num, -- 故障小车数
         t.mtbf_error_duration, -- 故障时长
         t.theory_time -- 理论运行时长
  FROM ${tmp_dbname}.tmp_amr_mtbf_breakdown_add t
  WHERE t.d >= '${pre1_date}'
)t6
ON t1.cur_date = t6.cur_date AND t1.cur_hour = t6.cur_hour AND t1.project_code = t6.project_code AND t1.agv_code = t6.agv_code
LEFT JOIN 
(
  SELECT TO_DATE(t.error_time) as cur_date,
         date_format(t.error_time,'yyyy-MM-dd HH:00:00') as cur_hour,
         t.project_code,
         cast(coalesce(t.agv_type_code, -1) as string) as agv_type_code,
         cast(coalesce(t.agv_code, -1) as string) as agv_code,
         concat_ws(',' , collect_list(t.breakdown_id)) as breakdown_id
  FROM breakdown t
  GROUP BY TO_DATE(t.error_time),date_format(t.error_time,'yyyy-MM-dd HH:00:00'),t.project_code,cast(coalesce(t.agv_type_code, -1) as string),cast(coalesce(t.agv_code, -1) as string)
)t7
ON t1.cur_date = t7.cur_date AND t1.cur_hour = t7.cur_hour AND t1.project_code = t7.project_code AND t1.agv_code = t7.agv_code


UNION ALL 

-- 分时分类型
SELECT '' as id, -- 主键
       NULL as data_time, -- 统计小时
       t1.project_code, -- 项目编码
       t1.cur_hour as happen_time, -- 统计小时
       'part' as type_class, -- 数据类型
       nvl(t1.agv_type,t1.agv_type_code) as amr_type, -- 机器人类型编码
       t1.agv_type_name as amr_type_des, -- 机器人类型名称
       NULL as amr_code, -- 机器人编码
       t2.breakndown_num as error_num, -- 故障次数
       t1.cyclecount_num + t1.guided_putaway_num + t1.picking_num + t1.send_workbin as carry_order_num, -- 订单量
       nvl(t3.move_job_num,0) as carry_task_num, -- 搬运任务数
       t5.theory_time, -- 理论运行时长
       nvl(t2.breakdown_duration,0) as error_duration, -- 故障时长
       nvl(t4.breakdown_duration,0) as mttr_error_duration, -- mttr故障时长
       nvl(t4.breakndown_num,0) as mttr_error_num, -- mttr错误次数
       cast(nvl((t6.theory_time - t6.mtbf_error_duration) / t6.mtbf_error_num,t6.theory_time) / t5.agv_num as decimal(10,2)) as add_mtbf, -- 累计mtbf
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
	   t7.breakdown_id as add_breakdown_id, -- 新增故障id
       SUBSTR(t1.cur_hour,1,10) as d,
       t1.project_code as pt
FROM 
(
  SELECT cur_week, -- 统计星期
         cur_date, -- 统计日期
         cur_hour, -- 统计小时
         project_code, -- 项目编码
         agv_type, -- 离线表机器人类型编码
         agv_type_code, -- 机器人类型编码
         agv_type_name, -- 机器人类型名称
         COUNT(DISTINCT agv_code) as agv_num, -- 机器人数量
         COUNT(DISTINCT cyclecount_num) as cyclecount_num, -- 盘点单
         COUNT(DISTINCT guided_putaway_num) as guided_putaway_num, -- 指导上架单
         COUNT(DISTINCT picking_num) as picking_num, -- 拣选单
         0 as send_workbin -- 作业单
  FROM order_id
  GROUP BY cur_week,cur_date,cur_hour,project_code,agv_type,agv_type_code,agv_type_name
  
  UNION ALL 
  
  SELECT cur_week, -- 统计星期
         cur_date, -- 统计日期
         cur_hour, -- 统计小时
         project_code, -- 项目编码
         agv_type, -- 离线表机器人类型编码
         agv_type_code, -- 机器人类型编码
         agv_type_name, -- 机器人类型名称
         COUNT(DISTINCT agv_code) as agv_num, -- 机器人数量
         0 as cyclecount_num, -- 盘点单
         0 as guided_putaway_num, -- 指导上架单
         0 as picking_num, -- 拣选单
         COUNT(DISTINCT job_id) as send_workbin -- 作业单
  FROM qpwork_id
  GROUP BY cur_week,cur_date,cur_hour,project_code,agv_type,agv_type_code,agv_type_name
  
  UNION ALL 
  
  SELECT cur_week, -- 统计星期
         cur_date, -- 统计日期
         cur_hour, -- 统计小时
         project_code, -- 项目编码
         agv_type, -- 离线表机器人类型编码
         agv_type_code, -- 机器人类型编码
         agv_type_name, -- 机器人类型名称
         COUNT(DISTINCT agv_code) as agv_num, -- 机器人数量
         0 as cyclecount_num, -- 盘点单
         0 as guided_putaway_num, -- 指导上架单
         0 as picking_num, -- 拣选单
         COUNT(DISTINCT job_id) as send_workbin -- 作业单
  FROM stwork_id
  GROUP BY cur_week,cur_date,cur_hour,project_code,agv_type,agv_type_code,agv_type_name
)t1
LEFT JOIN 
(
  SELECT t.cur_date, -- 统计日期
         t.cur_hour, -- 统计小时
         t.project_code, -- 项目编码
         t.agv_type_code, -- 机器人类型编码
         concat_ws(',' , collect_list(t.breakdown_id)) as breakndown_num, -- 故障次数
         COUNT(DISTINCT t.agv_code) as breakndown_agv_num, -- 故障小车数
         sum(t.breakdown_duration) as breakdown_duration -- 故障时长
  FROM err_breakdown t
  GROUP BY t.cur_date,t.cur_hour,t.project_code,t.agv_type_code
)t2
ON t1.cur_date = t2.cur_date AND t1.cur_hour = t2.cur_hour AND t1.project_code = t2.project_code AND t1.agv_type_code = t2.agv_type_code
LEFT JOIN 
(
  SELECT cur_date, -- 统计日期
         cur_hour, -- 统计小时
         project_code, -- 项目编码
         agv_type_code, -- 机器人类型
         COUNT(DISTINCT move_job_num) as move_job_num -- 搬运任务
  FROM movejob_id
  GROUP BY cur_date,cur_hour,project_code,agv_type_code
)t3
ON t1.cur_date = t3.cur_date AND t1.cur_hour = t3.cur_hour AND t1.project_code = t3.project_code AND t1.agv_type_code = t3.agv_type_code
LEFT JOIN 
(
  SELECT t.cur_date, -- 统计日期
         t.cur_hour, -- 统计小时
         t.project_code, -- 项目编码
         t.agv_type_code, -- 机器人类型编码
         COUNT(*) as breakndown_num, -- 故障次数
         COUNT(DISTINCT t.agv_code) as breakndown_agv_num, -- 故障小车数
         sum(t.breakdown_duration) as breakdown_duration -- 故障时长
  FROM end_breakdown t
  GROUP BY t.cur_date,t.cur_hour,t.project_code,t.agv_type_code
)t4
ON t1.cur_date = t4.cur_date AND t1.cur_hour = t4.cur_hour AND t1.project_code = t4.project_code AND t1.agv_type_code = t4.agv_type_code
LEFT JOIN 
(
  SELECT cur_date,
         cur_hour,
         project_code,
         agv_type_code,
         COUNT(agv_code) as agv_num,
         SUM(theory_time) as theory_time
  FROM base
  GROUP BY cur_date,cur_hour,project_code,agv_type_code
)t5
ON t1.cur_date = t5.cur_date AND t1.cur_hour = t5.cur_hour AND t1.project_code = t5.project_code AND t1.agv_type_code = t5.agv_type_code
LEFT JOIN 
(
  SELECT t.cur_date, -- 统计日期
         t.cur_hour, -- 统计小时
         t.project_code, -- 项目编码
         t.agv_type_code, -- 机器人类型编码
         SUM(t.mtbf_error_num) as mtbf_error_num, -- 故障小车数
         SUM(t.mtbf_error_duration) as mtbf_error_duration, -- 故障时长
         SUM(t.theory_time) as theory_time -- 理论运行时长
  FROM ${tmp_dbname}.tmp_amr_mtbf_breakdown_add t
  WHERE t.d >= '${pre1_date}'
  GROUP BY t.cur_date,t.cur_hour,t.project_code,t.agv_type_code
)t6
ON t1.cur_date = t6.cur_date AND t1.cur_hour = t6.cur_hour AND t1.project_code = t6.project_code AND t1.agv_type_code = t6.agv_type_code
LEFT JOIN 
(
  SELECT TO_DATE(t.error_time) as cur_date,
         date_format(t.error_time,'yyyy-MM-dd HH:00:00') as cur_hour,
         t.project_code,
         cast(coalesce(t.agv_type_code, -1) as string) as agv_type_code,
         concat_ws(',' , collect_list(t.breakdown_id)) as breakdown_id
  FROM breakdown t
  GROUP BY TO_DATE(t.error_time),date_format(t.error_time,'yyyy-MM-dd HH:00:00'),t.project_code,cast(coalesce(t.agv_type_code, -1) as string)
)t7
ON t1.cur_date = t7.cur_date AND t1.cur_hour = t7.cur_hour AND t1.project_code = t7.project_code AND t1.agv_type_code = t7.agv_type_code


UNION ALL 

-- 分时分项目
SELECT '' as id, -- 主键
       NULL as data_time, -- 统计小时
       t1.project_code, -- 项目编码
       t1.cur_hour as happen_time, -- 统计小时
       'all' as type_class, -- 数据类型
       NULL as amr_type, -- 机器人类型编码
       NULL as amr_type_des, -- 机器人类型名称
       NULL as amr_code, -- 机器人编码
       t2.breakndown_num as error_num, -- 故障次数
       t1.cyclecount_num + t1.guided_putaway_num + t1.picking_num + t1.send_workbin as carry_order_num, -- 订单量
       nvl(t3.move_job_num,0) as carry_task_num, -- 搬运任务数
       t5.theory_time, -- 理论运行时长
       nvl(t2.breakdown_duration,0) as error_duration, -- 故障时长
       nvl(t4.breakdown_duration,0) as mttr_error_duration, -- mttr故障时长
       nvl(t4.breakndown_num,0) as mttr_error_num, -- mttr错误次数
       cast(nvl((t6.theory_time - t6.mtbf_error_duration) / t6.mtbf_error_num,t6.theory_time) / t5.agv_num as decimal(10,2)) as add_mtbf, -- 累计mtbf
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
	   t7.breakdown_id as add_breakdown_id, -- 新增故障id
       SUBSTR(t1.cur_hour,1,10) as d,
       t1.project_code as pt
FROM 
(
  SELECT cur_week, -- 统计星期
         cur_date, -- 统计日期
         cur_hour, -- 统计小时
         project_code, -- 项目编码
         COUNT(DISTINCT agv_code) as agv_num, -- 机器人数量
         COUNT(DISTINCT cyclecount_num) as cyclecount_num, -- 盘点单
         COUNT(DISTINCT guided_putaway_num) as guided_putaway_num, -- 指导上架单
         COUNT(DISTINCT picking_num) as picking_num, -- 拣选单
         0 as send_workbin -- 作业单
  FROM order_id
  GROUP BY cur_week,cur_date,cur_hour,project_code
  
  UNION ALL 
  
  SELECT cur_week, -- 统计星期
         cur_date, -- 统计日期
         cur_hour, -- 统计小时
         project_code, -- 项目编码
         COUNT(DISTINCT agv_code) as agv_num, -- 机器人数量
         0 as cyclecount_num, -- 盘点单
         0 as guided_putaway_num, -- 指导上架单
         0 as picking_num, -- 拣选单
         COUNT(DISTINCT job_id) as send_workbin -- 作业单
  FROM qpwork_id
  GROUP BY cur_week,cur_date,cur_hour,project_code
  
  UNION ALL 
  
  SELECT cur_week, -- 统计星期
         cur_date, -- 统计日期
         cur_hour, -- 统计小时
         project_code, -- 项目编码
         COUNT(DISTINCT agv_code) as agv_num, -- 机器人数量
         0 as cyclecount_num, -- 盘点单
         0 as guided_putaway_num, -- 指导上架单
         0 as picking_num, -- 拣选单
         COUNT(DISTINCT job_id) as send_workbin -- 作业单
  FROM stwork_id
  GROUP BY cur_week,cur_date,cur_hour,project_code
)t1
LEFT JOIN 
(
  SELECT t.cur_date, -- 统计日期
         t.cur_hour, -- 统计小时
         t.project_code, -- 项目编码
         concat_ws(',' , collect_list(t.breakdown_id)) as breakndown_num, -- 故障次数
         COUNT(DISTINCT t.agv_code) as breakndown_agv_num, -- 故障小车数
         sum(t.breakdown_duration) as breakdown_duration -- 故障时长
  FROM err_breakdown t
  GROUP BY t.cur_date,t.cur_hour,t.project_code
)t2
ON t1.cur_date = t2.cur_date AND t1.cur_hour = t2.cur_hour AND t1.project_code = t2.project_code
LEFT JOIN 
(
  SELECT cur_date, -- 统计日期
         cur_hour, -- 统计小时
         project_code, -- 项目编码
         COUNT(DISTINCT move_job_num) as move_job_num -- 搬运任务
  FROM movejob_id
  GROUP BY cur_date,cur_hour,project_code
)t3
ON t1.cur_date = t3.cur_date AND t1.cur_hour = t3.cur_hour AND t1.project_code = t3.project_code
LEFT JOIN 
(
  SELECT t.cur_date, -- 统计日期
         t.cur_hour, -- 统计小时
         t.project_code, -- 项目编码
         COUNT(*) as breakndown_num, -- 故障次数
         COUNT(DISTINCT t.agv_code) as breakndown_agv_num, -- 故障小车数
         sum(t.breakdown_duration) as breakdown_duration -- 故障时长
  FROM end_breakdown t
  GROUP BY t.cur_date,t.cur_hour,t.project_code
)t4
ON t1.cur_date = t4.cur_date AND t1.cur_hour = t4.cur_hour AND t1.project_code = t4.project_code
LEFT JOIN 
(
  SELECT cur_date,
         cur_hour,
         project_code,
         COUNT(agv_code) as agv_num,
         SUM(theory_time) as theory_time
  FROM base
  GROUP BY cur_date,cur_hour,project_code
)t5
ON t1.cur_date = t5.cur_date AND t1.cur_hour = t5.cur_hour AND t1.project_code = t5.project_code
LEFT JOIN 
(
  SELECT t.cur_date, -- 统计日期
         t.cur_hour, -- 统计小时
         t.project_code, -- 项目编码
         SUM(t.mtbf_error_num) as mtbf_error_num, -- 故障小车数
         SUM(t.mtbf_error_duration) as mtbf_error_duration, -- 故障时长
         SUM(t.theory_time) as theory_time -- 理论运行时长
  FROM ${tmp_dbname}.tmp_amr_mtbf_breakdown_add t
  WHERE t.d >= '${pre1_date}'
  GROUP BY t.cur_date,t.cur_hour,t.project_code
)t6
ON t1.cur_date = t6.cur_date AND t1.cur_hour = t6.cur_hour AND t1.project_code = t6.project_code
LEFT JOIN 
(
  SELECT TO_DATE(t.error_time) as cur_date,
         date_format(t.error_time,'yyyy-MM-dd HH:00:00') as cur_hour,
         t.project_code,
         concat_ws(',' , collect_list(t.breakdown_id)) as breakdown_id
  FROM breakdown t
  GROUP BY TO_DATE(t.error_time),date_format(t.error_time,'yyyy-MM-dd HH:00:00'),t.project_code
)t7
ON t1.cur_date = t7.cur_date AND t1.cur_hour = t7.cur_hour AND t1.project_code = t7.project_code

;
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql" && hive_concatenate ads ads_amr_breakdown ${pre1_date}