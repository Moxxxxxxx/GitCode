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
-------------------------------------------------------------------------------------------------------------00
-- 机器人类型指标统计 ads_single_project_agv_type_info 

/*
-- v4故障
with breakdown as
(
    SELECT TO_DATE(t.error_time) as cur_date,
           t.project_code,
           cast(coalesce(t.agv_type_code, -1) as string) as agv_type_code,
           cast(coalesce(t.agv_code, -1) as string) as agv_code,
           unix_timestamp(coalesce(t.sort_time[0], t.sort_time[1], t.sort_time[2])) - unix_timestamp(t.error_time) as breakdown_duration -- 故障时长
    FROM 
    (
      SELECT tt1.*,
             ROW_NUMBER() over (PARTITION by tt1.project_code,tt1.agv_code,tt1.breakdown_id order by tt2.status_change_time asc) as rk,
             sort_array(ARRAY(tt1.next_error_time, tt2.status_change_time,concat(date_add(to_date(tt1.error_time), 1), ' ', '00:00:00'))) as sort_time,
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
               b.error_level
        FROM ${dwd_dbname}.dwd_agv_breakdown_astringe_v4_di b
        WHERE b.d = '${pre1_date}' AND b.error_level >= '3' 
      )tt1
      LEFT JOIN 
      (
        SELECT w.project_code,
               w.agv_code,
               w.status_log_time as status_change_time,
               w.working_status,
               w.online_status
        FROM ${dwd_dbname}.dwd_agv_working_status_incre_dt w
        WHERE w.d = '${pre1_date}' AND w.online_status = 'REGISTERED' AND w.working_status = 'BUSY' 
      ) tt2 
      ON tt2.project_code = tt1.project_code AND tt2.agv_code = tt1.agv_code
      WHERE tt2.status_change_time > tt1.error_time
    ) t
    WHERE t.rk = 1
),
*/
-- v5故障
with breakdown as
(
  SELECT b.project_code,
         TO_DATE(b.breakdown_log_time) as cur_date,
         cast(coalesce(b.agv_code, -1) as string) as agv_code,
         cast(coalesce(b.agv_type_code, -1) as string) as agv_type_code,
         b.breakdown_id,
         b.breakdown_log_time as error_time,
         b.breakdown_end_time as end_time,
         unix_timestamp(b.breakdown_end_time) - unix_timestamp(b.breakdown_log_time) as breakdown_duration -- 故障时长
  FROM ${dwd_dbname}.dwd_agv_breakdown_astringe_v5_di b
  WHERE b.d = '${pre1_date}'
),
tmp as 
(
  SELECT t2.project_code,
         t2.cur_date,
         t2.agv_type_code,
         t2.agv_code,
         t2.breakdown_duration,
         t2.breakndown_num,
         t1.agv_actual_run_duration,
         t1.agv_actual_run_duration - IF(nvl(t2.breakdown_duration,0) >= t1.agv_actual_run_duration,t1.agv_actual_run_duration,nvl(t2.breakdown_duration,0)) as no_error_time,
         (t1.agv_actual_run_duration - IF(nvl(t2.breakdown_duration,0) >= t1.agv_actual_run_duration,t1.agv_actual_run_duration,nvl(t2.breakdown_duration,0))) / nvl(t2.breakndown_num,0) as mtbf,
         (t1.agv_actual_run_duration - IF(nvl(t2.breakdown_duration,0) >= t1.agv_actual_run_duration,t1.agv_actual_run_duration,nvl(t2.breakdown_duration,0))) / nvl(t1.agv_actual_run_duration,0) as oee,
         nvl(nvl(t2.breakdown_duration,0) / nvl(t2.breakndown_num,0),0) as mttr
  FROM 
  (
    SELECT t.cur_date,
           t.project_code,
           t.agv_type_code,
           t.agv_code,
           COUNT(*) as breakndown_num,
           sum(t.breakdown_duration) as breakdown_duration -- 故障时长
    FROM breakdown t
    GROUP BY t.cur_date,t.project_code,t.agv_type_code,t.agv_code
  )t2
  LEFT JOIN
  (
    SELECT h.project_code,
           h.d,
           unix_timestamp(MAX(DATE_FORMAT(h.job_finish_time,'yyyy-MM-dd HH:mm:ss'))) 
		   - 
		   unix_timestamp(case when h.d != TO_DATE(MIN(DATE_FORMAT(h.job_accept_time,'yyyy-MM-dd HH:mm:ss'))) AND h.d = TO_DATE(MAX(DATE_FORMAT(h.job_finish_time,'yyyy-MM-dd HH:mm:ss'))) then DATE_FORMAT(h.d,'yyyy-MM-dd HH:mm:ss') 
                               else MIN(DATE_FORMAT(h.job_accept_time,'yyyy-MM-dd HH:mm:ss')) end) AS agv_actual_run_duration
    FROM ${dwd_dbname}.dwd_rcs_agv_job_history_info_di h
    WHERE h.d = '${pre1_date}' AND TO_DATE(h.job_accept_time) = '${pre1_date}'
    GROUP BY h.project_code,h.d
  )t1
  ON t1.project_code = t2.project_code
),
t1 as
(
  SELECT  t1.days as cur_date, -- 统计日期
          t2.project_code, -- 项目编码
          nvl(t3.agv_type,t3.agv_type_code) as agv_type_code, -- 机器人类型
          t3.agv_type_name, -- 机器人类型名称
          nvl(t3.agv_num,0) as agv_num, -- 机器人数量
          nvl(t4.breakndown_num,0) as breakndown_num, -- 故障次数
          nvl(t4.breakndown_agv_num,0) as breakndown_agv_num, -- 故障小车数
          nvl(t5.order_num,0) as order_num, -- 订单量
          nvl(t6.move_job_num,0) as move_job_num, -- 搬运任务数
          nvl(cast(nvl(t4.breakndown_num,0) / IF(t5.order_num = 0,1,t5.order_num) as decimal(10,4)),0) as order_breakndown_actual_rate, -- 订单故障率实际比值
          nvl(cast(nvl(t4.breakndown_num,0) / nvl(t6.move_job_num,1) as decimal(10,4)),0) as move_job_breakndown_actual_rate, -- 搬运任务故障率实际比值
          CASE WHEN nvl(t4.breakndown_num,0) = 0 AND nvl(t5.order_num,0) = 0 THEN '0 / 0'
               WHEN nvl(t4.breakndown_num,0) != 0 AND nvl(t5.order_num,0) = 0 THEN CONCAT(nvl(t4.breakndown_num,0),' / ',0) 
               WHEN nvl(t4.breakndown_num,0) = 0 AND nvl(t5.order_num,0) != 0 THEN CONCAT(0,' / ', nvl(t5.order_num,0)) 
               WHEN nvl(t4.breakndown_num,0) != 0 AND nvl(t5.order_num,0) != 0 AND nvl(t4.breakndown_num,0) <= nvl(t5.order_num,0) THEN concat(nvl(CAST(round(nvl(t4.breakndown_num,0) / nvl(t4.breakndown_num,0)) as int),0),' / ',nvl(CAST(round(nvl(t5.order_num,0) / nvl(t4.breakndown_num,0)) as int),0))
               WHEN nvl(t4.breakndown_num,0) != 0 AND nvl(t5.order_num,0) != 0 AND nvl(t4.breakndown_num,0) > nvl(t5.order_num,0) THEN concat(nvl(CAST(round(nvl(t4.breakndown_num,0) / nvl(t5.order_num,0)) as int),0),' / ',nvl(CAST(round(nvl(t5.order_num,0) / nvl(t5.order_num,0)) as int),0))
          END as order_breakndown_rate, -- 订单故障率
          CASE WHEN nvl(t4.breakndown_num,0) = 0 AND nvl(t6.move_job_num,0) = 0 THEN '0 / 0'
               WHEN nvl(t4.breakndown_num,0) != 0 AND nvl(t6.move_job_num,0) = 0 THEN CONCAT(nvl(t4.breakndown_num,0),' / ',0) 
               WHEN nvl(t4.breakndown_num,0) = 0 AND nvl(t6.move_job_num,0) != 0 THEN CONCAT(0,' / ', nvl(t6.move_job_num,0)) 
               WHEN nvl(t4.breakndown_num,0) != 0 AND nvl(t6.move_job_num,0) != 0 AND nvl(t4.breakndown_num,0) <= nvl(t6.move_job_num,0) THEN concat(nvl(CAST(round(nvl(t4.breakndown_num,0) / nvl(t4.breakndown_num,0)) as int),0),' / ',nvl(CAST(round(nvl(t6.move_job_num,0) / nvl(t4.breakndown_num,0)) as int),0))
               WHEN nvl(t4.breakndown_num,0) != 0 AND nvl(t6.move_job_num,0) != 0 AND nvl(t4.breakndown_num,0) > nvl(t6.move_job_num,0) THEN concat(nvl(CAST(round(nvl(t4.breakndown_num,0) / nvl(t6.move_job_num,0)) as int),0),' / ',nvl(CAST(round(nvl(t6.move_job_num,0) / nvl(t6.move_job_num,0)) as int),0))
          END as move_job_breakndown_rate, -- 搬运任务故障率
          nvl(t7.offline_maintain_num,0) as offline_maintain_num, -- 下线维修数量
          IF(t9.agv_actual_run_duration is not null,((nvl(t3.agv_num,0) - nvl(t4.breakndown_agv_num,0)) + nvl(t8.oee,0)) / nvl(t3.agv_num,0),NULL) as OEE, -- （无故障小车OEE + 有故障小车OEE）/ 小车总数
          IF(t9.agv_actual_run_duration is not null,(((nvl(t3.agv_num,0) - nvl(t4.breakndown_agv_num,0)) * t9.agv_actual_run_duration) + nvl(t8.mtbf,0)) / nvl(t3.agv_num,0),NULL) as MTBF, -- （无故障小车MTBF + 有故障小车MTBF）/ 小车总数
          (0 + nvl(t8.mttr,0)) / nvl(t3.agv_num,0) as MTTR -- (无故障小车MTTR + 有故障小车MTTR) / 小车总数
  FROM 
  (
    SELECT *
    FROM ${dim_dbname}.dim_day_date
    WHERE days = '${pre1_date}'
  )t1
  LEFT JOIN 
  (
    SELECT *
    FROM ${dim_dbname}.dim_collection_project_record_ful
  )t2
  LEFT JOIN
  (
    SELECT b.project_code,
           a.agv_type,
           b.agv_type_code,
           nvl(a.agv_type_name,b.agv_type_name) as agv_type_name,
           IF(COUNT(DISTINCT a.agv_code) = 0,COUNT(DISTINCT b.agv_code),COUNT(DISTINCT a.agv_code)) as agv_num
    FROM ${dwd_dbname}.dwd_rcs_agv_base_info_df b
    LEFT JOIN ${tmp_dbname}.tmp_basic_agv_data_offline_info a
    ON b.project_code = a.project_code AND a.agv_code = b.agv_code
    WHERE b.d = '${pre1_date}' AND (a.project_code is null OR a.active_status = '运营中')
    GROUP BY b.project_code,a.agv_type,b.agv_type_code,nvl(a.agv_type_name,b.agv_type_name)
  )t3
  ON t2.project_code = t3.project_code
  -- 故障次数
  LEFT JOIN
  (
    SELECT m.cur_date,
           m.project_code,
           m.agv_type_code,
           SUM(m.breakndown_num) as breakndown_num,
           COUNT(DISTINCT m.agv_code) as breakndown_agv_num
    FROM tmp m
    GROUP BY m.cur_date,m.project_code,m.agv_type_code
  )t4
  ON t1.days = t4.cur_date AND t2.project_code = t4.project_code AND t3.agv_type_code = t4.agv_type_code
  -- 订单量
  LEFT JOIN
  (
    SELECT t1.days as cur_date, -- 统计日期
           t2.project_code, -- 项目编码
           t3.agv_type_code, -- 机器人类型
           nvl(c.cyclecount_num,0) as cyclecount_num, -- 盘点订单数量（单）
           nvl(r1.guided_putaway_num,0) as guided_putaway_num, -- 指导上架订单数量（单）
           0 as putaway_num, -- 直接上架订单数量（单）
           nvl(p.picking_num,0) as picking_num, -- 拣选订单数量（单）
           nvl(c.cyclecount_num,0) + nvl(r1.guided_putaway_num,0) + 0 + nvl(p.picking_num,0) as order_num
    FROM 
    (
      SELECT *
      FROM ${dim_dbname}.dim_day_date
      WHERE days = '${pre1_date}'
    )t1
    LEFT JOIN 
    (
      SELECT *
      FROM ${dim_dbname}.dim_collection_project_record_ful
      WHERE project_product_type_code IN (1,2) 
    )t2
    LEFT JOIN 
    ( 
      SELECT b.d as cur_date,
             b.project_code,
             b.agv_type_code
      FROM ${dwd_dbname}.dwd_rcs_agv_base_info_df b
      WHERE b.d = '${pre1_date}'
      GROUP BY b.d,b.project_code,b.agv_type_code
    )t3
    ON t1.days = t3.cur_date AND t2.project_code = t3.project_code
    -- 盘点订单
    LEFT JOIN
    (
      SELECT cc.d as cur_date,
             cc.project_code,
             bat.agv_type_code,
             COUNT(DISTINCT cc.id) as cyclecount_num
      FROM ${dwd_dbname}.dwd_cyclecount_cycle_count_info_di cc
      LEFT JOIN ${dwd_dbname}.dwd_cyclecount_cycle_count_work_info_di ck
      ON cc.id = ck.cycle_count_id AND cc.d = ck.d AND cc.pt = ck.pt
      LEFT JOIN ${dwd_dbname}.dwd_g2p_countcheck_job_info_di cj
      ON ck.id = cj.work_id AND cc.d = cj.d AND cc.pt = cj.pt
      LEFT JOIN ${dwd_dbname}.dwd_rcs_basic_agv_info_df ba
      ON cj.agv_code = ba.agv_code AND cc.d = ba.d AND cc.pt = ba.pt
      LEFT JOIN ${dwd_dbname}.dwd_rcs_basic_agv_type_info_df bat 
      ON ba.agv_type_id = bat.id AND cc.d = bat.d AND cc.pt = bat.pt
      WHERE cc.d = '${pre1_date}'
      GROUP BY cc.d,cc.project_code,bat.agv_type_code
    )c
    ON t1.days = c.cur_date AND t2.project_code = c.project_code AND t3.agv_type_code = c.agv_type_code 
    -- 指导上架订单
    LEFT JOIN
    (
      SELECT r.d as cur_date,
             r.project_code,
             bat.agv_type_code,
             COUNT(DISTINCT r.id) as guided_putaway_num
      FROM ${dwd_dbname}.dwd_replenish_order_info_di r
      JOIN ${dwd_dbname}.dwd_g2p_guided_putaway_job_info_di g
      ON r.id = g.order_id AND r.d = g.d AND r.pt = g.pt
      LEFT JOIN ${dwd_dbname}.dwd_rcs_basic_agv_info_df ba
      ON g.agv_code = ba.agv_code AND r.d = ba.d AND r.pt = ba.pt
      LEFT JOIN ${dwd_dbname}.dwd_rcs_basic_agv_type_info_df bat 
      ON ba.agv_type_id = bat.id AND r.d = bat.d AND r.pt = bat.pt
      WHERE r.d = '${pre1_date}'
      GROUP BY r.d,r.project_code,bat.agv_type_code
    )r1
    ON t1.days = r1.cur_date AND t2.project_code = r1.project_code AND t3.agv_type_code = r1.agv_type_code
    -- 直接上架订单
    -- 拣选订单
    LEFT JOIN
    (
      SELECT p.d as cur_date,
             p.project_code,
             bat.agv_type_code,
             COUNT(DISTINCT p.id) as picking_num
      FROM ${dwd_dbname}.dwd_picking_order_info_di p
      LEFT JOIN ${dwd_dbname}.dwd_g2p_picking_job_info_di pj
      ON p.id = pj.order_id AND p.d = pj.d AND p.pt = pj.pt
      LEFT JOIN ${dwd_dbname}.dwd_rcs_basic_agv_info_df ba
      ON pj.agv_code = ba.agv_code AND p.d = ba.d AND p.pt = ba.pt
      LEFT JOIN ${dwd_dbname}.dwd_rcs_basic_agv_type_info_df bat 
      ON ba.agv_type_id = bat.id AND p.d = bat.d AND p.pt = bat.pt
      WHERE p.d = '${pre1_date}'
      GROUP BY p.d,p.project_code,bat.agv_type_code
    )p
    ON t1.days = p.cur_date AND t2.project_code = p.project_code AND t3.agv_type_code = p.agv_type_code
    
    UNION ALL 
    
    SELECT t1.days as cur_date, -- 统计日期
           t2.project_code, -- 项目编码
           t4.agv_type_code, -- 机器人类型
           0 as cyclecount_num, -- 盘点订单数量（单）
           0 as guided_putaway_num, -- 指导上架订单数量（单）
           0 as putaway_num, -- 直接上架订单数量（单）
           0 as picking_num, -- 拣选订单数量（单）
           nvl(t4.send_workbin,0) as order_num
    FROM 
    (
      SELECT *
      FROM ${dim_dbname}.dim_day_date
      WHERE days = '${pre1_date}'
    )t1
    LEFT JOIN 
    (
      SELECT *
      FROM ${dim_dbname}.dim_collection_project_record_ful
      WHERE project_product_type_code = 4 OR project_product_type IN ('标准搬运','堆高车搬运') OR project_code = 'A51346'
    )t2
    LEFT JOIN 
    (
      SELECT r.d as cur_date, 
             r.project_code,
             ba.agv_type_code,
             COUNT(DISTINCT r.job_id) as send_workbin
      FROM ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di r
      LEFT JOIN ${dwd_dbname}.dwd_rcs_agv_base_info_df ba
      ON r.agv_code = ba.agv_code AND r.d = ba.d AND r.pt = ba.pt
      WHERE r.d = '${pre1_date}'
      GROUP BY r.d,r.project_code,ba.agv_type_code
    )t4
    ON t1.days = t4.cur_date AND t2.project_code = t4.project_code
    WHERE t4.agv_type_code is not null 
    
    UNION ALL 
    
    SELECT t1.days as cur_date, -- 统计日期
           t2.project_code, -- 项目编码
           t4.agv_type_code, -- 机器人类型
           0 as cyclecount_num, -- 盘点订单数量（单）
           0 as guided_putaway_num, -- 指导上架订单数量（单）
           0 as putaway_num, -- 直接上架订单数量（单）
           0 as picking_num, -- 拣选订单数量（单）
           nvl(t4.send_workbin,0) as order_num
    FROM 
    (
      SELECT *
      FROM ${dim_dbname}.dim_day_date
      WHERE days = '${pre1_date}'
    )t1
    LEFT JOIN 
    (
      SELECT *
      FROM ${dim_dbname}.dim_collection_project_record_ful
      WHERE project_product_type IN ('Quickpick','料箱搬运QP') OR project_code = 'A51274'
    )t2
    LEFT JOIN 
    (
      SELECT r.cur_date, 
             r.project_code,
             r.agv_type_code,
             COUNT(DISTINCT r.job_id) as send_workbin
      FROM 
      (
        SELECT r.d as cur_date, 
               r.project_code,
               r.job_id,
               m.agv_code,
               m.agv_type as agv_type_code
        FROM ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di r
        LEFT JOIN ${dwd_dbname}.dwd_g2p_si_qp_extend_info_df e 
        ON r.project_code = e.project_code AND r.robot_job_id = e.job_id AND e.d = '${pre1_date}'
        LEFT JOIN ${dwd_dbname}.dwd_g2p_si_qp_move_job_info_di m
        ON r.project_code = m.project_code AND e.move_job_id = m.id AND m.d = '${pre1_date}'
        WHERE r.d = '${pre1_date}'
        
        UNION ALL 
        
        SELECT r.d as cur_date, 
               r.project_code,
               r.job_id,
               t.agv_code,
               t.agv_type
        FROM ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di r
        LEFT JOIN ${dwd_dbname}.dwd_g2p_si_qp_extend_info_df e 
        ON r.project_code = e.project_code AND r.robot_job_id = e.job_id AND e.d = '${pre1_date}'
        LEFT JOIN ${dwd_dbname}.dwd_g2p_si_qp_transfer_job_info_di t
        ON r.project_code = t.project_code AND e.transfer_job_id = t.id AND t.d = '${pre1_date}'
        WHERE r.d = '${pre1_date}'
      )r
      GROUP BY r.cur_date,r.project_code,r.agv_type_code
    )t4
    ON t1.days = t4.cur_date AND t2.project_code = t4.project_code
    WHERE t4.agv_type_code is not null 
  )t5
  ON t1.days = t5.cur_date AND t2.project_code = t5.project_code AND t3.agv_type_code = t5.agv_type_code
  -- 搬运任务数
  LEFT JOIN 
  (
    SELECT aj.d as cur_date,
           aj.project_code,
           ba.agv_type_code,
           COUNT(DISTINCT aj.id) as move_job_num
    FROM ${dwd_dbname}.dwd_rcs_agv_job_history_info_di aj
    LEFT JOIN ${dwd_dbname}.dwd_rcs_agv_base_info_df ba
    ON aj.agv_code = ba.agv_code AND aj.d = ba.d AND aj.pt = ba.pt
    WHERE aj.d = '${pre1_date}'
    GROUP BY aj.d,aj.project_code,ba.agv_type_code
  )t6
  ON t1.days = t6.cur_date AND t2.project_code = t6.project_code AND t3.agv_type_code = t6.agv_type_code
  -- 下线维修数量
  LEFT JOIN 
  (
    SELECT i.project_code,
           '${pre1_date}' as cur_date,
           d.agv_type,
           COUNT(DISTINCT i.agv_uuid) as offline_maintain_num
    FROM ${tmp_dbname}.tmp_basic_agv_inspection_data_offline_info i
    LEFT JOIN ${tmp_dbname}.tmp_basic_agv_data_offline_info d
    ON i.agv_uuid = d.agv_uuid AND i.project_code = d.project_code
    WHERE TO_DATE(i.inspection_start_time) = '${pre1_date}'
    GROUP BY i.project_code,d.agv_type
  )t7
  ON t1.days = t7.cur_date AND t2.project_code = t7.project_code AND t3.agv_type = t7.agv_type
  -- OEE\MTBF\MTTR
  LEFT JOIN 
  (
    SELECT a.project_code,
           a.cur_date,
           a.agv_type_code,
           SUM(nvl(a.mtbf,0)) as mtbf,
           SUM(nvl(a.oee,0)) as oee,
           SUM(nvl(a.mttr,0)) as mttr
    FROM tmp a
    GROUP BY a.project_code,a.cur_date,a.agv_type_code
  )t8
  ON t1.days = t8.cur_date AND t2.project_code = t8.project_code AND t3.agv_type_code = t8.agv_type_code
  -- 理论运行时长
  LEFT JOIN
  (
    SELECT h.project_code,
           h.d,
           unix_timestamp(MAX(DATE_FORMAT(h.job_finish_time,'yyyy-MM-dd HH:mm:ss'))) 
		   - 
		   unix_timestamp(case when h.d != TO_DATE(MIN(DATE_FORMAT(h.job_accept_time,'yyyy-MM-dd HH:mm:ss'))) AND h.d = TO_DATE(MAX(DATE_FORMAT(h.job_finish_time,'yyyy-MM-dd HH:mm:ss'))) then DATE_FORMAT(h.d,'yyyy-MM-dd HH:mm:ss') 
                          else MIN(DATE_FORMAT(h.job_accept_time,'yyyy-MM-dd HH:mm:ss')) end) AS agv_actual_run_duration
    FROM ${dwd_dbname}.dwd_rcs_agv_job_history_info_di h
    WHERE h.d = '${pre1_date}' AND TO_DATE(h.job_accept_time) = '${pre1_date}'
    GROUP BY h.project_code,h.d
  )t9
  ON t2.project_code = t9.project_code
)

INSERT overwrite table ${ads_dbname}.ads_single_project_agv_type_info partition(d='${pre1_date}')
SELECT '' as id, -- 主键
       cur_date, -- 统计日期
       project_code, -- 项目编码
       agv_type_code, -- 机器人类型
       agv_type_name, -- 机器人类型名称
       agv_num, -- 机器人数量
       breakndown_num, -- 故障次数
       order_breakndown_rate, -- 订单故障率
       order_num, -- 订单量
       move_job_breakndown_rate, -- 搬运任务故障率
       move_job_num, -- 搬运任务数
       OEE, -- （小车实际运行时长-故障时长）/小车实际运行时长
       MTBF, -- （小车实际运行时长-故障时长）/故障次数
       MTTR, -- 故障时长/故障次数
       offline_maintain_num, -- 下线维修数量
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM t1

UNION ALL

SELECT '' as id, -- 主键
       t1.cur_date, -- 统计日期
       t1.project_code, -- 项目编码
       'all' as agv_type_code, -- 机器人类型
       null as agv_type_name, -- 机器人类型名称
       t1.agv_num, -- 机器人数量
       t1.breakndown_num, -- 故障次数
       t1.order_breakndown_rate, -- 订单故障率
       t1.order_num, -- 订单量
       t1.move_job_breakndown_rate, -- 搬运任务故障率
       t1.move_job_num, -- 搬运任务数
       IF(t3.agv_actual_run_duration is not null,((nvl(t1.agv_num,0) - nvl(t1.breakndown_agv_num,0)) + nvl(t2.oee,0)) / nvl(t1.agv_num,0),NULL) as OEE, -- （无故障小车OEE + 有故障小车OEE）/ 小车总数
       IF(t3.agv_actual_run_duration is not null,(((nvl(t1.agv_num,0) - nvl(t1.breakndown_agv_num,0)) * t3.agv_actual_run_duration) + nvl(t2.mtbf,0)) / nvl(t1.agv_num,0),NULL) as MTBF, -- （无故障小车MTBF + 有故障小车MTBF）/ 小车总数
       (0 + nvl(t2.mttr,0)) / nvl(t1.agv_num,0) as MTTR, -- (无故障小车MTTR + 有故障小车MTTR) / 小车总数
       t1.offline_maintain_num, -- 下线维修数量
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM
(
  SELECT cur_date, -- 统计日期
         project_code, -- 项目编码
         SUM(nvl(agv_num,0)) as agv_num, -- 机器人数量
         SUM(nvl(breakndown_num,0)) as breakndown_num, -- 故障次数
         SUM(nvl(breakndown_agv_num,0)) as breakndown_agv_num, -- 故障小车数
         CASE WHEN SUM(nvl(breakndown_num,0)) = 0 AND SUM(nvl(order_num,0)) = 0 THEN '0 / 0'
              WHEN SUM(nvl(breakndown_num,0)) != 0 AND SUM(nvl(order_num,0)) = 0 THEN CONCAT(SUM(nvl(breakndown_num,0)),' / ',0) 
              WHEN SUM(nvl(breakndown_num,0)) = 0 AND SUM(nvl(order_num,0)) != 0 THEN CONCAT(0,' / ', SUM(nvl(order_num,0))) 
              WHEN SUM(nvl(breakndown_num,0)) != 0 AND SUM(nvl(order_num,0)) != 0 AND SUM(nvl(breakndown_num,0)) <= SUM(nvl(order_num,0)) THEN concat(nvl(CAST(round(SUM(nvl(breakndown_num,0)) / SUM(nvl(breakndown_num,0))) as int),0),' / ',nvl(CAST(round(SUM(nvl(order_num,0)) / SUM(nvl(breakndown_num,0))) as int),0))
              WHEN SUM(nvl(breakndown_num,0)) != 0 AND SUM(nvl(order_num,0)) != 0 AND SUM(nvl(breakndown_num,0)) > SUM(nvl(order_num,0)) THEN concat(nvl(CAST(round(SUM(nvl(breakndown_num,0)) / SUM(nvl(order_num,0))) as int),0),' / ',nvl(CAST(round(SUM(nvl(order_num,0)) / SUM(nvl(order_num,0))) as int),0))
         END as order_breakndown_rate, -- 订单故障率
         SUM(nvl(order_num,0)) as order_num, -- 订单量
         CASE WHEN SUM(nvl(breakndown_num,0)) = 0 AND SUM(nvl(move_job_num,0)) = 0 THEN '0 / 0'
              WHEN SUM(nvl(breakndown_num,0)) != 0 AND SUM(nvl(move_job_num,0)) = 0 THEN CONCAT(SUM(nvl(breakndown_num,0)),' / ',0) 
              WHEN SUM(nvl(breakndown_num,0)) = 0 AND SUM(nvl(move_job_num,0)) != 0 THEN CONCAT(0,' / ', SUM(nvl(move_job_num,0))) 
              WHEN SUM(nvl(breakndown_num,0)) != 0 AND SUM(nvl(move_job_num,0)) != 0 AND SUM(nvl(breakndown_num,0)) <= SUM(nvl(move_job_num,0)) THEN concat(nvl(CAST(round(SUM(nvl(breakndown_num,0)) / SUM(nvl(breakndown_num,0))) as int),0),' / ',nvl(CAST(round(SUM(nvl(move_job_num,0)) / SUM(nvl(breakndown_num,0))) as int),0))
              WHEN SUM(nvl(breakndown_num,0)) != 0 AND SUM(nvl(move_job_num,0)) != 0 AND SUM(nvl(breakndown_num,0)) > SUM(nvl(move_job_num,0)) THEN concat(nvl(CAST(round(SUM(nvl(breakndown_num,0)) / SUM(nvl(move_job_num,0))) as int),0),' / ',nvl(CAST(round(SUM(nvl(move_job_num,0)) / SUM(nvl(move_job_num,0))) as int),0))
         END as move_job_breakndown_rate, -- 搬运任务故障率
         SUM(nvl(move_job_num,0)) as move_job_num, -- 搬运任务数
         SUM(nvl(offline_maintain_num,0)) as offline_maintain_num -- 下线维修数量
  FROM t1
  GROUP BY cur_date,project_code
)t1
LEFT JOIN 
(
  SELECT a.project_code,
         a.cur_date,
         SUM(nvl(a.mtbf,0)) as mtbf,
         SUM(nvl(a.oee,0)) as oee,
         SUM(nvl(a.mttr,0)) as mttr
  FROM tmp a
  GROUP BY a.project_code,a.cur_date
)t2
ON t1.cur_date = t2.cur_date AND t1.project_code = t2.project_code
-- 理论运行时长
LEFT JOIN
(
    SELECT h.project_code,
           h.d,
           unix_timestamp(MAX(DATE_FORMAT(h.job_finish_time,'yyyy-MM-dd HH:mm:ss'))) 
		   - 
		   unix_timestamp(case when h.d != TO_DATE(MIN(DATE_FORMAT(h.job_accept_time,'yyyy-MM-dd HH:mm:ss'))) AND h.d = TO_DATE(MAX(DATE_FORMAT(h.job_finish_time,'yyyy-MM-dd HH:mm:ss'))) then DATE_FORMAT(h.d,'yyyy-MM-dd HH:mm:ss') 
                               else MIN(DATE_FORMAT(h.job_accept_time,'yyyy-MM-dd HH:mm:ss')) end) AS agv_actual_run_duration
    FROM ${dwd_dbname}.dwd_rcs_agv_job_history_info_di h
    WHERE h.d = '${pre1_date}' AND TO_DATE(h.job_accept_time) = '${pre1_date}'
    GROUP BY h.project_code,h.d
)t3
ON t1.project_code = t3.project_code;
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql" && hive_concatenate ads ads_single_project_agv_type_info ${pre1_date}