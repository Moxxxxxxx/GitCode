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
-- 搬运作业单分析统计 ads_carry_work_analyse_count 

with bucket_move_job as 
(
  SELECT mj.id,
         mj.project_code,
         IF(mj.source_waypoint_code is null or LENGTH(mj.source_waypoint_code) = 0,'NULL',mj.source_waypoint_code) as start_point,
         IF(mj.target_waypoint_code is null or LENGTH(mj.target_waypoint_code) = 0,'NULL',mj.target_waypoint_code) as target_point,
         CONCAT(IF(mj.source_waypoint_code is null or LENGTH(mj.source_waypoint_code) = 0,'NULL',mj.source_waypoint_code),' - ', IF(mj.target_waypoint_code is null or LENGTH(mj.target_waypoint_code) = 0,'NULL',mj.target_waypoint_code)) as work_path
  FROM ${dwd_dbname}.dwd_g2p_bucket_move_job_info_di mj 
  WHERE mj.d >= '${pre1_date}'
),
robot_num as 
(
  SELECT jsc.project_code,
         jsc.job_id,
         bat.first_classification,
         case when bat.first_classification = 'WORKBIN' then '料箱车'
              when bat.first_classification = 'STOREFORKBIN' then '存储一体式'
              when bat.first_classification = 'CARRIER' then '潜伏式机器人'
              when bat.first_classification = 'ROLLER' then '辊筒机器人'
              when bat.first_classification = 'FORKLIFT' then '堆高全向车'
              when bat.first_classification = 'DELIVER' then '投递车'
              when bat.first_classification = 'SC'then '四向穿梭车' 
         end as first_classification_desc,
         bat.agv_type_code,
         concat_ws(',',collect_set(cast(jsc.agv_code as string))) as agv_code,
         COUNT(DISTINCT jsc.agv_code) as robot_num
  FROM 
  (
    SELECT *
    FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da jsc
    WHERE jsc.d >= '${pre1_date}' AND LENGTH(jsc.agv_code) != 0
  )jsc
  LEFT JOIN 
  (
    SELECT *
    FROM ${dwd_dbname}.dwd_rcs_basic_agv_info_df ba
    WHERE ba.d >= '${pre1_date}'
  )ba
  ON nvl(jsc.project_code,'unknown1') = nvl(ba.project_code,'unknown2') AND nvl(jsc.agv_code,'unknown1') = nvl(ba.agv_code,'unknown2')  AND nvl(ba.d,'unknown1') = nvl(jsc.d,'unknown2')
  LEFT JOIN 
  (
    SELECT *
    FROM ${dwd_dbname}.dwd_rcs_basic_agv_type_info_df bat 
    WHERE bat.d >= '${pre1_date}'
  )bat
  ON nvl(ba.project_code,'unknown1') = nvl(bat.project_code,'unknown2') AND nvl(ba.agv_type_id,'unknown1') = nvl(bat.id,'unknown2') AND nvl(bat.d,'unknown1') = nvl(ba.d,'unknown2')
  GROUP BY jsc.project_code,jsc.job_id,bat.agv_type_code,bat.first_classification
),
bucket_robot_job as 
(
  SELECT brj.project_code,
         brj.d,
         brj.robot_job_id as upper_work_id,
         brj.job_id as work_id,
         IF(brj.start_point is null or LENGTH(brj.start_point) = 0,'NULL',brj.start_point) as start_point,
         IF(brj.target_point is null or LENGTH(brj.target_point) = 0,'NULL',brj.target_point) as target_point,
         CONCAT(IF(brj.start_point is null or LENGTH(brj.start_point) = 0,'NULL',brj.start_point),' - ', IF(brj.target_point is null or LENGTH(brj.target_point) = 0,'NULL',brj.target_point)) as work_path,
         CASE when brj.job_state = 'DONE' THEN '任务完成'
              when brj.job_state = 'CANCEL' THEN '取消'
              when brj.job_state = 'ABNORMAL_COMPLETED' THEN '异常完成'
              when brj.job_state = 'ABNORMAL_CANCEL' THEN '异常取消'
         end as work_state,
         brj.job_created_time as work_create_time,
         brj.job_updated_time as work_complete_time
  FROM ${dwd_dbname}. dwd_g2p_bucket_robot_job_info_di brj
  WHERE brj.d >= '${pre1_date}' AND brj.job_state IN ('DONE','CANCEL','ABNORMAL_COMPLETED','ABNORMAL_CANCEL')
)

INSERT overwrite table ${ads_dbname}.ads_carry_work_analyse_count partition(d,pt)

-- 到人
SELECT '' as id, -- 主键
       t3.upper_work_id, -- 上游作业单ID
       t3.work_id , -- 搬运作业单
       t3.work_path, -- 路径
       t3.start_point, -- 起始点
       t3.target_point, -- 目标点
       t3.work_state, -- 作业单状态
       t3.first_classification, -- 机器人类型
       t3.first_classification_desc, -- 机器人类型中文描述
       t3.agv_type_code, -- 机器人类型编码
       t3.agv_code, -- 机器人编码
       nvl(t3.robot_num,0) as robot_num, -- 分配机器人数量
       nvl(unix_timestamp(t3.work_complete_time) - unix_timestamp(t3.work_create_time),0) as wotk_duration_total, -- 总耗时
       t4.waiting_agv_duration as robot_assign_duration, -- 分车耗时
       t5.move_duration as robot_move_duration, -- 进站前搬运耗时
       t6.executor_duration as station_executor_duration, -- 站内实操耗时
       t3.work_create_time, -- 作业单创建时间
       t3.work_complete_time, -- 作业单完成时间
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time,
       t2.project_code,
       substr(t3.work_create_time,1,10) as d,
       t2.project_code as pt
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful
  WHERE project_product_type_code IN (1) and project_version like '2.%'
)t2
JOIN 
(
  SELECT po.project_code,
         po.picking_order_number as upper_work_id,
         pj.work_id,
         mj.start_point,
         mj.target_point,
         mj.work_path,
         pj.work_state,
         rn.first_classification,
         rn.first_classification_desc,
         rn.agv_type_code,
         rn.agv_code,
         rn.robot_num,
         pj.work_create_time,
         pj.work_complete_time
  FROM ${dwd_dbname}.dwd_picking_order_info_di po
  JOIN 
  (
    SELECT pj.order_id,
           pj.job_id as work_id,
           pj.project_code,
           pj.bucket_move_job_id,
           CASE when pj.job_state = 'DONE' THEN '任务完成'
                when pj.job_state = 'CANCEL' THEN '取消'
                when pj.job_state = 'ABNORMAL_COMPLETED' THEN '异常完成'
                when pj.job_state = 'ABNORMAL_CANCEL' THEN '异常取消'
           end as work_state,
           pj.agv_code,
           pj.job_created_time as work_create_time,
           pj.job_updated_time as work_complete_time
    FROM ${dwd_dbname}.dwd_g2p_picking_job_info_di pj 
    WHERE pj.d >= '${pre1_date}' AND pj.job_state IN ('DONE','CANCEL','ABNORMAL_COMPLETED','ABNORMAL_CANCEL')
  )pj
  ON nvl(pj.order_id,'unknown1') = nvl(po.id,'unknown2') AND nvl(pj.project_code,'unknown1') = nvl(po.project_code,'unknown2')
  JOIN bucket_move_job mj 
  ON nvl(pj.bucket_move_job_id,'unknown1') = nvl(mj.id,'unknown2') AND nvl(mj.project_code,'unknown1') = nvl(po.project_code,'unknown2')
  JOIN robot_num rn
  ON nvl(pj.work_id,'unknown1') = nvl(rn.job_id,'unknown2') AND nvl(pj.project_code,'unknown1') = nvl(rn.project_code,'unknown2')
  WHERE po.d >= '${pre1_date}'

  UNION ALL 

  SELECT co.project_code,
         co.cycle_count_number as upper_work_id,
         cj.job_id as work_id,
         mj.start_point,
         mj.target_point,
         mj.work_path,
         cj.work_state,
         rn.first_classification,
         rn.first_classification_desc,
         rn.agv_type_code,
         rn.agv_code,
         rn.robot_num,
         cj.work_create_time,
         cj.work_complete_time
  FROM ${dwd_dbname}.dwd_cyclecount_cycle_count_info_di co
  JOIN 
  (
    SELECT ck.id,
           ck.cycle_count_id,
           ck.project_code
    FROM ${dwd_dbname}.dwd_cyclecount_cycle_count_work_info_di ck 
    WHERE ck.d >= '${pre1_date}'
  )ck
  ON nvl(co.id,'unknown1') = nvl(ck.cycle_count_id,'unknown2') AND nvl(ck.project_code,'unknown1') = nvl(co.project_code,'unknown2')
  JOIN 
  (
    SELECT cj.work_id,
           cj.job_id,
           cj.project_code,
           cj.bucket_move_job_id,
           CASE when cj.job_state = 'DONE' THEN '任务完成'
                when cj.job_state = 'CANCEL' THEN '取消'
                when cj.job_state = 'ABNORMAL_COMPLETED' THEN '异常完成'
                when cj.job_state = 'ABNORMAL_CANCEL' THEN '异常取消'
           end as work_state,
           cj.agv_code,
           cj.job_created_time as work_create_time,
           cj.job_updated_time as work_complete_time
    FROM ${dwd_dbname}.dwd_g2p_countcheck_job_info_di cj
    WHERE cj.d >= '${pre1_date}' AND cj.job_state IN ('DONE','CANCEL','ABNORMAL_COMPLETED','ABNORMAL_CANCEL')
  )cj
  ON nvl(cj.work_id,'unknown1') = nvl(ck.id,'unknown2') AND nvl(cj.project_code,'unknown1') = nvl(ck.project_code,'unknown2')
  JOIN bucket_move_job mj 
  ON nvl(cj.bucket_move_job_id,'unknown1') = nvl(mj.id,'unknown2') AND nvl(mj.project_code,'unknown1') = nvl(cj.project_code,'unknown2')
  JOIN robot_num rn
  ON nvl(cj.job_id,'unknown1') = nvl(rn.job_id,'unknown2') AND nvl(cj.project_code,'unknown1') = nvl(rn.project_code,'unknown2')
  WHERE co.d >= '${pre1_date}'

  UNION ALL 

  SELECT ro.project_code,
         ro.replenish_order_number as upper_work_id,
         pj.work_id,
         mj.start_point,
         mj.target_point,
         mj.work_path,
         pj.work_state,
         rn.first_classification,
         rn.first_classification_desc,
         rn.agv_type_code,
         rn.agv_code,
         rn.robot_num,
         pj.work_create_time,
         pj.work_complete_time
  FROM ${dwd_dbname}.dwd_replenish_order_info_di ro
  JOIN 
  (
    SELECT pj.order_id,
           pj.job_id as work_id,
           pj.project_code,
           pj.bucket_move_job_id,
           CASE when pj.job_state = 'DONE' THEN '任务完成'
                when pj.job_state = 'CANCEL' THEN '取消'
                when pj.job_state = 'ABNORMAL_COMPLETED' THEN '异常完成'
                when pj.job_state = 'ABNORMAL_CANCEL' THEN '异常取消'
           end as work_state,
           pj.agv_code,
           pj.job_created_time as work_create_time,
           pj.job_updated_time as work_complete_time
    FROM ${dwd_dbname}.dwd_g2p_guided_putaway_job_info_di pj
    WHERE pj.d >= '${pre1_date}' AND pj.job_state IN ('DONE','CANCEL','ABNORMAL_COMPLETED','ABNORMAL_CANCEL')
  )pj
  ON nvl(ro.id,'unknown1') = nvl(pj.order_id,'unknown2') AND nvl(pj.project_code,'unknown1') = nvl(ro.project_code,'unknown2')
  JOIN bucket_move_job mj 
  ON nvl(pj.bucket_move_job_id,'unknown1') = nvl(mj.id,'unknown2') AND nvl(mj.project_code,'unknown1') = nvl(ro.project_code,'unknown2')
  JOIN robot_num rn
  ON nvl(pj.work_id,'unknown1') = nvl(rn.job_id,'unknown2') AND nvl(pj.project_code,'unknown1') = nvl(rn.project_code,'unknown2')
  WHERE ro.d >= '${pre1_date}'

  UNION ALL 
 
  SELECT pj.project_code,
         NULL as upper_work_id,
         pj.work_id,
         mj.start_point,
         mj.target_point,
         mj.work_path,
         pj.work_state,
         rn.first_classification,
         rn.first_classification_desc,
         rn.agv_type_code,
         rn.agv_code,
         rn.robot_num,
         pj.work_create_time,
         pj.work_complete_time
  FROM 
  (
    SELECT pj.job_id as work_id,
           pj.project_code,
           pj.bucket_move_job_id,
           CASE when pj.job_state = 'DONE' THEN '任务完成'
                when pj.job_state = 'CANCEL' THEN '取消'
                when pj.job_state = 'ABNORMAL_COMPLETED' THEN '异常完成'
                when pj.job_state = 'ABNORMAL_CANCEL' THEN '异常取消'
           end as work_state,
           pj.agv_code,
           pj.job_created_time as work_create_time,
           pj.job_updated_time as work_complete_time
    FROM ${dwd_dbname}. dwd_g2p_putaway_job_info_di pj
    WHERE pj.d >= '${pre1_date}' AND pj.job_state IN ('DONE','CANCEL','ABNORMAL_COMPLETED','ABNORMAL_CANCEL')
  )pj
  JOIN bucket_move_job mj 
  ON nvl(pj.bucket_move_job_id,'unknown1') = nvl(mj.id,'unknown2') AND nvl(mj.project_code,'unknown1') = nvl(pj.project_code,'unknown2')
  JOIN robot_num rn
  ON nvl(pj.work_id,'unknown1') = nvl(rn.job_id,'unknown2') AND nvl(pj.project_code,'unknown1') = nvl(rn.project_code,'unknown2')
)t3
ON nvl(t2.project_code,'unknown1') = nvl(t3.project_code,'unknown2')
-- 分车耗时
LEFT JOIN 
(
  SELECT c1.project_code,
         c1.job_id as work_id,
         unix_timestamp(c2.job_updated_time) - unix_timestamp(c1.job_updated_time) as waiting_agv_duration
  FROM 
  (
    SELECT *,
           row_number()over(PARTITION by jsc1.project_code,jsc1.job_id order by jsc1.job_created_time asc,jsc1.job_updated_time asc)rn
    FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da jsc1 
    WHERE jsc1.d >= '${pre1_date}' AND LENGTH(jsc1.agv_code) = 0
  )c1
  LEFT JOIN 
  (
    SELECT jsc1.project_code,
           jsc1.job_id,
           MAX(jsc1.job_updated_time) as job_updated_time
    FROM 
    (
      SELECT *,
             row_number()over(PARTITION by jsc1.project_code,jsc1.job_id,jsc1.agv_code order by jsc1.job_created_time,jsc1.job_updated_time desc)rn
      FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da jsc1
      WHERE jsc1.d >= '${pre1_date}' AND LENGTH(jsc1.agv_code) != 0
    )jsc1
    WHERE jsc1.rn = 1
    GROUP BY jsc1.project_code,jsc1.job_id
  )c2
  ON nvl(c1.project_code,'unknown1') = nvl(c2.project_code,'unknown2') AND nvl(c1.job_id,'unknown1') = nvl(c2.job_id,'unknown2')
  WHERE c1.rn = 1
)t4
ON nvl(t3.project_code,'unknown1') = nvl(t4.project_code,'unknown2') AND nvl(t3.work_id,'unknown1') = nvl(t4.work_id,'unknown2')
-- 进站前搬运耗时
LEFT JOIN 
(
  SELECT c1.project_code,
         c1.job_id as work_id,
         unix_timestamp(c2.job_updated_time) - unix_timestamp(c1.job_updated_time) as move_duration
  FROM 
  (
    SELECT jsc1.project_code,
           jsc1.job_id,
           MAX(jsc1.job_updated_time) as job_updated_time
    FROM 
    (
      SELECT *,
             row_number()over(PARTITION by jsc1.project_code,jsc1.job_id,jsc1.agv_code order by jsc1.job_created_time,jsc1.job_updated_time desc)rn
      FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da jsc1
      WHERE jsc1.d >= '${pre1_date}' AND LENGTH(jsc1.agv_code) != 0
    )jsc1
    WHERE jsc1.rn = 1
    GROUP BY jsc1.project_code,jsc1.job_id
  )c1
  LEFT JOIN 
  (
    SELECT *,
           row_number()over(PARTITION by jsc1.project_code,jsc1.job_id order by jsc1.job_created_time desc)rn
    FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da jsc1
    WHERE jsc1.d >= '${pre1_date}' AND jsc1.job_state = 'START_EXECUTOR'
  )c2
  ON nvl(c1.project_code,'unknown1') = nvl(c2.project_code,'unknown2') AND nvl(c1.job_id,'unknown1') = nvl(c2.job_id,'unknown2') AND c2.rn = 1
)t5
ON nvl(t3.project_code,'unknown1') = nvl(t5.project_code,'unknown2') AND nvl(t3.work_id,'unknown1') = nvl(t5.work_id,'unknown2')
-- 站内实操耗时
LEFT JOIN 
(
  SELECT c1.project_code,
         c1.job_id as work_id,
         unix_timestamp(c2.job_updated_time) - unix_timestamp(c1.job_updated_time) as executor_duration
  FROM 
  (
    SELECT *,
           row_number()over(PARTITION by jsc1.project_code,jsc1.job_id order by jsc1.job_created_time desc)rn
    FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da jsc1
    WHERE jsc1.d >= '${pre1_date}' AND jsc1.job_state = 'START_EXECUTOR'
  )c1
  LEFT JOIN 
  (
    SELECT *,
           row_number()over(PARTITION by jsc1.project_code,jsc1.job_id order by jsc1.job_created_time desc)rn
    FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da jsc1
    WHERE jsc1.d >= '${pre1_date}' AND jsc1.job_state = 'DONE'
  )c2
  ON nvl(c1.project_code,'unknown1') = nvl(c2.project_code,'unknown2') AND nvl(c1.job_id,'unknown1') = nvl(c2.job_id,'unknown2') AND c1.rn = c2.rn
  WHERE c1.rn = 1
)t6
ON nvl(t3.project_code,'unknown1') = nvl(t6.project_code,'unknown2') AND nvl(t3.work_id,'unknown1') = nvl(t6.work_id,'unknown2')

UNION ALL 

-- 标准搬运
SELECT '' as id, -- 主键
       t3.upper_work_id, -- 上游作业单ID
       t3.work_id , -- 搬运作业单
       t3.work_path, -- 路径
       t3.start_point, -- 起始点
       t3.target_point, -- 目标点
       t3.work_state, -- 作业单状态
       t3.first_classification, -- 机器人类型
       t3.first_classification_desc, -- 机器人类型中文描述	
       t3.agv_type_code, -- 机器人类型编码
       t3.agv_code, -- 机器人编码
       nvl(t3.robot_num,0) as robot_num, -- 分配机器人数量
       nvl(unix_timestamp(t3.work_complete_time) - unix_timestamp(t3.work_create_time),0) as wotk_duration_total, -- 总耗时
       t4.waiting_agv_duration as robot_assign_duration, -- 分车耗时
       t5.move_duration as robot_move_duration, -- 搬运耗时
       NULL as station_executor_duration, -- 进站实操耗时
       t3.work_create_time, -- 作业单创建时间
       t3.work_complete_time, -- 作业单完成时间
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time,
       t2.project_code,
       substr(t3.work_create_time,1,10) as d,
       t2.project_code as pt
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful
  WHERE (project_product_type_code = 4 OR project_product_type IN ('标准搬运') OR project_code = 'A51346') and project_version like '2.%'
)t2
JOIN 
(
  SELECT brj.*,
         rn.first_classification,
         rn.first_classification_desc,
         rn.agv_type_code,
         rn.agv_code,
         rn.robot_num
  FROM bucket_robot_job brj
  JOIN robot_num rn
  ON nvl(brj.work_id,'unknown1') = nvl(rn.job_id,'unknown2') AND nvl(brj.project_code,'unknown1') = nvl(rn.project_code,'unknown2')
)t3
ON nvl(t2.project_code,'unknown1') = nvl(t3.project_code,'unknown2')
-- 分车耗时
LEFT JOIN 
(
  SELECT c1.project_code,
         c1.job_id as work_id,
         unix_timestamp(c2.job_updated_time) - unix_timestamp(c1.job_updated_time) as waiting_agv_duration
  FROM 
  (
    SELECT *,
           row_number()over(PARTITION by jsc1.project_code,jsc1.job_id order by jsc1.job_created_time asc,jsc1.job_updated_time asc)rn
    FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da jsc1 
    WHERE jsc1.d >= '${pre1_date}' AND LENGTH(jsc1.agv_code) = 0
  )c1
  LEFT JOIN 
  (
    SELECT jsc1.project_code,
           jsc1.job_id,
           MAX(jsc1.job_updated_time) as job_updated_time
    FROM 
    (
      SELECT *,
             row_number()over(PARTITION by jsc1.project_code,jsc1.job_id,jsc1.agv_code order by jsc1.job_created_time,jsc1.job_updated_time desc)rn
      FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da jsc1
      WHERE jsc1.d >= '${pre1_date}' AND LENGTH(jsc1.agv_code) != 0
    )jsc1
    WHERE jsc1.rn = 1
    GROUP BY jsc1.project_code,jsc1.job_id
  )c2
  ON nvl(c1.project_code,'unknown1') = nvl(c2.project_code,'unknown2') AND nvl(c1.job_id,'unknown1') = nvl(c2.job_id,'unknown2')
  WHERE c1.rn = 1
)t4
ON nvl(t3.project_code,'unknown1') = nvl(t4.project_code,'unknown2') AND nvl(t3.work_id,'unknown1') = nvl(t4.work_id,'unknown2')
-- 搬运耗时
LEFT JOIN 
(
  SELECT c1.project_code,
         c1.job_id as work_id,
         unix_timestamp(c2.job_updated_time) - unix_timestamp(c1.job_updated_time) as move_duration
  FROM 
  (
    SELECT jsc1.project_code,
           jsc1.job_id,
           MAX(jsc1.job_updated_time) as job_updated_time
    FROM 
    (
      SELECT *,
             row_number()over(PARTITION by jsc1.project_code,jsc1.job_id,jsc1.agv_code order by jsc1.job_created_time asc,jsc1.job_updated_time asc)rn
      FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da jsc1
      WHERE jsc1.d >= '${pre1_date}' AND LENGTH(jsc1.agv_code) != 0
    )jsc1
    WHERE jsc1.rn = 1
    GROUP BY jsc1.project_code,jsc1.job_id
  )c1
  LEFT JOIN 
  (
    SELECT *,
           row_number()over(PARTITION by jsc1.project_code,jsc1.job_id order by jsc1.job_created_time desc)rn
    FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da jsc1
    WHERE jsc1.d >= '${pre1_date}' AND jsc1.job_state IN ('DONE')
  )c2
  ON nvl(c1.project_code,'unknown1') = nvl(c2.project_code,'unknown2') AND nvl(c1.job_id,'unknown1') = nvl(c2.job_id,'unknown2')
)t5
ON nvl(t3.project_code,'unknown1') = nvl(t5.project_code,'unknown2') AND nvl(t3.work_id,'unknown1') = nvl(t5.work_id,'unknown2')

-- 堆高车搬运

UNION ALL

-- QP搬运
SELECT '' as id, -- 主键
       t3.upper_work_id, -- 上游作业单ID
       t3.work_id , -- 搬运作业单
       t3.work_path, -- 路径
       t3.start_point, -- 起始点
       t3.target_point, -- 目标点
       t3.work_state, -- 作业单状态
       t3.first_classification, -- 机器人类型
       t3.first_classification_desc, -- 机器人类型中文描述
       t3.agv_type_code, -- 机器人类型编码
       t3.agv_code, -- 机器人编码
       nvl(t3.robot_num,0) as robot_num, -- 分配机器人数量
       nvl(unix_timestamp(t3.work_complete_time) - unix_timestamp(t3.work_create_time),0) as wotk_duration_total, -- 总耗时
       t4.waiting_agv_duration as robot_assign_duration, -- 分车耗时
       t5.move_duration as robot_move_duration, -- 搬运耗时
       NULL as station_executor_duration, -- 进站实操耗时
       t3.work_create_time, -- 作业单创建时间
       t3.work_complete_time, -- 作业单完成时间
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time,
       t2.project_code,
       substr(t3.work_create_time,1,10) as d,
       t2.project_code as pt
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful
  WHERE (project_product_type IN ('Quickpick','料箱搬运QP') OR project_code IN ('A51274','C35052')) and project_version like '2.%' 
)t2
JOIN 
(
  SELECT brj.project_code,
         brj.upper_work_id,
         IF(qe.work_id is null,brj.work_id,qe.work_id) as work_id,
         IF(qe.source_point_code is null or LENGTH(qe.source_point_code) = 0,brj.start_point,qe.source_point_code) as start_point,
         IF(qe.target_point_code is null or LENGTH(qe.target_point_code) = 0,brj.target_point,qe.target_point_code) as target_point,
         CONCAT(IF(qe.source_point_code is null or LENGTH(qe.source_point_code) = 0,brj.start_point,qe.source_point_code),' - ', IF(qe.target_point_code is null or LENGTH(qe.target_point_code) = 0,brj.target_point,qe.target_point_code)) as work_path,
         qe.work_state,
         qe.first_classification,
         qe.first_classification_desc,
         qe.agv_type_code,
         concat_ws(',',collect_set(cast(qe.agv_code as string))) as agv_code,
         COUNT(DISTINCT qe.agv_code) as robot_num,
         qe.job_created_time as work_create_time,
         qe.job_updated_time as work_complete_time
  FROM bucket_robot_job brj
  JOIN 
  (
    SELECT qe.project_code,
           qe.job_id,
           qe.d,
           qm.job_id as work_id,
           qm.source_point_code,
           qm.target_point_code,
           qm.agv_code,
           bat.first_classification,
           case when bat.first_classification = 'WORKBIN' then '料箱车'
                when bat.first_classification = 'STOREFORKBIN' then '存储一体式'
                when bat.first_classification = 'CARRIER' then '潜伏式机器人'
                when bat.first_classification = 'ROLLER' then '辊筒机器人'
                when bat.first_classification = 'FORKLIFT' then '堆高全向车'
                when bat.first_classification = 'DELIVER' then '投递车'
                when bat.first_classification = 'SC'then '四向穿梭车' 
           end as first_classification_desc,
           bat.agv_type_code,
           qm.job_state,
           CASE when qm.job_state = 'DONE' THEN '任务完成'
                when qm.job_state = 'CANCEL' THEN '取消'
                when qm.job_state = 'ABNORMAL_COMPLETED' THEN '异常完成'
                when qm.job_state = 'ABNORMAL_CANCEL' THEN '异常取消'
           end as work_state,
           qm.job_created_time,
           qm.job_updated_time
    FROM ${dwd_dbname}.dwd_g2p_si_qp_extend_info_df qe
    JOIN 
    (
      SELECT *
      FROM ${dwd_dbname}. dwd_g2p_si_qp_move_job_info_di qm
      WHERE qm.d >= '${pre1_date}'
    )qm
    ON nvl(qe.project_code,'unknown1') = nvl(qm.project_code,'unknown2') AND nvl(qe.move_job_id,'unknown1') = nvl(qm.id,'unknown2') 
    LEFT JOIN 
    (
      SELECT *
      FROM ${dwd_dbname}.dwd_rcs_basic_agv_info_df ba
      WHERE ba.d >= '${pre1_date}'
    )ba
    ON nvl(qm.project_code,'unknown1') = nvl(ba.project_code,'unknown2') AND nvl(qm.agv_code,'unknown1') = nvl(ba.agv_code,'unknown2') AND nvl(ba.d,'unknown1') = nvl(qm.d,'unknown2')
    LEFT JOIN 
    (
      SELECT *
      FROM ${dwd_dbname}.dwd_rcs_basic_agv_type_info_df bat 
      WHERE bat.d >= '${pre1_date}'
    )bat 
    ON nvl(ba.project_code,'unknown1') = nvl(bat.project_code,'unknown2') AND nvl(ba.agv_type_id,'unknown1') = nvl(bat.id,'unknown2') AND nvl(bat.d,'unknown1') = nvl(ba.d,'unknown2')
    WHERE qe.d >= '${pre1_date}'
    
    UNION ALL 
    
    SELECT qe.project_code,
           qe.job_id,
           qe.d,
           qt.job_id as work_id,
           qt.source_point_code,
           qt.target_point_code,
           qt.agv_code,
           bat.first_classification,
           case when bat.first_classification = 'WORKBIN' then '料箱车'
                when bat.first_classification = 'STOREFORKBIN' then '存储一体式'
                when bat.first_classification = 'CARRIER' then '潜伏式机器人'
                when bat.first_classification = 'ROLLER' then '辊筒机器人'
                when bat.first_classification = 'FORKLIFT' then '堆高全向车'
                 when bat.first_classification = 'DELIVER' then '投递车'
                when bat.first_classification = 'SC'then '四向穿梭车' 
           end as first_classification_desc,
           bat.agv_type_code,
           qt.job_state,
           CASE when qt.job_state = 'DONE' THEN '任务完成'
                when qt.job_state = 'CANCEL' THEN '取消'
                when qt.job_state = 'ABNORMAL_COMPLETED' THEN '异常完成'
                when qt.job_state = 'ABNORMAL_CANCEL' THEN '异常取消'
           end as work_state,
           qt.job_created_time,
           qt.job_updated_time
    FROM ${dwd_dbname}.dwd_g2p_si_qp_extend_info_df qe
    JOIN 
    (
      SELECT *
      FROM ${dwd_dbname}.dwd_g2p_si_qp_transfer_job_info_di qt
      WHERE qt.d >= '${pre1_date}'
    )qt
    ON qe.project_code = qt.project_code AND qe.transfer_job_id = qt.id AND qe.d = qt.d
    LEFT JOIN 
    (
      SELECT *
      FROM ${dwd_dbname}.dwd_rcs_basic_agv_info_df ba
      WHERE ba.d >= '${pre1_date}'
    )ba
    ON nvl(qt.project_code,'unknown1') = nvl(ba.project_code,'unknown2') AND nvl(qt.agv_code,'unknown1') = nvl(ba.agv_code,'unknown2') AND nvl(ba.d,'unknown1') = nvl(qt.d,'unknown2')
    LEFT JOIN 
    (
      SELECT *
      FROM ${dwd_dbname}.dwd_rcs_basic_agv_type_info_df bat 
      WHERE bat.d >= '${pre1_date}'
    )bat 
    ON nvl(ba.project_code,'unknown1') = nvl(bat.project_code,'unknown2') AND nvl(ba.agv_type_id,'unknown1') = nvl(bat.id,'unknown2') AND nvl(bat.d,'unknown1') = nvl(ba.d,'unknown2')
    WHERE qe.d >= '${pre1_date}'
  )qe
  ON nvl(brj.project_code,'unknown1') = nvl(qe.project_code,'unknown2') AND nvl(brj.upper_work_id,'unknown1') = nvl(qe.job_id,'unknown2') AND nvl(brj.d,'unknown1') = nvl(qe.d,'unknown2')
  WHERE qe.work_state IN ('完成','取消','异常完成','异常取消')
  GROUP BY brj.project_code,
           brj.upper_work_id,
           IF(qe.work_id is null,brj.work_id,qe.work_id),
           IF(qe.source_point_code is null or LENGTH(qe.source_point_code) = 0,brj.start_point,qe.source_point_code),
           IF(qe.target_point_code is null or LENGTH(qe.target_point_code) = 0,brj.target_point,qe.target_point_code),
           CONCAT(IF(qe.source_point_code is null or LENGTH(qe.source_point_code) = 0,brj.start_point,qe.source_point_code),' - ', IF(qe.target_point_code is null or LENGTH(qe.target_point_code) = 0,brj.target_point,qe.target_point_code)),
           qe.work_state,
           qe.first_classification,
           qe.first_classification_desc,
           qe.agv_type_code,
           qe.job_created_time,
           qe.job_updated_time
)t3
ON nvl(t2.project_code,'unknown1') = nvl(t3.project_code,'unknown2')
-- 分车耗时
LEFT JOIN 
(
  SELECT c1.project_code,
         c1.job_id as work_id,
         unix_timestamp(c2.job_updated_time) - unix_timestamp(c1.job_updated_time) as waiting_agv_duration
  FROM 
  (
    SELECT *,
           row_number()over(PARTITION by jsc1.project_code,jsc1.job_id order by jsc1.job_created_time asc,jsc1.job_updated_time asc)rn
    FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da jsc1 
    WHERE jsc1.d >= '${pre1_date}' AND LENGTH(jsc1.agv_code) = 0
  )c1
  LEFT JOIN 
  (
    SELECT jsc1.project_code,
           jsc1.job_id,
           MAX(jsc1.job_updated_time) as job_updated_time
    FROM 
    (
      SELECT *,
             row_number()over(PARTITION by jsc1.project_code,jsc1.job_id,jsc1.agv_code order by jsc1.job_created_time,jsc1.job_updated_time desc)rn
      FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da jsc1
      WHERE jsc1.d >= '${pre1_date}' AND LENGTH(jsc1.agv_code) != 0
    )jsc1
    WHERE jsc1.rn = 1
    GROUP BY jsc1.project_code,jsc1.job_id
  )c2
  ON nvl(c1.project_code,'unknown1') = nvl(c2.project_code,'unknown2') AND nvl(c1.job_id,'unknown1') = nvl(c2.job_id,'unknown2')
  WHERE c1.rn = 1
)t4
ON nvl(t3.project_code,'unknown1') = nvl(t4.project_code,'unknown2') AND nvl(t3.work_id,'unknown1') = nvl(t4.work_id,'unknown2')
-- 搬运耗时
LEFT JOIN 
(
  SELECT c1.project_code,
         c1.job_id as work_id,
         unix_timestamp(c2.job_updated_time) - unix_timestamp(c1.job_updated_time) as move_duration
  FROM 
  (
    SELECT jsc1.project_code,
           jsc1.job_id,
           MAX(jsc1.job_updated_time) as job_updated_time
    FROM 
    (
      SELECT *,
             row_number()over(PARTITION by jsc1.project_code,jsc1.job_id,jsc1.agv_code order by jsc1.job_created_time asc,jsc1.job_updated_time asc)rn
      FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da jsc1
      WHERE jsc1.d >= '${pre1_date}' AND LENGTH(jsc1.agv_code) != 0
    )jsc1
    WHERE jsc1.rn = 1
    GROUP BY jsc1.project_code,jsc1.job_id
  )c1
  LEFT JOIN 
  (
    SELECT *,
           row_number()over(PARTITION by jsc1.project_code,jsc1.job_id order by jsc1.job_created_time desc)rn
    FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da jsc1
    WHERE jsc1.d >= '${pre1_date}' AND jsc1.job_state IN ('DONE')
  )c2
  ON nvl(c1.project_code,'unknown1') = nvl(c2.project_code,'unknown2') AND nvl(c1.job_id,'unknown1') = nvl(c2.job_id,'unknown2')
)t5
ON nvl(t3.project_code,'unknown1') = nvl(t5.project_code,'unknown2') AND nvl(t3.work_id,'unknown1') = nvl(t5.work_id,'unknown2')


-- 凤凰3.X CARRIER逻辑   
union all
SELECT ''                                                      as id,                        -- 主键
       td.upstream_order_no                                    as upper_work_id,             -- 上游作业单ID
       td.order_no                                             as work_id,                   -- 搬运作业单
       td.work_path,                                                                         -- 路径
       td.start_point                                          as start_point,               -- 起始点
       td.target_point                                         as target_point,              -- 目标点
       td.order_state                                          as work_state,                -- 作业单状态
       td.dispatch_first_classification_str                    as first_classification,      -- 机器人类型
       td.dispatch_first_classification_name_str               as first_classification_desc, -- 机器人类型中文描述	
       td.dispatch_robot_type_code_str                         as agv_type_code,             -- 机器人类型编码
       td.dispatch_robot_code_str                              as agv_code,                  -- 机器人编码
       nvl(td.dispatch_robot_code_num, 0)                      as robot_num,                 -- 分配机器人数量
       nvl(td.total_cost / 1000, 0)                            as wotk_duration_total,       -- 总耗时
       nvl(td.assign_cost / 1000, 0)                           as robot_assign_duration,     -- 分车耗时
       nvl(td.only_carry_total_time_consuming / 1000, 0)       as robot_move_duration,       -- 搬运耗时
       NULL                                                    as station_executor_duration, -- 进站实操耗时
       td.order_create_time,                                                                 -- 作业单创建时间
       td.order_update_time,                                                                 -- 作业单完成时间
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
       c.project_code,
       substr(td.order_update_time, 1, 10)                     as d,
       c.project_code                                          as pt
FROM (SELECT *
      FROM ${dim_dbname}.dim_collection_project_record_ful
      WHERE project_version like '3.%') c
         inner join
     (select tc.project_code,
             tc.upstream_order_no,
             tc.order_no,
             case
                 when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code
                 else 'unknown' end                                      as start_point,  -- 起始点
             case
                 when t.start_area_code <> '' and t.start_area_code is not null then t.start_area_code
                 else 'unknown' end                                      as start_area,   -- 起始区域
             case
                 when t.target_point_code <> '' and t.target_point_code is not null then t.target_point_code
                 else 'unknown' end                                      as target_point, -- 目标点
             case
                 when t.target_area_code <> '' and t.target_area_code is not null then t.target_area_code
                 else 'unknown' end                                      as target_area,  -- 目标区域
             CONCAT(case
                        when t.start_point_code <> '' and t.start_point_code is not null then t.start_point_code
                        else 'unknown' end, '-', case
                                                     when t.target_point_code <> '' and t.target_point_code is not null
                                                         then t.target_point_code
                                                     else 'unknown' end) as work_path,
             t.order_state,
             tc.total_cost,
             tc.assign_cost,
             tc.move_cost,
             tc.total_cost - tc.assign_cost - tc.move_cost               as only_carry_total_time_consuming,
             t.order_create_time,
             tc.order_update_time,
             tj.dispatch_robot_code_str,                                                  -- 分配的机器人
             tj.dispatch_robot_type_code_str,
             tj.dispatch_robot_type_name_str,
             tj.dispatch_first_classification_str,
             tj.dispatch_first_classification_name_str,
             tj.dispatch_robot_code_num

      from ${dwd_dbname}.dwd_phx_rss_transport_order_carrier_cost_info_di tc
               left join ${dwd_dbname}.dwd_phx_rss_transport_order_info_di t
                         on tc.project_code = t.project_code and t.order_no = tc.order_no and
                            t.d >= DATE_ADD('${pre1_date}', -1)
               left join
           (select tc.project_code,
                   tc.order_no,
                   concat_ws(',', collect_set(tj.robot_code))           as dispatch_robot_code_str, -- 分配的机器人
                   concat_ws(',', collect_set(tr.robot_type_code))      as dispatch_robot_type_code_str,
                   concat_ws(',', collect_set(tr.robot_type_name))      as dispatch_robot_type_name_str,
                   concat_ws(',', collect_set(tr.first_classification)) as dispatch_first_classification_str,
                   concat_ws(',', collect_set(
                           case
                               when tr.first_classification = 'WORKBIN' then '料箱车'
                               when tr.first_classification = 'STOREFORKBIN' then '存储一体式'
                               when tr.first_classification = 'CARRIER' then '潜伏式机器人'
                               when tr.first_classification = 'ROLLER' then '辊筒机器人'
                               when tr.first_classification = 'FORKLIFT' then '堆高全向车'
                               when tr.first_classification = 'DELIVER' then '投递车'
                               when tr.first_classification = 'SC' then '四向穿梭车'
                               end))                                    as dispatch_first_classification_name_str,
                   count(distinct tj.robot_code)                        as dispatch_robot_code_num
            from ${dwd_dbname}.dwd_phx_rss_transport_order_carrier_cost_info_di tc
                     left join ${dwd_dbname}.dwd_phx_rss_transport_order_carrier_job_info_di tj
                               on tj.project_code = tc.project_code and tj.order_no = tc.order_no and
                                  tj.d >= DATE_ADD('${pre1_date}', -1)
                     left join ${dwd_dbname}.dwd_phx_basic_robot_base_info_df tr
                               on tr.project_code = tc.project_code and tr.robot_code = tj.robot_code and tr.d = tj.d
            where tc.d >= '${pre1_date}'
            group by tc.project_code, tc.order_no) tj on tj.order_no = tc.order_no
      where tc.d >= '${pre1_date}') td on td.project_code = c.project_code

;
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"