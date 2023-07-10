#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp
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
-- 智能搬运系统运营统计 ads_single_project_intelligent_handling 

INSERT overwrite table ${ads_dbname}.ads_single_project_intelligent_handling
SELECT '' as id, -- 主键
       t1.days as cur_date, -- 统计日期
       t2.hourofday as cur_hour, -- 统计小时
       date_format(concat(t1.days,' ',t2.startofhour),'yyyy-MM-dd HH:mm:ss') as cur_datetime,
       t3.project_code, -- 项目编码
       t3.project_name, -- 项目名称
       nvl(t4.send_workbin,0) as send_workbin, -- 下发作业单量 
       nvl(t5.exc_workbin,0) as exc_workbin, -- 异常作业单量 
       nvl(t6.cancel_workbin,0) as cancel_workbin, -- 取消作业单量
       cast(nvl(t7.agv_use_rate,0) as decimal(10,4)) as agv_use_rate, -- 机器人利用率
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_day_date
  WHERE days = '${pre1_date}'
)t1
LEFT JOIN ${dim_dbname}.dim_day_of_hour t2
LEFT JOIN 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful
  WHERE project_product_type_code IN (3,4) OR project_product_type IN ('标准搬运','堆高车搬运','Quickpick','料箱搬运QP')
)t3
-- 下发搬运作业单
LEFT JOIN 
(
  SELECT r.d as cur_date,
         lpad(HOUR(r.job_created_time),2,0) as cur_hour,
         r.project_code,
         COUNT(DISTINCT r.job_id) as send_workbin
  FROM ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di r
  WHERE r.d = '${pre1_date}'
  GROUP BY r.d,lpad(HOUR(r.job_created_time),2,0),r.project_code
)t4
ON t1.days = t4.cur_date AND t2.hourofday = t4.cur_hour AND t3.project_code = t4.project_code
-- 异常作业单量
LEFT JOIN 
(
  SELECT r.d as cur_date,
         lpad(HOUR(r.job_created_time),2,0) as cur_hour,
         r.project_code,
         COUNT(DISTINCT r.job_id) as exc_workbin
  FROM ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di r
  WHERE r.d = '${pre1_date}' AND r.job_state IN ('ABNORMAL_CANCEL','ABNORMAL_COMPLETED','PENDING','ROLLBACK')
  GROUP BY r.d,lpad(HOUR(r.job_created_time),2,0),r.project_code
)t5
ON t1.days = t5.cur_date AND t2.hourofday = t5.cur_hour AND t3.project_code = t5.project_code
-- 取消作业单量
LEFT JOIN 
(
  SELECT r.d as cur_date,
         lpad(HOUR(r.job_created_time),2,0) as cur_hour,
         r.project_code,
         COUNT(DISTINCT r.job_id) as cancel_workbin
  FROM ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di r
  WHERE r.d = '${pre1_date}' AND r.job_state IN ('CANCEL')
  GROUP BY r.d,lpad(HOUR(r.job_created_time),2,0),r.project_code
)t6
ON t1.days = t6.cur_date AND t2.hourofday = t6.cur_hour AND t3.project_code = t6.project_code
-- 机器人利用率
LEFT JOIN 
(
  SELECT t1.project_code,
         t1.cur_date,
         t1.cur_hour,
         SUM(unix_timestamp(t1.end_time) - unix_timestamp(t1.start_time)),
         COUNT(DISTINCT t1.agv_code),
         SUM(unix_timestamp(t1.end_time) - unix_timestamp(t1.start_time)) / COUNT(DISTINCT t1.agv_code) / 3600 as agv_use_rate
  FROM 
  (
    SELECT tmp.project_code,
           tmp.agv_code,
           tmp.job_id,
           TO_DATE(IF(b.pos = 0,tmp.job_accept_time,from_unixtime((unix_timestamp(tmp.job_accept_time) + (b.pos * 60 * 60)),'yyyy-MM-dd HH:00:00.000'))) as cur_date,
           HOUR(IF(b.pos = 0,tmp.job_accept_time,from_unixtime((unix_timestamp(tmp.job_accept_time) + (b.pos * 60 * 60)),'yyyy-MM-dd HH:00:00.000'))) as cur_hour,
           tmp.job_accept_time,
           tmp.job_finish_time,
           b.pos,
           IF(b.pos = 0,tmp.job_accept_time,from_unixtime((unix_timestamp(tmp.job_accept_time) + (b.pos * 60 * 60)),'yyyy-MM-dd HH:00:00.000')) as start_time,
           case when b.pos = 0 and hour(tmp.job_finish_time) - hour(tmp.job_accept_time) = b.pos then tmp.job_finish_time
                when hour(tmp.job_finish_time) - hour(tmp.job_accept_time) != b.pos then from_unixtime((unix_timestamp(tmp.job_accept_time) + ((b.pos + 1)  * 60 * 60)),'yyyy-MM-dd HH:00:00.000')
                when b.pos != 0 and hour(tmp.job_finish_time) - hour(tmp.job_accept_time) = b.pos then tmp.job_finish_time end as end_time
    FROM
    (
      SELECT c.project_code,
             j.job_id,
             j.agv_code,
             m2.init_job_job_updated_time as job_accept_time,
             m3.done_job_updated_time as job_finish_time
      FROM 
      (
        SELECT *
        FROM ${dim_dbname}.dim_collection_project_record_ful c
        WHERE project_product_type_code = 4 OR project_product_type = '标准搬运' OR project_code = 'A51346'
      )c
      LEFT JOIN 
      (
        SELECT *
        FROM ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di j
        WHERE j.d = '${pre1_date}' AND j.job_state = 'DONE' -- 作业单完成
      )j 
      ON c.project_code = j.project_code
      LEFT JOIN 
      (
        SELECT project_code,
               job_id,
               agv_code,
               job_created_time as init_job_job_created_time,
               job_updated_time as init_job_job_updated_time,
        row_number()over(PARTITION by m.project_code,m.job_id order by m.job_created_time asc,id asc)rn
        FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da m
        WHERE m.d = '${pre1_date}' AND IF(job_type = 'SI_BUCKET_MOVE',LENGTH(m.agv_code) != 0 AND m.job_state != 'WAITING_DISPATCHER',m.job_state = 'INIT_JOB')
      )m2
      ON c.project_code = m2.project_code AND j.job_id = m2.job_id AND m2.rn = 1
      LEFT JOIN
      (
        SELECT project_code,
               job_id,
               agv_code,
               job_created_time as done_job_created_time,
               job_updated_time as done_job_updated_time,
        row_number()over(PARTITION by m.project_code,m.job_id order by m.job_created_time desc,id desc)rn
        FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da m
        WHERE m.d = '${pre1_date}' AND IF(job_type = 'SI_BUCKET_MOVE',LENGTH(m.agv_code) != 0 AND m.job_state != 'WAITING_DISPATCHER',m.job_state = 'DONE')
      )m3
      ON c.project_code = m3.project_code AND m2.job_id = m3.job_id AND m3.rn = 1
      
      UNION ALL
      
      SELECT c.project_code,
             m2.job_id,
             m2.agv_code,
             m2.init_job_job_updated_time as job_accept_time,
             m3.done_job_updated_time as job_finish_time
      FROM 
      (
        SELECT *
        FROM ${dim_dbname}.dim_collection_project_record_ful c
        WHERE project_product_type = '堆高车搬运' 
      )c
      LEFT JOIN 
      (
        SELECT project_code,
               job_id,
               agv_code,
               job_created_time as init_job_job_created_time,
               job_updated_time as init_job_job_updated_time,
        row_number()over(PARTITION by m.project_code,m.job_id order by m.job_created_time asc,id asc)rn
        FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da m
        WHERE m.d = '${pre1_date}' AND job_id LIKE 'SIFork_%'
      )m2
      ON c.project_code = m2.project_code AND m2.rn = 1
      LEFT JOIN
      (
        SELECT project_code,
               job_id,
               agv_code,
               job_created_time as done_job_created_time,
               job_updated_time as done_job_updated_time,
        row_number()over(PARTITION by m.project_code,m.job_id order by m.job_created_time desc,id desc)rn
        FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da m
        WHERE m.d = '${pre1_date}' AND job_id LIKE 'SIFork_%'
      )m3
      ON c.project_code = m3.project_code AND m2.job_id = m3.job_id AND m3.rn = 1
      
      UNION ALL 
      
      SELECT c.project_code,
             j.job_id,
             m1.agv_code,
             m2.init_job_job_updated_time as job_accept_time,
             m3.done_job_updated_time as job_finish_time
      FROM 
      (
        SELECT *
        FROM ${dim_dbname}.dim_collection_project_record_ful c
        WHERE project_product_type IN ('Quickpick','料箱搬运QP') OR project_code = 'A51274'
      )c
      LEFT JOIN 
      (
        SELECT j.project_code,
               j.robot_job_id,
               j.job_id
        FROM ${dwd_dbname}.dwd_g2p_bucket_robot_job_info_di j
        WHERE j.d = '${pre1_date}' AND j.job_state = 'DONE' -- 作业单完成
      )j 
      ON c.project_code = j.project_code
      LEFT JOIN 
      (
        SELECT e.project_code,
               e.job_id,
               e.move_job_id
        FROM ${dwd_dbname}.dwd_g2p_si_qp_extend_info_df e
        WHERE e.d = '${pre1_date}' 
      )e
      ON c.project_code = e.project_code AND j.robot_job_id = e.job_id
      LEFT JOIN 
      (
        SELECT m.project_code,
               m.id,
               m.job_id,
               m.agv_code
        FROM ${dwd_dbname}.dwd_g2p_si_qp_move_job_info_di m
        WHERE m.d = '${pre1_date}' AND m.job_state = 'DONE' -- 作业单完成
      )m1
      ON c.project_code = m1.project_code AND e.move_job_id = m1.id
      LEFT JOIN 
      (
        SELECT project_code,
               job_id,
               agv_code,
               job_created_time as init_job_job_created_time,
               job_updated_time as init_job_job_updated_time,
        row_number()over(PARTITION by m.project_code,m.job_id order by m.job_created_time asc,id asc)rn
        FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da m
        WHERE m.d = '${pre1_date}'  AND m.job_state = 'INIT_JOB'
      )m2
      ON c.project_code = m2.project_code AND j.job_id = m2.job_id AND m2.rn = 1
      LEFT JOIN
      (
        SELECT project_code,
               job_id,
               agv_code,
               job_state,
               job_created_time as done_job_created_time,
               job_updated_time as done_job_updated_time,
        row_number()over(PARTITION by m.project_code,m.job_id order by m.job_created_time desc,id desc)rn
        FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da m
        WHERE m.d = '${pre1_date}'
      )m3
      ON c.project_code = m3.project_code AND m2.job_id = m3.job_id AND m3.rn = 1
    )tmp
    lateral view posexplode(split(repeat('o',(hour(tmp.job_finish_time) - hour(tmp.job_accept_time))),'o')) b -- 炸裂跨天跨小时时间
  )t1
  GROUP BY t1.project_code,t1.cur_date,t1.cur_hour
)t7
ON t1.days = t7.cur_date AND t2.hourofday = t7.cur_hour AND t3.project_code = t7.project_code;
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"