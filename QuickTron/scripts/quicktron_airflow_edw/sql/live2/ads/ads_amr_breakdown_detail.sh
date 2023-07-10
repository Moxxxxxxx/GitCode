#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads
tmp_dbname=tmp
pre1_date=`date -d "-8 day" +%F`

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
-- 机器人故障明细 ads_amr_breakdown_detail 

INSERT overwrite table ${ads_dbname}.ads_amr_breakdown_detail partition(d,pt)
/*
-- v4故障
SELECT '' as id, -- 主键
       NULL as data_time, -- 统计小时
       bd.project_code, -- 项目编码
       bd.error_time as happen_time, -- 故障触发时间
       bd.first_classification_desc as carr_type_des, -- 机器人大类
       nvl(a.agv_type,bd.agv_type_code) as amr_type, -- 机器人类型编码
       nvl(a.agv_type_name,bd.agv_type_name) as amr_type_des, -- 机器人类型名称
       bd.agv_code as amr_code, -- 机器人编码
       bd.error_level, -- 故障等级
       bd.error_display_name as error_des, -- 故障描述
       bd.error_name as error_code, -- 故障编码
       NULL as error_module, -- 故障模块
       bd.error_end_time as end_time, -- 故障结束时间
       bd.breakdown_duration as error_duration, -- 故障时长
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
       SUBSTR(bd.error_time,1,10) as d,
       bd.project_code as pt
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful
  where project_version LIKE '2.%'
)pt
JOIN 
(
  SELECT t.*,
         coalesce(t.sort_time[0], t.sort_time[1]) as error_end_time,
         unix_timestamp(coalesce(t.sort_time[0], t.sort_time[1])) - unix_timestamp(t.error_time) as breakdown_duration
  FROM 
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
             b.agv_type_name,
             b.breakdown_id,
             b.error_code,
             b.error_name,
             b.error_display_name,
             b.error_level,
             b.d,
             case when b.first_classification = 'WORKBIN' then '料箱车'
                  when b.first_classification = 'STOREFORKBIN' then '存储一体式'
                  when b.first_classification = 'CARRIER' then '潜伏式机器人'
                  when b.first_classification = 'ROLLER' then '辊筒机器人'
                  when b.first_classification = 'FORKLIFT' then '堆高全向车'
                  when b.first_classification = 'DELIVER' then '投递车'
                  when b.first_classification = 'SC'then '四向穿梭车' 
             end as first_classification_desc
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
      WHERE r.d >= '${pre1_date}' AND r.pt IN (SELECT project_code FROM ${dim_dbname}.dim_collection_project_record_ful WHERE is_nonetwork = 1 and project_version LIKE '2.%')
    ) tt2 
    ON tt2.project_code = tt1.project_code AND tt2.agv_code = tt1.agv_code AND tt2.d = tt1.d
    WHERE tt2.status_change_time > tt1.error_time
  )t
  WHERE t.rk = 1
)bd
ON pt.project_code = bd.project_code
LEFT JOIN ${tmp_dbname}.tmp_basic_agv_data_offline_info a
ON bd.project_code = a.project_code AND a.agv_code = bd.agv_code
*/
-- v5故障
SELECT '' as id, -- 主键
       NULL as data_time, -- 统计小时
       bd.project_code, -- 项目编码
       bd.error_time as happen_time, -- 故障触发时间
       bd.first_classification_desc as carr_type_des, -- 机器人大类
       nvl(a.agv_type,bd.agv_type_code) as amr_type, -- 机器人类型编码
       nvl(a.agv_type_name,bd.agv_type_name) as amr_type_des, -- 机器人类型名称
       bd.agv_code as amr_code, -- 机器人编码
       bd.error_level, -- 故障等级
       bd.error_display_name as error_des, -- 故障描述
       bd.error_name as error_code, -- 故障编码
       NULL as error_module, -- 故障模块
       bd.end_time, -- 故障结束时间
       bd.error_duration, -- 故障时长
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
       SUBSTR(bd.error_time,1,10) as d,
       bd.project_code as pt
FROM 
(
  SELECT *
  FROM ${dim_dbname}.dim_collection_project_record_ful
  where project_version LIKE '2.%'
)pt
JOIN 
(
  SELECT b.project_code,
         b.breakdown_log_time as error_time,
         b.breakdown_end_time as end_time,
         b.agv_code,
         b.agv_type_code,
         b.agv_type_name,
         b.breakdown_id,
         b.error_code,
         b.error_name,
         b.error_display_name,
         b.error_level,
         b.d,
         case when b.first_classification = 'WORKBIN' then '料箱车'
              when b.first_classification = 'STOREFORKBIN' then '存储一体式'
              when b.first_classification = 'CARRIER' then '潜伏式机器人'
              when b.first_classification = 'ROLLER' then '辊筒机器人'
              when b.first_classification = 'FORKLIFT' then '堆高全向车'
              when b.first_classification = 'DELIVER' then '投递车'
              when b.first_classification = 'SC'then '四向穿梭车' 
         end as first_classification_desc,
         unix_timestamp(b.breakdown_end_time) - unix_timestamp(b.breakdown_log_time) as error_duration
  FROM ${dwd_dbname}.dwd_agv_breakdown_astringe_v5_di b
  WHERE b.d >= '${pre1_date}'
)bd
ON pt.project_code = bd.project_code
LEFT JOIN ${tmp_dbname}.tmp_basic_agv_data_offline_info a
ON bd.project_code = a.project_code AND a.agv_code = bd.agv_code;
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"