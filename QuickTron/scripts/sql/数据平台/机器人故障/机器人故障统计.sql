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
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=5000;
-------------------------------------------------------------------------------------------------------------00
-- 机器人故障统计 ads_amr_breakdown 

with breakdown as
(
  SELECT tt1.*,
         ROW_NUMBER() over (PARTITION by tt1.project_code,tt1.agv_code,tt1.breakdown_id,tt1.d order by tt2.status_change_time asc) as rk,
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
           b.error_level,
           b.d
    FROM ${dwd_dbname}.dwd_agv_breakdown_astringe_v4_di b
    WHERE b.d = '${pre1_date}' AND b.error_level >= '3' 
    -- b.d >= '${pre1_date}'
  )tt1
  LEFT JOIN 
  (
    SELECT w.project_code,
           w.agv_code,
           w.status_log_time as status_change_time,
           w.working_status,
           w.online_status,
           w.d
    FROM ${dwd_dbname}.dwd_agv_working_status_incre_dt w
    WHERE 1 = 1 AND w.d = '${pre1_date}' AND w.online_status = 'REGISTERED' AND w.working_status = 'BUSY'  
    -- w.d >= '${pre1_date}'
  ) tt2 
  ON tt2.project_code = tt1.project_code AND tt2.agv_code = tt1.agv_code AND tt2.d = tt1.d
  WHERE tt2.status_change_time > tt1.error_time
),
err_breakdown as 
(
  SELECT tmp.cur_date,
         IF(b.pos = 0,tmp.cur_hour,from_unixtime((unix_timestamp(tmp.cur_hour) + b.pos * 3600),'yyyy-MM-dd HH:mm:ss')) as cur_hour,
         IF(b.pos = 0,tmp.error_time,from_unixtime((unix_timestamp(tmp.cur_hour) + b.pos * 3600),'yyyy-MM-dd HH:mm:ss')) as error_time,
         case when b.pos = 0 and hour(tmp.end_time) - hour(tmp.error_time) = b.pos then tmp.end_time
              when hour(tmp.end_time) - hour(tmp.error_time) != b.pos then from_unixtime((unix_timestamp(tmp.cur_hour) + (b.pos + 1) * 3600),'yyyy-MM-dd HH:mm:ss')
              when b.pos != 0 and hour(tmp.end_time) - hour(tmp.error_time) = b.pos then tmp.end_time
         end as end_time,
         tmp.project_code,
         tmp.agv_type_code,
         tmp.agv_code,
         unix_timestamp(case when b.pos = 0 and hour(tmp.end_time) - hour(tmp.error_time) = b.pos then tmp.end_time
                             when hour(tmp.end_time) - hour(tmp.error_time) != b.pos then from_unixtime((unix_timestamp(tmp.cur_hour) + (b.pos + 1) * 3600),'yyyy-MM-dd HH:mm:ss')
                             when b.pos != 0 and hour(tmp.end_time) - hour(tmp.error_time) = b.pos then tmp.end_time end) 
         - 
         unix_timestamp(IF(b.pos = 0,tmp.error_time,from_unixtime((unix_timestamp(tmp.cur_hour) + b.pos * 3600),'yyyy-MM-dd HH:mm:ss'))) as breakdown_duration,
         tmp.breakdown_id
  FROM 
  (
    SELECT TO_DATE(t.error_time) as cur_date,
           date_format(t.error_time,'yyyy-MM-dd HH:00:00') as cur_hour,
           t.error_time,
           coalesce(t.sort_time[0], t.sort_time[1], t.sort_time[2]) as end_time,
           t.project_code,
           cast(coalesce(t.agv_type_code, -1) as string) as agv_type_code,
           cast(coalesce(t.agv_code, -1) as string) as agv_code,
           unix_timestamp(coalesce(t.sort_time[0], t.sort_time[1], t.sort_time[2])) - unix_timestamp(t.error_time) as breakdown_duration, -- 故障时长
           t.breakdown_id
    FROM breakdown t
    WHERE t.rk = 1
  )tmp
  lateral view posexplode(split(repeat('o',(hour(tmp.end_time) - hour(tmp.error_time))),'o')) b
),
end_breakdown as 
(
  SELECT TO_DATE(coalesce(t.sort_time[0], t.sort_time[1], t.sort_time[2])) as cur_date,
         date_format(coalesce(t.sort_time[0], t.sort_time[1], t.sort_time[2]),'yyyy-MM-dd HH:00:00') as cur_hour,
         t.error_time,
         coalesce(t.sort_time[0], t.sort_time[1], t.sort_time[2]) as end_time,
         t.project_code,
         cast(coalesce(t.agv_type_code, -1) as string) as agv_type_code,
         cast(coalesce(t.agv_code, -1) as string) as agv_code,
         unix_timestamp(coalesce(t.sort_time[0], t.sort_time[1], t.sort_time[2])) - unix_timestamp(t.error_time) as breakdown_duration -- 故障时长
  FROM breakdown t
  WHERE t.rk = 1
)