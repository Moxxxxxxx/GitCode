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
-- 机器人故障统计临时表 tmp_amr_mtbf_breakdown_add 

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
err_times as 
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
  FROM err_times tmp
  lateral view posexplode(split(repeat('o',(hour(tmp.end_time) - hour(tmp.error_time))),'o')) b
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
  WHERE t.d = '${pre1_date}'  --b.d >= '${pre1_date}'
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
         t3.agv_code -- 机器人编码
  FROM 
  (
    SELECT WEEKOFYEAR(t1.days) as cur_week,
           t1.days as cur_date,
           date_format(concat(t1.days,' ',tt1.hourofday,':00:00'),'yyyy-MM-dd HH:00:00') as cur_hour
    FROM ${dim_dbname}.dim_day_date t1
    LEFT JOIN ${dim_dbname}.dim_day_of_hour tt1
    WHERE t1.days = '${pre1_date}' --and t1.days <= DATE_ADD(current_date(),-1) --t1.days >= '${pre1_date}'
  )t1
  LEFT JOIN 
  (
    SELECT project_code
    FROM ${dim_dbname}.dim_collection_project_record_ful
    WHERE project_code IN ('A51223','A51118','A51431') --project_product_type_code IN (1,2) 
  )t2
  LEFT JOIN agv_num t3
  ON t2.project_code = t3.project_code
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
       cast(t1.mtbf_error_num + nvl(t2.mtbf_error_num,0) as int) as mtbf_error_num, -- mtbf故障次数
       cast(t1.mtbf_error_duration + nvl(t2.mtbf_error_duration,0) as int) as mtbf_error_duration, -- mtbf故障时长
       cast((3600 * ROW_NUMBER() over (PARTITION by t1.project_code,t1.agv_code order by t1.cur_date,t1.cur_hour asc)) + nvl(t2.theory_time,0) as int) as theory_time, -- 理论运行时长
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
       SUBSTR(t1.cur_date,1,10) as d,
       t1.project_code as pt
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
         t1.mtbf_error_duration -- mtbf故障时长
  FROM 
  (
    --每天新增
    SELECT t1.cur_week, -- 统计星期
           t1.cur_date, -- 统计日期
           t1.cur_hour, -- 统计小时
           t1.project_code, -- 项目编码
           t1.agv_type, -- 离线表机器人类型编码
           t1.agv_type_code, -- 机器人类型编码
           t1.agv_type_name, -- 机器人类型名称
           t1.agv_code, -- 机器人编码
           sum(cast(sum(nvl(t2.breakdown_duration,0)) as string)) over(PARTITION BY t1.cur_date,t1.project_code,t1.agv_code order by t1.cur_hour) as mtbf_error_duration -- mtbf故障时长 
    FROM base t1
    LEFT JOIN err_breakdown t2
    ON t1.cur_date = t2.cur_date AND t1.cur_hour = t2.cur_hour AND t1.project_code = t2.project_code AND t1.agv_code = t2.agv_code
    GROUP BY t1.cur_week,t1.cur_date,t1.cur_hour,t1.project_code,t1.agv_type,t1.agv_type_code,t1.agv_type_name,t1.agv_code
  )t1
  LEFT JOIN 
  (
    --每天新增
    SELECT t1.cur_week, -- 统计星期
           t1.cur_date, -- 统计日期
           t1.cur_hour, -- 统计小时
           t1.project_code, -- 项目编码
           t1.agv_type, -- 离线表机器人类型编码
           t1.agv_type_code, -- 机器人类型编码
           t1.agv_type_name, -- 机器人类型名称
           t1.agv_code, -- 机器人编码
           sum(COUNT(DISTINCT t2.breakdown_id)) over(PARTITION BY t1.cur_date,t1.project_code,t1.agv_code order by t1.cur_hour) as mtbf_error_num -- mtbf故障次数
    FROM base t1
    LEFT JOIN err_times t2
    ON t1.cur_date = t2.cur_date AND t1.cur_hour = t2.cur_hour AND t1.project_code = t2.project_code AND t1.agv_code = t2.agv_code
    GROUP BY t1.cur_week,t1.cur_date,t1.cur_hour,t1.project_code,t1.agv_type,t1.agv_type_code,t1.agv_type_name,t1.agv_code
  )t2
  ON t1.cur_date = t2.cur_date AND t1.cur_hour = t2.cur_hour AND t1.project_code = t2.project_code AND t1.agv_code = t2.agv_code
)t1
-- 累计值
LEFT JOIN 
(
  SELECT *,ROW_NUMBER() over (PARTITION by cur_date,project_code,agv_code order by cur_hour desc) as rn
  FROM ${tmp_dbname}.tmp_amr_mtbf_breakdown_add
  WHERE d = DATE_ADD('${pre1_date}',-1)
)t2
ON t2.rn = 1 AND t1.project_code = t2.project_code AND t1.agv_code = t2.agv_code;
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"



echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "
