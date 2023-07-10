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
    WHERE project_version LIKE '2.%' AND (project_product_type IN ('Quickpick','料箱搬运QP') OR project_code IN ('A51274'))
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
),



-- 凤凰
-- 所有符合执行范围内的机器人故障明细
phx_robot_error_detail as (
select 
te.project_code,
te.id,
te.robot_code,
te.robot_type_code,
te.robot_type_name,
te.first_classification,
te.error_code,
tde.error_name,
te.error_start_time,
te.error_end_time,
unix_timestamp(te.error_end_time)-unix_timestamp(te.error_start_time) as error_duration,
te.error_level,
te.error_detail, 
te.error_module,
to_date(te.error_start_time) as error_start_date
from ${dwd_dbname}.dwd_phx_robot_breakdown_astringe_v1_di te
left join ${dim_dbname}.dim_phx_basic_error_info_ful tde on tde.error_code =te.error_code
where te.d >= '${pre1_date}' and te.d<= DATE_ADD(current_date(),-1)
and te.error_module='robot' and te.error_level>=3
and (
(te.error_start_time >= '${pre1_date}' and te.error_start_time < current_date() and coalesce(te.error_end_time, current_timestamp) < current_date()) 
or(te.error_start_time >= '${pre1_date}' and te.error_start_time < current_date() and coalesce(te.error_end_time, current_timestamp) >= current_date())
or(te.error_start_time < '${pre1_date}' and coalesce(te.error_end_time,current_timestamp) >= current_date() and coalesce(te.error_end_time, current_timestamp) < current_date()) 
or(te.error_start_time < '${pre1_date}' and coalesce(te.error_end_time, current_timestamp) >= current_date())
)
),
-- 搬运作业单机器人任务明细
phx_create_order_job_detail as (
select 
t1.project_code,
t1.order_create_time,
t1.order_no,
t2.job_create_time,
t2.job_sn,
t2.robot_code,
tr.robot_type_code,
tr.robot_type_name 
from ${dwd_dbname}.dwd_phx_rss_transport_order_info_di t1 
left join ${dwd_dbname}.dwd_phx_rss_transport_order_carrier_job_info_di t2 on t2.pt=t1.pt and t2.order_id =t1.id and t2.d>='${pre1_date}'
left join  ${dwd_dbname}.dwd_phx_basic_robot_base_info_df tr on tr.pt=t2.pt and tr.d=t2.d and tr.robot_code=t2.robot_code and tr.d>='${pre1_date}'
where t1.d>='${pre1_date}'
),
-- 机器人小时列表
phx_hour_robot_list_detail  as (
select 
tt.cur_week, -- 统计星期
tt.cur_date, -- 统计日期
tt.cur_hour, -- 统计小时
tt.project_code, -- 项目编码
trl.robot_code,  -- 机器人编码
trl.robot_type_code,  -- 机器人类型编码
trl.robot_type_name    -- 机器人类型名称
-- 时间维度信息
from 
(select 
t1.cur_week, -- 统计星期
t1.cur_date, -- 统计日期
t1.cur_hour, -- 统计小时
thp.project_code -- 项目编码
from 
(SELECT 
WEEKOFYEAR(t1.days) as cur_week,
t1.days as cur_date,
date_format(concat(t1.days,' ',tt1.hourofday,':00:00'),'yyyy-MM-dd HH:00:00') as cur_hour
FROM ${dim_dbname}.dim_day_date t1
LEFT JOIN ${dim_dbname}.dim_day_of_hour tt1
WHERE t1.days >= '${pre1_date}' AND t1.days <= DATE_ADD(current_date(),-1))t1 
left join 
(SELECT project_code
FROM ${dim_dbname}.dim_collection_project_record_ful
where project_version like '3%')thp)tt  
-- 机器人信息
join 
(select 
tr.d as cur_date,
tr.project_code,
tr.robot_code,
tr.robot_type_code,
tr.robot_type_name 
from ${dwd_dbname}.dwd_phx_basic_robot_base_info_df tr 
inner join 
(SELECT project_code
FROM ${dim_dbname}.dim_collection_project_record_ful
where project_version like '3%')thp on thp.project_code =tr.project_code 
where tr.d>='${pre1_date}' and tr.d<= DATE_ADD(current_date(),-1)
and tr.robot_usage_state ='using')trl on trl.project_code=tt.project_code and trl.cur_date=tt.cur_date
),
-- 机器人小时内理论运行时长（秒）
phx_robot_hour_theory_run_duration as (
select 
ts.project_code,
ts.cur_date,
ts.cur_hour,
ts.robot_code,
ts.robot_type_code,  -- 机器人类型编码
ts.robot_type_name,    -- 机器人类型名称
COALESCE(sum(ts.state_keep_duration),0)+COALESCE(sum(ts.before_state_keep_duration),0) as theory_run_duration  -- 理论运行时长（秒）
from 
(select 
t.project_code,
t.cur_date,
t.cur_hour,
t.robot_code,
t.robot_type_code,  -- 机器人类型编码
t.robot_type_name,    -- 机器人类型名称
t.id           as                           state_id,
t.create_time     as                           state_create_time,
t.next_state_create_time,
t.network_state,
t.online_state,
t.work_state,
t.job_sn,
t.is_error,
t.duration / 1000 as                           duration,
case when t.desc_rk=1 then (unix_timestamp(t.cur_hour)+3600)-unix_timestamp(from_unixtime(unix_timestamp(t.create_time),'yyyy-MM-dd HH:mm:ss')) 
else unix_timestamp(from_unixtime(unix_timestamp(t.next_state_create_time),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(from_unixtime(unix_timestamp(t.create_time),'yyyy-MM-dd HH:mm:ss'))  end as state_keep_duration,  
t.asc_rk,
t.desc_rk,
tf.online_state as before_online_state,
tf.work_state as before_work_state,
tf.is_error as before_is_error,
case when t.asc_rk=1 and  t.id is null then 3600 when t.asc_rk=1 and t.id is not null then unix_timestamp(from_unixtime(unix_timestamp(t.create_time),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(t.cur_hour) end as before_state_keep_duration  -- 小时前最后一条状态在本小时内持续时长
from 
(select 
hrl.project_code,
hrl.cur_date,
hrl.cur_hour,
hrl.robot_code,
hrl.robot_type_code,  -- 机器人类型编码
hrl.robot_type_name,    -- 机器人类型名称
trs.id,
trs.create_time,
trs.network_state,
trs.online_state,
trs.work_state,
trs.job_sn,
trs.is_error,
trs.duration,
ROW_NUMBER() over(PARTITION by hrl.project_code,hrl.robot_code,hrl.cur_hour order by trs.create_time asc) as asc_rk,
ROW_NUMBER() over(PARTITION by hrl.project_code,hrl.robot_code,hrl.cur_hour order by trs.create_time desc) as desc_rk,
lead(trs.create_time,1) over(PARTITION by hrl.project_code,hrl.robot_code,hrl.cur_hour order by trs.create_time asc) as next_state_create_time
from phx_hour_robot_list_detail hrl 
left join ${dwd_dbname}.dwd_phx_rms_robot_state_info_di trs on trs.d>=DATE_ADD('${pre1_date}',-1) and trs.pt=hrl.project_code and trs.robot_code=hrl.robot_code and date_format(trs.create_time,'yyyy-MM-dd HH:00:00')=hrl.cur_hour
-- where hrl.robot_code='qilin31_51'
)t
-- 找到每个小时之前的第一条数据
left join 
(select 
hrl.project_code,
hrl.cur_date,
hrl.cur_hour,
hrl.robot_code,
trs.id,
trs.create_time,
trs.network_state,
trs.online_state,
trs.work_state,
trs.job_sn,
trs.is_error,
trs.duration,
ROW_NUMBER() over(PARTITION by hrl.project_code,hrl.robot_code,hrl.cur_hour order by trs.create_time desc) as rk 
from phx_hour_robot_list_detail hrl 
left join ${dwd_dbname}.dwd_phx_rms_robot_state_info_di trs on trs.d>=DATE_ADD('${pre1_date}',-30) and trs.pt=hrl.project_code and trs.robot_code=hrl.robot_code and trs.create_time<hrl.cur_hour
-- where hrl.robot_code='qilin31_51'
)tf on tf.rk=1 and tf.project_code=t.project_code and tf.robot_code=t.robot_code and tf.cur_date=t.cur_date and tf.cur_hour=t.cur_hour
)ts
where (ts.online_state = 'REGISTERED' or ts.work_state = 'ERROR' or ts.is_error = 1)or(ts.before_online_state = 'REGISTERED' or ts.before_work_state = 'ERROR' or ts.before_is_error = 1)
group by ts.project_code,ts.cur_date,ts.cur_hour,ts.robot_code,ts.robot_type_code,ts.robot_type_name
),
-- 机器人每小时故障时长
phx_hour_robot_error_duration as (
select 
t.cur_week,
t.cur_date,
t.cur_hour,
t.cur_next_hour,
t.project_code,
t.robot_code,
t.robot_type_code,
t.robot_type_name,
concat_ws(',' , collect_set(cast(t.id as string))) as error_id_list, -- 故障编码
count(distinct t.id) as error_num,
sum(unix_timestamp(from_unixtime(unix_timestamp(t.stat_end_time),'yyyy-MM-dd HH:mm:ss'))-unix_timestamp(from_unixtime(unix_timestamp(t.stat_start_time),'yyyy-MM-dd HH:mm:ss'))) as error_duration  -- 该小时故障时长
from 
(select 
t1.cur_week, -- 统计星期
t1.cur_date, -- 统计日期
t1.cur_hour, -- 统计小时
from_unixtime(unix_timestamp(t1.cur_hour)+3600) as cur_next_hour,  -- 统计小时的下一个小时
te.project_code,
te.id,
te.robot_code,
te.robot_type_code,
te.robot_type_name,
te.error_start_time,
te.error_end_time,
case when te.error_start_time<t1.cur_hour then t1.cur_hour else te.error_start_time end as stat_start_time,
case when te.error_end_time is null then from_unixtime(unix_timestamp(t1.cur_hour)+3600) when te.error_end_time>=from_unixtime(unix_timestamp(t1.cur_hour)+3600) then from_unixtime(unix_timestamp(t1.cur_hour)+3600) else te.error_end_time end as stat_end_time
from 
(SELECT 
WEEKOFYEAR(t1.days) as cur_week,
t1.days as cur_date,
date_format(concat(t1.days,' ',tt1.hourofday,':00:00'),'yyyy-MM-dd HH:00:00') as cur_hour
FROM ${dim_dbname}.dim_day_date t1
LEFT JOIN ${dim_dbname}.dim_day_of_hour tt1
WHERE t1.days >= '${pre1_date}' AND t1.days <= DATE_ADD(current_date(),-1))t1 
inner join phx_robot_error_detail te on 
((te.error_start_time >= t1.cur_hour and te.error_start_time < from_unixtime(unix_timestamp(t1.cur_hour)+3600) and coalesce(te.error_end_time, current_timestamp) < from_unixtime(unix_timestamp(t1.cur_hour)+3600)) 
or(te.error_start_time >= t1.cur_hour and te.error_start_time < from_unixtime(unix_timestamp(t1.cur_hour)+3600) and coalesce(te.error_end_time, current_timestamp) >= from_unixtime(unix_timestamp(t1.cur_hour)+3600))
or(te.error_start_time < t1.cur_hour and coalesce(te.error_end_time,current_timestamp) >= from_unixtime(unix_timestamp(t1.cur_hour)+3600) and coalesce(te.error_end_time, current_timestamp) < from_unixtime(unix_timestamp(t1.cur_hour)+3600)) 
or(te.error_start_time < t1.cur_hour and coalesce(te.error_end_time, current_timestamp) >= from_unixtime(unix_timestamp(t1.cur_hour)+3600))
)
)t
group by t.cur_week,t.cur_date,t.cur_hour,t.cur_next_hour,t.project_code,t.robot_code,t.robot_type_code,t.robot_type_name
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
         cast(sum(t.breakdown_duration) as string) as breakdown_duration -- 故障时长
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
         cast(sum(t.breakdown_duration) as string) as breakdown_duration -- 故障时长
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
  WHERE t.rk = 1
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
         cast(sum(t.breakdown_duration) as string) as breakdown_duration -- 故障时长
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
         cast(sum(t.breakdown_duration) as string) as breakdown_duration -- 故障时长
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
  WHERE t.rk = 1
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
         cast(sum(t.breakdown_duration) as string) as breakdown_duration -- 故障时长
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
         cast(sum(t.breakdown_duration) as string) as breakdown_duration -- 故障时长
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
  WHERE t.rk = 1
  GROUP BY TO_DATE(t.error_time),date_format(t.error_time,'yyyy-MM-dd HH:00:00'),t.project_code
)t7
ON t1.cur_date = t7.cur_date AND t1.cur_hour = t7.cur_hour AND t1.project_code = t7.project_code


union all 

-- 凤凰 
-- 分时分小车
SELECT 
'' as id, -- 主键
NULL as data_time, -- 统计小时
rl.project_code, -- 项目编码
rl.cur_hour as happen_time, -- 统计小时
'single' as type_class, -- 数据类型
rl.robot_type_code as amr_type,  -- 机器人类型编码
rl.robot_type_name as amr_type_des,    -- 机器人类型名称
rl.robot_code as amr_code,  -- 机器人编码
ted.error_id_list as breakdown_id, -- 故障集合
COALESCE(tor.create_order_num,0) as carry_order_num,  -- 搬运作业单胆量
COALESCE(tor.create_order_job_num,0) as carry_task_num,  -- 机器人任务量
nvl(trd.theory_run_duration,0) as theory_time, -- 理论运行时长
nvl(ted.error_duration,0) as error_duration, -- 故障时长
nvl(tee.end_error_sum_duration,0) as mttr_error_duration, -- mttr故障时长
nvl(tee.end_error_num,0) as mttr_error_num, -- mttr错误次数
cast(nvl((tadd.add_theory_time-tadd.add_error_duration)/tadd.add_error_num,tadd.add_theory_time) as decimal(10,2))  as add_mtbf, -- 累计mtbf
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
tce.start_error_id_list as add_breakdown_id, -- 新增故障id集合
SUBSTR(rl.cur_hour,1,10) as d,
rl.project_code as pt


from phx_hour_robot_list_detail rl 
left join 
(select 
project_code,
robot_code,
to_date(order_create_time) as cur_date,
date_format(order_create_time,'yyyy-MM-dd HH:00:00') as cur_hour,
count(distinct order_no) as create_order_num,
count(distinct job_sn) as create_order_job_num
from phx_create_order_job_detail
group by project_code,robot_code,to_date(order_create_time),date_format(order_create_time,'yyyy-MM-dd HH:00:00')
)tor on tor.project_code=rl.project_code and tor.robot_code=rl.robot_code and tor.cur_date=rl.cur_date and tor.cur_hour=rl.cur_hour
left join phx_hour_robot_error_duration ted  on ted.project_code=rl.project_code and ted.robot_code=rl.robot_code and ted.cur_date=rl.cur_date and ted.cur_hour=rl.cur_hour
left join phx_robot_hour_theory_run_duration trd on trd.project_code=rl.project_code and trd.robot_code=rl.robot_code and trd.cur_date=rl.cur_date and trd.cur_hour=rl.cur_hour
left join 
(select 
project_code,
robot_code,
to_date(error_end_time) as cur_date, 
date_format(error_end_time,'yyyy-MM-dd HH:00:00') as cur_hour,
count(distinct id) as end_error_num,
sum(error_duration) as end_error_sum_duration
from phx_robot_error_detail
group by project_code,robot_code,to_date(error_end_time),date_format(error_end_time,'yyyy-MM-dd HH:00:00'))tee on tee.project_code=rl.project_code and tee.robot_code=rl.robot_code and tee.cur_date=rl.cur_date and tee.cur_hour=rl.cur_hour
left join 
(select 
project_code,
robot_code,
to_date(error_start_time) as cur_date, 
date_format(error_start_time,'yyyy-MM-dd HH:00:00') as cur_hour,
count(distinct id) as start_error_num,
concat_ws(',' , collect_set(cast(id as string))) as start_error_id_list 
from phx_robot_error_detail
group by project_code,robot_code,to_date(error_start_time),date_format(error_start_time,'yyyy-MM-dd HH:00:00'))tce on tce.project_code=rl.project_code and tce.robot_code=rl.robot_code and tce.cur_date=rl.cur_date and tce.cur_hour=rl.cur_hour
left join 
(select 
t.project_code,
t.robot_code,
t.robot_type_code,	   
t.robot_type_name,
t.cur_hour,
add_theory_time,
add_error_duration,
add_error_num,
d,
pt
FROM ${tmp_dbname}.tmp_phx_error_mtbf_add t
where t.d >= '${pre1_date}')tadd on tadd.project_code=rl.project_code and tadd.robot_code=rl.robot_code and tadd.cur_hour=rl.cur_hour


UNION ALL 
-- 分时分类型
SELECT 
'' as id, -- 主键
NULL as data_time, -- 统计小时
rl.project_code, -- 项目编码
rl.cur_hour as happen_time, -- 统计小时
'part' as type_class, -- 数据类型
rl.robot_type_code as amr_type,  -- 机器人类型编码
rl.robot_type_name as amr_type_des,    -- 机器人类型名称
null as amr_code,  -- 机器人编码
ted.error_id_list as breakdown_id, -- 故障集合
COALESCE(tor.create_order_num,0) as carry_order_num,  -- 搬运作业单胆量
COALESCE(tor.create_order_job_num,0) as carry_task_num,  -- 机器人任务量
nvl(trd.theory_run_duration,0) as theory_time, -- 理论运行时长
nvl(ted.error_duration,0) as error_duration, -- 故障时长
nvl(tee.end_error_sum_duration,0) as mttr_error_duration, -- mttr故障时长
nvl(tee.end_error_num,0) as mttr_error_num, -- mttr错误次数
cast(nvl((tadd.add_theory_time-tadd.add_error_duration)/tadd.add_error_num,tadd.add_theory_time) as decimal(10,2))  as add_mtbf, -- 累计mtbf
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
tce.start_error_id_list as add_breakdown_id, -- 新增故障id集合
SUBSTR(rl.cur_hour,1,10) as d,
rl.project_code as pt

from 
(select  
cur_date,
cur_hour,
project_code,
robot_type_code,
robot_type_name 
from phx_hour_robot_list_detail
group by cur_date,cur_hour,project_code,robot_type_code,robot_type_name) rl 
left join 
(select 
to_date(order_create_time) as cur_date,
date_format(order_create_time,'yyyy-MM-dd HH:00:00') as cur_hour,
project_code,
robot_type_code,
robot_type_name, 
count(distinct order_no) as create_order_num,
count(distinct job_sn) as create_order_job_num
from phx_create_order_job_detail
group by to_date(order_create_time),date_format(order_create_time,'yyyy-MM-dd HH:00:00'),project_code,robot_type_code,robot_type_name
)tor on tor.project_code=rl.project_code and tor.cur_date=rl.cur_date and tor.cur_hour=rl.cur_hour and tor.robot_type_code=rl.robot_type_code and tor.robot_type_name=rl.robot_type_name
left join 
(select cur_date,cur_hour,project_code,robot_type_code,robot_type_name,
concat_ws(',' , collect_set(cast(error_id_list as string))) as error_id_list,
sum(error_duration) as error_duration
from phx_hour_robot_error_duration
group by cur_date,cur_hour,project_code,robot_type_code,robot_type_name) ted  
on ted.project_code=rl.project_code and ted.cur_date=rl.cur_date and ted.cur_hour=rl.cur_hour and ted.robot_type_code=rl.robot_type_code and ted.robot_type_name=rl.robot_type_name
left join 
(select cur_date,cur_hour,project_code,robot_type_code,robot_type_name,
sum(theory_run_duration) as theory_run_duration
from phx_robot_hour_theory_run_duration
group by cur_date,cur_hour,project_code,robot_type_code,robot_type_name 
)trd on trd.project_code=rl.project_code and trd.cur_date=rl.cur_date and trd.cur_hour=rl.cur_hour and trd.robot_type_code=rl.robot_type_code and trd.robot_type_name=rl.robot_type_name
left join 
(select 
project_code,
robot_type_code,
robot_type_name,
to_date(error_end_time) as cur_date, 
date_format(error_end_time,'yyyy-MM-dd HH:00:00') as cur_hour,
count(distinct id) as end_error_num,
sum(error_duration) as end_error_sum_duration
from phx_robot_error_detail
group by project_code,robot_type_code,robot_type_name,to_date(error_end_time),date_format(error_end_time,'yyyy-MM-dd HH:00:00'))tee 
on tee.project_code=rl.project_code and tee.cur_date=rl.cur_date and tee.cur_hour=rl.cur_hour and tee.robot_type_code=rl.robot_type_code and tee.robot_type_name=rl.robot_type_name
left join 
(select 
project_code,
robot_type_code,
robot_type_name,
to_date(error_start_time) as cur_date, 
date_format(error_start_time,'yyyy-MM-dd HH:00:00') as cur_hour,
count(distinct id) as start_error_num,
concat_ws(',' , collect_set(cast(id as string))) as start_error_id_list 
from phx_robot_error_detail
group by project_code,robot_type_code,robot_type_name,
to_date(error_start_time),date_format(error_start_time,'yyyy-MM-dd HH:00:00'))tce 
on tce.project_code=rl.project_code and tce.cur_date=rl.cur_date and tce.cur_hour=rl.cur_hour and tce.robot_type_code=rl.robot_type_code and tce.robot_type_name=rl.robot_type_name
left join 
(select 
t.project_code,
t.cur_hour,
t.robot_type_code,	   
t.robot_type_name,
sum(add_theory_time) as add_theory_time,
sum(add_error_duration) as add_error_duration,
sum(add_error_num) as add_error_num
FROM ${tmp_dbname}.tmp_phx_error_mtbf_add t
where t.d >= '${pre1_date}'
group by t.project_code,t.cur_hour,t.robot_type_code,t.robot_type_name
)tadd on tadd.project_code=rl.project_code and tadd.cur_hour=rl.cur_hour and tadd.robot_type_code=rl.robot_type_code and tadd.robot_type_name=rl.robot_type_name 


UNION ALL 
-- 分时分项目
SELECT 
'' as id, -- 主键
NULL as data_time, -- 统计小时
rl.project_code, -- 项目编码
rl.cur_hour as happen_time, -- 统计小时
'all' as type_class, -- 数据类型
null as amr_type,  -- 机器人类型编码
null as amr_type_des,    -- 机器人类型名称
null as amr_code,  -- 机器人编码
ted.error_id_list as breakdown_id, -- 故障集合
COALESCE(tor.create_order_num,0) as carry_order_num,  -- 搬运作业单胆量
COALESCE(tor.create_order_job_num,0) as carry_task_num,  -- 机器人任务量
nvl(trd.theory_run_duration,0) as theory_time, -- 理论运行时长
nvl(ted.error_duration,0) as error_duration, -- 故障时长
nvl(tee.end_error_sum_duration,0) as mttr_error_duration, -- mttr故障时长
nvl(tee.end_error_num,0) as mttr_error_num, -- mttr错误次数
cast(nvl((tadd.add_theory_time-tadd.add_error_duration)/tadd.add_error_num,tadd.add_theory_time) as decimal(10,2))  as add_mtbf, -- 累计mtbf
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time,
tce.start_error_id_list as add_breakdown_id, -- 新增故障id集合
SUBSTR(rl.cur_hour,1,10) as d,
rl.project_code as pt

from 
(select  
cur_date,
cur_hour,
project_code
from phx_hour_robot_list_detail
group by cur_date,cur_hour,project_code) rl 
left join 
(select 
to_date(order_create_time) as cur_date,
date_format(order_create_time,'yyyy-MM-dd HH:00:00') as cur_hour,
project_code,
count(distinct order_no) as create_order_num,
count(distinct job_sn) as create_order_job_num
from phx_create_order_job_detail
group by to_date(order_create_time),date_format(order_create_time,'yyyy-MM-dd HH:00:00'),project_code
)tor on tor.project_code=rl.project_code and tor.cur_date=rl.cur_date and tor.cur_hour=rl.cur_hour 
left join 
(select cur_date,cur_hour,project_code,
concat_ws(',' , collect_set(cast(error_id_list as string))) as error_id_list,
sum(error_duration) as error_duration
from phx_hour_robot_error_duration
group by cur_date,cur_hour,project_code) ted  
on ted.project_code=rl.project_code and ted.cur_date=rl.cur_date and ted.cur_hour=rl.cur_hour 
left join 
(select cur_date,cur_hour,project_code,
sum(theory_run_duration) as theory_run_duration
from phx_robot_hour_theory_run_duration
group by cur_date,cur_hour,project_code
)trd on trd.project_code=rl.project_code and trd.cur_date=rl.cur_date and trd.cur_hour=rl.cur_hour 
left join 
(select 
project_code,
to_date(error_end_time) as cur_date, 
date_format(error_end_time,'yyyy-MM-dd HH:00:00') as cur_hour,
count(distinct id) as end_error_num,
sum(error_duration) as end_error_sum_duration
from phx_robot_error_detail
group by project_code,to_date(error_end_time),date_format(error_end_time,'yyyy-MM-dd HH:00:00'))tee 
on tee.project_code=rl.project_code and tee.cur_date=rl.cur_date and tee.cur_hour=rl.cur_hour 
left join 
(select 
project_code,
to_date(error_start_time) as cur_date, 
date_format(error_start_time,'yyyy-MM-dd HH:00:00') as cur_hour,
count(distinct id) as start_error_num,
concat_ws(',' , collect_set(cast(id as string))) as start_error_id_list 
from phx_robot_error_detail
group by project_code,to_date(error_start_time),date_format(error_start_time,'yyyy-MM-dd HH:00:00'))tce 
on tce.project_code=rl.project_code and tce.cur_date=rl.cur_date and tce.cur_hour=rl.cur_hour 
left join 
(select 
t.project_code,
t.cur_hour,
sum(add_theory_time) as add_theory_time,
sum(add_error_duration) as add_error_duration,
sum(add_error_num) as add_error_num
FROM ${tmp_dbname}.tmp_phx_error_mtbf_add t
where t.d >= '${pre1_date}'
group by t.project_code,t.cur_hour
)tadd on tadd.project_code=rl.project_code and tadd.cur_hour=rl.cur_hour

;
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql" && hive_concatenate ads ads_amr_breakdown ${pre1_date}