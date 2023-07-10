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
--set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
-- 单项目综合指标统计 ads_single_project_synthesis_target 

with picking_order_timeinfo as -- 拣选订单开始和结束时间
(
  SELECT tmp.project_code,tmp.station_code,tmp.created_date,tmp.last_updated_date,
         concat_ws(';' , collect_set(cast(CONCAT(tmp.created_time,',',tmp.done_date) as string))) as con_time,
         concat_ws(',',collect_list(cast(tmp.rn as string))) as con_rn
  FROM 
  (
    SELECT po.project_code,
           pjsc.station_code,
           TO_DATE(pjsc.created_time) as created_date,
           TO_DATE(po.done_date) as last_updated_date, 
           pjsc.created_time,
           po.done_date,
           row_number() over(partition by po.project_code,pjsc.station_code order by pjsc.created_time asc) as rn
    FROM ${dim_dbname}.dim_collection_project_record_ful c
    LEFT JOIN 
    (
      SELECT *
      FROM ${dwd_dbname}.dwd_picking_order_info po 
      WHERE TO_DATE(po.done_date) = '${pre1_date}' AND po.d >= DATE_ADD(CURRENT_DATE(), -7) 
    ) po
    ON c.project_code = po.project_code
    INNER JOIN
    (
      SELECT pj.project_code,
             pj.order_id, 
             pj.station_code,
             MIN(jsc.job_updated_time) as created_time
      FROM ${dwd_dbname}.dwd_g2p_picking_job_info pj
      INNER JOIN ${dwd_dbname}.dwd_g2p_job_state_change_info jsc 
      ON jsc.job_id = pj.job_id AND pj.d = jsc.d AND pj.pt = jsc.pt
      WHERE TO_DATE(pj.job_updated_time) = '${pre1_date}' AND pj.d >= DATE_ADD(CURRENT_DATE(), -7) AND jsc.job_state = 'WAITING_RESOURCE' AND pj.job_state = 'DONE'
      GROUP BY pj.project_code,pj.order_id,pj.station_code
    ) pjsc 
    ON pjsc.order_id = po.id AND pjsc.project_code = po.project_code
    WHERE c.project_product_type_code IN (1,2)
  )tmp
  GROUP BY tmp.project_code,tmp.station_code,tmp.created_date,tmp.last_updated_date
),
start_end_time as -- 去掉交叉之后得到的订单时间
(
  SELECT po.project_code,
         po.station_code,
         po.created_date as cur_date,
         split(tmp.con_time, ',')[0] as start_time,
         split(tmp.con_time, ',')[1] as end_time,
         row_number()over(PARTITION by po.project_code,po.station_code,po.created_date order by split(tmp.con_time, ',')[0] asc) rn
  FROM picking_order_timeinfo po
  LATERAL VIEW posexplode(split(dwd.time_union_set(po.con_time,',',';') ,';')) tmp as single_id_index,con_time
),
station_entry as -- 补充第一张订单（作为出站记录）到第一次进站的记录 + 每次出站到下次进站的记录（剔除掉最后一次出站的记录）
(
  SELECT t1.cur_date,t1.project_code,t1.station_code,t1.start_time as exit_time,s1.entry_time,'第一条进出站' as entry_type
  FROM start_end_time t1
  LEFT JOIN 
  (
    SELECT TO_DATE(se.d) as cur_date, -- 统计日期
           se.project_code, -- 项目编码
           se.station_code, -- 工作站编码
           se.entry_time, -- 进站时间
           se.exit_time, -- 出站时间
           row_number()over(PARTITION by se.d,se.project_code,se.station_code order by se.entry_time asc)rn
    FROM ${dwd_dbname}.dwd_station_station_entry_info se
    WHERE se.d = '${pre1_date}' AND se.idempotent_id LIKE '%Picking%' -- 拣选任务
  )s1
  ON t1.cur_date = s1.cur_date AND t1.project_code = s1.project_code AND t1.station_code = s1.station_code AND s1.rn = 1 
  WHERE t1.rn = 1 -- 补充第一条
    
  UNION ALL

  SELECT s1.cur_date,s1.project_code,s1.station_code,s1.exit_time,s2.entry_time,'正常进出站' as entry_type
  FROM
  (
    SELECT TO_DATE(se.d) as cur_date, -- 统计日期
           se.project_code, -- 项目编码
           se.station_code, -- 工作站编码
           se.entry_time, -- 进站时间
           se.exit_time, -- 出站时间
           row_number()over(PARTITION by se.d,se.project_code,se.station_code order by se.entry_time asc)rn
    FROM ${dwd_dbname}.dwd_station_station_entry_info se
    WHERE se.d = '${pre1_date}' AND se.idempotent_id LIKE '%Picking%' -- 拣选任务
  )s1
  LEFT JOIN 
  (
    SELECT se.d as cur_date, -- 统计日期
           se.project_code, -- 项目编码
           se.station_code, -- 工作站编码
           se.entry_time, -- 进站时间
           se.exit_time, -- 出站时间
           row_number()over(PARTITION by se.d,se.project_code,se.station_code order by se.entry_time asc)rn
    FROM ${dwd_dbname}.dwd_station_station_entry_info se
    WHERE se.d = '${pre1_date}' AND se.idempotent_id LIKE '%Picking%' -- 拣选任务
  )s2
  ON s1.cur_date = s2.cur_date AND s1.project_code = s2.project_code AND s1.station_code = s2.station_code AND s1.rn = s2.rn - 1
  WHERE s2.entry_time is not NULL -- 剔除最后一条出站记录
),
station_login as -- 工作站登录上下线记录（跨天补零）
(
  SELECT l.project_code,
         l.station_code,
         date_add(TO_DATE(l.login_time),b.pos) as login_date,
         IF(b.pos = 0,l.login_time,DATE_FORMAT(date_add(TO_DATE(l.login_time),b.pos),'yyyy-MM-dd 00:00:00.000')) as login_time,
         case when b.pos = 0 and datediff(nvl(l.logout_time,DATE_FORMAT(l.login_time,'yyyy-MM-dd 23:59:59.999')),l.login_time) = b.pos then nvl(l.logout_time,DATE_FORMAT(date_add(TO_DATE(l.login_time),(b.pos + 1)),'yyyy-MM-dd 00:00:00.000'))
              when datediff(nvl(l.logout_time,DATE_FORMAT(l.login_time,'yyyy-MM-dd 23:59:59.999')),l.login_time) != b.pos then DATE_FORMAT(date_add(TO_DATE(l.login_time),(b.pos + 1)),'yyyy-MM-dd 00:00:00.000')
              when b.pos != 0 and datediff(nvl(l.logout_time,DATE_FORMAT(l.login_time,'yyyy-MM-dd 23:59:59.999')),l.login_time) = b.pos then nvl(l.logout_time,DATE_FORMAT(date_add(TO_DATE(l.login_time),(b.pos + 1)),'yyyy-MM-dd 00:00:00.000')) end as logout_time,
         row_number()over(PARTITION by l.project_code,l.station_code,date_add(TO_DATE(l.login_time),b.pos) order by IF(b.pos = 0,l.login_time,DATE_FORMAT(date_add(TO_DATE(l.login_time),b.pos),'yyyy-MM-dd 00:00:00.000')) asc) as rn
  FROM ${dwd_dbname}.dwd_station_station_login_info_di l
  lateral view posexplode(split(repeat('o',datediff(nvl(l.logout_time,DATE_FORMAT(DATE_ADD(CURRENT_DATE(), -1),'yyyy-MM-dd 23:59:59.999')),l.login_time)),'o')) b
  WHERE l.d >= DATE_ADD(CURRENT_DATE(), -7) AND l.biz_type IN ('PICKING_ONLINE_w2P_B2P', 'PICKING_ONLINE_G2P_W2P', 'PICKING_ONLINE_G2P_B2P')
),
eff_station_entry as -- 第一张订单到第一次进站的记录 + 正常进站出站之间的记录
(
  SELECT t1.cur_date,t1.project_code,t1.station_code,t1.start_time as entry_time,s1.entry_time as exit_time,'第一条进出站' as entry_type
  FROM start_end_time t1
  LEFT JOIN 
  (
    SELECT TO_DATE(se.d) as cur_date, -- 统计日期
           se.project_code, -- 项目编码
           se.station_code, -- 工作站编码
           se.entry_time, -- 进站时间
           se.exit_time, -- 出站时间
           row_number()over(PARTITION by se.d,se.project_code,se.station_code order by se.entry_time asc)rn
    FROM ${dwd_dbname}.dwd_station_station_entry_info se
    WHERE se.d = '${pre1_date}' AND se.idempotent_id LIKE '%Picking%' -- 拣选任务
  )s1
  ON t1.cur_date = s1.cur_date AND t1.project_code = s1.project_code AND t1.station_code = s1.station_code AND s1.rn = 1 
  WHERE t1.rn = 1 -- 补充第一条
  
  UNION ALL

  SELECT s1.cur_date,s1.project_code,s1.station_code,s1.entry_time,s1.exit_time,'正常进出站' as entry_type
  FROM
  (
    SELECT TO_DATE(se.d) as cur_date, -- 统计日期
           se.project_code, -- 项目编码
           se.station_code, -- 工作站编码
           se.entry_time, -- 进站时间
           se.exit_time, -- 出站时间
           row_number()over(PARTITION by se.d,se.project_code,se.station_code order by se.entry_time asc)rn
    FROM ${dwd_dbname}.dwd_station_station_entry_info se
    WHERE se.d = '${pre1_date}' AND se.idempotent_id LIKE '%Picking%' -- 拣选任务
  )s1
),
into_station_times as -- 进站次数以及在站时长
(
  SELECT se.d as cur_date, -- 统计日期
         se.project_code, -- 项目编码
         se.station_code, -- 工作站编码
         COUNT(DISTINCT pj.bucket_move_job_id) as into_station_times, -- 进站次数
         SUM(unix_timestamp(se.exit_time) - unix_timestamp(se.entry_time)) as instation_duration -- 在站时长
  FROM ${dwd_dbname}.dwd_station_station_entry_info se
  LEFT JOIN 
  (
    SELECT *
    FROM ${dwd_dbname}.dwd_g2p_picking_job_info pj
    WHERE pj.d = '${pre1_date}' AND pj.job_state = 'DONE'
  )pj
  ON pj.job_id = se.idempotent_id AND pj.pt = se.pt
  WHERE se.d = '${pre1_date}' AND se.idempotent_id LIKE '%Picking%' -- 拣选任务
  GROUP BY se.d,se.project_code,se.station_code
),
into_station_interval as -- 人等车时间（即货架进站间隔）：订单范围的出进站内剔除离线数据
(
  SELECT se.cur_date,
         se.project_code,
         se.station_code,
         SUM(
         unix_timestamp(case when se.exit_time >= sl.login_time AND se.entry_time <= sl.logout_time then se.entry_time
                             when se.entry_time >= sl.login_time AND se.entry_time <= sl.logout_time then se.entry_time
                             when se.exit_time >= sl.login_time AND se.exit_time <= sl.logout_time then sl.logout_time end)
         -
         unix_timestamp(case when se.exit_time >= sl.login_time AND se.entry_time <= sl.logout_time then se.exit_time
                             when se.entry_time >= sl.login_time AND se.entry_time <= sl.logout_time then sl.login_time
                             when se.exit_time >= sl.login_time AND se.exit_time <= sl.logout_time then se.exit_time end)
         ) as into_station_interval  
  FROM station_entry se
  LEFT JOIN station_login sl
  ON se.project_code = sl.project_code AND se.station_code = sl.station_code AND se.cur_date = sl.login_date AND ((se.exit_time >= sl.login_time AND se.entry_time <= sl.logout_time) OR (se.entry_time >= sl.login_time AND se.entry_time <= sl.logout_time) OR (se.exit_time >= sl.login_time AND se.exit_time <= sl.logout_time))
  GROUP BY se.cur_date,se.project_code,se.station_code
),
eff_duration as -- 工作站实际工作时间（进出站内时间 + 人等车时间）
(
  SELECT tmp.cur_date,
         tmp.project_code,
         tmp.station_code,
         nvl(tmp.eff_duration,0) + nvl(si.into_station_interval,0) as eff_duration
  FROM  
  -- 进站工作时间
  (
    SELECT se.cur_date,
           se.project_code,
           se.station_code,
           SUM(
           unix_timestamp(case when se.entry_time >= sl.login_time AND se.exit_time <= sl.logout_time then se.exit_time
                               when se.exit_time >= sl.login_time AND se.exit_time <= sl.logout_time then se.exit_time
                               when se.entry_time >= sl.login_time AND se.entry_time <= sl.logout_time then sl.logout_time end)
           -
           unix_timestamp(case when se.entry_time >= sl.login_time AND se.exit_time <= sl.logout_time then se.entry_time
                               when se.exit_time >= sl.login_time AND se.exit_time <= sl.logout_time then sl.login_time
                               when se.entry_time >= sl.login_time AND se.entry_time <= sl.logout_time then se.entry_time end)
           ) as eff_duration
    FROM eff_station_entry se
    LEFT JOIN station_login sl
    ON se.project_code = sl.project_code AND se.station_code = sl.station_code AND se.cur_date = sl.login_date AND ((se.entry_time >= sl.login_time AND se.exit_time <= sl.logout_time) OR (se.exit_time >= sl.login_time AND se.exit_time <= sl.logout_time) OR (se.entry_time >= sl.login_time AND se.entry_time <= sl.logout_time))
    GROUP BY se.cur_date,se.project_code,se.station_code
  )tmp
  LEFT JOIN into_station_interval si
  ON tmp.project_code = si.project_code AND tmp.cur_date = si.cur_date AND tmp.station_code = si.station_code
),
order_detail as -- 订单数量
(
  SELECT pj.d as cur_date,
         pj.project_code,
         pj.station_code,
         COUNT(DISTINCT pj.order_id) as picking_order_num,
         COUNT(DISTINCT pj.picking_work_detail_id) as order_linenum,
         SUM(pj.actual_quantity) as picking_quantity
  FROM ${dwd_dbname}.dwd_g2p_picking_job_info pj
  WHERE pj.d = '${pre1_date}' AND pj.job_state = 'DONE'
  GROUP BY pj.d,pj.project_code,pj.station_code
)

  

INSERT overwrite table ${ads_dbname}.ads_single_project_synthesis_target
SELECT '' as id, -- 主键
       t1.days as cur_date, -- 统计日期
       t2.project_code, -- 项目编码
       nvl(t3.bucket_num_total,0) as bucket_num_total, -- 货架总数
       nvl(t3.bucket_num_actual,0) as bucket_num_actual, -- 货架占用量（剔除零库存）
       nvl(t3.bucket_using_rate,0) as bucket_using_rate, -- 货架占用率（剔除零库存）
       nvl(t3.slot_num_total,0) as slot_num_total, -- 货位总数
       nvl(t3.slot_num_actual,0) as slot_num_actual, -- 货位占用量（剔除零库存）
       nvl(t3.slot_using_rate,0) as slot_using_rate, -- 货位占用率（剔除零库存）
       nvl(t4.sku_num_total,0) as sku_num_total, -- sku总数（个）
       nvl(t3.sku_num_actual,0) as sku_num_actual, -- sku在库数量（个）
       nvl(t3.quantity_total,0) as quantity_total, -- 库存总数（件）
       nvl(t3.inventory_depth,0) as inventory_depth, -- 平均库存深度（件）
       nvl(t5.station_num,0) as station_num, -- 开启工作站数量
       nvl(t6.station_num_total,0) as station_num_total, -- 工作站总数量
       nvl(CAST(nvl(t5.station_num,0) / nvl(t6.station_num_total,0) as decimal(10,2)),0) as station_free_rate, -- 工作站使用率
       nvl(CAST(nvl(t19.picking_orderline_efficiency,0) / nvl(t5.station_num,0) as decimal(10,2)),0) as picking_orderline_efficiency, -- 工作站拣选效率（订单行/h/工作站）
       nvl(CAST(nvl(t19.picking_quantity_efficiency,0) / nvl(t5.station_num,0) as decimal(10,2)),0) as picking_quantity_efficiency, -- 工作站拣选效率（件/h/工作站）
       nvl(CAST(nvl(t21.once_into_station_times,0) / nvl(t5.station_num,0) as decimal(10,2)),0) as once_into_station_times,-- 工作站拣选效率（入站次数/h/工作站）
       nvl(t22.once_instation_duration,0) as once_instation_duration, -- 单次进站平均在站时长（s）
       null as once_win_open_times, -- 单次进站平均弹窗次数（次）
       nvl(t8.once_picking_sku_num,0) as once_picking_sku, -- 单次进站平均命中sku数
       nvl(t8.once_picking_order,0) as once_picking_order, -- 单次进站平均命中订单数
       nvl(t8.once_picking_orderline,0) as once_picking_orderline, -- 单次进站平均命中行数（行）
       nvl(t8.once_picking_quantity,0) as once_picking_quantity, -- 单次进站拣选订单件数（件）
       nvl(t12.once_into_station_interval,0) as once_station_interval, -- 单次进站平均入站间隔（人等车）（s）
       nvl(t13.order_num,0) as order_num, -- 现场工单数量
       null as order_rate, -- 现场工单比例
       nvl(t14.sys_order_num,0) as sys_order_num, -- 现场系统工单数量
       null as sys_order_rate, -- 现场系统工单比例
       nvl(t15.trans_dev_order_num,0) as trans_dev_order_num, -- 转研发工单数量
       null as trans_dev_order_rate, -- 转研发工单比例
       nvl(t16.dev_trouble_num,0) as dev_trouble_num, -- 研发缺陷工单数量
       null as dev_trouble_rate, -- 研发缺陷工单比例	
       nvl(t17.total_fix_time,0) as total_fix_time, -- 累计维修次数
       nvl(t17.total_fix_num,0) as total_fix_num, -- 累计维修车辆数
       nvl(t17.avg_fix_duration,0) as avg_fix_duration, -- 平均维修时长
       null as total, -- 自恢复次数总数
       null as fail_time, -- 自恢复失败次数
       null as succes_time, -- 自恢复成功次数
       null as avg_recover_duration, -- 平均自恢复时长/s
       nvl(t18.dead_lock_num,0) as dead_lock_num, -- 死锁次数
       null as avg_duration, -- 平均解死锁时长
       null as manul_reduce, -- 人工介入恢复次数
       null as traffic_jam_num, -- 拥堵次数
       null as avgtraffic_duration, -- 平均拥堵时长
       null as car_num, -- 拥堵影响车辆数
       nvl(t23.agv_num,0) as agv_num, -- 机器人数量
       nvl(t24.charger_num,0) as charger_num, -- 充电桩数量
       nvl(t25.warehouse_area,0) as warehouse_area, -- 地图面积
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
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
-- 库存统计
-- 货架数、货位数
LEFT JOIN 
(
  SELECT bb.d as cur_date, -- 统计日期
         bb.project_code, -- 项目编码
         COUNT(DISTINCT bb.bucket_code) as bucket_num_total, -- 货架总数
         COUNT(DISTINCT li.bucket_code) as bucket_num_actual, -- 货架占用量
         CAST(COUNT(DISTINCT li.bucket_code) / COUNT(DISTINCT bb.bucket_code) as decimal(10,2)) as bucket_using_rate, -- 货架占用率
         COUNT(DISTINCT sb.slot_code) as slot_num_total, -- 货位总数
         COUNT(DISTINCT li.bucket_slot_code) as slot_num_actual, -- 货位占用量
         CAST(COUNT(DISTINCT li.bucket_slot_code) / COUNT(DISTINCT sb.slot_code) as decimal(10,2)) as slot_using_rate, -- 货位占用率
         SUM(li.quantity) as quantity_total, -- 库存总量
         CAST(SUM(li.quantity) / COUNT(DISTINCT li.bucket_slot_code) as decimal(10,2)) as inventory_depth, -- 库存深度
         COUNT(DISTINCT li.sku_id) as sku_num_actual -- sku在库数量
  FROM ${dwd_dbname}.dwd_basic_bucket_base_info_df bb
  LEFT JOIN 
  (
    SELECT sb.project_code,
           sb.slot_code,
           sb.bucket_id
    FROM ${dwd_dbname}.dwd_basic_slot_base_info_df sb
    WHERE sb.d = '${pre1_date}' AND sb.slot_state = 'effective'
  )sb
  ON bb.project_code = sb.project_code AND bb.id = sb.bucket_id 
  LEFT JOIN 
  (
    SELECT li.project_code,
           li.bucket_code,
           li.bucket_slot_code,
           li.sku_id,
           li.quantity
    FROM ${dwd_dbname}.dwd_inventory_level3_inventory_info_df li
    WHERE li.d = '${pre1_date}' AND li.quantity != 0 -- 剔除零库存
  )li
  ON bb.project_code = li.project_code AND bb.bucket_code = li.bucket_code AND sb.slot_code = li.bucket_slot_code 
  WHERE bb.d = '${pre1_date}' AND bb.bucket_state = 'effective' AND ((bb.pt = 'A51118' AND bb.bucket_type_id = 57) OR (bb.pt = 'A51149' AND bb.bucket_type_id IN (33,36,37)) OR (bb.pt = 'A51264' AND bb.bucket_type_id = 1) OR (bb.pt = 'A51203' AND bb.bucket_type_id IN (124,119)) OR (bb.pt = 'C35052' AND bb.bucket_type_id IN (122,128))) -- 正常货架
  GROUP BY bb.d,bb.project_code
)t3
ON t1.days = t3.cur_date AND t2.project_code = t3.project_code
-- sku总数  
LEFT JOIN 
(
  SELECT b.d as cur_date, -- 统计日期
         b.project_code, -- 项目编码
         COUNT(DISTINCT b.id) as sku_num_total -- sku总数（个）
  FROM ${dwd_dbname}.dwd_wes_basic_sku_info_df b
  WHERE b.d = '${pre1_date}' AND b.sku_state = 'effective' -- 状态有效
  GROUP BY b.d,b.project_code
)t4
ON t1.days = t4.cur_date AND t2.project_code = t4.project_code
-- 开启工作站数量
LEFT JOIN 
(
  SELECT s.d as cur_date, -- 统计日期
         s.project_code, -- 项目编码
         COUNT(DISTINCT station_code) as station_num -- 开启工作站数量
  FROM ${dwd_dbname}.dwd_station_station_entry_info s
  WHERE s.d = '${pre1_date}' AND s.idempotent_id LIKE '%Picking%' -- 拣选任务
  GROUP BY s.d,s.project_code
)t5
ON t1.days = t5.cur_date AND t2.project_code = t5.project_code 
-- 工作站总数量
LEFT JOIN
(
  SELECT '${pre1_date}' as cur_date,
         b.project_code,
         b.basic_units_qyt as station_num_total
  FROM ${tmp_dbname}.tmp_basic_live_data_offline_info b
  WHERE b.basic_code = 0003
)t6
ON t1.days = t6.cur_date AND t2.project_code = t6.project_code 
-- 拣选订单行、拣选件数、sku数、订单数
LEFT JOIN 
(
  SELECT tmp.cur_date,
         tmp.project_code,
         CAST(SUM(tmp.picking_sku_num / st.into_station_times) / COUNT(DISTINCT tmp.station_code) as decimal(10,2)) as once_picking_sku_num,
         CAST(SUM(tmp.picking_order_num / st.into_station_times) / COUNT(DISTINCT tmp.station_code) as decimal(10,2)) as once_picking_order,
         CAST(SUM(tmp.picking_orderline_num / st.into_station_times) / COUNT(DISTINCT tmp.station_code) as decimal(10,2)) as once_picking_orderline,
         CAST(SUM(tmp.picking_quantity_num / st.into_station_times) / COUNT(DISTINCT tmp.station_code) as decimal(10,2)) as once_picking_quantity
  FROM
  (
    SELECT pj.cur_date,
           pj.project_code,
           pj.station_code,
           SUM(pj.picking_sku_num) as picking_sku_num,
           SUM(pj.picking_order_num) as picking_order_num,
           SUM(pj.picking_orderline_num) as picking_orderline_num,
           SUM(pj.picking_quantity_num) as picking_quantity_num
    FROM
    (
      SELECT pj.d as cur_date,
             pj.project_code,
             pj.station_code,
             pj.bucket_move_job_id,
             COUNT(DISTINCT pj.sku_id) as picking_sku_num,
             COUNT(DISTINCT pj.order_id) as picking_order_num,
             COUNT(DISTINCT pj.picking_work_detail_id) as picking_orderline_num,
             SUM(pj.actual_quantity) as picking_quantity_num
      FROM ${dwd_dbname}.dwd_g2p_picking_job_info pj
      WHERE pj.d = '${pre1_date}' AND pj.job_state = 'DONE'
      GROUP BY pj.d,pj.project_code,pj.station_code,pj.bucket_move_job_id
    )pj
    GROUP BY pj.cur_date,pj.project_code,pj.station_code
  )tmp
  LEFT JOIN into_station_times st
  ON tmp.cur_date = st.cur_date AND tmp.project_code = st.project_code AND tmp.station_code = st.station_code
  GROUP BY tmp.cur_date,tmp.project_code
)t8
ON t1.days = t8.cur_date AND t2.project_code = t8.project_code
-- 平均等车
LEFT JOIN 
(
  SELECT tmp.cur_date,
         tmp.project_code,
         CAST(SUM(nvl(tmp.into_station_interval,0) / nvl(se.into_station_times,0)) / COUNT(DISTINCT se.station_code) as decimal(10,2)) as once_into_station_interval
  FROM into_station_interval tmp
  LEFT JOIN into_station_times se
  ON tmp.project_code = se.project_code AND tmp.cur_date = se.cur_date AND tmp.station_code = se.station_code
  GROUP BY tmp.cur_date,tmp.project_code
)t12
ON t1.days = t12.cur_date AND t2.project_code = t12.project_code
-- 现场工单
LEFT JOIN 
(
  SELECT w.d as cur_date,
         w.project_code,
         COUNT(DISTINCT w.ticket_id) as order_num
  FROM ${dwd_dbname}.dwd_ones_work_order_info_df w
  WHERE w.d = '${pre1_date}' AND w.work_order_status != '已驳回' 
  GROUP BY w.d,w.project_code
)t13
ON t1.days = t13.cur_date AND t2.project_code = t13.project_code 
-- 现场系统工单
LEFT JOIN 
(
  SELECT w.d as cur_date,
         w.project_code,
         COUNT(DISTINCT w.ticket_id) as sys_order_num
  FROM ${dwd_dbname}.dwd_ones_work_order_info_df w
  WHERE w.d = '${pre1_date}' AND w.work_order_status != '已驳回' AND w.first_category = '系统'
  GROUP BY w.d,w.project_code
)t14
ON t1.days = t14.cur_date AND t2.project_code = t14.project_code 
-- 转研发工单
LEFT JOIN 
(
  SELECT '${pre1_date}' as cur_date,
         w.project_code,
         COUNT(DISTINCT t.uuid) as trans_dev_order_num
  FROM ${dwd_dbname}.dwd_ones_work_order_info_df w
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_field_value_info_ful v
  ON v.field_value = w.ticket_id AND v.field_uuid = 'S993wZTA'
  LEFT JOIN 
  (
    SELECT *
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    WHERE t.project_classify_name = '工单问题汇总' AND t.issue_type_cname = '任务'
  )t
  ON t.uuid = v.task_uuid 
  WHERE w.d = '${pre1_date}' AND w.work_order_status != '已驳回' AND w.first_category = '系统'
  GROUP BY w.project_code
)t15
ON t1.days = t15.cur_date AND t2.project_code = t15.project_code
-- 研发缺陷工单
LEFT JOIN 
(
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         w.project_code,
         COUNT(DISTINCT t.uuid) as dev_trouble_num
  FROM ${dwd_dbname}.dwd_ones_work_order_info_df w
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_field_value_info_ful v
  ON v.field_value = w.ticket_id AND v.field_uuid = 'S993wZTA'
  LEFT JOIN 
  (
    SELECT *
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    WHERE t.project_classify_name = '工单问题汇总' AND t.issue_type_cname = '缺陷'
  )t
  ON t.uuid = v.task_uuid 
  WHERE w.d = '${pre1_date}' AND w.work_order_status != '已驳回' AND w.first_category = '系统'
  GROUP BY w.project_code
)t16
ON t1.days = t16.cur_date AND t2.project_code = t16.project_code 
-- 累计维修次数
LEFT JOIN 
(
  SELECT DATE_ADD(CURRENT_DATE(), -1) as cur_date,
         i.project_code,
         COUNT(i.project_code) as total_fix_time, -- 累计维修次数
         COUNT(DISTINCT i.agv_uuid) as total_fix_num, -- 累计维修车辆数
         CAST(SUM(unix_timestamp(i.inspection_finish_time) - unix_timestamp(inspection_start_time)) / COUNT(i.project_code) as decimal(10,2)) as avg_fix_duration
  FROM ${tmp_dbname}.tmp_basic_agv_inspection_data_offline_info i
  GROUP BY i.project_code
)t17
ON t1.days = t17.cur_date AND t2.project_code = t17.project_code 
-- 死锁次数
LEFT JOIN 
(
  SELECT j.d as cur_date,
         j.project_code,
         COUNT(DISTINCT j.job_id) as dead_lock_num
  FROM ${dwd_dbname}.dwd_rcs_agv_job_history_info_di j
  WHERE j.pt = '${pre1_date}' AND j.job_type = 'DEAD_LOCK_MOVE_JOB' AND job_state = 'JOB_COMPLETED'
  GROUP BY j.d,j.project_code
)t18
ON t1.days = t18.cur_date AND t2.project_code = t18.project_code 
-- 工作站拣选效率
LEFT JOIN 
(
  SELECT t1.project_code,
         t1.cur_date,
         CAST(SUM(t2.order_linenum / (t1.eff_duration / 3600)) as decimal(10,2)) as picking_orderline_efficiency,
         CAST(SUM(t2.picking_quantity / (t1.eff_duration / 3600)) as decimal(10,2)) as picking_quantity_efficiency
  FROM eff_duration t1
  LEFT JOIN order_detail t2
  ON t1.project_code = t2.project_code AND t1.station_code = t2.station_code AND t1.cur_date = t2.cur_date
  GROUP BY t1.project_code,t1.cur_date
)t19
ON t1.days = t19.cur_date AND t2.project_code = t19.project_code
-- 平均入站次数
LEFT JOIN
( 
  SELECT se.cur_date,
         se.project_code,
         CAST(SUM(se.into_station_times / (t1.eff_duration / 3600)) as decimal(10,2)) as once_into_station_times
  FROM into_station_times se
  LEFT JOIN eff_duration t1
  ON t1.project_code = se.project_code AND t1.cur_date = se.cur_date AND t1.station_code = se.station_code
  GROUP by se.cur_date,se.project_code
)t21
ON t1.days = t21.cur_date AND t2.project_code = t21.project_code 
-- 平均在站时长
LEFT JOIN 
(
  SELECT se.cur_date,
         se.project_code,
         CAST(SUM(se.instation_duration / se.into_station_times) / COUNT(DISTINCT se.station_code) as decimal(10,2)) as once_instation_duration
  FROM into_station_times se
  GROUP by se.cur_date,se.project_code
)t22
ON t1.days = t22.cur_date AND t2.project_code = t22.project_code
-- 机器人数量
LEFT JOIN 
(
  SELECT '${pre1_date}' as cur_date,
         b.project_code,
         IF(COUNT(DISTINCT a.agv_code) = 0,COUNT(DISTINCT b.agv_code),COUNT(DISTINCT a.agv_code)) as agv_num
  FROM ${dwd_dbname}.dwd_rcs_basic_agv_info b
  LEFT JOIN ${tmp_dbname}.tmp_basic_agv_data_offline_info a
  ON b.project_code = b.project_code AND a.agv_code = b.agv_code AND b.d = '${pre1_date}' AND a.active_status = '运营中'
  WHERE b.d = '${pre1_date}'
  GROUP BY b.project_code
)t23
ON t1.days = t23.cur_date AND t2.project_code = t23.project_code
-- 充电桩数量
LEFT JOIN 
(
  SELECT '${pre1_date}' as cur_date,
         b.project_code,
         COUNT(DISTINCT b.charger_code) as charger_num
  FROM ${dwd_dbname}.dwd_rcs_basic_charger_info_df b
  WHERE b.d = '${pre1_date}'
  GROUP BY b.project_code
)t24
ON t1.days = t24.cur_date AND t2.project_code = t24.project_code
-- 地图面积
LEFT JOIN 
(
  SELECT '${pre1_date}' as cur_date,
         project_code,
         basic_units_qyt as warehouse_area
  FROM ${tmp_dbname}.tmp_basic_live_data_offline_info
  WHERE basic_code = 0001
)t25
ON t1.days = t25.cur_date AND t2.project_code = t25.project_code;
-----------------------------------------------------------------------------------------------------------------------------00

"

printf "##############################################start-executor-sql####################################################################\n$sql\n##############################################end-executor-sql####################################################################"


$hive -e "$sql"



echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "


#!/bin/bash



#设置datax参数
datax=/opt/module/datax/bin/datax.py
json_dir=/opt/module/datax/job/
json_name=(ads_project_view_stock_count.json ads_project_view_pick_efficiency.json ads_project_view_trans_sys_order_count.json ads_project_view_amr_repair.json ads_project_view_manual_recovery_count.json ads_project_view_dead_lock_num.json ads_project_view_traffic_jam_num.json ads_project_view_project_equipment.json)

#ssh -tt hadoop@003.bg.qkt <<effo
for json in ${json_name[@]}; do $datax  -p "-Dpre1_date='${pre1_date}'" $json_dir$json;done
#exit
#effo