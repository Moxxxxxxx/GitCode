#!/bin/bash
hive=/opt/module/hive-3.1.2/bin/hive
dwd_dbname=dwd
ads_dbname=ads
pre_dbname=pre
dim_dbname=dim
tmp_dbname=tmp

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
--ads_project_healthy_info    --项目健康度

--------------项目编码、项目名称、项目阶段、近7日项目活跃度、近7日新增工单数量、超7日未关闭工单数量、近7日小车故障数、近7日小车MTBF、近7日服务宕机总时长、近7日系统故障次数、近7日升级次数-------------------
INSERT overwrite table ${ads_dbname}.ads_project_healthy_info
SELECT '' as id,
       tmp.cur_date,
       tmp.date_zone,
       tmp.project_code,
       tmp.project_name,
       tmp.product_name,
       tmp.project_ft,
       tmp.project_area,
       tmp.project_priority,
       tmp.project_operation_state,
       tmp.project_activation_sevendays,
       CASE WHEN tmp.red_risk_level >= 2 THEN 3
            WHEN tmp.red_risk_level = 1 or (tmp.red_risk_level = 0 AND tmp.yellow_risk_level >= 2) THEN 2
            WHEN tmp.yellow_risk_level = 1 or (tmp.yellow_risk_level = 0 AND tmp.white_risk_level >= 0) THEN 1 END as risk_level,
       tmp.workorder_num_sevendays,
       tmp.unsolve_workorder_num_sevendays,
       tmp.agv_breakdown_num_sevendays,
       tmp.agv_MTBF_sevendays,
       tmp.service_downtime_sevendays,
       tmp.system_breakdown_num_sevendays,
       tmp.upgrade_num_sevendays,    
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM            
(
  SELECT tt.cur_date,
         tt.date_zone,
         tt.project_code,
         tt.project_name,
         tt.product_name,
         tt.project_ft,
         tt.project_area,
         tt.project_priority,
         tt.project_operation_state,
         tt.project_activation_sevendays,
         SUM(IF(tt.workorder_num_sevendays >= 10,1,0) + IF(tt.unsolve_workorder_num_sevendays >= 3,1,0) + IF(IF(tt.agv_breakdown_num_sevendays is null,0,tt.agv_breakdown_num_sevendays) >= 0.3,1,0) + IF(IF(tt.agv_MTBF_sevendays is null,100,tt.agv_MTBF_sevendays) < 72,1,0) + IF(IF(tt.service_downtime_sevendays is null,0,tt.service_downtime_sevendays) >= 60,1,0) + IF(IF(tt.system_breakdown_num_sevendays is null,0,tt.system_breakdown_num_sevendays) >= 3,1,0) + IF(tt.upgrade_num_sevendays >= 3,1,0)) as red_risk_level, --红色风险等级
         SUM(IF(tt.workorder_num_sevendays >= 5 AND tt.workorder_num_sevendays < 10,1,0) + IF(tt.unsolve_workorder_num_sevendays >= 1 AND tt.unsolve_workorder_num_sevendays < 3,1,0) + IF(IF(tt.agv_breakdown_num_sevendays is null,0,tt.agv_breakdown_num_sevendays) >= 0.1 AND IF(tt.agv_breakdown_num_sevendays is null,0,tt.agv_breakdown_num_sevendays) < 0.3,1,0) + IF(IF(tt.agv_MTBF_sevendays is null,100,tt.agv_MTBF_sevendays) >= 72 AND IF(tt.agv_MTBF_sevendays is null,100,tt.agv_MTBF_sevendays) < 100,1,0) + IF(IF(tt.service_downtime_sevendays is null,0,tt.service_downtime_sevendays) >= 10 AND IF(tt.service_downtime_sevendays is null,0,tt.service_downtime_sevendays) < 60,1,0) + IF(IF(tt.system_breakdown_num_sevendays is null,0,tt.system_breakdown_num_sevendays) >= 1 AND IF(tt.system_breakdown_num_sevendays is null,0,tt.system_breakdown_num_sevendays) < 3,1,0) + IF(tt.upgrade_num_sevendays >= 2 AND tt.upgrade_num_sevendays < 3,1,0)) as yellow_risk_level, --黄色风险等级
         SUM(IF(tt.workorder_num_sevendays < 5,1,0) + IF(tt.unsolve_workorder_num_sevendays < 1,1,0) + IF(IF(tt.agv_breakdown_num_sevendays is null,0,tt.agv_breakdown_num_sevendays) < 0.1,1,0) + IF(IF(tt.agv_MTBF_sevendays is null,100,tt.agv_MTBF_sevendays) >= 100,1,0) + IF(IF(tt.service_downtime_sevendays is null,0,tt.service_downtime_sevendays) < 10,1,0) + IF(IF(tt.system_breakdown_num_sevendays is null,0,tt.system_breakdown_num_sevendays) < 1,1,0) + IF(tt.upgrade_num_sevendays < 2,1,0)) as white_risk_level, --白色风险等级
         SUM(tt.workorder_num_sevendays) as workorder_num_sevendays,
         SUM(tt.unsolve_workorder_num_sevendays) as unsolve_workorder_num_sevendays,
         SUM(tt.agv_breakdown_num_sevendays) as agv_breakdown_num_sevendays,
         SUM(tt.agv_MTBF_sevendays) as agv_MTBF_sevendays,
         SUM(tt.service_downtime_sevendays) as service_downtime_sevendays,
         SUM(tt.system_breakdown_num_sevendays) as system_breakdown_num_sevendays,
         SUM(tt.upgrade_num_sevendays) as upgrade_num_sevendays
  FROM 
  (
    SELECT '${pre1_date}' as cur_date, --统计日期
           CONCAT(DATE_ADD('${pre1_date}', -6),'~','${pre1_date}') as date_zone,
           p.project_code, --项目编码
           p.project_name, --项目名称
           nvl(p.project_product_name,'未知') as product_name, --项目产品名称
           nvl(p.project_ft,'未知') as project_ft, --项目所属ft
           nvl(p.project_area,'未知') as project_area, -- 项目区域
           nvl(p.project_priority,'未知') as project_priority, -- 归属FT
           nvl(p.project_dispaly_state,'未知') as project_operation_state, --项目阶段
           CASE WHEN CAST(t4.picking_job_num/t5.agv_num AS DECIMAL(10,0)) > 20 THEN '非常活跃'
                WHEN CAST(t4.picking_job_num/t5.agv_num AS DECIMAL(10,0)) > 10 AND CAST(t4.picking_job_num/t5.agv_num AS DECIMAL(10,0)) <= 20 THEN '中度活跃'
                WHEN CAST(t4.picking_job_num/t5.agv_num AS DECIMAL(10,0)) > 1 AND CAST(t4.picking_job_num/t5.agv_num AS DECIMAL(10,0)) <= 10 THEN '轻度活跃'
                WHEN CAST(t4.picking_job_num/t5.agv_num AS DECIMAL(10,0)) <= 1 THEN '不活跃' 
           ELSE '未知' END AS project_activation_sevendays, --近7日项目活跃度
           nvl(t2.workorder_num_sevendays,0) as workorder_num_sevendays, --近7日新增工单数量
           nvl(t3.unsolve_workorder_num_sevendays,0) as unsolve_workorder_num_sevendays, --超7日未关闭工单数量
           t6.value as agv_breakdown_num_sevendays,--近7日小车故障数
           t7.value as agv_MTBF_sevendays,--近7日小车MTBF
           t9.value as service_downtime_sevendays,--近7日服务宕机总时长
           t8.value as system_breakdown_num_sevendays,--近7日系统故障次数
           nvl(t1.upgrade_num_sevendays,0) as upgrade_num_sevendays --近7日升级次数
    FROM ${tmp_dbname}.tmp_pms_project_general_view_detail p
    LEFT JOIN 
    (
      SELECT p.project_code,
             COUNT(DISTINCT v.process_instance_id) as upgrade_num_sevendays
      FROM ${dwd_dbname}.dwd_dtk_version_evaluation_info_df v
      LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail p
      ON nvl(v.project_code,'unknown1') = nvl(p.project_code,'unknown2') OR nvl(v.project_code,'unknown1') = nvl(p.project_sale_code,'unknown2')
      WHERE v.d = '${pre1_date}' AND p.project_code is not null AND v.upgrade_date >= DATE_ADD('${pre1_date}', -6) AND v.upgrade_date <= '${pre1_date}' --近7日升级次数
      GROUP BY p.project_code
    ) t1
    ON p.project_code = t1.project_code
    LEFT JOIN 
    (
      SELECT p.project_code,
             COUNT(DISTINCT o.ticket_id) as workorder_num_sevendays
      FROM ${dwd_dbname}.dwd_ones_work_order_info_df o
      LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail p
      ON nvl(o.project_code,'unknown1') = nvl(p.project_code,'unknown2') OR nvl(o.project_code,'unknown1') = nvl(p.project_sale_code,'unknown2')
      WHERE o.d = '${pre1_date}' AND p.project_code is not null AND o.created_time >= DATE_ADD('${pre1_date}', -6) AND o.created_time <= '${pre1_date}' AND o.work_order_status != '已驳回'  --近7日新增工单数量
      GROUP BY p.project_code
    ) t2
    ON p.project_code = t2.project_code
    LEFT JOIN 
    (
      SELECT p.project_code,
             COUNT(DISTINCT o.ticket_id) as unsolve_workorder_num_sevendays
      FROM ${dwd_dbname}.dwd_ones_work_order_info_df o
      LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail p
      ON nvl(o.project_code,'unknown1') = nvl(p.project_code,'unknown2') OR nvl(o.project_code,'unknown1') = nvl(p.project_sale_code,'unknown2')
      WHERE o.d = '${pre1_date}' AND p.project_code is not null AND o.work_order_status != '已驳回' AND o.work_order_status != '已关闭' AND unix_timestamp(DATE_FORMAT(o.d,'yyyy-MM-dd 00:00:00')) - unix_timestamp(o.created_time) >= 604800  --超7日未关闭工单数量
      GROUP BY p.project_code
    ) t3
    ON p.project_code = t3.project_code
    LEFT JOIN 
    (
      SELECT '${pre1_date}' as d,
             c.project_code,
             COUNT(DISTINCT c.job_id) as picking_job_num
      FROM ${dwd_dbname}.dwd_g2p_job_state_change_info_da c
      WHERE c.d >= DATE_ADD('${pre1_date}', -6) AND c.job_state ='DONE' AND (c.job_type ='W2P_ONLINE_PICK' OR c.job_type ='G2P_ONLINE_PICK')
      GROUP BY c.project_code
    ) t4
    ON p.project_code = t4.project_code
    LEFT JOIN 
    (
      SELECT '${pre1_date}' as d,
             ba.project_code,
             COUNT(DISTINCT ba.agv_code) as agv_num
      FROM ${dwd_dbname}.dwd_rcs_basic_agv_info_df ba
      WHERE ba.d = '${pre1_date}'
      GROUP BY ba.project_code
    ) t5
    ON p.project_code = t5.project_code
    -- 近7日平均单台小车故障数（收敛V4）:近7天日均单台小车平均发生的3级以及3级以上的故障数量=近7天项目所有小车发生的3级以及3级以上的故障数/(7*近7天项目平均机器人数)(收敛V4)
    LEFT JOIN 
    (
      SELECT t1.project_code,
             round(t2.breakdown_num / (7 * t1.total_agv_num),2) as value,
             '${pre1_date}' as dt
      FROM 
      (
        SELECT project_code,
               avg(total_agv_num) as total_agv_num
        FROM 
        (
          SELECT project_code,
                 d,
                 count(distinct agv_code) as total_agv_num
          FROM ${dwd_dbname}.dwd_rcs_basic_agv_info_df
          WHERE d >= DATE_ADD('${pre1_date}', -6) AND d <= '${pre1_date}'
          GROUP BY project_code,d
        )t
        GROUP BY project_code
      )t1
      INNER JOIN 
      (
        SELECT project_code,
               count(distinct concat(agv_code, '-', breakdown_id, '-', error_code)) as breakdown_num
        FROM ${dwd_dbname}.dwd_agv_breakdown_astringe_v4_di
        WHERE d >= DATE_ADD('${pre1_date}', -6) AND d <= '${pre1_date}' AND error_level >= 3
        GROUP BY project_code
      )t2
      ON t2.project_code = t1.project_code
    ) t6
    ON p.project_code = t6.project_code
    -- 近7日小车MTBF（收敛V4）:近7天平均无故障时长（单位：小时/次）=（24小时*当天小车数*7-7天全部小车的故障累计时长）/7天全部小车的故障数（收敛V4）
    LEFT JOIN 
    (
      SELECT t1.project_code,
             case when COALESCE(t3.breakdown_num, 0) > 0 then round((t1.total_agv_num * 24 * 3600 - COALESCE(t2.errorSeconds, 0)) / 3600 / COALESCE(t3.breakdown_num, 0), 2)
                  else round((t1.total_agv_num * 24 * 3600 - COALESCE(t2.errorSeconds, 0)) / 3600, 2) end value,
             '${pre1_date}' as dt
      FROM
      --项目小车数
      (
        SELECT project_code,
               sum(total_agv_num) as total_agv_num
        FROM 
        (
          SELECT project_code,
                 d,
                 count(distinct agv_code) as total_agv_num
          FROM ${dwd_dbname}.dwd_rcs_basic_agv_info_df
          WHERE d >= DATE_ADD('${pre1_date}', -6) AND d <= '${pre1_date}'
          GROUP BY project_code, d
        )t
        GROUP BY project_code
      )t1
      --项目小车故障时长
      LEFT JOIN 
      (
        SELECT project_code,
               sum(diff_seconds) as errorSeconds
        FROM
        (
          SELECT d,
                 project_code,
                 agv_code,
                 working_status,
                 status_log_time,
                 lead(status_log_time, 1, concat(date_add(status_log_time, 1), ' ', '00:00:00')) over (partition by d,project_code,agv_code order by status_log_time asc) as next_1_time,
                 unix_timestamp(status_log_time) as unixtime,
                 unix_timestamp(lead(status_log_time, 1, concat(date_add(status_log_time, 1), ' ', '00:00:00')) over (partition by d,project_code,agv_code order by status_log_time asc)) as next_1_unixtime,
                 unix_timestamp(lead(status_log_time, 1, concat(date_add(status_log_time, 1), ' ', '00:00:00')) over (partition by d,project_code,agv_code order by status_log_time asc)) - unix_timestamp(status_log_time) as diff_seconds
          FROM ${dwd_dbname}.dwd_agv_working_status_incre_dt
          WHERE d >= DATE_ADD('${pre1_date}', -6) AND d <= '${pre1_date}'
        )t
        WHERE t.working_status = 'ERROR'
        GROUP BY project_code
      )t2 
      ON t2.project_code = t1.project_code
      --项目小车故障数
      LEFT JOIN 
      (
        SELECT project_code,
               count(distinct concat(agv_code, '-', breakdown_id, '-', error_code)) as breakdown_num
        FROM ${dwd_dbname}.dwd_agv_breakdown_astringe_v4_di
        WHERE d >= DATE_ADD('${pre1_date}', -6) AND d <= '${pre1_date}' AND error_level >= 3
        GROUP BY project_code
      )t3 
      ON t3.project_code = t1.project_code
      --没有采集故障的项目不考虑
      LEFT JOIN 
      (
        SELECT distinct pt as project_code
        FROM ${dwd_dbname}.dwd_agv_breakdown_detail_incre_dt
        WHERE d >= DATE_ADD('${pre1_date}', -6) AND d <= '${pre1_date}'
      )tp 
      ON tp.project_code = t1.project_code
      WHERE tp.project_code is not null
    ) t7
    ON p.project_code = t7.project_code
    -- 近7日系统故障次数:系统发生3级以及3级以上的系统故障次数/7天
    LEFT JOIN 
    (
      SELECT t1.project_code,
             round(count(distinct t1.message_id) / 7,2) as value,
             '${pre1_date}' as dt
      FROM 
      (
        SELECT t.*,
               ROW_NUMBER() over (partition by d,pt,agv_code,md5(message_body) order by notify_start_time asc) rk
        FROM ${dwd_dbname}.dwd_notification_message_info_di t
        WHERE d >= DATE_ADD('${pre1_date}', -6) AND d <= '${pre1_date}'
      ) t1
      LEFT JOIN ${dim_dbname}.dim_sys_error_info_offline t2 
      ON t2.sys_error_code = t1.message_title
      WHERE t1.rk = 1
        AND t2.sys_error_level >= 3 AND t2.sys_error_code is not null
      GROUP BY t1.project_code
    ) t8
    ON p.project_code = t8.project_code
    -- 近7日项目有服务宕机总时长:近7日现场部署服务环境宕机的总时长（单位：分钟）
    LEFT JOIN 
    (
      SELECT t.project_code,
             round(count(distinct second_of_day) / 60, 2) as value,
             '${pre1_date}' as dt
      FROM
      (
        SELECT t1.project_code,
               t.second_of_day,
               count(t1.id) as have_breakdown_cnt
        FROM
        (
          SELECT date_format(concat(td.days, ' ', ts.second_of_day), 'yyyy-MM-dd HH:mm:ss') as second_of_day
          FROM ${dim_dbname}.dim_day_date td
          LEFT JOIN ${dim_dbname}.dim_day_of_second ts
          WHERE td.days >= DATE_ADD('${pre1_date}', -6) AND td.days <= '${pre1_date}'
        )t
        LEFT JOIN
        (
          SELECT id,
                 project_code,
                 project_name,
                 host,
                 item_name,
                 breakdown_level,
                 item_status,
                 breakdown_start_time,
                 breakdown_end_time,
                 extra_breakdown_info,
                 work_order_status,
                 dingtalk_status
          FROM ${dwd_dbname}.dwd_sys_breakdown_info_df
          WHERE d = '${pre1_date}'
            AND ((date_format(breakdown_start_time, 'yyyy-MM-dd HH:mm:ss') >= date_format(concat(DATE_ADD('${pre1_date}', -6), ' ', '00:00:00'), 'yyyy-MM-dd HH:mm:ss')
                   AND date_format(breakdown_start_time, 'yyyy-MM-dd HH:mm:ss') < date_format(concat(DATE_ADD('${pre1_date}', 1), ' ', '00:00:00'), 'yyyy-MM-dd HH:mm:ss')
                   AND date_format(breakdown_end_time, 'yyyy-MM-dd HH:mm:ss') < date_format(concat(DATE_ADD('${pre1_date}', 1), ' ', '00:00:00'), 'yyyy-MM-dd HH:mm:ss')) 
              OR (date_format(breakdown_start_time, 'yyyy-MM-dd HH:mm:ss') >= date_format(concat(DATE_ADD('${pre1_date}', -6), ' ', '00:00:00'), 'yyyy-MM-dd HH:mm:ss')
                   AND date_format(breakdown_start_time, 'yyyy-MM-dd HH:mm:ss') < date_format(concat(DATE_ADD('${pre1_date}', 1), ' ', '00:00:00'), 'yyyy-MM-dd HH:mm:ss')
                   AND date_format(breakdown_end_time, 'yyyy-MM-dd HH:mm:ss') > date_format(concat(DATE_ADD('${pre1_date}', 1), ' ', '00:00:00'), 'yyyy-MM-dd HH:mm:ss')) 
              OR (date_format(breakdown_start_time, 'yyyy-MM-dd HH:mm:ss') < date_format(concat(DATE_ADD('${pre1_date}', -6), ' ', '00:00:00'), 'yyyy-MM-dd HH:mm:ss')
                   AND date_format(breakdown_start_time, 'yyyy-MM-dd HH:mm:ss') < date_format(concat(DATE_ADD('${pre1_date}', 1), ' ', '00:00:00'), 'yyyy-MM-dd HH:mm:ss')
                   AND date_format(breakdown_end_time, 'yyyy-MM-dd HH:mm:ss') >= date_format(concat(DATE_ADD('${pre1_date}', -6), ' ', '00:00:00'), 'yyyy-MM-dd HH:mm:ss')
                   AND date_format(breakdown_end_time, 'yyyy-MM-dd HH:mm:ss') < date_format(concat(DATE_ADD('${pre1_date}', 1), ' ', '00:00:00'), 'yyyy-MM-dd HH:mm:ss')) 
              OR (date_format(breakdown_start_time, 'yyyy-MM-dd HH:mm:ss') < date_format(concat(DATE_ADD('${pre1_date}', -6), ' ', '00:00:00'), 'yyyy-MM-dd HH:mm:ss')
                   AND date_format(breakdown_start_time, 'yyyy-MM-dd HH:mm:ss') < date_format(concat(DATE_ADD('${pre1_date}', 1), ' ', '00:00:00'), 'yyyy-MM-dd HH:mm:ss')
                   AND date_format(breakdown_end_time, 'yyyy-MM-dd HH:mm:ss') > date_format(concat(DATE_ADD('${pre1_date}', 1), ' ', '00:00:00'), 'yyyy-MM-dd HH:mm:ss')))
            AND item_status = 'resolved'
            AND item_name like '%异常%'
        )t1
        WHERE t.second_of_day >= date_format(t1.breakdown_start_time, 'yyyy-MM-dd HH:mm:ss') AND t.second_of_day <= date_format(t1.breakdown_end_time, 'yyyy-MM-dd HH:mm:ss')
        GROUP BY t1.project_code,t.second_of_day
      )t
      WHERE t.have_breakdown_cnt > 0
      GROUP BY t.project_code
    ) t9
    ON p.project_code = t9.project_code
  ) tt
  GROUP BY tt.cur_date,tt.date_zone,tt.project_code,tt.project_name,tt.product_name,tt.project_ft,tt.project_area,tt.project_priority,tt.project_operation_state,tt.project_activation_sevendays
) tmp;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"