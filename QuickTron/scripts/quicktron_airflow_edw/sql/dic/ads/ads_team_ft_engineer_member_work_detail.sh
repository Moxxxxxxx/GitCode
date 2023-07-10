#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-12-19 创建
#-- 2 wangyingying 2022-12-26 增加日均应排人数逻辑
#-- 3 wangyingying 2022-12-28 增加日工单排序逻辑
#-- 4 wangyingying 2022-12-29 校正逻辑
#-- 5 wangyingying 2022-12-30 增加物料排序逻辑
#-- 6 wangyingying 2023-01-03 增加计划未分配数据的逻辑
#-- 7 wangyingying 2023-01-03 增加机型匹配的逻辑（涉及到离线表串列字段 后续会调整）
#-- 8 wangyingying 2023-01-05 增加排产标准工时字段
#-- 9 wangyingying 2023-02-22 增加物料名称字段
# ------------------------------------------------------------------------------------------------


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
set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--生产制造部生产数据统计明细表 ads_team_ft_engineer_member_work_detail

INSERT overwrite table ${ads_dbname}.ads_team_ft_engineer_member_work_detail
SELECT '' AS id, -- 主键
       r.process_instance_id, -- 数据ID
       r.business_id, -- 审批编号
       r.process_start_time, -- 工单创建时间
       r.applicant_user_name, -- 工单创建人
       r.production_date, -- 生产日期
       r.work_order_number, -- 工单号
       r.project_code, -- 项目编码
       b.project_name, -- 项目名称
       b.project_attr_ft AS project_ft, -- 项目所属ft
       r.product_process, -- 组别
       r.product_part_number, -- 产品料号
       k.material_name, -- 物料名称
       r.model_code, -- 车型代号
       r.product_name, -- 产品名称
       w.standard_working_hour AS production_standard_working_hour, -- 排产标准工时（分钟）
       nvl(nvl(r.agv_standard_time,harness_or_parts_standard_time),0) AS standard_time_minutes, -- 标准工时（分钟）
       nvl(r.agv_standard_time,0) AS agv_standard_time, -- agv标准工时（分钟）
       nvl(r.harness_or_parts_standard_time,0) AS harness_or_parts_standard_time, -- 线束/部品标准工时（分钟）
       nvl(mp.queue_number_month,0) AS queue_number_month, -- 月应排人数
       nvl(CAST(nvl(mp.queue_number_month,0) * (nvl(p.plan_num,0) / nvl(mp.plan_num_month,0)) AS DECIMAL(10,4)),0) AS queue_number_day, -- 天应排人数
       nvl(mp.plan_num_month,0) AS plan_num_month, -- 月计划数量
       nvl(p.plan_num,0) AS plan_num_day, -- 天计划数量
       nvl(CAST(nvl(p.plan_num,0) / nvl(mp.plan_num_month,0) AS DECIMAL(10,4)),0) AS plan_num_rate, -- 天计划数量占比
       nvl(mp.queue_number_month,0) * 480 AS predict_production_hours_month, -- 月预估生产工时
       nvl(CAST(nvl(mp.queue_number_month,0) * 480 * (nvl(p.plan_num,0) / nvl(mp.plan_num_month,0)) AS DECIMAL(10,4)),0) AS predict_production_hours_day, -- 天预估生产工时
       r.production_number, -- 生产数量
       r.all_working_hours_minutes, -- 总投入工时（分钟）
       r.operator_name, -- 作业员
       IF(tt1.leave_type IN ('全天请假','上半天请假','下半天请假'),tt1.leave_type,NULL) AS leave_type, -- 请假类型
       CASE WHEN tt1.leave_type IN ('全天请假') THEN 0
            WHEN tt1.leave_type IN ('上半天请假','下半天请假') THEN 10
            ELSE 20 END AS free_time, -- 休息时间
       r.working_hours, -- 登记投入工时（分钟）
       nvl(CAST(r.production_number * (r.working_hours / r.all_working_hours_minutes) AS DECIMAL(10,4)),0) AS person_production_number, -- 个人检验合格数量（分摊）
       nvl(CAST(nvl(r.agv_standard_time,harness_or_parts_standard_time) * (r.production_number * (r.working_hours / r.all_working_hours_minutes)) AS DECIMAL(10,4)),0) AS person_production_hours_minutes, -- 个人生产工时（分钟）
       nvl(r.all_losing_hours_minutes,0) AS all_losing_hours_minutes, -- 总损失工时（分钟）
       nvl(CAST(r.all_losing_hours_minutes * (r.working_hours / r.all_working_hours_minutes) AS DECIMAL(10,4)),0) AS person_losing_hours_minutes, -- 个人损失工时（分摊）
       nvl(CAST((nvl(r.agv_standard_time,harness_or_parts_standard_time) * (r.production_number * (r.working_hours / r.all_working_hours_minutes))) / (r.working_hours - nvl(r.all_losing_hours_minutes * (r.working_hours / r.all_working_hours_minutes),0)) AS DECIMAL(10,4)),0) AS production_efficiency, -- 生产效率 => 生产工时（标准工时* 生产数量）/（登记投入工时 - 损失工时）
       nvl(CAST((nvl(r.agv_standard_time,harness_or_parts_standard_time) * (r.production_number * (r.working_hours / r.all_working_hours_minutes))) / r.working_hours AS DECIMAL(10,4)),0) AS attendance_efficiency, -- 出勤效率 => 生产工时（标准工时* 生产数量）/ 登记投入工时
       nvl(CAST((nvl(p.plan_num,0) * nvl(r.agv_standard_time,harness_or_parts_standard_time)) / (nvl(mp.queue_number_month,0) * 480 * nvl(CAST(nvl(p.plan_num,0) / nvl(mp.plan_num_month,0) AS DECIMAL(10,4)),0)) AS DECIMAL(10,4)),0) AS scheduling_efficiency, -- 排产效率
       nvl(CAST((nvl(r.agv_standard_time,harness_or_parts_standard_time) * (r.production_number * (r.working_hours / r.all_working_hours_minutes))) / (nvl(p.plan_num,0) * nvl(r.agv_standard_time,harness_or_parts_standard_time)) AS DECIMAL(10,4)),0) AS plan_reach_rate, -- 计划达成率
       nvl(CAST(r.all_losing_hours_minutes / r.all_working_hours_minutes AS DECIMAL(10,4)),0) AS losing_rate, -- 损失率
       nvl(a.production_efficiency_targets,0) AS production_efficiency_targets, -- 生产效率目标 
       nvl(a.attendance_efficiency_targets,0) AS attendance_efficiency_targets, -- 出勤效率目标 
       nvl(a.scheduling_efficiency_targets,0) AS scheduling_efficiency_targets, -- 排产效率目标 
       nvl(a.plan_achievement_rate,0) AS plan_achievement_rate, -- 计划达成率目标
       r.working_hours - (nvl(m.normal_work_hour * 60,0) + nvl(IF(m.workday_overtime != 0,m.workday_overtime,m.weekend_overtime) * 60,0)) AS working_hours_deviation, -- 登记-打卡偏差工时
       nvl(m.normal_work_hour * 60,0) + nvl(IF(m.workday_overtime != 0,m.workday_overtime,m.weekend_overtime) * 60,0) AS total_work_hour, -- 打卡统计出勤工时（分钟）
       nvl(m.normal_work_hour * 60,0) AS normal_work_hour,-- 打卡标准出勤工时（分钟）
       nvl(IF(m.workday_overtime != 0,m.workday_overtime,m.weekend_overtime) * 60,0) AS overtime_work_hour, -- 打卡加班工时（分钟）
       nvl(CAST((unix_timestamp(m.attendance_off_time) - unix_timestamp(m.attendance_working_time)) / 60 AS DECIMAL(10,2)),0) AS actual_work_hour, -- 实际打卡工时（分钟）
       IF(SUBSTR(m.attendance_working_time,12,18) = SUBSTR(m.attendance_off_time,12,18),CONCAT(SUBSTR(m.attendance_working_time,12,18),'~','N/A'),CONCAT(SUBSTR(m.attendance_working_time,12,18),'~',SUBSTR(m.attendance_off_time,12,18))) AS work_hour_scope, -- 打卡范围
       ROW_NUMBER()OVER(PARTITION BY r.production_date,r.work_order_number,r.product_part_number,r.product_name,r.business_id ORDER BY r.applicant_user_name ASC) AS work_order_sort, -- 工单排序
	   ROW_NUMBER()OVER(PARTITION BY r.production_date,r.work_order_number,r.product_part_number,r.product_name ORDER BY r.applicant_user_name ASC) AS product_part_sort, -- 物料排序
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM ${dwd_dbname}.dwd_dtk_daily_production_report_info_df r -- 生产日报
LEFT JOIN ${tmp_dbname}.tmp_team_ft_engineer_member_work_efficiency m -- 制造部人员考勤明细 
ON r.production_date = m.work_date AND r.operator_name = m.team_member
LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b  -- 项目大表
ON b.d = '${pre1_date}' AND (r.project_code = b.project_code OR r.project_code = b.project_sale_code)
-- 生产计划
LEFT JOIN 
(
  SELECT project_code, 
         work_order_number, 
         material_number, 
         IF(group_name IN ('部品','线束'),NULL,machine_type) AS model_code,
         IF(group_name IN ('部品','线束'),name,NULL) AS product_name,
         group_name AS product_process,
         queue_number, 
         plan_num,
         start_date, 
         start_month
  FROM ${dim_dbname}.dim_product_plan_info_offline p
)p
ON r.production_date = p.start_date AND r.work_order_number = p.work_order_number AND r.product_part_number = p.material_number AND IF(r.product_process IN ('部品','线束'),r.product_part_number = p.material_number,r.model_code = p.model_code AND r.product_process = p.product_process)
-- 月生产计划
LEFT JOIN 
(
  SELECT work_order_number, -- 工单号
         material_number, -- 物料号
         IF(group_name IN ('部品','线束'),NULL,machine_type) AS model_code,
         IF(group_name IN ('部品','线束'),name,NULL) AS product_name,
         group_name AS product_process,
         start_month, -- 月份
         nvl(queue_number,0) AS queue_number_month, -- 月应排人数
         SUM(nvl(plan_num,0)) AS plan_num_month -- 月计划数量
  FROM ${dim_dbname}.dim_product_plan_info_offline p
  GROUP BY work_order_number,material_number,IF(group_name IN ('部品','线束'),NULL,machine_type),IF(group_name IN ('部品','线束'),name,NULL),group_name,start_month,queue_number
)mp
ON p.work_order_number = mp.work_order_number AND p.material_number = mp.material_number AND p.start_month = mp.start_month AND IF(p.product_process IN ('部品','线束'),p.material_number = mp.material_number,p.model_code = mp.model_code AND p.product_process = mp.product_process)
LEFT JOIN ${dim_dbname}.dim_product_plan_achievement_rate_offline a -- 计划达成率
ON r.product_process = a.process
-- 标准工时
LEFT JOIN 
(
  SELECT *,ROW_NUMBER()OVER(PARTITION BY w.product_process,w.product_part_number,w.model_code ORDER BY w.start_date ASC,w.end_date ASC) AS rn
  FROM ${dwd_dbname}.dwd_dtk_standard_working_hour_info_df w 
  WHERE w.d = '${pre1_date}' 
)w
ON ((r.production_date >= w.start_date AND r.production_date <= w.end_date) OR (w.rn = 1 AND r.production_date < w.start_date)) AND IF(r.model_code IS NOT NULL,r.product_process = w.product_process AND r.model_code = w.model_code,r.product_part_number = w.product_part_number)
-- 物料名称映射表
LEFT JOIN 
(
  SELECT *,ROW_NUMBER()OVER(PARTITION BY k.material_number ORDER BY k.material_id ASC) AS rn
  FROM ${dwd_dbname}.dwd_kde_bd_material_info_df k
  WHERE k.d = '${pre1_date}'
)k
ON k.rn = 1 AND r.product_part_number = k.material_number
-- 请假统计
LEFT JOIN 
(
  SELECT l1.originator_user_id,
         l1.applicant_name,
         l1.stat_date,
         case when l2.leave_type is null THEN l1.leave_type else '全天请假' END as leave_type
  FROM 
  (
    SELECT l.originator_user_id,
           l.applicant_name,
           cast(l.leave_date as date) as stat_date,
           CASE when l.period_type = '全天' THEN '全天请假'
                when l.period_type = '下午' THEN '下半天请假'
                when l.period_type = '上午' THEN '上半天请假' 
                when l.period_type = '其它' THEN '哺乳假' end as leave_type,
           row_number()over(PARTITION by l.originator_user_id,cast(l.leave_date as date) order by CASE when l.period_type = '全天' THEN '全天请假'
                                                                                                       when l.period_type = '下午' THEN '下半天请假'
                                                                                                       when l.period_type = '上午' THEN '上半天请假' 
                                                                                                       when l.period_type = '其它' THEN '哺乳假' end asc)rn
    FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l
    WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = '${pre1_date}'
  )l1
  LEFT JOIN 
  (
    SELECT l.originator_user_id,
           l.applicant_name,
           cast(l.leave_date as date) as stat_date,
           CASE when l.period_type = '全天' THEN '全天请假'
                when l.period_type = '下午' THEN '下半天请假'
                when l.period_type = '上午' THEN '上半天请假' 
                when l.period_type = '其它' THEN '哺乳假' end as leave_type,
           row_number()over(PARTITION by l.originator_user_id,cast(l.leave_date as date) order by CASE when l.period_type = '全天' THEN '全天请假'
                                                                                                       when l.period_type = '下午' THEN '下半天请假'
                                                                                                       when l.period_type = '上午' THEN '上半天请假' 
                                                                                                       when l.period_type = '其它' THEN '哺乳假' end asc)rn
    FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l
    WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = '${pre1_date}'
  )l2
  ON l1.originator_user_id = l2.originator_user_id AND l1.stat_date = l2.stat_date AND l1.leave_type != l2.leave_type
  WHERE l1.rn = 1 
)tt1
ON tt1.applicant_name = r.operator_name AND tt1.stat_date = r.production_date
WHERE r.d = '${pre1_date}' AND r.approval_result = 'agree' AND r.approval_status = 'COMPLETED' AND r.is_valid = 1 -- 审批状态:已结束 且 审批结果:审批通过 且 有效记录 

UNION ALL

SELECT '' AS id, -- 主键
       NULL AS process_instance_id, -- 数据ID
       NULL AS business_id, -- 审批编号
       NULL AS process_start_time, -- 工单创建时间
       NULL AS applicant_user_name, -- 工单创建人
       r.start_date AS production_date, -- 生产日期
       r.work_order_number, -- 工单号
       r.project_code, -- 项目编码
       b.project_name, -- 项目名称
       b.project_attr_ft AS project_ft, -- 项目所属ft
       r.product_process, -- 组别
       r.material_number AS product_part_number, -- 产品料号
       k.material_name, -- 物料名称
       r.model_code, -- 车型代号
       r.product_name, -- 产品名称
       w.standard_working_hour AS production_standard_working_hour, -- 排产标准工时（分钟）
       NULL AS standard_time_minutes, -- 标准工时（分钟）
       NULL AS agv_standard_time, -- agv标准工时（分钟）
       NULL AS harness_or_parts_standard_time, -- 线束/部品标准工时（分钟）
       mp.queue_number_month AS queue_number_month, -- 月应排人数
       CAST(mp.queue_number_month * (r.plan_num / mp.plan_num_month) AS DECIMAL(10,4)) AS queue_number_day, -- 天应排人数
       mp.plan_num_month, -- 月计划数量
       r.plan_num AS plan_num_day, -- 天计划数量
       CAST(r.plan_num / mp.plan_num_month AS DECIMAL(10,4)) AS plan_num_rate, -- 天计划数量占比
       mp.queue_number_month * 480 AS predict_production_hours_month, -- 月预估生产工时
       CAST(mp.queue_number_month * 480 * (r.plan_num / mp.plan_num_month) AS DECIMAL(10,4)) AS predict_production_hours_day, -- 天预估生产工时
       NULL AS production_number, -- 生产数量
       NULL AS all_working_hours_minutes, -- 总投入工时（分钟）
       NULL AS operator_name, -- 作业员
       NULL AS leave_type, -- 请假类型
       NULL AS free_time, -- 休息时间
       NULL AS working_hours, -- 登记投入工时（分钟）
       NULL AS person_production_number, -- 个人检验合格数量（分摊）
       NULL AS person_production_hours_minutes, -- 个人生产工时（分钟）
       NULL AS all_losing_hours_minutes, -- 总损失工时（分钟）
       NULL AS person_losing_hours_minutes, -- 个人损失工时（分摊）
       NULL AS production_efficiency, -- 生产效率 => 生产工时（标准工时* 生产数量）/（登记投入工时 - 损失工时）
       NULL AS attendance_efficiency, -- 出勤效率 => 生产工时（标准工时* 生产数量）/ 登记投入工时
       NULL AS scheduling_efficiency, -- 排产效率
       NULL AS plan_reach_rate, -- 计划达成率
       NULL AS losing_rate, -- 损失率
       NULL AS production_efficiency_targets, -- 生产效率目标 
       NULL AS attendance_efficiency_targets, -- 出勤效率目标 
       NULL AS scheduling_efficiency_targets, -- 排产效率目标 
       NULL AS plan_achievement_rate, -- 计划达成率目标
       NULL AS working_hours_deviation, -- 登记-打卡偏差工时
       NULL AS total_work_hour, -- 打卡统计出勤工时（分钟）
       NULL AS normal_work_hour,-- 打卡标准出勤工时（分钟）
       NULL AS overtime_work_hour, -- 打卡加班工时（分钟）
       NULL AS actual_work_hour, -- 实际打卡工时（分钟）
       NULL AS work_hour_scope, -- 打卡范围
       ROW_NUMBER()OVER(PARTITION BY r.start_date,r.work_order_number,r.material_number,r.machine_type,r.business_id ORDER BY r.applicant_user_name ASC) AS work_order_sort, -- 工单排序
	   ROW_NUMBER()OVER(PARTITION BY r.start_date,r.work_order_number,r.material_number,r.machine_type ORDER BY r.applicant_user_name ASC) AS product_part_sort, -- 物料排序
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM 
(
  SELECT p.project_code, 
         p.work_order_number, 
         p.material_number, 
         IF(p.group_name IN ('部品','线束'),NULL,p.machine_type) AS model_code,
         IF(p.group_name IN ('部品','线束'),p.name,NULL) AS product_name,
         p.group_name AS product_process,
         p.machine_type,
         p.queue_number, 
         p.plan_num,
         p.start_date, 
         p.start_month,
         r.business_id,
         r.applicant_user_name
  FROM ${dim_dbname}.dim_product_plan_info_offline p -- 生产计划表
  LEFT JOIN 
  (
    SELECT *
    FROM ${dwd_dbname}.dwd_dtk_daily_production_report_info_df r -- 生产日报
    WHERE r.d = '${pre1_date}' AND r.approval_result = 'agree' AND r.approval_status = 'COMPLETED' AND r.is_valid = 1 -- 审批状态:已结束 且 审批结果:审批通过 且 有效记录 
  )r
  ON p.work_order_number = r.work_order_number AND p.start_date = r.production_date AND p.material_number = r.product_part_number AND IF(p.group_name IN ('部品','线束'),p.material_number = r.product_part_number,IF(p.group_name IN ('部品','线束'),NULL,p.machine_type) = r.model_code AND p.group_name = r.product_process)
  WHERE r.process_instance_id IS NULL 
)r
LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b -- 项目大表
ON b.d = '${pre1_date}' AND (r.project_code = b.project_code OR r.project_code = b.project_sale_code)
-- 月生产计划
LEFT JOIN 
(
  SELECT work_order_number, -- 工单号
         material_number, -- 物料号
         IF(group_name IN ('部品','线束'),NULL,machine_type) AS model_code,
         IF(group_name IN ('部品','线束'),name,NULL) AS product_name,
         group_name AS product_process,
         start_month, -- 月份
         queue_number AS queue_number_month, -- 月应排人数
         SUM(nvl(plan_num,0)) AS plan_num_month -- 月计划数量
  FROM ${dim_dbname}.dim_product_plan_info_offline p
  GROUP BY work_order_number,material_number,IF(group_name IN ('部品','线束'),NULL,machine_type),IF(group_name IN ('部品','线束'),name,NULL),group_name,start_month,queue_number
)mp
ON r.work_order_number = mp.work_order_number AND r.material_number = mp.material_number AND r.start_month = mp.start_month AND IF(r.product_process IN ('部品','线束'),r.material_number = mp.material_number,r.model_code = mp.model_code AND r.product_process = mp.product_process)
-- 标准工时
LEFT JOIN 
(
  SELECT *,ROW_NUMBER()OVER(PARTITION BY w.product_process,w.product_part_number,w.model_code ORDER BY w.start_date ASC,w.end_date ASC) AS rn
  FROM ${dwd_dbname}.dwd_dtk_standard_working_hour_info_df w 
  WHERE w.d = '${pre1_date}' 
)w
ON ((r.start_date >= w.start_date AND r.start_date <= w.end_date) OR (w.rn = 1 AND r.start_date < w.start_date)) AND IF(r.model_code IS NOT NULL,r.product_process = w.product_process AND r.model_code = w.model_code,r.material_number = w.product_part_number)
-- 物料名称映射表
LEFT JOIN 
(
  SELECT *,ROW_NUMBER()OVER(PARTITION BY k.material_number ORDER BY k.material_id ASC) AS rn
  FROM ${dwd_dbname}.dwd_kde_bd_material_info_df k
  WHERE k.d = '${pre1_date}'
)k
ON k.rn = 1 AND r.material_number = k.material_number;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"      