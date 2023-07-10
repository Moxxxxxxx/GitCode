#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-07-20 创建
#-- 3 wangyingying 2022-12-16 增加出差数据源
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
-- 人员工作明细表 ads_member_work_detail 

INSERT overwrite table ${ads_dbname}.ads_member_work_detail
SELECT '' as id, -- 主键
       main.emp_id as dtk_user_id, -- 钉钉用户id
       main.emp_name as team_member, -- 人员姓名
       main.team_group as org_path, -- 组织架构
       main.emp_position as emp_position, -- 人员职位
       main.days as work_date, -- 日期
       nvl(owh.ones_work_hour,0) as ones_work_hour, -- 登记工时数
       nvl(ciwh.clock_in_work_hour,0) as clock_in_work_hour, -- 打卡工时数
       nvl(owh.ones_work_hour,0) - nvl(ciwh.clock_in_work_hour,0) as workhour_deviation, -- 偏差工时数
       IF(nvl(owh.ones_work_hour,0) - nvl(ciwh.clock_in_work_hour,0) > 0,1,0) as is_deviation, -- 是否偏差
       nvl(ib.business_days,0) as is_business, -- 是否出差
       nvl(il.leave_days,0) as is_leave, -- 是否请假
       IF(ho.checkin_date IS NULL,0,1) as is_home_office, -- 是否居家办公
       ho.home_office_location, -- 居家办公地点
       doi.demand_ones_id, -- 登记工时的需求id
       toi.task_ones_id, -- 登记工时的任务id
       boi.bug_ones_id, -- 登记工时的缺陷id
       woi.work_order_ones_id, -- 登记工时的工单id
       nvl(mwh.mgmt_work_hour,0) as mgmt_work_hour, -- 技术&管理工作工时
       nvl(iwh.ineffective_work_hour,0) as ineffective_work_hour, -- 无效工时
       CASE WHEN nvl(owh.ones_work_hour,0) < 6 THEN '未饱和'
            WHEN nvl(owh.ones_work_hour,0) >= 6 AND nvl(owh.ones_work_hour,0) <= 10 THEN '饱和'
            WHEN nvl(owh.ones_work_hour,0) > 10 THEN '过饱和' END as saturation, -- 饱和度
       nvl(cewh.code_error_work_hour,0) as code_error_work_hour, -- 外部客户项目编码异常工时
       nvl(owh.wh_check_times,0) as wh_check_times, -- 工时登记次数
       nvl(owh.unusual_wh_check_times,0) as unusual_wh_check_times, -- 单次登记大于6小时次数
       nvl(ipwh.internal_project_wh,0) as internal_project_wh, -- 内部研发汇总工时
       ipwh.internal_project_summary, -- 内部项目按项目聚合明细
       nvl(epwh.external_project_wh,0) as external_project_wh, -- 外部项目汇总工时
       epwh.external_project_summary, -- 外部项目按项目聚合明细
       odd.ones_demand_detail, -- ones需求工作项明细
       otd.ones_task_detail, -- ones任务工作项明细
       obd.ones_bug_detail, -- ones缺陷工作项明细
       owd.ones_work_detail, -- ones工单工作项明细
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')  as update_time
FROM 
(
  SELECT dtk.*,
         td.*
  -- 人员主数据
  FROM
  (
    SELECT te.emp_id,
           te.emp_name,
           te.email,
           te.emp_position,
           te.is_job,
           te.hired_date,
           te.quit_date,
           m.team_group
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
    LEFT JOIN 
    (
      SELECT m.emp_id,
             m.emp_name,
             m.org_start_date,
             m.org_end_date,
             CONCAT(m.dept_name,IF(m.team_org_name_map['team1'] is null,'','-'),IF(m.team_org_name_map['team1'] is null,'',m.team_org_name_map['team1'])) as team_group,
             row_number()over(PARTITION by m.emp_id order by m.org_end_date desc)rn
      FROM ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df m
      WHERE m.d = '${pre1_date}' AND m.is_valid = 1 AND m.is_need_fill_manhour = 1 AND (m.org_end_date = '9999-01-01' OR m.org_end_date <= '${pre1_date}')
    )m
    ON te.emp_id = m.emp_id AND m.rn = 1 
    WHERE te.d = '${pre1_date}' AND te.org_company_name = '上海快仓智能科技有限公司' 
      AND (te.is_job = 1 OR te.quit_date <= '${pre1_date}')
      AND m.emp_id is not NULL 
  )dtk
  -- 日期
  LEFT JOIN 
  (
    SELECT days
	FROM ${dim_dbname}.dim_day_date
	WHERE days >= '2021-01-01' AND days <= '${pre1_date}'
  )td
  ON td.days >= dtk.hired_date AND td.days <= IF(dtk.is_job = 1,'${pre1_date}',dtk.quit_date)
)main
-- 登记工时数、工时登记次数
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         round(COALESCE(SUM(t.task_spend_hours), 0), 2) as ones_work_hour,
         COUNT(DISTINCT t.uuid) as wh_check_times,
         SUM(IF(t.task_spend_hours > 6,1,0)) as unusual_wh_check_times 
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- 剔除无效工时
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
  GROUP BY to_date(t.task_start_time),tou.user_email
)owh
ON main.email = owh.user_email AND main.days = owh.stat_date
-- 打卡工时数
LEFT JOIN 
(
  SELECT e.emp_id,
         e.emp_name,
         e.attendance_work_date,
         e.attendance_working_time,
         e.attendance_off_time,
         cast((unix_timestamp(e.attendance_off_time) - unix_timestamp(e.attendance_working_time))/3600 as decimal(10,1)) as clock_in_work_hour
  FROM ${dwd_dbname}.dwd_dtk_emp_attendance_day_info_di e
)ciwh
ON main.emp_id = ciwh.emp_id AND main.days = ciwh.attendance_work_date
-- 是否出差
LEFT JOIN 
(
  SELECT t.originator_user_id,
         cast(t.travel_date as date) as stat_date,
         t.every_days as business_days
  FROM ${dwd_dbname}.dwd_dtk_process_business_travel_dayily_info_df t
  WHERE t.is_valid = 1 AND t.d = '${pre1_date}' AND t.approval_result = 'agree' AND t.approval_status = 'COMPLETED' AND t.data_source = 'DTK'
  GROUP BY t.originator_user_id,cast(t.travel_date as date),t.every_days
  
  UNION ALL 
  
  SELECT t.originator_user_id,
         cast(t.travel_date as date) as stat_date,
         t.every_days as business_days
  FROM ${dwd_dbname}.dwd_dtk_process_business_travel_dayily_info_df t
  WHERE t.d = '${pre1_date}' AND t.approval_status = '审批通过' AND t.data_source = 'HLY'
  GROUP BY t.originator_user_id,cast(t.travel_date as date),t.every_days
)ib
ON main.emp_id = ib.originator_user_id AND main.days = ib.stat_date
-- 是否请假
LEFT JOIN 
(
  SELECT l.originator_user_id,
         cast(l.leave_date as date) as stat_date,
         l.every_days as leave_days
  FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l
  WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = '${pre1_date}' AND l.period_type != '其它'
  GROUP BY l.originator_user_id,cast(l.leave_date as date),l.every_days
)il
ON main.emp_id = il.originator_user_id AND main.days = il.stat_date
-- 是否居家办公、居家办公地点
LEFT JOIN 
(
  SELECT c.emp_id,
         c.emp_name,
         c.checkin_date,
         c.first_checkin_detail_place,
         c.last_checkin_detail_place,
         IF(c.first_checkin_detail_place = c.last_checkin_detail_place,c.first_checkin_detail_place,IF(c.last_checkin_detail_place is null,c.first_checkin_detail_place,CONCAT(c.first_checkin_detail_place,',', c.last_checkin_detail_place))) as home_office_location
  FROM ${dwd_dbname}.dwd_dtk_group_day_checkin_info_di c
)ho
ON main.emp_id = ho.emp_id AND main.days = ho.checkin_date
-- 登记工时的需求
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as demand_ones_id
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- 剔除无效工时
    AND i.issue_type_cname = '需求' AND t.project_classify_name != '工单问题汇总'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
  GROUP BY to_date(t.task_start_time),tou.user_email
)doi
ON main.email = doi.user_email AND main.days = doi.stat_date
-- 登记工时的任务
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as task_ones_id
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- 剔除无效工时
    AND i.issue_type_cname = '任务' AND t.project_classify_name != '工单问题汇总'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
  GROUP BY to_date(t.task_start_time),tou.user_email
)toi
ON main.email = toi.user_email AND main.days = toi.stat_date
-- 登记工时的缺陷
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as bug_ones_id
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- 剔除无效工时
    AND i.issue_type_cname = '缺陷' AND t.project_classify_name != '工单问题汇总'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
  GROUP BY to_date(t.task_start_time),tou.user_email
)boi
ON main.email = boi.user_email AND main.days = boi.stat_date
-- 登记工时的工单
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as work_order_ones_id
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- 剔除无效工时
    AND t.project_classify_name = '工单问题汇总'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
  GROUP BY to_date(t.task_start_time),tou.user_email
)woi
ON main.email = woi.user_email AND main.days = woi.stat_date
-- 技术&管理工作工时
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         round(COALESCE(SUM(t.task_spend_hours), 0), 2) as mgmt_work_hour
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- 剔除无效工时
    AND t.project_type_name = '技术&管理工作'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
  GROUP BY to_date(t.task_start_time),tou.user_email
)mwh
ON main.email = mwh.user_email AND main.days = mwh.stat_date
-- 无效工时
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         round(COALESCE(SUM(t.task_spend_hours), 0), 2) as ineffective_work_hour
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is null -- 无效工时
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
  GROUP BY to_date(t.task_start_time),tou.user_email
)iwh
ON main.email = iwh.user_email AND main.days = iwh.stat_date
-- 外部客户项目编码异常工时
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         round(COALESCE(SUM(t.task_spend_hours), 0), 2) as code_error_work_hour
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name = '外部客户项目' -- 只保留外部客户项目
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
    AND i.external_project_code IS NULL 
  GROUP BY to_date(t.task_start_time),tou.user_email
)cewh
ON main.email = cewh.user_email AND main.days = cewh.stat_date
-- 内部研发汇总工时
LEFT JOIN 
(
  SELECT tmp.stat_date,
         tmp.user_email,
         SUM(tmp.internal_project_wh) as internal_project_wh,
         concat_ws('\#\#\#',collect_set(cast(CONCAT(tmp.internal_project_summary,'\$\$\$',tmp.internal_project_wh) as string))) as internal_project_summary
  FROM
  (
    SELECT to_date(t.task_start_time) as stat_date,
		   tou.user_email,
           round(COALESCE(SUM(t.task_spend_hours), 0), 2) as internal_project_wh,
           CONCAT(IF(i.project_bpm_code is null,'未知项目编码',i.project_bpm_code),'-',IF(b.project_name is null,'未知项目名称',b.project_name)) as internal_project_summary
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
    ON tou.uuid = t.user_uuid
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
    ON t.task_uuid = i.uuid
    LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b
    ON b.d = '${pre1_date}' AND (IF(i.project_bpm_code is null,'未知项目编码',i.project_bpm_code) = b.project_code OR IF(i.project_bpm_code is null,'未知项目编码',i.project_bpm_code) = b.project_sale_code) AND ((b.data_source = 'BPM' AND b.project_type_name IN ('内部项目','公司级项目','硬件部项目','软件部项目') AND b.project_operation_state NOT IN ('9.项目暂停','10.项目暂停','11.项目取消')) OR (b.data_source = 'PMS' AND b.is_external_project = 0))
    WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- 剔除无效工时
      AND t.project_type_name = '内部研发项目' 
      -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
    GROUP BY to_date(t.task_start_time),tou.user_email,CONCAT(IF(i.project_bpm_code is null,'未知项目编码',i.project_bpm_code),'-',IF(b.project_name is null,'未知项目名称',b.project_name))
  )tmp
  GROUP BY tmp.stat_date,tmp.user_email
)ipwh
ON main.email = ipwh.user_email AND main.days = ipwh.stat_date
-- 外部项目汇总工时
LEFT JOIN 
(
  SELECT tmp.stat_date,
         tmp.user_email,
         SUM(tmp.external_project_wh) as external_project_wh,
         concat_ws('\#\#\#',collect_set(cast(CONCAT(tmp.external_project_summary,'\$\$\$',tmp.external_project_wh) as string))) as external_project_summary
  FROM
  (
    SELECT to_date(t.task_start_time) as stat_date,
		   tou.user_email,
           round(COALESCE(SUM(t.task_spend_hours), 0), 2) as external_project_wh,
           CONCAT(IF(i.external_project_code is null,'未知项目编码',nvl(b.project_code,i.external_project_code)),'-',IF(b.project_name is null,'未知项目名称',b.project_name)) as external_project_summary
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
    ON tou.uuid = t.user_uuid
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
    ON t.task_uuid = i.uuid
    LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b
    ON b.d = '${pre1_date}' AND IF(i.external_project_code like 'S-%',SUBSTRING(i.external_project_code,3) = b.project_code,(IF(i.external_project_code is null,'未知项目编码',i.external_project_code) = b.project_code OR IF(i.external_project_code is null,'未知项目编码',i.external_project_code) = b.project_sale_code))
    WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- 剔除无效工时
      AND t.project_type_name = '外部客户项目' 
      -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
    GROUP BY to_date(t.task_start_time),tou.user_email,CONCAT(IF(i.external_project_code is null,'未知项目编码',nvl(b.project_code,i.external_project_code)),'-',IF(b.project_name is null,'未知项目名称',b.project_name))
  )tmp
  GROUP BY tmp.stat_date,tmp.user_email
)epwh
ON main.email = epwh.user_email AND main.days = epwh.stat_date
-- ones需求工作项明细
LEFT JOIN 
(
  SELECT tmp.stat_date,
         tmp.user_email,
         concat_ws('\#\#\#',collect_set(cast(CONCAT(tmp.work_id,'\$\$\$',tmp.summary,'\$\$\$',tmp.stat_date,'\$\$\$',tmp.work_hour,'\$\$\$',tmp.task_status_cname) as string))) as ones_demand_detail
  FROM
  (
    SELECT to_date(t.task_start_time) as stat_date,
		   tou.user_email,
           round(COALESCE(SUM(t.task_spend_hours), 0), 2) as work_hour,
           i.\`number\` as work_id,
           i.summary,
           i.task_status_cname
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
    ON tou.uuid = t.user_uuid
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
    ON t.task_uuid = i.uuid
    WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null 
      AND t.project_type_name is not null -- 剔除无效工时
      AND i.issue_type_cname = '需求' AND i.project_classify_name != '工单问题汇总'
      -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
    GROUP BY to_date(t.task_start_time),tou.user_email,i.\`number\`,i.summary,i.task_status_cname
  )tmp
  GROUP BY tmp.stat_date,tmp.user_email
)odd
ON main.email = odd.user_email AND main.days = odd.stat_date
-- ones任务工作项明细
LEFT JOIN 
(
  SELECT tmp.stat_date,
         tmp.user_email,
         concat_ws('\#\#\#',collect_set(cast(CONCAT(tmp.work_id,'\$\$\$',tmp.summary,'\$\$\$',tmp.stat_date,'\$\$\$',tmp.work_hour,'\$\$\$',tmp.task_status_cname) as string))) as ones_task_detail
  FROM
  (
    SELECT to_date(t.task_start_time) as stat_date,
		   tou.user_email,
           round(COALESCE(SUM(t.task_spend_hours), 0), 2) as work_hour,
           i.\`number\` as work_id,
           i.summary,
           i.task_status_cname
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
    ON tou.uuid = t.user_uuid
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
    ON t.task_uuid = i.uuid
    WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null 
      AND t.project_type_name is not null -- 剔除无效工时
      AND i.issue_type_cname = '任务' AND i.project_classify_name != '工单问题汇总'
      -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
    GROUP BY to_date(t.task_start_time),tou.user_email,i.\`number\`,i.summary,i.task_status_cname
  )tmp
  GROUP BY tmp.stat_date,tmp.user_email
)otd
ON main.email = otd.user_email AND main.days = otd.stat_date
-- ones缺陷工作项明细
LEFT JOIN 
(
  SELECT tmp.stat_date,
         tmp.user_email,
         concat_ws('\#\#\#',collect_set(cast(CONCAT(tmp.work_id,'\$\$\$',tmp.summary,'\$\$\$',tmp.stat_date,'\$\$\$',tmp.work_hour,'\$\$\$',tmp.task_status_cname) as string))) as ones_bug_detail
  FROM
  (
    SELECT to_date(t.task_start_time) as stat_date,
		   tou.user_email,
           round(COALESCE(SUM(t.task_spend_hours), 0), 2) as work_hour,
           i.\`number\` as work_id,
           i.summary,
           i.task_status_cname
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
    ON tou.uuid = t.user_uuid
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
    ON t.task_uuid = i.uuid
    WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null 
      AND t.project_type_name is not null -- 剔除无效工时
      AND i.issue_type_cname = '缺陷' AND I.project_classify_name != '工单问题汇总'
      -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
    GROUP BY to_date(t.task_start_time),tou.user_email,i.\`number\`,i.summary,i.task_status_cname
  )tmp
  GROUP BY tmp.stat_date,tmp.user_email
)obd
ON main.email = obd.user_email AND main.days = obd.stat_date
-- ones工单工作项明细
LEFT JOIN 
(
  SELECT tmp.stat_date,
         tmp.user_email,
         concat_ws('\#\#\#',collect_set(cast(CONCAT(tmp.work_id,'\$\$\$',tmp.summary,'\$\$\$',tmp.stat_date,'\$\$\$',tmp.work_hour,'\$\$\$',tmp.task_status_cname) as string))) as ones_work_detail
  FROM
  (
    SELECT to_date(t.task_start_time) as stat_date,
		   tou.user_email,
           round(COALESCE(SUM(t.task_spend_hours), 0), 2) as work_hour,
           i.\`number\` as work_id,
           i.summary,
           i.task_status_cname
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
    ON tou.uuid = t.user_uuid
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
    ON t.task_uuid = i.uuid
    WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null 
      AND t.project_type_name is not null -- 剔除无效工时
      AND i.project_classify_name = '工单问题汇总'
      -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
    GROUP BY to_date(t.task_start_time),tou.user_email,i.\`number\`,i.summary,i.task_status_cname
  )tmp
  GROUP BY tmp.stat_date,tmp.user_email
)owd
ON main.email = owd.user_email AND main.days = owd.stat_date;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"