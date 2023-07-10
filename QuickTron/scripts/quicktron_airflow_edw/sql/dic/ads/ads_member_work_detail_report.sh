#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-09-13 创建
#-- 2 wangyingying 2022-12-16 增加出差、公出数据源
#-- 3 wangyingying 2023-01-11 调整出差、公出逻辑
# ------------------------------------------------------------------------------------------------


hive=/opt/module/hive-3.1.2/bin/hive
dim_dbname=dim
dwd_dbname=dwd
pre_dbname=pre
ads_dbname=ads


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
-- 成员工时周报(excel版)明细表 ads_member_work_detail_report 


WITH travel_detail AS 
(
  SELECT *,
         IF(t.rn1 = 1 AND t.rn2 = 1,1,0) AS is_valid -- 是否有效
  FROM 
  (
    SELECT tt.*,
           DENSE_RANK() OVER(partition by tt.originator_user_id,tt.stat_date order by tt.travel_days DESC) as rn1,
           ROW_NUMBER() OVER(PARTITION BY tt.originator_user_id,tt.stat_date,tt.period_type ORDER BY tt.period_type) as rn2
    FROM 
    (
      SELECT t.create_time,
             t.originator_user_id,
             t.travel_date AS stat_date,
             t.every_days AS travel_days,
             t.period_type,
             CASE WHEN t.period_type = '全天' THEN '全天出差'
                  WHEN t.period_type = '下午' THEN '下半天出差'
                  WHEN t.period_type = '上午' THEN '上半天出差' END AS travel_type,
             t.data_source
      FROM ${dwd_dbname}.dwd_dtk_process_business_travel_dayily_info_df t
      WHERE t.d = '${pre1_date}' AND IF(t.data_source = 'DTK',t.is_valid = 1 AND t.approval_result = 'agree' AND t.approval_status = 'COMPLETED',t.approval_status = '审批通过')
    )tt
  )t
),
business_detail AS 
(
  SELECT *,
         IF(t.rn1 = 1 AND t.rn2 = 1,1,0) AS is_valid -- 是否有效
  FROM 
  (
    SELECT tt.*,
           DENSE_RANK() OVER(partition by tt.originator_user_id,tt.stat_date order by tt.travel_days DESC) as rn1,
           ROW_NUMBER() OVER(PARTITION BY tt.originator_user_id,tt.stat_date,tt.period_type ORDER BY tt.period_type) as rn2
    FROM 
    (
      SELECT t.create_time,
             t.originator_user_id,
             t.attend_bus_date AS stat_date,
             t.every_days AS travel_days,
             t.period_type,
             CASE WHEN t.period_type = '全天' THEN '全天公出'
                  WHEN t.period_type = '下午' THEN '下半天公出'
                  WHEN t.period_type = '上午' THEN '上半天公出' END AS travel_type,
             t.data_source
      FROM ${dwd_dbname}.dwd_dtk_process_attendance_business_dayily_info_df t
      WHERE t.d = '${pre1_date}' AND IF(t.data_source = 'DTK',t.is_valid = 1 AND t.approval_result = 'agree' AND t.approval_status = 'COMPLETED',t.approval_status = '审批通过')
    )tt
  )t
),
work_day AS 
(
  SELECT nvl(tmp1.originator_user_id,nvl(tmp2.originator_user_id,tmp3.originator_user_id)) as originator_user_id,
         nvl(tmp1.stat_date,nvl(tmp2.stat_date,tmp3.stat_date)) as stat_date,
         tmp1.create_time as travel_create_time,
         tmp1.travel_type as travel_type,
         tmp2.create_time as work_home_create_time,
         tmp2.travel_type as work_home_type,
         tmp3.create_time as attend_bus_create_time,
         tmp3.travel_type as attend_bus_type
  FROM
  (
    SELECT t1.originator_user_id,
           t1.stat_date,
           t1.create_time,
           t1.travel_type,
           t1.travel_days,
           t1.data_source
    FROM travel_detail t1
    WHERE t1.is_valid = 1
  )tmp1
  FULL JOIN 
  (
    SELECT t1.originator_user_id,
           t1.stat_date,
           t1.create_time,
           t1.travel_type,
           t1.travel_days    
    FROM 
    (
      SELECT w.originator_user_id,
             w.create_time,
             cast(w.work_home_date as date) as stat_date,
             CASE when w.period_type = '全天' THEN '全天居家'
                  when w.period_type = '下午' THEN '下半天居家'
                  when w.period_type = '上午' THEN '上半天居家' end as travel_type,
             w.every_days as travel_days,
             row_number()over(PARTITION by w.originator_user_id,cast(w.work_home_date as date) order by w.create_time desc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_work_for_home_dayily_info_df w
      WHERE w.is_valid = 1 AND w.d = '${pre1_date}' AND w.approval_result = 'agree' AND w.approval_status = 'COMPLETED' 
    )t1
    WHERE t1.rn = 1 
  )tmp2
  ON tmp1.originator_user_id = tmp2.originator_user_id AND tmp1.stat_date = tmp2.stat_date
  FULL JOIN 
  (
    SELECT t1.originator_user_id,
           t1.stat_date,
           t1.create_time,
           t1.travel_type,
           t1.travel_days,
           t1.data_source
    FROM business_detail t1
    WHERE t1.is_valid = 1
  )tmp3
  ON tmp1.originator_user_id = tmp3.originator_user_id AND tmp1.stat_date = tmp3.stat_date
)


INSERT overwrite table ${ads_dbname}.ads_member_work_detail_report
SELECT  '' as id,
	   IF(tud.is_job = 0,tud.team_ft,tud.team_sub_group) as org_name, -- 离职填充一级部门，在职取最底层
       tud.user_name as team_member,
       tud.emp_id as dtk_emp_id,
	   tud.emp_position,
       cast(tud.days as date) as work_date,
       tud.day_type,
       cast(nvl(t1.work_hour, 0) as decimal(10, 2)) as work_hour, -- 登记工时
       cast(nvl(t2.clock_in_work_hour, 0) as decimal(10, 2)) as clock_in_hour, -- 打卡工时
       t2.attendance_working_time, -- 上班打卡时间
       t2.attendance_working_place, -- 上班打卡地点
       t2.attendance_off_time, -- 下班打卡时间
       t2.attendance_off_place, -- 下班打卡地点
       cast(nvl(t3.leave_days, 0) as decimal(10, 2)) as leave_days, -- 请假天数
       cast(nvl(t4.travel_days, 0) as decimal(10, 2)) as travel_days, -- 出差天数
       cast(nvl(t5.home_office_days, 0) as decimal(10, 2)) as home_office_days, -- 居家办公天数
       cast(nvl(t14.official_out_days, 0) as decimal(10, 2)) as official_out_days, -- 公出天数
       cast(nvl(t6.demand_qty, 0) as bigint) as demand_qty, -- 需求数量
       t6.demand_ones_ids, -- 需求id
       cast(nvl(t7.bug_qty, 0) as bigint) as bug_qty, -- 缺陷数量
       t7.bug_ones_ids, -- 缺陷id
       cast(nvl(t8.task_qty, 0) as bigint) as task_qty, -- 任务数量
       t8.task_ones_ids, -- 任务id
       cast(nvl(t9.work_order_qty, 0) as bigint) as work_order_qty, -- 工单数量
       t9.work_order_ones_ids, -- 工单id
       cast(nvl(t10.code_quantity, 0) as bigint) as add_lines_count, -- 代码贡献量
       cast(nvl(t11.internal_wh, 0) as decimal(10, 2)) as internal_wh, -- 内部研发工时
       nvl(cast(nvl(t11.internal_wh, 0) / nvl(t1.work_hour, 0) as decimal(10, 4)),0) as internal_occ, -- 内部工时占比
       cast(nvl(t12.external_wh, 0) as decimal(10, 2)) as external_wh, -- 外部研发工时
       nvl(cast(nvl(t12.external_wh, 0) / nvl(t1.work_hour, 0) as decimal(10, 4)),0) as external_occ, -- 外部工时占比
       cast(nvl(t13.mgmt_wh, 0) as decimal(10, 2)) as mgmt_wh, -- 技术&管理工作工时
       nvl(cast(nvl(t13.mgmt_wh, 0) / nvl(t1.work_hour, 0) as decimal(10, 4)),0) as mgmt_occ, -- 管理工时占比
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT tu.team_ft,
	     tu.team_sub_group,
         tu.emp_id,
         tu.emp_name as user_name,
         tu.email as user_email,
         tu.is_job,
         tu.emp_position,
         td.days,
         CASE when td.week_date = 1 then '周一'
              when td.week_date = 2 then '周二'
              when td.week_date = 3 then '周三'
              when td.week_date = 4 then '周四'
              when td.week_date = 5 then '周五'
              when td.week_date = 6 then '周六'
              when td.week_date = 7 then '周日' end as day_type    
  FROM
  (
    SELECT te.emp_id,
           te.emp_name,
           te.email,
           te.emp_position,
           te.is_job,
           te.hired_date,
           te.quit_date,
           m.dept_name as team_ft,
           m.org_name as team_sub_group
    FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
    LEFT JOIN 
    (
      SELECT m.emp_id,
             m.emp_name,
             m.org_start_date,
             m.org_end_date,
             m.dept_name,
             m.org_name,
             row_number()over(PARTITION by m.emp_id order by m.org_end_date desc)rn
      FROM ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df m
      WHERE m.d = DATE_ADD(CURRENT_DATE(), -1) AND m.is_valid = 1 AND m.is_need_fill_manhour = 1 AND (m.org_end_date = '9999-01-01' OR m.org_end_date <= DATE_ADD(CURRENT_DATE(), -1))
    )m
    ON te.emp_id = m.emp_id AND m.rn = 1 
    WHERE te.d = '${pre1_date}' AND te.org_company_name = '上海快仓智能科技有限公司' 
      AND (te.is_job = 1 OR te.quit_date <= '${pre1_date}')
      AND m.emp_id is not NULL
  ) tu
  LEFT JOIN 
  (
    SELECT DISTINCT days,
                    day_type,
                    week_date
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1 AND days >= '2021-01-01' AND days <= '${pre1_date}'
  ) td
  WHERE td.days >= tu.hired_date AND td.days <= IF(tu.is_job = 0,tu.quit_date,'${pre1_date}') 
)tud
-- 登记工时
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         t.user_uuid,
		 tou.user_email,
         round(COALESCE(sum(t.task_spend_hours), 0), 2) as work_hour
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除无效工时和违规登记
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email
)t1
ON t1.user_email = tud.user_email AND t1.stat_date = tud.days
-- 打卡工时
LEFT JOIN 
(
  SELECT e.emp_id,
         e.emp_name,
         e.att_checkin_work_date as attendance_work_date,
         e.att_checkin_start_time as attendance_working_time,
         IF(e.att_checkin_end_time != e.att_checkin_start_time,e.att_checkin_end_time,NULL) as attendance_off_time,
         IF(e.start_att_checkin_type = '钉钉签到',e.att_checkin_start_place,e.start_att_checkin_type) as attendance_working_place,
         IF(e.end_att_checkin_type = '钉钉签到',IF(e.att_checkin_end_time != e.att_checkin_start_time,e.att_checkin_end_place,NULL),IF(e.att_checkin_end_time != e.att_checkin_start_time,e.end_att_checkin_type,NULL)) as attendance_off_place,
         cast((unix_timestamp(e.att_checkin_end_time) - unix_timestamp(e.att_checkin_start_time))/3600 as decimal(10,1)) as clock_in_work_hour
  FROM ${dwd_dbname}.dwd_dtk_emp_attendance_checkin_day_info_di e
)t2
ON tud.emp_id = t2.emp_id AND tud.days = t2.attendance_work_date
-- 请假统计
LEFT JOIN 
(
  SELECT l1.originator_user_id,
         l1.stat_date,
         case when l2.leave_type is null THEN l1.leave_type else '全天请假' END as leave_type,
         case when l2.leave_type is null THEN l1.leave_days else 1 END as leave_days
  FROM 
  (
    SELECT l.originator_user_id,
           cast(l.leave_date as date) as stat_date,
           CASE when l.period_type = '全天' THEN '全天请假'
                when l.period_type = '下午' THEN '下半天请假'
                when l.period_type = '上午' THEN '上半天请假' 
                when l.period_type = '其它' THEN '哺乳假' end as leave_type,
           l.every_days as leave_days,
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
           cast(l.leave_date as date) as stat_date,
           CASE when l.period_type = '全天' THEN '全天请假'
                when l.period_type = '下午' THEN '下半天请假'
                when l.period_type = '上午' THEN '上半天请假' 
                when l.period_type = '其它' THEN '哺乳假' end as leave_type,
           l.every_days as leave_days,
           row_number()over(PARTITION by l.originator_user_id,cast(l.leave_date as date) order by CASE when l.period_type = '全天' THEN '全天请假'
                                                                                                       when l.period_type = '下午' THEN '下半天请假'
                                                                                                       when l.period_type = '上午' THEN '上半天请假' 
                                                                                                       when l.period_type = '其它' THEN '哺乳假' end asc)rn
    FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l
    WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = '${pre1_date}'
  )l2
  ON l1.originator_user_id = l2.originator_user_id AND l1.stat_date = l2.stat_date AND l1.leave_type != l2.leave_type
  WHERE l1.rn = 1 
)t3
ON t3.originator_user_id = tud.emp_id AND t3.stat_date = tud.days
-- 出差统计
LEFT JOIN 
(
  SELECT originator_user_id,
         stat_date,
         SUM(CASE when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '全天%' THEN 1
                  when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%半天%' THEN 0.5 end) as travel_days
  FROM work_day
  WHERE split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%出差'
  GROUP BY originator_user_id,stat_date
)t4
ON t4.originator_user_id = tud.emp_id AND t4.stat_date = tud.days
-- 居家统计
LEFT JOIN 
(
  SELECT originator_user_id,
         stat_date,
         SUM(CASE when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '全天%' THEN 1
                  when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%半天%' THEN 0.5 end) as home_office_days
  FROM work_day
  WHERE split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%居家'
  GROUP BY originator_user_id,stat_date
)t5
ON t5.originator_user_id = tud.emp_id AND t5.stat_date = tud.days
-- 公出统计
LEFT JOIN 
(
  SELECT originator_user_id,
         stat_date,
         SUM(CASE when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '全天%' THEN 1
                  when split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%半天%' THEN 0.5 end) as official_out_days
  FROM work_day
  WHERE split(dwd.get_attend_type(travel_create_time,travel_type,work_home_create_time,work_home_type,attend_bus_create_time,attend_bus_type),';')[1] like '%公出'
  GROUP BY originator_user_id,stat_date
)t14
ON t14.originator_user_id = tud.emp_id AND t14.stat_date = tud.days
-- 登记工时的需求
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
		 COUNT(DISTINCT i.\`number\`) as demand_qty,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as demand_ones_ids
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- 剔除无效工时
    AND i.issue_type_cname = '需求' AND t.project_classify_name != '工单问题汇总'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
  GROUP BY to_date(t.task_start_time),tou.user_email
)t6
ON tud.user_email = t6.user_email AND tud.days = t6.stat_date
-- 登记工时的缺陷
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
		 COUNT(DISTINCT i.\`number\`) as bug_qty,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as bug_ones_ids
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- 剔除无效工时
    AND i.issue_type_cname = '缺陷' AND t.project_classify_name != '工单问题汇总'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
  GROUP BY to_date(t.task_start_time),tou.user_email
)t7
ON tud.user_email = t7.user_email AND tud.days = t7.stat_date
-- 登记工时的任务
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
		 COUNT(DISTINCT i.\`number\`) as task_qty,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as task_ones_ids
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- 剔除无效工时
    AND i.issue_type_cname = '任务' AND t.project_classify_name != '工单问题汇总'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
  GROUP BY to_date(t.task_start_time),tou.user_email
)t8
ON tud.user_email = t8.user_email AND tud.days = t8.stat_date
-- 登记工时的工单
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
		 COUNT(DISTINCT i.\`number\`) as work_order_qty,
         concat_ws(',',collect_set(cast(i.\`number\` as string))) as work_order_ones_ids
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful i 
  ON t.task_uuid = i.uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- 剔除无效工时
    AND t.project_classify_name = '工单问题汇总'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
  GROUP BY to_date(t.task_start_time),tou.user_email
)t9
ON tud.user_email = t9.user_email AND tud.days = t9.stat_date
-- 代码量统计
LEFT JOIN 
(
  SELECT to_date(t1.git_commit_date) as stat_date,
         t1.git_author_email as true_email,
         SUM(IF(nvl(t1.add_lines_count,0) >= 2000,2000,nvl(t1.add_lines_count,0))) as code_quantity
  FROM ${dwd_dbname}.dwd_git_commit_detail_info_da t1
  WHERE t1.git_repository NOT LIKE '%software/phoenix/aio/phoenix-rcs-aio.git'
  GROUP BY to_date(t1.git_commit_date),t1.git_author_email
)t10
ON t10.true_email = tud.user_email AND t10.stat_date = tud.days
-- 内部研发汇总工时
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         round(COALESCE(SUM(t.task_spend_hours), 0), 2) as internal_wh
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- 剔除无效工时
    AND t.project_type_name = '内部研发项目' 
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
  GROUP BY to_date(t.task_start_time),tou.user_email
)t11
ON tud.user_email = t11.user_email AND tud.days = t11.stat_date
-- 外部项目汇总工时
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
         tou.user_email,
         round(COALESCE(SUM(t.task_spend_hours), 0), 2) as external_wh
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- 剔除无效工时
    AND t.project_type_name = '外部客户项目' 
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
  GROUP BY to_date(t.task_start_time),tou.user_email
)t12
ON tud.user_email = t12.user_email AND tud.days = t12.stat_date
-- 技术&管理工作工时
LEFT JOIN 
(
  SELECT to_date(t.task_start_time) as stat_date,
		 tou.user_email,
         round(COALESCE(SUM(t.task_spend_hours), 0), 2) as mgmt_wh
  FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
  LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
  ON tou.uuid = t.user_uuid
  WHERE 1 = 1 AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null AND t.project_type_name is not null -- 剔除无效工时
    AND t.project_type_name = '技术&管理工作'
    -- AND DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) <= 7 -- 剔除违规登记
  GROUP BY to_date(t.task_start_time),tou.user_email
)t13
ON tud.user_email = t13.user_email AND tud.days = t13.stat_date;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"