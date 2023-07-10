#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-12-08 创建
#-- 2 wangyingying 2022-12-13 增加项目分类字段
#-- 3 wangyingying 2022-12-14 修改取值逻辑
#-- 4 wangyingying 2022-12-15 修改取值逻辑
#-- 5 wangyingying 2023-01-03 增加归属月份字段
#-- 6 wangyingying 2023-02-27 增加及时解决缺陷数量、修复时长字段
#-- 7 wangyingying 2023-03-23 增加ones项目分类
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
-- set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--研发成员质量统计表 ads_team_ft_rd_member_quality_count

WITH days AS 
(
  SELECT TO_DATE(d1.start_date) AS start_date,
         TO_DATE(nvl(d2.end_date,'${pre1_date}')) AS end_date,
         CONCAT(d1.start_date,'~',nvl(d2.end_date,'${pre1_date}')) AS date_scope,
         TO_DATE(CONCAT(SUBSTR(d1.start_date,1,7),'-','01')) AS month_date
  FROM 
  (
    SELECT d.days AS start_date,
           d.week_date, 
           d.week_year_date,
           ROW_NUMBER()OVER(ORDER BY d.days) rn
    FROM ${dim_dbname}.dim_day_date d
    WHERE d.days >= '2020-12-31' AND d.days <= '${pre1_date}' AND d.week_date = 4 -- 上周四
  )d1
  LEFT JOIN 
  (
    SELECT d.days AS end_date,
           d.week_date,
           d.week_year_date,
           ROW_NUMBER()OVER(ORDER BY d.days) rn
    FROM ${dim_dbname}.dim_day_date d
    WHERE d.days >= '2020-12-31' AND d.week_date = 3 -- 本周三
  )d2
  ON d1.rn = d2.rn
  WHERE '${pre1_date}' >= TO_DATE(d1.start_date) AND '${pre1_date}' <= TO_DATE(nvl(d2.end_date,'${pre1_date}'))
),
total_bug_num AS
(
  SELECT d.start_date, -- 开始日期
         d.end_date, -- 结束日期
         d.date_scope, -- 日期区间
         t.bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         t.project_classify_name, -- 项目分类名称
         t.severity_level, -- 严重等级
         COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2]) AS task_belong_email, -- 人员邮箱
         t.uuid -- 新增缺陷数量
  FROM days d 
  LEFT JOIN 
  (
    SELECT t.uuid, -- 工作项uuid
           TO_DATE(t.task_create_time) AS task_create_date, -- 工作项创建时间
           t.project_classify_name, -- 项目类型名称
           IF(t.project_classify_name = '工单问题汇总','线上缺陷','线下缺陷') as bug_type, -- 缺陷类型
           nvl(t.severity_level,'未知') AS severity_level, -- 严重等级
           t.defect_source_user_email, -- 缺陷来源人邮箱
           t.task_repair_user_email, -- 修复人邮箱
           t.task_assign_email, -- 负责人邮箱
           ARRAY(t.defect_source_user_email,t.task_repair_user_email,t.task_assign_email) AS task_belong_email -- 最后统计归属人邮箱
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    WHERE t.issue_type_cname ='缺陷' -- 缺陷
      AND (t.severity_level IN (1,2,3) OR t.severity_level IS NULL) -- 严重等级为3级以上或为空
      AND t.status = 1 -- 工作项有效未被删除
      AND (t.defect_validity = '有效' OR t.defect_validity IS NULL) -- 缺陷有效
      AND t.project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]2.11WES产品包','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[智能搬运FT]锂电产品线项目','[硬件自动化]智驾车载软件、硬件开发') -- 项目类型
	  AND t.task_create_time >= '2022-10-27' -- 创建时间大于2022-10-27
  )t
  ON t.task_create_date >= d.start_date AND t.task_create_date <= d.end_date -- 创建时间在周期范围内
  GROUP BY d.start_date,d.end_date,d.date_scope,t.bug_type,t.project_classify_name,t.severity_level,COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2]),t.uuid
  UNION ALL 
  SELECT d.start_date, -- 开始日期
         d.end_date, -- 结束日期
         d.date_scope, -- 日期区间
         t.bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         t.project_classify_name, -- 项目分类名称
         t.severity_level, -- 严重等级
         COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2]) AS task_belong_email, -- 人员邮箱
         t.uuid -- 本期修复并验证缺陷数量
  FROM days d 
  LEFT JOIN 
  (
    SELECT t.uuid,
           TO_DATE(t.task_create_time) AS task_create_date,
           t.project_classify_name,
           IF(t.project_classify_name = '工单问题汇总','线上缺陷','线下缺陷') as bug_type,
           nvl(t.severity_level,'未知') AS severity_level,
           t.defect_source_user_email,
           t.task_repair_user_email,
           t.task_assign_email,
           c.task_process_date,
           ARRAY(t.defect_source_user_email,t.task_repair_user_email,t.task_assign_email) AS task_belong_email
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    LEFT JOIN 
    (
      SELECT c.task_uuid,
             TO_DATE(c.task_process_time) AS task_process_date,
             c.task_process_field,
             c.new_task_field_value,
             c.task_process_user,
             ROW_NUMBER()OVER(PARTITION BY c.task_uuid ORDER BY c.task_process_time DESC) rn
      FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
      WHERE c.task_process_field = 'field005' AND c.new_task_field_value = '已关闭'
    )c
    ON c.task_uuid = t.uuid AND c.rn = 1
    WHERE t.issue_type_cname ='缺陷' -- 缺陷
      AND (t.severity_level IN (1,2,3) OR t.severity_level IS NULL) -- 严重等级为3级以上或为空
      AND t.status = 1 -- 工作项有效未被删除
      AND (t.defect_validity = '有效' OR t.defect_validity IS NULL) -- 缺陷有效
      AND t.project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]2.11WES产品包','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[智能搬运FT]锂电产品线项目','[硬件自动化]智驾车载软件、硬件开发') -- 项目类型
	  AND t.task_create_time >= '2022-10-27' -- 创建时间大于2022-10-27
  )t
  ON t.task_process_date >= d.start_date AND t.task_process_date <= d.end_date
  GROUP BY d.start_date,d.end_date,d.date_scope,t.bug_type,t.project_classify_name,t.severity_level,COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2]),t.uuid
)


INSERT overwrite table ${ads_dbname}.ads_team_ft_rd_member_quality_count
SELECT '' AS id, -- 主键
       d.start_date, -- 开始日期
       d.end_date, -- 结束日期
       d.date_scope, -- 日期范围
       d.month_date, -- 所属月份
       bt.bug_type, -- 缺陷类型
       pc.project_classify_name, -- 项目分类名称
       sl.severity_level, -- 严重等级
       emp.team_ft, -- 一级部门
       emp.team_group, -- 二级部门
       emp.team_sub_group, -- 三级部门
       emp.team_last_group, -- 四级部门
       emp.emp_name AS team_member, -- 人员名称
       emp.is_count, -- 是否计算人均
       nvl(t1.new_bug_num,0) AS new_bug_num, -- 本期新增缺陷数量|分配给FT团队成员的缺陷总数
       nvl(t2.solve_bug_num,0) AS solve_bug_num, -- 本期已修复并验证缺陷数量
       nvl(t3.total_bug_num,0) AS total_bug_num, -- 缺陷总数
       nvl(t4.best_find_bug_num,0) AS best_find_bug_num, -- 本期缺陷最佳发现时机-实现阶段缺陷数量
       nvl(t5.total_pending_bug_num,0) AS total_pending_bug_num, -- 当前累计挂起缺陷数量
       nvl(t6.repairing_bug_num,0) AS repairing_bug_num, -- 本期修复中缺陷数量
       nvl(t7.unpending_bug_num,0) AS unpending_bug_num, -- 本期解挂缺陷数量
       nvl(t8.pending_bug_num,0) AS pending_bug_num, -- 本期挂起缺陷数量
       nvl(t9.history_legacy_bug_num,0) AS history_legacy_bug_num, -- 历史遗留缺陷数量
       nvl(t10.history_pending_bug_num,0) AS history_pending_bug_num, -- 历史挂起缺陷数量
       nvl(t11.solve_bug_num_new,0) AS solve_bug_num_new, -- 本期创建且已修复并验证缺陷数量
       nvl(t12.best_find_bug_xqps_num,0) AS best_find_bug_xqps_num, -- 缺陷最佳发现阶段：'需求评审' 
       nvl(t12.best_find_bug_scbs_num,0) AS best_find_bug_scbs_num, -- 缺陷最佳发现阶段：'生产部署' 
       nvl(t12.best_find_bug_svtcs_num,0) AS best_find_bug_svtcs_num, -- 缺陷最佳发现阶段：'SVT测试' 
       nvl(t12.best_find_bug_fatcs_num,0) AS best_find_bug_fatcs_num, -- 缺陷最佳发现阶段：'FAT测试' 
       nvl(t12.best_find_bug_kfsj_num,0) AS best_find_bug_kfsj_num, -- 缺陷最佳发现阶段：'开发设计' 
       nvl(t12.best_find_bug_scsyx_num,0) AS best_find_bug_scsyx_num, -- 缺陷最佳发现阶段：'生产试运行' 
       nvl(t12.best_find_bug_dgncs_num,0) AS best_find_bug_dgncs_num, -- 缺陷最佳发现阶段：'单功能测试' 
       nvl(t12.best_find_bug_jccs_num,0) AS best_find_bug_jccs_num, -- 缺陷最佳发现阶段：'集成测试' 
       nvl(t12.best_find_bug_sxjd_num,0) AS best_find_bug_sxjd_num, -- 缺陷最佳发现阶段：'实现阶段'
       nvl(t13.find_bug_dgncs_num,0) AS find_bug_dgncs_num, -- 缺陷发现阶段：'单功能测试' 
       nvl(t13.find_bug_scbs_num,0) AS find_bug_scbs_num, -- 缺陷发现阶段：'生产部署' 
       nvl(t13.find_bug_jccs_num,0) AS find_bug_jccs_num, -- 缺陷发现阶段：'集成测试' 
       nvl(t13.find_bug_scsyx_num,0) AS find_bug_scsyx_num, -- 缺陷发现阶段：'生产试运行' 
       nvl(t13.find_bug_fatcs_num,0) AS find_bug_fatcs_num, -- 缺陷发现阶段：'FAT测试' 
       nvl(t13.find_bug_shyx_num,0) AS find_bug_shyx_num, -- 缺陷发现阶段：'售后运行' 
       nvl(t13.find_bug_svtcs_num,0) AS find_bug_svtcs_num, -- 缺陷发现阶段：'SVT测试'
       nvl(t14.timely_solve_num,0) AS timely_solve_num, -- 及时解决缺陷数量
       nvl(t14.solve_duration,0) AS solve_duration, -- 修复时长
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time      
FROM days d
-- 线上缺陷|线下缺陷
LEFT JOIN
(
  SELECT '线上缺陷' AS bug_type 
  UNION ALL
  SELECT '线下缺陷' AS bug_type -- 目前第一期只有线下缺陷，线上缺陷均为0
)bt
-- 严重等级
LEFT JOIN
(
  SELECT '1' AS severity_level 
  UNION ALL
  SELECT '2' AS severity_level
  UNION ALL
  SELECT '3' AS severity_level
  UNION ALL
  SELECT '未知' AS severity_level
)sl
-- 项目分类名称
LEFT JOIN
(
  SELECT project_classify_name
  FROM ${dwd_dbname}.dwd_ones_project_classify_info_ful
  WHERE project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]2.11WES产品包','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[智能搬运FT]锂电产品线项目','[硬件自动化]智驾车载软件、硬件开发')
)pc
-- 人员离线数据
LEFT JOIN
(
  SELECT m.dept_name AS team_ft, -- 一级部门
         m.team_org_name_map[\"team1\"] AS team_group, -- 二级部门
         IF(m.emp_name IN ('艾纯亮','周兴海'),'LES产研组',m.team_org_name_map[\"team2\"]) AS team_sub_group, -- 三级部门
         m.team_org_name_map[\"team3\"] AS team_last_group, -- 四级部门
         m.emp_id, -- 人员编码
         m.emp_name, -- 人员名称
         m.email, -- 人员邮箱
         IF(r.emp_id IS NULL,1,0) AS is_count -- 是否计算人均
  FROM 
  (
    SELECT *,ROW_NUMBER()OVER(PARTITION BY m.emp_id ORDER BY m.org_name DESC) rn -- 去重
    FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
    WHERE m.is_rd_emp = 1
  )m
  LEFT JOIN  
  (
    SELECT *,ROW_NUMBER()OVER(PARTITION BY r.emp_id ORDER BY r.org_path_name DESC) rn -- 去重
    FROM ${dim_dbname}.dim_dtk_org_role_info_offline r
    WHERE r.is_org_role = 5 -- 是否研发质量需剔除人员
  )r
  ON m.emp_id = r.emp_id AND m.org_id = r.org_name AND r.rn = 1
  WHERE m.rn = 1
)emp
-- 本期新增缺陷数量 => 创建时间在周期范围内 + 优先取缺陷来源人 其次取修复人 最后取负责人
LEFT JOIN 
(
  SELECT d.start_date, -- 开始日期
         d.end_date, -- 结束日期
         d.date_scope, -- 日期区间
         t.bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         t.project_classify_name, -- 项目分类名称
         t.severity_level, -- 严重等级
         COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2]) AS task_belong_email, -- 人员邮箱
         COUNT(DISTINCT t.uuid) AS new_bug_num -- 本期新增缺陷数量
  FROM days d 
  LEFT JOIN 
  (
    SELECT t.uuid, -- 工作项uuid
           TO_DATE(t.task_create_time) AS task_create_date, -- 工作项创建时间
           t.project_classify_name, -- 项目类型名称
           IF(t.project_classify_name = '工单问题汇总','线上缺陷','线下缺陷') as bug_type, -- 缺陷类型
           nvl(t.severity_level,'未知') AS severity_level, -- 严重等级
           t.defect_source_user_email, -- 缺陷来源人邮箱
           t.task_repair_user_email, -- 修复人邮箱
           t.task_assign_email, -- 负责人邮箱
           ARRAY(t.defect_source_user_email,t.task_repair_user_email,t.task_assign_email) AS task_belong_email -- 最后统计归属人邮箱
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    WHERE t.issue_type_cname ='缺陷' -- 缺陷
      AND (t.severity_level IN (1,2,3) OR t.severity_level IS NULL) -- 严重等级为3级以上或为空
      AND t.status = 1 -- 工作项有效未被删除
      AND (t.defect_validity = '有效' OR t.defect_validity IS NULL) -- 缺陷有效
      AND t.project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]2.11WES产品包','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[智能搬运FT]锂电产品线项目','[硬件自动化]智驾车载软件、硬件开发') -- 项目类型
	  AND t.task_create_time >= '2022-10-27' -- 创建时间大于2022-10-27
  )t
  ON t.task_create_date >= d.start_date AND t.task_create_date <= d.end_date -- 创建时间在周期范围内
  GROUP BY d.start_date,d.end_date,d.date_scope,t.bug_type,t.project_classify_name,t.severity_level,COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2])
)t1
ON t1.start_date = d.start_date AND t1.end_date = d.end_date AND t1.date_scope = d.date_scope AND t1.bug_type = bt.bug_type AND t1.task_belong_email = emp.email AND t1.project_classify_name = pc.project_classify_name AND t1.severity_level = sl.severity_level
-- 本期已修复并验证缺陷数量 => 创建时间无限制 + 状态切换为已关闭的时间在周期范围内 + 优先取缺陷来源人 其次取修复人 最后取负责人
LEFT JOIN 
(
  SELECT d.start_date, -- 开始日期
         d.end_date, -- 结束日期
         d.date_scope, -- 日期区间
         t.bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         t.project_classify_name, -- 项目分类名称
         t.severity_level, -- 严重等级
         COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2]) AS task_belong_email, -- 人员邮箱
         COUNT(DISTINCT t.uuid) AS solve_bug_num -- 本期修复并验证缺陷数量
  FROM days d 
  LEFT JOIN 
  (
    SELECT t.uuid,
           TO_DATE(t.task_create_time) AS task_create_date,
           t.project_classify_name,
           IF(t.project_classify_name = '工单问题汇总','线上缺陷','线下缺陷') as bug_type,
           nvl(t.severity_level,'未知') AS severity_level,
           t.defect_source_user_email,
           t.task_repair_user_email,
           t.task_assign_email,
           c.task_process_date,
           ARRAY(t.defect_source_user_email,t.task_repair_user_email,t.task_assign_email) AS task_belong_email
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    LEFT JOIN 
    (
      SELECT c.task_uuid,
             TO_DATE(c.task_process_time) AS task_process_date,
             c.task_process_field,
             c.new_task_field_value,
             c.task_process_user,
             ROW_NUMBER()OVER(PARTITION BY c.task_uuid ORDER BY c.task_process_time DESC) rn
      FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
      WHERE c.task_process_field = 'field005' AND c.new_task_field_value = '已关闭'
    )c
    ON c.task_uuid = t.uuid AND c.rn = 1
    WHERE t.issue_type_cname ='缺陷' -- 缺陷
      AND (t.severity_level IN (1,2,3) OR t.severity_level IS NULL) -- 严重等级为3级以上或为空
      AND t.status = 1 -- 工作项有效未被删除
      AND (t.defect_validity = '有效' OR t.defect_validity IS NULL) -- 缺陷有效
      AND t.project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]2.11WES产品包','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[智能搬运FT]锂电产品线项目','[硬件自动化]智驾车载软件、硬件开发') -- 项目类型
	  AND t.task_create_time >= '2022-10-27' -- 创建时间大于2022-10-27
  )t
  ON t.task_process_date >= d.start_date AND t.task_process_date <= d.end_date
  GROUP BY d.start_date,d.end_date,d.date_scope,t.bug_type,t.project_classify_name,t.severity_level,COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2])
)t2
ON t2.start_date = d.start_date AND t2.end_date = d.end_date AND t2.date_scope = d.date_scope AND t2.bug_type = bt.bug_type AND t2.task_belong_email = emp.email AND t2.project_classify_name = pc.project_classify_name AND t2.severity_level = sl.severity_level
-- 分配给FT团队成员的缺陷总数 => 创建时间无限制且周期内修复并验证的缺陷 + 创建时间在周期内新增的缺陷 => 有修复人:缺陷填写修复人的时间在时间范围内 | 无修复人：缺陷创建时间在时间范围内）  + 优先取修复人其次取负责人
LEFT JOIN 
(
  SELECT start_date, -- 开始日期
         end_date, -- 结束日期
         date_scope, -- 日期区间
         bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         project_classify_name, -- 项目分类名称
         severity_level, -- 严重等级
         task_belong_email, -- 人员邮箱
         COUNT(DISTINCT uuid) AS total_bug_num -- 本期总缺陷数量    
  FROM total_bug_num
  GROUP BY start_date,end_date,date_scope,bug_type,project_classify_name,severity_level,task_belong_email
)t3
ON t3.start_date = d.start_date AND t3.end_date = d.end_date AND t3.date_scope = d.date_scope AND t3.bug_type = bt.bug_type AND t3.task_belong_email = emp.email AND t3.project_classify_name = pc.project_classify_name AND t3.severity_level = sl.severity_level
-- 本期缺陷最佳发现时机-实现阶段的缺陷数量 => 缺陷切换状态的时间在时间范围内 + 优先取修复人其次取负变更人
LEFT JOIN 
(
  SELECT t.start_date, -- 开始日期
         t.end_date, -- 结束日期
         t.date_scope, -- 日期区间
         t.bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         t.project_classify_name, -- 项目分类名称
         t.severity_level, -- 严重等级
         t.task_belong_email, -- 人员邮箱
         COUNT(DISTINCT c.task_uuid) AS best_find_bug_num -- 本期缺陷最佳发现时机-实现阶段的缺陷数量
  FROM 
  (
    SELECT start_date, -- 开始日期
           end_date, -- 结束日期
           date_scope, -- 日期区间
           bug_type, -- 缺陷类型：线上缺陷|线下缺陷
           project_classify_name, -- 项目分类名称
           severity_level, -- 严重等级
           task_belong_email, -- 人员邮箱
           uuid -- 本期总缺陷数量    
    FROM total_bug_num
    GROUP BY start_date,end_date,date_scope,bug_type,project_classify_name,severity_level,task_belong_email,uuid
  )t
  LEFT JOIN 
  (
    SELECT c.task_uuid,
           TO_DATE(c.task_process_time) AS task_process_date,
           c.task_process_field,
           c.new_task_field_value,
           c.task_process_user,
           ROW_NUMBER()OVER(PARTITION BY c.task_uuid ORDER BY c.task_process_time DESC) rn
    FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
    WHERE c.task_process_field = '5Uo3GJs5' AND c.new_task_field_value IN ('实现阶段')
  )c
  ON c.task_uuid = t.uuid AND c.rn = 1
  GROUP BY t.start_date,t.end_date,t.date_scope,t.bug_type,t.project_classify_name,t.severity_level,t.task_belong_email
)t4
ON t4.start_date = d.start_date AND t4.end_date = d.end_date AND t4.date_scope = d.date_scope AND t4.bug_type = bt.bug_type AND t4.task_belong_email = emp.email AND t4.project_classify_name = pc.project_classify_name AND t4.severity_level = sl.severity_level
-- 当前累计挂起缺陷数量 => 所有时间范围的缺陷处于延期修复状态
LEFT JOIN 
(    
  SELECT d.start_date, -- 开始日期
         d.end_date, -- 结束日期
         d.date_scope, -- 日期区间
         t.bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         t.project_classify_name, -- 项目分类名称
         t.severity_level, -- 严重等级
         COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2]) AS task_belong_email, -- 人员邮箱
         COUNT(DISTINCT t.uuid) AS total_pending_bug_num -- 当前累计挂起缺陷数量
  FROM days d 
  LEFT JOIN 
  (
    SELECT t.uuid,
           TO_DATE(t.task_create_time) AS task_create_date,
           t.project_classify_name,
           IF(t.project_classify_name = '工单问题汇总','线上缺陷','线下缺陷') as bug_type,
           nvl(t.severity_level,'未知') AS severity_level,
           t.task_status_cname,
           t.defect_source_user_email,
           t.task_repair_user_email,
           t.task_assign_email,
           ARRAY(t.defect_source_user_email,t.task_repair_user_email,t.task_assign_email) AS task_belong_email
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    WHERE t.issue_type_cname ='缺陷' -- 缺陷
     AND (t.severity_level IN (1,2,3) OR t.severity_level IS NULL) -- 严重等级为3级以上或为空
      AND t.status = 1 -- 工作项有效未被删除
      AND (t.defect_validity = '有效' OR t.defect_validity IS NULL) -- 缺陷有效
      AND t.project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]2.11WES产品包','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[智能搬运FT]锂电产品线项目','[硬件自动化]智驾车载软件、硬件开发') -- 项目类型
	  AND t.task_create_time >= '2022-10-27' -- 创建时间大于2022-10-27
	  AND t.task_status_cname = '延期修复'
  )t
  ON t.task_create_date <= d.end_date
  GROUP BY d.start_date,d.end_date,d.date_scope,t.bug_type,t.project_classify_name,t.severity_level,COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2])
)t5
ON t5.start_date = d.start_date AND t5.end_date = d.end_date AND t5.date_scope = d.date_scope AND t5.bug_type = bt.bug_type AND t5.task_belong_email = emp.email AND t5.project_classify_name = pc.project_classify_name AND t5.severity_level = sl.severity_level
-- 本期修复中缺陷数量 => 所有时间范围的缺陷处于激活|已修复状态
LEFT JOIN 
(    
  SELECT d.start_date, -- 开始日期
         d.end_date, -- 结束日期
         d.date_scope, -- 日期区间
         t.bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         t.project_classify_name, -- 项目分类名称
         t.severity_level, -- 严重等级
         COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2]) AS task_belong_email, -- 人员邮箱
         COUNT(DISTINCT t.uuid) AS repairing_bug_num -- 本期修复中缺陷数量
  FROM days d 
  LEFT JOIN 
  (
    SELECT t.uuid,
           TO_DATE(t.task_create_time) AS task_create_date,
           t.project_classify_name,
           IF(t.project_classify_name = '工单问题汇总','线上缺陷','线下缺陷') as bug_type,
           nvl(t.severity_level,'未知') AS severity_level,
           t.task_status_cname,
           t.defect_source_user_email,
           t.task_repair_user_email,
           t.task_assign_email,
           ARRAY(t.defect_source_user_email,t.task_repair_user_email,t.task_assign_email) AS task_belong_email
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    WHERE t.issue_type_cname ='缺陷' -- 缺陷
     AND (t.severity_level IN (1,2,3) OR t.severity_level IS NULL) -- 严重等级为3级以上或为空
      AND t.status = 1 -- 工作项有效未被删除
      AND (t.defect_validity = '有效' OR t.defect_validity IS NULL) -- 缺陷有效
      AND t.project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]2.11WES产品包','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[智能搬运FT]锂电产品线项目','[硬件自动化]智驾车载软件、硬件开发') -- 项目类型
	  AND t.task_create_time >= '2022-10-27' -- 创建时间大于2022-10-27
	  AND t.task_status_cname IN ('激活','已修复')
  )t
  ON t.task_create_date <= d.end_date
  GROUP BY d.start_date,d.end_date,d.date_scope,t.bug_type,t.project_classify_name,t.severity_level,COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2])
)t6
ON t6.start_date = d.start_date AND t6.end_date = d.end_date AND t6.date_scope = d.date_scope AND t6.bug_type = bt.bug_type AND t6.task_belong_email = emp.email AND t6.project_classify_name = pc.project_classify_name AND t6.severity_level = sl.severity_level
-- 本期解挂缺陷数量 => 切换状态时间在周期范围内 + 优先取变更人其次取负责人
LEFT JOIN 
(
  SELECT d.start_date, -- 开始日期
         d.end_date, -- 结束日期
         d.date_scope, -- 日期区间
         t.bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         t.project_classify_name, -- 项目分类名称
         t.severity_level, -- 严重等级
         COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2]) AS task_belong_email, -- 人员邮箱
         COUNT(DISTINCT t.uuid) AS unpending_bug_num -- 本期解挂缺陷数量
  FROM days d 
  LEFT JOIN 
  (
    SELECT t.uuid,
           TO_DATE(t.task_create_time) AS task_create_date,
           t.project_classify_name,
           IF(t.project_classify_name = '工单问题汇总','线上缺陷','线下缺陷') as bug_type,
           nvl(t.severity_level,'未知') AS severity_level,
           t.task_status_cname,
           t.defect_source_user_email,
           t.task_repair_user_email,
           t.task_assign_email,
           c.task_process_date,
           ARRAY(t.defect_source_user_email,t.task_repair_user_email,t.task_assign_email) AS task_belong_email
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    LEFT JOIN 
    (
      SELECT c.task_uuid,
             TO_DATE(c.task_process_time) AS task_process_date,
             c.task_process_field,
             c.new_task_field_value,
             c.task_process_user,
             ROW_NUMBER()OVER(PARTITION BY c.task_uuid ORDER BY c.task_process_time DESC) rn
      FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
      WHERE c.task_process_field = 'field005' AND c.old_task_field_value = '延期修复'
    )c
    ON c.task_uuid = t.uuid AND c.rn = 1
    WHERE t.issue_type_cname ='缺陷' -- 缺陷
      AND (t.severity_level IN (1,2,3) OR t.severity_level IS NULL) -- 严重等级为3级以上或为空
      AND t.status = 1 -- 工作项有效未被删除
      AND (t.defect_validity = '有效' OR t.defect_validity IS NULL) -- 缺陷有效
      AND t.project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]2.11WES产品包','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[智能搬运FT]锂电产品线项目','[硬件自动化]智驾车载软件、硬件开发') -- 项目类型
      AND t.task_create_time >= '2022-10-27' -- 创建时间大于2022-10-27
	  AND t.task_status_cname = c.new_task_field_value
  )t
  ON t.task_process_date >= d.start_date AND t.task_process_date <= d.end_date
  GROUP BY d.start_date,d.end_date,d.date_scope,t.bug_type,t.project_classify_name,t.severity_level,COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2])
)t7
ON t7.start_date = d.start_date AND t7.end_date = d.end_date AND t7.date_scope = d.date_scope AND t7.bug_type = bt.bug_type AND t7.task_belong_email = emp.email AND t7.project_classify_name = pc.project_classify_name AND t7.severity_level = sl.severity_level
-- 本期挂起缺陷数量 => 变更时间在时间范围内 + 优先取变更人其次取负责人
LEFT JOIN 
(
  SELECT d.start_date, -- 开始日期
         d.end_date, -- 结束日期
         d.date_scope, -- 日期区间
         t.bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         t.project_classify_name, -- 项目分类名称
         t.severity_level, -- 严重等级
         COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2]) AS task_belong_email, -- 人员邮箱
         COUNT(DISTINCT t.uuid) AS pending_bug_num -- 本期挂起缺陷数量
  FROM days d 
  LEFT JOIN 
  (
    SELECT t.uuid,
           TO_DATE(t.task_create_time) AS task_create_date,
           t.project_classify_name,
           IF(t.project_classify_name = '工单问题汇总','线上缺陷','线下缺陷') as bug_type,
           nvl(t.severity_level,'未知') AS severity_level,
           t.task_status_cname,
           t.defect_source_user_email,
           t.task_repair_user_email,
           t.task_assign_email,
           c.task_process_date,
           ARRAY(t.defect_source_user_email,t.task_repair_user_email,t.task_assign_email) AS task_belong_email
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    LEFT JOIN 
    (
      SELECT c.task_uuid,
             TO_DATE(c.task_process_time) AS task_process_date,
             c.task_process_field,
             c.new_task_field_value,
             c.task_process_user,
             ROW_NUMBER()OVER(PARTITION BY c.task_uuid ORDER BY c.task_process_time DESC) rn
      FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
      WHERE c.task_process_field = 'field005' AND c.new_task_field_value = '延期修复'
    )c
    ON c.task_uuid = t.uuid AND c.rn = 1
    WHERE t.issue_type_cname ='缺陷' -- 缺陷
      AND (t.severity_level IN (1,2,3) OR t.severity_level IS NULL) -- 严重等级为3级以上或为空
      AND t.status = 1 -- 工作项有效未被删除
      AND (t.defect_validity = '有效' OR t.defect_validity IS NULL) -- 缺陷有效
      AND t.project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]2.11WES产品包','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[智能搬运FT]锂电产品线项目','[硬件自动化]智驾车载软件、硬件开发') -- 项目类型
      AND t.task_create_time >= '2022-10-27' -- 创建时间大于2022-10-27
	  AND t.task_status_cname = '延期修复'
  )t
  ON t.task_process_date >= d.start_date AND t.task_process_date <= d.end_date
  GROUP BY d.start_date,d.end_date,d.date_scope,t.bug_type,t.project_classify_name,t.severity_level,COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2])
)t8
ON t8.start_date = d.start_date AND t8.end_date = d.end_date AND t8.date_scope = d.date_scope AND t8.bug_type = bt.bug_type AND t8.task_belong_email = emp.email AND t8.project_classify_name = pc.project_classify_name AND t8.severity_level = sl.severity_level
-- 历史遗留缺陷数量 => 缺陷创建时间小于时间范围 且 缺陷无变更记录时|缺陷有变更记录时：变更时间在时间范围内 + 优先取变更人其次取负责人
LEFT JOIN 
(
  SELECT d.start_date, -- 开始日期
         d.end_date, -- 结束日期
         d.date_scope, -- 日期区间
         t.bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         t.project_classify_name, -- 项目分类名称
         t.severity_level, -- 严重等级
         COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2]) AS task_belong_email, -- 人员邮箱
         COUNT(DISTINCT t.uuid) AS history_legacy_bug_num -- 历史遗留缺陷数量
  FROM days d 
  LEFT JOIN 
  (
    SELECT t.uuid,
           TO_DATE(t.task_create_time) AS task_create_date,
           t.project_classify_name,
           IF(t.project_classify_name = '工单问题汇总','线上缺陷','线下缺陷') as bug_type,
           nvl(t.severity_level,'未知') AS severity_level,
           t.task_status_cname,
           t.defect_source_user_email,
           t.task_repair_user_email,
           t.task_assign_email,
           c.task_process_date,
           ARRAY(t.defect_source_user_email,t.task_repair_user_email,t.task_assign_email) AS task_belong_email
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    LEFT JOIN 
    (
      SELECT c.task_uuid,
             TO_DATE(c.task_process_time) AS task_process_date,
             c.task_process_field,
             c.new_task_field_value,
             c.task_process_user,
             ROW_NUMBER()OVER(PARTITION BY c.task_uuid ORDER BY c.task_process_time DESC) rn
      FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
      WHERE c.task_process_field = 'field005' AND c.new_task_field_value = '已关闭'
    )c
    ON c.task_uuid = t.uuid AND c.rn = 1
    WHERE t.issue_type_cname ='缺陷' -- 缺陷
      AND (t.severity_level IN (1,2,3) OR t.severity_level IS NULL) -- 严重等级为3级以上或为空
      AND t.status = 1 -- 工作项有效未被删除
      AND (t.defect_validity = '有效' OR t.defect_validity IS NULL) -- 缺陷有效
      AND t.project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]2.11WES产品包','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[智能搬运FT]锂电产品线项目','[硬件自动化]智驾车载软件、硬件开发') -- 项目类型
      AND t.task_create_time >= '2022-10-27' -- 创建时间大于2022-10-27
  )t
  ON t.task_create_date < d.start_date AND (t.task_process_date < d.start_date OR t.task_process_date IS NULL)
  WHERE t.task_process_date IS NULL
  GROUP BY d.start_date,d.end_date,d.date_scope,t.bug_type,t.project_classify_name,t.severity_level,COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2])
)t9
ON t9.start_date = d.start_date AND t9.end_date = d.end_date AND t9.date_scope = d.date_scope AND t9.bug_type = bt.bug_type AND t9.task_belong_email = emp.email AND t9.project_classify_name = pc.project_classify_name AND t9.severity_level = sl.severity_level
-- 历史挂起缺陷数量 => 缺陷创建时间小于时间范围 且 缺陷无变更记录时|缺陷有变更记录时：变更时间在时间范围内 + 优先取变更人其次取负责人
LEFT JOIN 
(
  SELECT d.start_date, -- 开始日期
         d.end_date, -- 结束日期
         d.date_scope, -- 日期区间
         t.bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         t.project_classify_name, -- 项目分类名称
         t.severity_level, -- 严重等级
         COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2]) AS task_belong_email, -- 人员邮箱
         COUNT(DISTINCT t.uuid) AS history_pending_bug_num -- 历史挂起缺陷数量:往期new_task_field_value 当期task_status_cname
  FROM days d 
  LEFT JOIN 
  (
    SELECT t.uuid,
           TO_DATE(t.task_create_time) AS task_create_date,
           t.project_classify_name,
           IF(t.project_classify_name = '工单问题汇总','线上缺陷','线下缺陷') as bug_type,
           nvl(t.severity_level,'未知') AS severity_level,
           t.task_status_cname,
           t.defect_source_user_email,
           t.task_repair_user_email,
           t.task_assign_email,
           c.task_process_date,
           ARRAY(t.defect_source_user_email,t.task_repair_user_email,t.task_assign_email) AS task_belong_email
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    LEFT JOIN 
    (
      SELECT c.task_uuid,
             TO_DATE(c.task_process_time) AS task_process_date,
             c.task_process_field,
             c.new_task_field_value,
             c.task_process_user,
             ROW_NUMBER()OVER(PARTITION BY c.task_uuid ORDER BY c.task_process_time DESC) rn
      FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
      WHERE c.task_process_field = 'field005' AND c.new_task_field_value = '延期修复'
    )c
    ON c.task_uuid = t.uuid AND c.rn = 1
    WHERE t.issue_type_cname ='缺陷' -- 缺陷
      AND (t.severity_level IN (1,2,3) OR t.severity_level IS NULL) -- 严重等级为3级以上或为空
      AND t.status = 1 -- 工作项有效未被删除
      AND (t.defect_validity = '有效' OR t.defect_validity IS NULL) -- 缺陷有效
      AND t.project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]2.11WES产品包','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[智能搬运FT]锂电产品线项目','[硬件自动化]智驾车载软件、硬件开发') -- 项目类型
      AND t.task_create_time >= '2022-10-27' -- 创建时间大于2022-10-27
	  AND t.task_status_cname = '延期修复'
  )t
  ON t.task_create_date < d.start_date AND t.task_process_date < d.start_date
  GROUP BY d.start_date,d.end_date,d.date_scope,t.bug_type,t.project_classify_name,t.severity_level,COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2])
)t10
ON t10.start_date = d.start_date AND t10.end_date = d.end_date AND t10.date_scope = d.date_scope AND t10.bug_type = bt.bug_type AND t10.task_belong_email = emp.email AND t10.project_classify_name = pc.project_classify_name AND t10.severity_level = sl.severity_level
-- 本期已修复并验证缺陷数量 => 创建时间在周期范围内 + 状态切换为已关闭的时间在周期范围内 + 优先取缺陷来源人 其次取修复人 最后取负责人
LEFT JOIN 
(
  SELECT d.start_date, -- 开始日期
         d.end_date, -- 结束日期
         d.date_scope, -- 日期区间
         t.bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         t.project_classify_name, -- 项目分类名称
         t.severity_level, -- 严重等级
         COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2]) AS task_belong_email, -- 人员邮箱
         COUNT(DISTINCT t.uuid) AS solve_bug_num_new -- 本期修复并验证缺陷数量
  FROM days d 
  LEFT JOIN 
  (
    SELECT t.uuid,
           TO_DATE(t.task_create_time) AS task_create_date,
           t.project_classify_name,
           IF(t.project_classify_name = '工单问题汇总','线上缺陷','线下缺陷') as bug_type,
           nvl(t.severity_level,'未知') AS severity_level,
           t.task_assign_email,
           t.task_repair_user_email,
           c.task_process_date,
           ARRAY(t.defect_source_user_email,t.task_repair_user_email,t.task_assign_email) AS task_belong_email
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    LEFT JOIN 
    (
      SELECT c.task_uuid,
             TO_DATE(c.task_process_time) AS task_process_date,
             c.task_process_field,
             c.new_task_field_value,
             c.task_process_user,
             ROW_NUMBER()OVER(PARTITION BY c.task_uuid ORDER BY c.task_process_time DESC) rn
      FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
      WHERE c.task_process_field = 'field005' AND c.new_task_field_value = '已关闭'
    )c
    ON c.task_uuid = t.uuid AND c.rn = 1
    WHERE t.issue_type_cname ='缺陷' -- 缺陷
      AND (t.severity_level IN (1,2,3) OR t.severity_level IS NULL) -- 严重等级为3级以上或为空
      AND t.status = 1 -- 工作项有效未被删除
      AND (t.defect_validity = '有效' OR t.defect_validity IS NULL) -- 缺陷有效
      AND t.project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]2.11WES产品包','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[智能搬运FT]锂电产品线项目','[硬件自动化]智驾车载软件、硬件开发') -- 项目类型
	  AND t.task_create_time >= '2022-10-27' -- 创建时间大于2022-10-27
  )t
  ON (t.task_create_date >= d.start_date AND t.task_create_date <= d.end_date) AND (t.task_process_date >= d.start_date AND t.task_process_date <= d.end_date)
  GROUP BY d.start_date,d.end_date,d.date_scope,t.bug_type,t.project_classify_name,t.severity_level,COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2])
)t11
ON t11.start_date = d.start_date AND t11.end_date = d.end_date AND t11.date_scope = d.date_scope AND t11.bug_type = bt.bug_type AND t11.task_belong_email = emp.email AND t11.project_classify_name = pc.project_classify_name AND t11.severity_level = sl.severity_level
-- 缺陷最佳发现阶段缺陷数量 => 缺陷无变更记录时：创建时间在时间范围内|缺陷有变更记录时：变更时间在时间范围内 + 优先取变更人其次取负责人
LEFT JOIN 
(
  SELECT d.start_date, -- 开始日期
         d.end_date, -- 结束日期
         d.date_scope, -- 日期区间
         t.bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         t.project_classify_name, -- 项目分类名称
         t.severity_level, -- 严重等级
         COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2]) AS task_belong_email, -- 人员邮箱
         SUM(IF(t.defect_best_discovery_stage = '需求评审',1,0)) AS best_find_bug_xqps_num, -- 缺陷最佳发现阶段：'需求评审' 
         SUM(IF(t.defect_best_discovery_stage = '生产部署',1,0)) AS best_find_bug_scbs_num, -- 缺陷最佳发现阶段：'生产部署' 
         SUM(IF(t.defect_best_discovery_stage = 'SVT测试',1,0)) AS best_find_bug_svtcs_num, -- 缺陷最佳发现阶段：'SVT测试' 
         SUM(IF(t.defect_best_discovery_stage = 'FAT测试',1,0)) AS best_find_bug_fatcs_num, -- 缺陷最佳发现阶段：'FAT测试' 
         SUM(IF(t.defect_best_discovery_stage = '开发设计',1,0)) AS best_find_bug_kfsj_num, -- 缺陷最佳发现阶段：'开发设计' 
         SUM(IF(t.defect_best_discovery_stage = '生产试运行',1,0)) AS best_find_bug_scsyx_num, -- 缺陷最佳发现阶段：'生产试运行' 
         SUM(IF(t.defect_best_discovery_stage = '单功能测试',1,0)) AS best_find_bug_dgncs_num, -- 缺陷最佳发现阶段：'单功能测试' 
         SUM(IF(t.defect_best_discovery_stage = '集成测试',1,0)) AS best_find_bug_jccs_num, -- 缺陷最佳发现阶段：'集成测试' 
         SUM(IF(t.defect_best_discovery_stage = '实现阶段',1,0)) AS best_find_bug_sxjd_num -- 缺陷最佳发现阶段：'实现阶段'
  FROM days d 
  LEFT JOIN 
  (
    SELECT t.uuid,
           TO_DATE(t.task_create_time) AS task_create_date,
           t.project_classify_name,
           IF(t.project_classify_name = '工单问题汇总','线上缺陷','线下缺陷') as bug_type,
           nvl(t.severity_level,'未知') AS severity_level,
           t.task_assign_email,
           t.task_repair_user_email,
           c.task_process_date,
           ARRAY(t.defect_source_user_email,t.task_repair_user_email,t.task_assign_email) AS task_belong_email,
           t.defect_best_discovery_stage
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    LEFT JOIN 
    (
      SELECT c.task_uuid,
             TO_DATE(c.task_process_time) AS task_process_date,
             c.task_process_field,
             c.new_task_field_value,
             c.task_process_user,
             ROW_NUMBER()OVER(PARTITION BY c.task_uuid ORDER BY c.task_process_time DESC) rn
      FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
      WHERE c.task_process_field IN ('5Uo3GJs5')
    )c
    ON c.task_uuid = t.uuid AND c.rn = 1
    WHERE t.issue_type_cname ='缺陷' -- 缺陷
      AND (t.severity_level IN (1,2,3) OR t.severity_level IS NULL) -- 严重等级为3级以上或为空
      AND t.status = 1 -- 工作项有效未被删除
      AND (t.defect_validity = '有效' OR t.defect_validity IS NULL) -- 缺陷有效
      AND t.project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]2.11WES产品包','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[智能搬运FT]锂电产品线项目','[硬件自动化]智驾车载软件、硬件开发') -- 项目类型
	  AND t.task_create_time >= '2022-10-27' -- 创建时间大于2022-10-27
  )t
  ON nvl(t.task_process_date,t.task_create_date) >= d.start_date AND nvl(t.task_process_date,t.task_create_date) <= d.end_date
  GROUP BY d.start_date,d.end_date,d.date_scope,t.bug_type,t.project_classify_name,t.severity_level,COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2])
)t12
ON t12.start_date = d.start_date AND t12.end_date = d.end_date AND t12.date_scope = d.date_scope AND t12.bug_type = bt.bug_type AND t12.task_belong_email = emp.email AND t12.project_classify_name = pc.project_classify_name AND t12.severity_level = sl.severity_level
-- 缺陷发现阶段缺陷数量 => 缺陷无变更记录时：创建时间在时间范围内|缺陷有变更记录时：变更时间在时间范围内 + 优先取变更人其次取负责人
LEFT JOIN 
(
  SELECT d.start_date, -- 开始日期
         d.end_date, -- 结束日期
         d.date_scope, -- 日期区间
         t.bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         t.project_classify_name, -- 项目分类名称
         t.severity_level, -- 严重等级
         COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2]) AS task_belong_email, -- 人员邮箱
         SUM(IF(t.defect_discovery_stage = '单功能测试',1,0)) AS find_bug_dgncs_num, -- 缺陷发现阶段：'单功能测试' 
         SUM(IF(t.defect_discovery_stage = '生产部署',1,0)) AS find_bug_scbs_num, -- 缺陷发现阶段：'生产部署' 
         SUM(IF(t.defect_discovery_stage = '集成测试',1,0)) AS find_bug_jccs_num, -- 缺陷发现阶段：'集成测试' 
         SUM(IF(t.defect_discovery_stage = '生产试运行',1,0)) AS find_bug_scsyx_num, -- 缺陷发现阶段：'生产试运行' 
         SUM(IF(t.defect_discovery_stage = 'FAT测试',1,0)) AS find_bug_fatcs_num, -- 缺陷发现阶段：'FAT测试' 
         SUM(IF(t.defect_discovery_stage = '售后运行',1,0)) AS find_bug_shyx_num, -- 缺陷发现阶段：'售后运行' 
         SUM(IF(t.defect_discovery_stage = 'SVT测试',1,0)) AS find_bug_svtcs_num -- 缺陷发现阶段：'SVT测试'
  FROM days d 
  LEFT JOIN 
  (
    SELECT t.uuid,
           TO_DATE(t.task_create_time) AS task_create_date,
           t.project_classify_name,
           IF(t.project_classify_name = '工单问题汇总','线上缺陷','线下缺陷') as bug_type,
           nvl(t.severity_level,'未知') AS severity_level,
           t.task_assign_email,
           t.task_repair_user_email,
           c.task_process_date,
           ARRAY(t.defect_source_user_email,t.task_repair_user_email,t.task_assign_email) AS task_belong_email,
           t.defect_discovery_stage
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t
    LEFT JOIN 
    (
      SELECT c.task_uuid,
             TO_DATE(c.task_process_time) AS task_process_date,
             c.task_process_field,
             c.new_task_field_value,
             c.task_process_user,
             ROW_NUMBER()OVER(PARTITION BY c.task_uuid ORDER BY c.task_process_time DESC) rn
      FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
      WHERE c.task_process_field IN ('5pnQz9ai')
    )c
    ON c.task_uuid = t.uuid AND c.rn = 1
    WHERE t.issue_type_cname ='缺陷' -- 缺陷
      AND (t.severity_level IN (1,2,3) OR t.severity_level IS NULL) -- 严重等级为3级以上或为空
      AND t.status = 1 -- 工作项有效未被删除
      AND (t.defect_validity = '有效' OR t.defect_validity IS NULL) -- 缺陷有效
      AND t.project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]2.11WES产品包','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[智能搬运FT]锂电产品线项目','[硬件自动化]智驾车载软件、硬件开发') -- 项目类型
	  AND t.task_create_time >= '2022-10-27' -- 创建时间大于2022-10-27
  )t
  ON nvl(t.task_process_date,t.task_create_date) >= d.start_date AND nvl(t.task_process_date,t.task_create_date) <= d.end_date
  GROUP BY d.start_date,d.end_date,d.date_scope,t.bug_type,t.project_classify_name,t.severity_level,COALESCE(t.task_belong_email[0],t.task_belong_email[1],t.task_belong_email[2])
)t13
ON t13.start_date = d.start_date AND t13.end_date = d.end_date AND t13.date_scope = d.date_scope AND t13.bug_type = bt.bug_type AND t13.task_belong_email = emp.email AND t13.project_classify_name = pc.project_classify_name AND t13.severity_level = sl.severity_level
-- 缺陷及时解决数量
LEFT JOIN 
(
  SELECT tmp.start_date, -- 开始日期
         tmp.end_date, -- 结束日期
         tmp.date_scope, -- 日期区间
         tmp.bug_type, -- 缺陷类型：线上缺陷|线下缺陷
         tmp.project_classify_name, -- 项目分类名称
         tmp.severity_level, -- 严重等级
         COALESCE(tmp.task_belong_email[0],tmp.task_belong_email[1],tmp.task_belong_email[2]) AS task_belong_email, -- 人员邮箱
         SUM(nvl(tmp.timely_solve_num,0)) AS timely_solve_num, -- 及时解决缺陷数量
         SUM(nvl(tmp.solve_duration,0)) AS solve_duration -- 修复时长
  FROM
  (
    SELECT d.start_date,
           d.end_date,
           d.date_scope,
           t.uuid,
           t.project_classify_name,
           t.bug_type,
           t.severity_level,
           t.task_create_time,
           t.task_create_date,
           t.day_type,
           t.task_process_time,
           t.task_process_date,
           t.task_belong_email,
           t.solve_days,
           COUNT(tud.days) AS weekend_days,
           t.solve_days - COUNT(tud.days) AS work_days,
           CASE WHEN t.task_process_date IS NULL THEN 0 -- 所有未解决的缺陷
                -- 工作日创建
                WHEN t.day_type = 0 AND COUNT(tud.days) = 0 AND t.severity_level = 1 AND t.task_create_time < DATE_FORMAT(CONCAT(t.task_create_date,' ','18:00:00'),'yyyy-MM-dd HH:mm:ss') AND t.solve_days = 0 THEN 1
                WHEN t.day_type = 0 AND COUNT(tud.days) = 0 AND t.severity_level = 1 AND t.task_create_time >= DATE_FORMAT(CONCAT(t.task_create_date,' ','18:00:00'),'yyyy-MM-dd HH:mm:ss') AND t.solve_days <= 1 THEN 1
                WHEN t.day_type = 0 AND COUNT(tud.days) = 0 AND t.severity_level = 2 AND t.task_create_time < DATE_FORMAT(CONCAT(t.task_create_date,' ','18:00:00'),'yyyy-MM-dd HH:mm:ss') AND t.solve_days <= 1 THEN 1
                WHEN t.day_type = 0 AND COUNT(tud.days) = 0 AND t.severity_level = 2 AND t.task_create_time >= DATE_FORMAT(CONCAT(t.task_create_date,' ','18:00:00'),'yyyy-MM-dd HH:mm:ss') AND t.solve_days <= 2 THEN 1
                WHEN t.day_type = 0 AND COUNT(tud.days) = 0 AND t.severity_level = 3 AND t.task_create_time < DATE_FORMAT(CONCAT(t.task_create_date,' ','18:00:00'),'yyyy-MM-dd HH:mm:ss') AND t.solve_days <= 2 THEN 1
                WHEN t.day_type = 0 AND COUNT(tud.days) = 0 AND t.severity_level = 3 AND t.task_create_time >= DATE_FORMAT(CONCAT(t.task_create_date,' ','18:00:00'),'yyyy-MM-dd HH:mm:ss') AND t.solve_days <= 3 THEN 1
                -- 工作日创建（跨节假日）
                WHEN t.day_type = 0 AND COUNT(tud.days) != 0 AND t.severity_level = 1 AND t.task_create_time < DATE_FORMAT(CONCAT(t.task_create_date,' ','18:00:00'),'yyyy-MM-dd HH:mm:ss') AND t.solve_days - COUNT(tud.days) = 0 THEN 1
                WHEN t.day_type = 0 AND COUNT(tud.days) != 0 AND t.severity_level = 1 AND t.task_create_time >= DATE_FORMAT(CONCAT(t.task_create_date,' ','18:00:00'),'yyyy-MM-dd HH:mm:ss') AND t.solve_days - COUNT(tud.days) <= 1 THEN 1
                WHEN t.day_type = 0 AND COUNT(tud.days) != 0 AND t.severity_level = 2 AND t.task_create_time < DATE_FORMAT(CONCAT(t.task_create_date,' ','18:00:00'),'yyyy-MM-dd HH:mm:ss') AND t.solve_days - COUNT(tud.days) <= 1 THEN 1
                WHEN t.day_type = 0 AND COUNT(tud.days) != 0 AND t.severity_level = 2 AND t.task_create_time >= DATE_FORMAT(CONCAT(t.task_create_date,' ','18:00:00'),'yyyy-MM-dd HH:mm:ss') AND t.solve_days - COUNT(tud.days) <= 2 THEN 1
                WHEN t.day_type = 0 AND COUNT(tud.days) != 0 AND t.severity_level = 3 AND t.task_create_time < DATE_FORMAT(CONCAT(t.task_create_date,' ','18:00:00'),'yyyy-MM-dd HH:mm:ss') AND t.solve_days - COUNT(tud.days) <= 2 THEN 1
                WHEN t.day_type = 0 AND COUNT(tud.days) != 0 AND t.severity_level = 3 AND t.task_create_time >= DATE_FORMAT(CONCAT(t.task_create_date,' ','18:00:00'),'yyyy-MM-dd HH:mm:ss') AND t.solve_days - COUNT(tud.days) <= 3 THEN 1
                -- 非工作日创建
                WHEN t.day_type != 0 AND t.severity_level = 1 AND t.solve_days - COUNT(tud.days) <= 0 THEN 1
                WHEN t.day_type != 0 AND t.severity_level = 2 AND t.solve_days - COUNT(tud.days) <= 1 THEN 1
                WHEN t.day_type != 0 AND t.severity_level = 3 AND t.solve_days - COUNT(tud.days) <= 2 THEN 1
           ELSE 0 END AS timely_solve_num, -- 及时解决缺陷数量
           t.solve_duration -- 修复时长
    FROM days d 
    LEFT JOIN
    (
      SELECT t.uuid,
             DATE_FORMAT(t.task_create_time,'yyyy-MM-dd HH:mm:ss') as task_create_time, 
             TO_DATE(t.task_create_time) AS task_create_date,
             d.day_type,
             t.project_classify_name,
             IF(t.project_classify_name = '工单问题汇总','线上缺陷','线下缺陷') as bug_type,
             nvl(t.severity_level,'未知') AS severity_level,
             t.task_repair_user_email,
             DATE_FORMAT(c.task_process_time,'yyyy-MM-dd HH:mm:ss') as task_process_time,
             TO_DATE(c.task_process_time) AS task_process_date,
             ARRAY(t.defect_source_user_email,t.task_repair_user_email,t.task_assign_email) AS task_belong_email,
             DATEDIFF(TO_DATE(c.task_process_time),TO_DATE(t.task_create_time)) as solve_days,
             unix_timestamp(nvl(c.task_process_time,DATE_FORMAT(DATE_ADD('${pre1_date}',1),'yyyy-MM-dd HH:mm:ss'))) - unix_timestamp(t.task_create_time) as solve_duration
      FROM ${dwd_dbname}.dwd_ones_task_info_ful t
      LEFT JOIN 
      (
        SELECT c.task_uuid,
               c.task_process_time,
               c.task_process_field,
               c.new_task_field_value,
               c.task_process_user,
               ROW_NUMBER()OVER(PARTITION BY c.task_uuid ORDER BY c.task_process_time DESC) rn
        FROM ${dwd_dbname}.dwd_one_task_process_change_info_his c
        WHERE c.task_process_field IN ('field005') AND c.new_task_field_value = '已修复'
      )c
      ON c.task_uuid = t.uuid AND c.rn = 1
      LEFT JOIN ${dim_dbname}.dim_day_date d
      ON TO_DATE(t.task_create_time) = d.days
      WHERE t.issue_type_cname ='缺陷' -- 缺陷
          AND (t.severity_level IN (1,2,3) OR t.severity_level IS NULL) -- 严重等级为3级以上或为空
          AND t.status = 1 -- 工作项有效未被删除
          AND (t.defect_validity = '有效' OR t.defect_validity IS NULL) -- 缺陷有效
          AND t.project_classify_name IN ('[系统中台]3.0凤凰项目','[系统中台]devops平台','[箱式FT]货到人产品线','[箱式FT]2.11WES产品包','[箱式FT]货到人外部项目汇总','[箱式FT]货架到人外部项目汇总','[箱式FT]箱式搬运产品线','[箱式FT]箱式FT-SVT','[箱式FT]料箱搬运外部项目汇总','[箱式FT]料箱到人外部项目汇总','[箱式FT]料箱到人产品线','[箱式FT]货架到人产品线','[智能搬运FT]LES外部客户项目汇总','[智能搬运FT]叉式产品线项目','[智能搬运FT]智能搬运产品线','[智能搬运FT]机器人管理后台项目','[智能搬运FT]LES产品研发项目','[智能搬运FT]智能搬运外部项目汇总','[智能搬运FT]锂电产品线项目','[硬件自动化]智驾车载软件、硬件开发') -- 项目类型
	      AND t.task_create_time >= '2022-10-27' -- 创建时间大于2022-10-27
    )t
    ON t.task_create_date >= d.start_date AND t.task_create_date <= d.end_date
    LEFT JOIN 
    (
      SELECT days
      FROM ${dim_dbname}.dim_day_date d
      WHERE d.days >= '2022-10-27' AND d.days <= '${pre1_date}' AND d.day_type IN (1,2) -- 周末
    )tud
    ON t.task_create_date <= tud.days AND t.task_process_date >= tud.days
    GROUP BY d.start_date,d.end_date,d.date_scope,t.uuid,t.project_classify_name,t.bug_type,t.severity_level,t.task_create_time,t.task_create_date,t.day_type,t.task_process_time,t.task_process_date,t.task_belong_email,t.solve_days,t.solve_duration
  )tmp
  GROUP BY tmp.start_date,tmp.end_date,tmp.date_scope,tmp.bug_type,tmp.project_classify_name,tmp.severity_level,COALESCE(tmp.task_belong_email[0],tmp.task_belong_email[1],tmp.task_belong_email[2])
)t14
ON t14.start_date = d.start_date AND t14.end_date = d.end_date AND t14.date_scope = d.date_scope AND t14.bug_type = bt.bug_type AND t14.task_belong_email = emp.email AND t14.project_classify_name = pc.project_classify_name AND t14.severity_level = sl.severity_level;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"      