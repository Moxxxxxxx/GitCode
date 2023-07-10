#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-10-20 创建
#-- 2 wangyingying 2022-10-20 融合pms项目数据
#-- 3 wangyingying 2022-10-20 替换项目大表临时表
#-- 4 wangyingying 2022-10-21 增加历史外包劳务数据
#-- 5 wangyingying 2022-10-25 修改项目阶段的取值逻辑
#-- 6 wangyingying 2022-11-29 增加大区组字段
#-- 7 wangyingying 2023-01-06 增加考勤相关、是否有效、来源、费用字段
#-- 8 wangyingying 2023-02-13 增加是否活跃字段
# ------------------------------------------------------------------------------------------------


hive=/opt/module/hive-3.1.2/bin/hive
dwd_dbname=dwd
ads_dbname=ads
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
-- ads_pms_process_service_log_detail    --pms 外包劳务日志

WITH attendamce_detail AS
(
  SELECT tt1.cur_date, -- 统计日期
         tt1.business_id, -- 审批编码
         nvl(pvd.project_code,tt1.project_code) AS project_code, -- 项目编码
         pvd.project_name, -- 项目名称
         pvd.project_dispaly_state AS project_operation_state, -- 项目运营状态
         pvd.project_area, -- 项目区域
         pvd.project_ft, -- 项目所属ft
         pvd.project_priority, -- 项目等级
         pvd.project_progress_stage, -- 项目进展阶段
         pvd.project_area_group, -- 项目区域（国内|国外）
		 pvd.is_active, -- 是否活跃
         tt1.originator_dept_name AS team_name, -- 团队名称
         tt1.originator_user_name AS member_name, -- 人员名称
         tt1.service_type, -- 劳务类型
         '劳务' AS member_function, -- 人员属性
         tt1.check_duration AS check_duration_hour, -- 考勤时长（小时）
         ROW_NUMBER()OVER(PARTITION BY tt1.cur_date,nvl(pvd.project_code,tt1.project_code),tt1.originator_dept_name,tt1.originator_user_name,tt1.service_type ORDER BY tt1.business_id) rn
  FROM 
  (
    SELECT TO_DATE(a.checkin_time) AS cur_date, -- 统计时间
           a.business_id, -- 审批编号
           a.project_code, -- 项目编号
           a.originator_dept_name, -- 团队名称
           IF(a.originator_user_name IS NULL,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) AS originator_user_name, -- 成员名称
           IF(a.service_type IS NULL,'运维劳务',a.service_type) AS service_type, -- 劳务类型
           IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) AS check_duration, -- 考勤时长（小时）,
           a.checkin_time, -- 考勤签到时间
           a.checkout_time, -- 考勤签退时间
           ROW_NUMBER()OVER(PARTITION BY DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name IS NULL,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) ORDER BY a.checkin_time,a.create_time) rn
    FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
    WHERE a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' -- 审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准
  )tt1
  LEFT JOIN 
  (
    SELECT TO_DATE(a.checkin_time) AS cur_date, -- 统计时间
           a.business_id, -- 审批编号
           a.project_code, -- 项目编号
           a.originator_dept_name, -- 团队名称
           IF(a.originator_user_name IS NULL,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) AS originator_user_name, -- 成员名称
           IF(a.service_type IS NULL,'运维劳务',a.service_type) AS service_type, -- 劳务类型
           IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) AS check_duration, -- 考勤时长（小时）,
           a.checkin_time, -- 考勤签到时间
           a.checkout_time, -- 考勤签退时间
           ROW_NUMBER()OVER(PARTITION BY DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name IS NULL,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) ORDER BY a.checkin_time,a.create_time) rn
    FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
    WHERE a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' -- 审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准
  )tt2
  ON nvl(tt1.cur_date,'unknown1') = nvl(tt2.cur_date,'unknown2') AND nvl(tt1.project_code,'unknown1') = nvl(tt2.project_code,'unknown2') AND nvl(tt1.originator_user_name,'unknown1') = nvl(tt2.originator_user_name,'unknown2') AND tt1.rn = tt2.rn + 1
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail pvd
  ON nvl(pvd.project_code,'unknown1') = nvl(tt1.project_code,'unknown2') OR nvl(pvd.project_sale_code,'unknown1') = nvl(tt1.project_code,'unknown2')
  WHERE tt2.rn IS NULL OR tt1.checkin_time NOT BETWEEN tt2.checkin_time AND tt2.checkout_time
)

INSERT overwrite table ${ads_dbname}.ads_pms_process_service_log_detail
SELECT '' AS id, --主键 
       t1.cur_date, -- 统计日期
       t1.business_id, -- 审批编码
       t1.project_code, -- 项目编号
       t1.project_name, -- 项目名称
       t1.project_operation_state, -- 项目运营状态
       t1.project_area, -- 项目区域
       t1.project_ft, -- 项目所属ft
       t1.project_priority, -- 项目等级
       t1.project_progress_stage, -- 项目进展阶段
       t1.project_area_group, -- 项目区域（国内|国外）
	   t1.is_active, -- 是否活跃
       t1.team_name, -- 团队名称
       t1.member_name, -- 人员名称
       t1.service_type, -- 劳务类型
       t1.member_function, -- 人员属性
       t1.check_duration_hour, -- 考勤时长（小时）
       t1.rn AS check_sort, -- 考勤排序
       t2.check_duration_day_hour, -- 当天考勤时长（小时）
       t2.check_duration_day, -- 当天考勤时长（天）
       t2.service_cost, -- 劳务费用
       'dtk' AS data_source, -- 数据来源
       IF(t1.cur_date >= '2022-01-01',1,0) AS is_valid, -- 是否有效
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM attendamce_detail t1
LEFT JOIN 
(
  SELECT t1.cur_date, -- 统计日期
         t1.project_code, -- 项目编号
         t1.team_name, -- 团队名称
         t1.member_name, -- 人员名称
         t1.service_type, -- 劳务类型
         SUM(t1.check_duration_hour) AS check_duration_day_hour, -- 当天考勤时长（小时）
         CASE WHEN SUM(t1.check_duration_hour) < 4 THEN '0天'
              WHEN SUM(t1.check_duration_hour) >= 4 AND SUM(t1.check_duration_hour) < 8 THEN '0.5天'
              WHEN SUM(t1.check_duration_hour) >= 8 AND SUM(t1.check_duration_hour) <= 10 THEN '1天'
              WHEN SUM(t1.check_duration_hour) > 10 THEN CONCAT('1天',(SUM(t1.check_duration_hour) - 10),'小时') END AS check_duration_day, -- 当天考勤时长（天）
         CASE WHEN SUM(t1.check_duration_hour) < 4 THEN 0
              WHEN SUM(t1.check_duration_hour) >= 4 AND SUM(t1.check_duration_hour) < 8 THEN 350
              WHEN SUM(t1.check_duration_hour) >= 8 AND SUM(t1.check_duration_hour) <= 10 THEN 550
              WHEN SUM(t1.check_duration_hour) > 10 THEN 550 + (SUM(t1.check_duration_hour) - 10) * 2 * 20 END AS service_cost -- 劳务费用
  FROM attendamce_detail t1
  GROUP BY t1.cur_date,t1.project_code,t1.team_name,t1.member_name,t1.service_type
)t2
ON t1.cur_date = t2.cur_date AND t1.project_code = t2.project_code AND t1.team_name = t2.team_name AND t1.member_name = t2.member_name AND t1.service_type = t2.service_type

UNION ALL

SELECT '' AS id, --主键 
       TO_DATE(p.log_date) AS cur_date, -- 统计日期
       p.process_instance_id AS business_id, -- 审批编码
       nvl(pvd.project_code,p.project_code) AS project_code, -- 项目编号
       pvd.project_name, -- 项目名称
       pvd.project_dispaly_state AS project_operation_state, -- 项目运营状态
       pvd.project_area, -- 项目区域
       pvd.project_ft, -- 项目所属ft
       pvd.project_priority, -- 项目等级
       pvd.project_progress_stage, -- 项目进展阶段
       pvd.project_area_group, -- 项目区域（国内|国外）
	   pvd.is_active, -- 是否活跃
       p.org_path_name AS team_name, -- 团队名称
       p.applicant_user_name AS member_name, -- 人员名称
       IF(p.role_type = 'IMP','实施劳务','运维劳务') AS service_type, -- 劳务类型
       '劳务' AS member_function, -- 人员属性
       p.working_hours AS check_duration_hour, -- 考勤时长（小时）
       ROW_NUMBER()OVER(PARTITION BY TO_DATE(p.log_date),nvl(pvd.project_code,p.project_code),p.org_path_name,p.applicant_user_name,IF(p.role_type = 'IMP','实施劳务','运维劳务') ORDER BY p.process_instance_id) AS check_sort, -- 考勤排序
       CAST(p.working_hours AS DECIMAL(10,2)) AS check_duration_day_hour, -- 当天考勤时长（小时）
       CASE WHEN p.working_hours < 4 THEN '0天'
            WHEN p.working_hours >= 4 AND p.working_hours < 8 THEN '0.5天'
            WHEN p.working_hours >= 8 AND p.working_hours <= 10 THEN '1天'
            WHEN p.working_hours > 10 THEN CONCAT('1天',(p.working_hours - 10),'小时') END AS check_duration_day, -- 当天考勤时长（天）
       CASE WHEN p.working_hours < 4 THEN 0
              WHEN p.working_hours >= 4 AND p.working_hours < 8 THEN 350
              WHEN p.working_hours >= 8 AND p.working_hours <= 10 THEN 550
              WHEN p.working_hours > 10 THEN 550 + (p.working_hours - 10) * 2 * 20 END AS service_cost, -- 劳务费用
       'pms' AS data_source, -- 数据来源
       0 AS is_valid, -- 是否有效
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM ${dwd_dbname}.dwd_pms_project_emp_log_info_df p
LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail pvd
ON p.project_code = pvd.project_code OR p.project_code = pvd.project_sale_code
WHERE p.d = '${pre1_date}' AND p.role_type IN ('IMP','OPS'); 
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"