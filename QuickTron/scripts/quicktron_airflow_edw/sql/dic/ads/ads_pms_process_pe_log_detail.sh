#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-10-19 创建
#-- 2 wangyingying 2022-10-20 融合pms项目数据
#-- 3 wangyingying 2022-10-20 替换项目大表临时表
#-- 4 wangyingying 2022-10-25 修改项目阶段的取值逻辑
#-- 5 wangyingying 2022-11-24 优化代码
#-- 6 wangyingying 2022-11-29 增加大区组字段
#-- 7 wangyingying 2023-01-06 增加费用和是否有效字段
#-- 8 wangyingying 2023-01-12 增加出差匹配
#-- 9 wangyingying 2023-02-13 增加是否活跃字段
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
-- ads_pms_process_pe_log_detail    --pms PE日志

INSERT overwrite table ${ads_dbname}.ads_pms_process_pe_log_detail
SELECT '' AS id, -- 主键
       tt.org_name_2, -- 二级部门
       tt.org_name_3, -- 三级部门
       tt.emp_name, -- 人员名称
       tt.emp_position, -- 人员职位
       tt.is_job, -- 是否在职
       tt.days, -- 日期
       tt.day_type, -- 日期类型
       tt.ischeck, -- 是否打卡
       tt.work_status, -- 出勤状态
       tt.business_id, -- 审批编号
       tt.log_date, -- 日志日期
       tt.project_code, -- 项目编码
       tt.project_name, -- 项目名称
       tt.project_manage, -- 项目经理
       tt.project_area, -- 区域-PM
       tt.project_ft, -- 大区/FT => <技术方案评审>ft
       tt.project_priority, -- 项目等级
       tt.project_progress_stage, -- 项目阶段
       tt.project_area_group, -- 大区组
	   tt.is_active, -- 是否活跃
       tt.job_content, -- 工作内容
       tt.working_hours, -- 工作时长
       tt.unusual_res, -- 异常原因
       tt.data_source, -- 数据来源
       CASE WHEN tt.is_valid = 0 THEN 0 -- 重复数据不计算
            WHEN tt.project_code IS NOT NULL AND (tt.day_type = '工作日' OR tt.day_type = '节假日' OR tt.day_type = '调休' OR tt.day_type = '周末') AND tt.data_source = 'abroad-offline' THEN 1 * 1671 -- 全天工作 * 离线数据1671
            WHEN tt.project_code IS NOT NULL AND (tt.day_type = '上半天请假' OR tt.day_type = '下半天请假') AND tt.data_source = 'abroad-offline' THEN 0.5 * 1671 -- 半天工作 * 离线数据1671
            WHEN tt.project_code IS NOT NULL AND (tt.day_type = '工作日' OR tt.day_type = '节假日' OR tt.day_type = '调休' OR tt.day_type = '周末') AND tt.org_name_2 = '箱式FT' THEN 1 * 700 -- 全天工作 * 箱式FT700
            WHEN tt.project_code IS NOT NULL AND (tt.day_type = '工作日' OR tt.day_type = '节假日' OR tt.day_type = '调休' OR tt.day_type = '周末') AND tt.org_name_2 = '营销中心' AND (tt.org_name_3 = '华东大区' OR tt.org_name_3 = '华中大区' OR tt.org_name_3 = '华北大区' OR tt.org_name_3 = '华南大区' OR tt.org_name_3 = '西南大区' OR tt.org_name_3 = '福建子公司') THEN 1 * 700 -- 全天工作 * 营销中心700
            WHEN tt.project_code IS NOT NULL AND (tt.day_type = '工作日' OR tt.day_type = '节假日' OR tt.day_type = '调休' OR tt.day_type = '周末') AND tt.org_name_2 = '项目部' THEN 1 * 500 -- 全天工作 * 项目部500
            WHEN tt.project_code IS NOT NULL AND (tt.day_type = '工作日' OR tt.day_type = '节假日' OR tt.day_type = '调休' OR tt.day_type = '周末') AND tt.org_name_2 = '海外事业部' THEN 1 * 1671 -- 全天工作 * 海外事业部1671
            WHEN tt.project_code IS NOT NULL AND (tt.day_type = '上半天请假' OR tt.day_type = '下半天请假') AND tt.org_name_2 = '箱式FT' THEN 0.5 * 700 -- 半天工作 * 箱式FT700
            WHEN tt.project_code IS NOT NULL AND (tt.day_type = '上半天请假' OR tt.day_type = '下半天请假') AND tt.org_name_2 = '营销中心' AND (tt.org_name_3 = '华东大区' OR tt.org_name_3 = '华中大区' OR tt.org_name_3 = '华北大区' OR tt.org_name_3 = '华南大区' OR tt.org_name_3 = '西南大区' OR tt.org_name_3 = '福建子公司') THEN 0.5 * 700 -- 半天工作 * 营销中心700
            WHEN tt.project_code IS NOT NULL AND (tt.day_type = '上半天请假' OR tt.day_type = '下半天请假') AND tt.org_name_2 = '项目部' THEN 0.5 * 500 -- 半天工作 * 项目部500
            WHEN tt.project_code IS NOT NULL AND (tt.day_type = '上半天请假' OR tt.day_type = '下半天请假') AND tt.org_name_2 = '海外事业部' THEN 0.5 * 1671 -- 半天工作 * 海外事业部1671
            WHEN tt.project_code IS NOT NULL AND tt.day_type = '全天请假' THEN 0 -- 全天请假
       ELSE 0 END AS pe_cost, -- pe费用
       nvl(tt.is_valid,1) AS is_valid, -- 是否有效
       tt1.business_id AS travel_business_id, -- 出差审批单号
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM 
(
  SELECT tud.org_name_2, -- 二级部门
         tud.org_name_3, -- 三级部门
         tud.emp_id, -- 人员编码
         tud.emp_name, -- 人员名称
         tud.emp_position, -- 人员职位
         tud.is_job, -- 是否在职
         tud.days, -- 日期
         IF(t12.leave_type IS NOT NULL,t12.leave_type,tud.day_type) AS day_type, -- 日期类型
         IF(t1.log_date IS NOT NULL OR t12.leave_type IS NOT NULL,'已打卡','未打卡') AS ischeck, -- 是否打卡
         t1.*
  FROM 
  (
    SELECT tu.org_name_2,
           tu.org_name_3,
           tu.emp_id,
           tu.emp_name,
           tu.emp_position,
           tu.is_job,
           tu.hired_date,
           tu.quit_date,
           td.days,
           CASE WHEN td.day_type = 0 THEN '工作日'
                WHEN td.day_type = 1 THEN '周末'
                WHEN td.day_type = 2 THEN '节假日'
                WHEN td.day_type = 3 THEN '调休' END AS day_type
    FROM
    (
      SELECT tmp.org_name_2, -- 一级部门
             tmp.org_name_3, -- 二级部门
             tmp.emp_id, -- 人员编码
             tmp.emp_name, -- 人员姓名
             tmp.emp_position, -- 人员职位
             tmp.is_job, -- 是否在职
             tmp.hired_date, -- 入职日期
             tmp.quit_date -- 离职日期
      FROM
      (
        SELECT split(tg.org_path_name,'/')[1] AS org_name_2, -- 一级部门
               split(tg.org_path_name,'/')[2] AS org_name_3, -- 二级部门
               te.emp_id, -- 人员编码
               te.emp_name, -- 人员姓名
               te.emp_position, -- 人员职位
               te.prg_path_name, -- 组织架构
               te.is_job, -- 是否在职
               DATE(te.hired_date) AS hired_date, -- 入职日期
               DATE(te.quit_date) AS quit_date, -- 离职日期
               ROW_NUMBER()OVER(PARTITION BY te.emp_id ORDER BY split(tg.org_path_name,'/')[1] ASC,split(tg.org_path_name,'/')[2] ASC) rn -- 多组织人员 => 优先按一级部门排正序，其次按二级部门排正序
        FROM ${dwd_dbname}.dwd_dtk_emp_info_df te -- 钉钉员工基本信息表
        LEFT JOIN 
        (
          SELECT m.emp_id, -- 人员编码
                 m.emp_name, -- 人员姓名
                 m.org_id, -- 组织编码
                 m.org_role_type, -- 组织角色类型
                 m.is_need_fill_manhour, -- 是否需要填写工时
                 m.org_start_date, -- 组织开始时间
                 m.org_end_date, -- 组织结束时间
                 m.is_job -- 是否在职
          FROM ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df m -- 钉钉员工历史组织架构记录表
          WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.d = '${pre1_date}' 
            AND m.is_valid = 1 -- 取有效的（区分多组织）
            AND m.org_end_date = IF(m.is_job = 1,'9999-01-01',m.org_end_date) -- 在职的取最新组织，离职的最后组织
          GROUP BY m.emp_id,m.emp_name,m.org_id,m.org_role_type,m.is_need_fill_manhour,m.org_start_date,m.org_end_date,m.is_job
        )tmp
        ON te.emp_id = tmp.emp_id
        LEFT JOIN ${dim_dbname}.dim_dtk_org_history_info_df tg -- 钉钉组织架构历史组织表
        ON tg.org_id = tmp.org_id AND tg.d = IF(tmp.org_end_date = '9999-01-01','${pre1_date}',IF(tmp.is_job = 0 ,DATE_ADD(tmp.org_end_date, -1),tmp.org_end_date)) -- 如果在职取最新天的组织架构，如果离职取离职前一天的，否则取离职当天的
        WHERE te.d = '${pre1_date}' AND te.org_company_name = '上海快仓智能科技有限公司' 
          AND te.is_active = 1 -- 取人员有效的
          AND te.emp_function_role = 'PE' -- 取花名册人员角色为PE的
        GROUP BY split(tg.org_path_name,'/')[1],split(tg.org_path_name,'/')[2],te.emp_id,te.emp_name,te.emp_position,te.prg_path_name,te.is_job,DATE(te.hired_date),DATE(te.quit_date)
      )tmp
      WHERE tmp.rn = 1 -- 实现一人员对应一组织架构
    )tu  
    LEFT JOIN
    (
      SELECT days, -- 日期
             day_type -- 日期类型
      FROM ${dim_dbname}.dim_day_date -- 日期维表
      WHERE days >= '2021-07-01' AND days <= '${pre1_date}' -- 只取2021年7月1日之后的日期补零
      GROUP BY days,day_type
    )td
    ON td.days >= tu.hired_date AND td.days <= IF(tu.quit_date IS NULL,'${pre1_date}',tu.quit_date) -- 取入职日期作为开始补零时间，取离职日期作为结束补零时间，未离职的取至今
  )tud
  LEFT JOIN 
  (
    SELECT p.applicant_user_id,
           p.work_status, 
           p.job_content,
           p.log_date,
           IF(p.working_hours IS NULL AND p.log_date IS NOT NULL,0,p.working_hours) AS working_hours,
           nvl(pvd.project_code,p.project_code) AS project_code,
           p.project_manager AS project_manage,
           IF(p.data_source = 'dtk',p.id,p.process_instance_id) AS business_id,
           p.applicant_user_name AS originator_user_name,
           p.data_source,
           pvd.project_name,
           pvd.project_area,
           pvd.project_ft,
           pvd.project_priority,
           pvd.project_progress_stage,
           pvd.project_area_group,
		   pvd.is_active,
           CASE WHEN p.project_code IS NOT NULL AND pvd.project_code IS NULL THEN '项目编码异常' END AS unusual_res,
           IF(ROW_NUMBER()OVER(PARTITION BY p.log_date,pvd.project_code,p.applicant_user_id ORDER BY p.data_source DESC,IF(p.data_source = 'dtk',p.id,p.process_instance_id) DESC) > 1,0,1) AS is_valid
    FROM ${dwd_dbname}.dwd_pms_project_emp_log_info_df p -- pms人员日志数据信息
    LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail pvd -- 项目大表
    ON p.project_code = pvd.project_code OR p.project_code = pvd.project_sale_code
    WHERE p.d = '${pre1_date}' AND p.role_type = 'PE' -- 筛选PE数据
  )t1
  ON t1.applicant_user_id = tud.emp_id AND tud.days = t1.log_date 
  LEFT JOIN 
  (
    SELECT l1.originator_user_id AS emp_id, -- 人员编码
           l1.stat_date, -- 请假日期
           CASE WHEN l2.leave_type IS NULL THEN l1.leave_type ELSE '全天请假' END AS leave_type -- 请假类型
    FROM 
    (
      SELECT l.originator_user_id, -- 人员编码
             CAST(l.leave_date AS DATE) AS stat_date, -- 请假日期
             CASE WHEN l.period_type = '全天' THEN '全天请假'
                  WHEN l.period_type = '下午' THEN '下半天请假'
                  WHEN l.period_type = '上午' THEN '上半天请假' 
                  WHEN l.period_type = '其它' THEN '哺乳假' END AS leave_type, -- 请假类型
             ROW_NUMBER()OVER(PARTITION BY l.originator_user_id,CAST(l.leave_date AS DATE) ORDER BY CASE WHEN l.period_type = '全天' THEN '全天请假'
                                                                                                         WHEN l.period_type = '下午' THEN '下半天请假'
                                                                                                         WHEN l.period_type = '上午' THEN '上半天请假' 
                                                                                                         WHEN l.period_type = '其它' THEN '哺乳假' END ASC) rn -- 按请假类型排序
      FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l -- 钉钉员工请假日天记录表
      WHERE l.d = '${pre1_date}' AND l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' -- 只取有效记录 且 审批通过 且 审批状态完成
    )l1
    LEFT JOIN 
    (
      SELECT l.originator_user_id, -- 人员编码
             CAST(l.leave_date AS DATE) AS stat_date, -- 请假日期
             CASE WHEN l.period_type = '全天' THEN '全天请假'
                  WHEN l.period_type = '下午' THEN '下半天请假'
                  WHEN l.period_type = '上午' THEN '上半天请假' 
                  WHEN l.period_type = '其它' THEN '哺乳假' END AS leave_type, -- 请假类型
             ROW_NUMBER()OVER(PARTITION BY l.originator_user_id,CAST(l.leave_date AS DATE) ORDER BY CASE WHEN l.period_type = '全天' THEN '全天请假'
                                                                                                         WHEN l.period_type = '下午' THEN '下半天请假'
                                                                                                         WHEN l.period_type = '上午' THEN '上半天请假' 
                                                                                                         WHEN l.period_type = '其它' THEN '哺乳假' END ASC) rn -- 按请假类型排序
      FROM ${dwd_dbname}.dwd_dtk_process_leave_dayily_info_df l -- 钉钉员工请假日天记录表
      WHERE l.d = '${pre1_date}' AND l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' -- 只取有效记录 且 审批通过 且 审批状态完成
    )l2
    ON l1.originator_user_id = l2.originator_user_id AND l1.stat_date = l2.stat_date AND l1.leave_type != l2.leave_type -- 请假类型不同的
    WHERE l1.rn = 1 -- 保证一天只有一种请假类型输出
  ) t12 
  ON t12.emp_id = tud.emp_id AND t12.stat_date = tud.days
)tt
LEFT JOIN 
(
  SELECT concat_ws(',',collect_set(t.business_id)) AS business_id, -- 审批编码
         t.originator_user_id,-- 发起人编码
         t.travel_date -- 出差日期
  FROM 
  (
    SELECT *,
           DENSE_RANK() OVER(partition by originator_user_id,travel_date order by travel_days DESC) as rn1,
           ROW_NUMBER() OVER(PARTITION BY originator_user_id,travel_date,period_type ORDER BY period_type) as rn2
    FROM 
    (
      SELECT t.process_instance_id,
             t.business_id,
             t.create_time AS business_create_time,
             t.finish_time AS business_finish_time,
             t.originator_dept_name,
             t.originator_user_id,
             t.originator_user_name,
             t.project_code AS original_project_code,
             regexp_replace(t.business_trip, '\\\\s+', ' ') AS business_trip,
             t.travel_date,
             t.every_days AS travel_days,
             t.period_type,
             CASE WHEN t.period_type = '全天' THEN '全天出差'
                  WHEN t.period_type = '下午' THEN '下半天出差'
                  WHEN t.period_type = '上午' THEN '上半天出差' END travel_type,
             t.data_source
      FROM ${dwd_dbname}.dwd_dtk_process_business_travel_dayily_info_df t
      WHERE t.d = '${pre1_date}' AND IF(t.data_source = 'DTK',t.is_valid = 1 AND t.approval_result = 'agree' AND t.approval_status = 'COMPLETED',t.approval_status = '审批通过')
    )t1
  )t
  WHERE t.rn1 = 1 AND t.rn2 = 1
  GROUP BY t.originator_user_id,t.travel_date
)tt1
ON tt.emp_id = tt1.originator_user_id AND tt.days = tt1.travel_date;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"