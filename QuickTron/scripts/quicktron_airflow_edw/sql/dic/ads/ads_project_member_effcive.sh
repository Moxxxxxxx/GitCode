#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-06-16 创建
#-- 2 wangyingying 2022-06-16 增加累计未上线数量、当月上线数量字段
#-- 3 wangyingying 2022-06-17 增加月份首天日期
#-- 4 wangyingying 2022-06-18 优化PE日志逻辑
#-- 5 wangyingying 2022-07-05 优化PE日志请假加班逻辑
#-- 6 wangyingying 2022-07-25 优化PE日志请假数据排序规则
#-- 7 wangyingying 2022-08-22 优化PE日志请假加班逻辑
#-- 8 wangyingying 2022-10-25 替换项目大表临时表
#-- 9 wangyingying 2022-11-24 优化代码
#-- 10 wangyingying 2023-02-13 增加是否活跃字段
#-- 11 wangyingying 2023-02-14 优化PE日志逻辑
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
--ads_project_member_effcive    --项目进展人员效率

-- pe日志
WITH pe_detail AS 
(
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
       tt.project_progress_stage, -- 项目进展阶段
	   tt.project_stage, -- 项目阶段
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
		   pvd.project_stage,
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
ON tt.emp_id = tt1.originator_user_id AND tt.days = tt1.travel_date
)

INSERT overwrite table ${ads_dbname}.ads_project_member_effcive
SELECT '' AS id, -- 主键
       t1.month_scope, -- 统计月份
       DATE(CONCAT(t1.month_scope,'-01')) AS month_scope_first_day, -- 统计月份首天
       t1.project_area, -- 大区
       t1.project_priority, -- 项目等级
       t1.project_ft, -- 项目ft
       t1.project_stage, -- 项目阶段
       t1.project_area_group, -- 项目区域组
	   t1.is_active, -- 是否活跃
       t1.no_online_num_total, -- 累计未上线项目数量
       t1.no_online_amount_total, -- 累计未上线项目金额
       t1.online_num_month, -- 当月上线项目数量
       t1.online_amount_month, -- 当月上线项目金额
       t1.no_final_inspection_num_total, -- 累计未验收项目数量
       t1.no_final_inspection_amount_total, -- 累计未验收项目金额
       t1.final_inspection_num_month, -- 当月验收项目数量
       t1.final_inspection_amount_month, -- 当月验收项目金额
       nvl(t2.suspend_num_month,0) AS suspend_num_month, -- 当月暂停项目数量
       nvl(t2.suspend_amount_month,0) AS suspend_amount_month, -- 当月暂停项目金额
       nvl(t3.pe_num,0) AS pe_num, -- pe人员数量
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
-- 累计未上线/未验收项目总数 + 当月上线/验收项目数量
FROM
(
  SELECT a.project_code_class, -- 项目编码分类
         a.month_scope, -- 统计月份
         a.project_area, -- 大区
         a.project_priority, -- 项目等级
         a.project_ft, -- 项目ft
         a.project_stage, -- 项目阶段
         a.project_area_group, -- 项目区域组
		 a.is_active, -- 是否活跃
         SUM(b.handover_num) AS handover_num_total, -- 累计交接项目数量
         SUM(b.handover_amount) AS handover_amount_total, -- 累计交接项目金额
         AVG(a.online_num) AS online_num_month, -- 当月上线项目数量
         AVG(a.online_amount) AS online_amount_month, -- 当月上线项目金额
         SUM(b.online_num) AS online_num_total, -- 累计上线项目数量
         SUM(b.online_amount) AS online_amount_total, -- 累计上线项目金额
         SUM(b.no_online_num) AS no_online_num_total, -- 累计未上线项目数量 
         SUM(b.no_online_amount) AS no_online_amount_total, -- 累计未上线项目金额
         AVG(a.final_inspection_num) AS final_inspection_num_month, -- 当月验收项目数量
         AVG(a.final_inspection_amount) AS final_inspection_amount_month, -- 当月验收项目金额
         SUM(b.final_inspection_num) AS final_inspection_num_total, -- 累计验收项目数量
         SUM(b.final_inspection_amount) AS final_inspection_amount_total, -- 累计验收项目金额
         SUM(b.no_final_inspection_num) AS no_final_inspection_num_total, -- 累计未验收项目数量 
         SUM(b.no_final_inspection_amount) AS no_final_inspection_amount_total -- 累计未验收项目金额
  FROM 
  (
    SELECT total.project_code_class, -- 项目编码分类
           total.month_scope, -- 统计月份
           total.project_area, -- 大区
           total.project_priority, -- 项目等级
           total.project_ft, -- 项目ft
           total.project_stage, -- 项目阶段
           total.project_area_group, -- 项目区域组
		   total.is_active, -- 是否活跃
           total.handover_num, -- 交接项目数量
           total.handover_amount, -- 交接项目金额
           nvl(online.online_num,0) AS online_num, -- 上线项目数量
           nvl(online.online_amount,0) AS online_amount, -- 上线项目数量
           total.handover_num - nvl(online.online_num,0) AS no_online_num, -- 未上线项目数量 
           total.handover_amount - nvl(online.online_amount,0) AS no_online_amount, -- 未上线项目金额
           nvl(final_inspection.final_inspection_num,0) AS final_inspection_num, -- 验收项目数量
           nvl(final_inspection.final_inspection_amount,0) AS final_inspection_amount, -- 验收项目金额
           total.handover_num - nvl(final_inspection.final_inspection_num,0) AS no_final_inspection_num, -- 未验收项目数量 
           total.handover_amount - nvl(final_inspection.final_inspection_amount,0) AS no_final_inspection_amount -- 验收项目金额
    FROM 
    -- 交接阶段
    (
      SELECT i.project_code_class, -- 项目编码分类
             td.month_scope, -- 统计月份
             i.project_area, -- 大区
             i.project_priority, -- 项目等级
             i.project_ft, -- 项目ft
             i.project_stage, -- 项目阶段
             i.project_area_group, -- 项目区域组
			 i.is_active, -- 是否活跃
             SUM(CASE WHEN tmp1.project_handover_end_time IS NOT NULL THEN 1 ELSE 0 END) AS handover_num, -- 交接项目数量
             SUM(CASE WHEN tmp1.project_handover_end_time is not null THEN nvl(tmp1.amount,0) ELSE 0 END) AS handover_amount -- 交接项目金额
      FROM 
      (
        SELECT CONCAT(year_date,'-',LPAD(CAST(month_date AS STRING),2,'0')) AS month_scope
        FROM ${dim_dbname}.dim_day_date
        WHERE days >= '2018-01-01' AND days <= '${pre1_date}'
        GROUP BY CONCAT(year_date,'-',LPAD(CAST(month_date AS STRING),2,'0'))
      ) td
      LEFT JOIN
      (
	    SELECT pcc.project_code_class, -- 项目编码分类
               pa.project_area, -- 大区
               pp.project_priority, -- 项目等级
               pf.project_ft, -- 项目ft
               ps.project_stage, -- 项目阶段
               pag.project_area_group, -- 项目区域组
			   ia.is_active -- 是否活跃
        FROM 
        (
          SELECT split(project_code_class,',') AS a, -- 项目编码分类
                 split(nvl(project_area,'未知'),',') AS b, -- 大区
                 split(nvl(project_priority,'未知'),',') AS c, -- 项目等级
                 split(nvl(project_ft,'未知'),',') AS d, -- 项目ft
                 split(nvl(project_stage,'未知'),',') AS e, -- 项目阶段
                 split(nvl(project_area_group,'未知'),',') AS f, -- 项目区域组
				 split(nvl(is_active,'未知'),',') AS g -- 是否活跃
          FROM ${tmp_dbname}.tmp_pms_project_general_view_detail
          WHERE project_code_class = 'A' -- 只筛选A开头的正式项目
          GROUP BY project_code_class,project_area,project_priority,project_ft,project_stage,project_area_group,is_active
        ) tmp
        LATERAL view explode(a) pcc AS project_code_class -- 项目编码分类
        LATERAL view explode(b) pa AS project_area -- 大区
        LATERAL view explode(c) pp AS project_priority -- 项目等级
        LATERAL view explode(d) pf AS project_ft -- 项目ft
        LATERAL view explode(e) ps AS project_stage -- 项目阶段
        LATERAL view explode(f) pag AS project_area_group -- 项目区域组
		LATERAL view explode(g) ia AS is_active -- 是否活跃
      ) i
      LEFT JOIN 
      (
	    SELECT *
	    FROM ${tmp_dbname}.tmp_pms_project_general_view_detail d
	    WHERE d.project_code_class = 'A' -- 只筛选A开头的正式项目
      ) tmp1
      ON nvl(i.project_code_class,'unknown1') = nvl(tmp1.project_code_class,'unknown2') AND nvl(td.month_scope,'unknown1') = nvl(date_format(tmp1.project_handover_end_time,'yyyy-MM'),'unknown2') AND nvl(i.project_area,'unknown1') = nvl(tmp1.project_area,'unknown2') AND nvl(i.project_priority,'unknown1') = nvl(tmp1.project_priority,'unknown2') AND nvl(i.project_ft,'unknown1') = nvl(tmp1.project_ft,'unknown2') AND nvl(i.project_stage,'unknown1') = nvl(tmp1.project_stage,'unknown2') AND nvl(i.project_area_group,'unknown1') = nvl(tmp1.project_area_group,'unknown2') AND nvl(i.is_active,'unknown1') = nvl(tmp1.is_active,'unknown2')
      GROUP BY i.project_code_class,td.month_scope,i.project_area,i.project_priority,i.project_ft,i.project_stage,i.project_area_group,i.is_active
    )total
    -- 上线阶段
    LEFT JOIN
    (
      SELECT tmp2.project_code_class, -- 项目编码分类
             tmp2.online_process_month, -- 上线审批完成月份
             tmp2.project_area, -- 大区
             tmp2.project_priority, -- 项目等级
             tmp2.project_ft, -- 项目ft
             tmp2.project_stage, -- 项目阶段
             tmp2.project_area_group, -- 项目区域组
			 tmp2.is_active, -- 是否活跃
             SUM(CASE WHEN tmp2.is_online = '已上线' THEN 1 ELSE 0 END) AS online_num, -- 上线项目数量
             SUM(CASE WHEN tmp2.is_online = '已上线' THEN nvl(tmp2.amount,0) ELSE 0 END) AS online_amount -- 上线项目数量
      FROM 
      (
	    SELECT *
	    FROM ${tmp_dbname}.tmp_pms_project_general_view_detail d
	    WHERE d.project_code_class = 'A' -- 只筛选A开头的正式项目
      )tmp2
      WHERE tmp2.is_online = '已上线' -- 只筛选已上线的
      GROUP BY tmp2.project_code_class,tmp2.online_process_month,tmp2.project_area,tmp2.project_priority,tmp2.project_ft,tmp2.project_stage,tmp2.project_area_group,tmp2.is_active
    )online
    ON nvl(total.project_code_class,'unknown1') = nvl(online.project_code_class,'unknown2') AND nvl(total.month_scope,'unknown1') = nvl(online.online_process_month,'unknown2') AND nvl(total.project_area,'unknown1') = nvl(online.project_area,'unknown2') AND nvl(total.project_priority,'unknown1') = nvl(online.project_priority,'unknown2') AND nvl(total.project_ft,'unknown1') = nvl(online.project_ft,'unknown2') AND nvl(total.project_stage,'unknown1') = nvl(online.project_stage,'unknown2') AND nvl(total.project_area_group,'unknown1') = nvl(online.project_area_group,'unknown2') AND nvl(total.is_active,'unknown1') = nvl(online.is_active,'unknown2')
    LEFT JOIN
    -- 验收极端
    (
      SELECT tmp3.project_code_class, -- 项目编码分类
             tmp3.final_inspection_process_month, -- 验收审批完成月份
             tmp3.project_area, -- 大区
             tmp3.project_priority, -- 项目等级
             tmp3.project_ft, -- 项目ft
             tmp3.project_stage, -- 项目阶段
             tmp3.project_area_group, -- 项目区域组
			 tmp3.is_active, -- 是否活跃
             SUM(CASE WHEN tmp3.is_final_inspection = '已验收' THEN 1 ELSE 0 END) AS final_inspection_num, -- 验收项目数量
             SUM(CASE WHEN tmp3.is_final_inspection = '已验收' THEN nvl(tmp3.amount,0) ELSE 0 END) AS final_inspection_amount -- 验收项目金额
      FROM 
      (
	    SELECT *
	    FROM ${tmp_dbname}.tmp_pms_project_general_view_detail d
	    WHERE d.project_code_class = 'A' -- 只筛选A开头的正式项目
      )tmp3
      WHERE tmp3.is_final_inspection = '已验收' -- 只筛选已验收的
      GROUP BY tmp3.project_code_class,tmp3.final_inspection_process_month,tmp3.project_area,tmp3.project_priority,tmp3.project_ft,tmp3.project_stage,tmp3.project_area_group,tmp3.is_active
    )final_inspection
    ON nvl(total.project_code_class,'unknown1') = nvl(final_inspection.project_code_class,'unknown2') AND nvl(total.month_scope,'unknown1') = nvl(final_inspection.final_inspection_process_month,'unknown2') AND nvl(total.project_area,'unknown1') = nvl(final_inspection.project_area,'unknown2') AND nvl(total.project_priority,'unknown1') = nvl(final_inspection.project_priority,'unknown2') AND nvl(total.project_ft,'unknown1') = nvl(final_inspection.project_ft,'unknown2') AND nvl(total.project_stage,'unknown1') = nvl(final_inspection.project_stage,'unknown2') AND nvl(total.project_area_group,'unknown1') = nvl(final_inspection.project_area_group,'unknown2') AND nvl(total.is_active,'unknown1') = nvl(final_inspection.is_active,'unknown2')
  )a 
  LEFT JOIN 
  (
    SELECT total.project_code_class, -- 项目编码分类
           total.month_scope, -- 统计月份
           total.project_area, -- 大区
           total.project_priority, -- 项目等级
           total.project_ft, -- 项目ft
           total.project_stage, -- 项目阶段
           total.project_area_group, -- 项目区域组
		   total.is_active, -- 是否活跃
           total.handover_num, -- 交接项目数量
           total.handover_amount, -- 交接项目金额
           nvl(online.online_num,0) AS online_num, -- 上线项目数量
           nvl(online.online_amount,0) AS online_amount, -- 上线项目数量
           total.handover_num - nvl(online.online_num,0) AS no_online_num, -- 未上线项目数量 
           total.handover_amount - nvl(online.online_amount,0) AS no_online_amount, -- 未上线项目金额
           nvl(final_inspection.final_inspection_num,0) AS final_inspection_num, -- 验收项目数量
           nvl(final_inspection.final_inspection_amount,0) AS final_inspection_amount, -- 验收项目金额
           total.handover_num - nvl(final_inspection.final_inspection_num,0) AS no_final_inspection_num, -- 未验收项目数量 
           total.handover_amount - nvl(final_inspection.final_inspection_amount,0) AS no_final_inspection_amount -- 验收项目金额
    FROM 
    -- 交接阶段
    (
      SELECT i.project_code_class, -- 项目编码分类
             td.month_scope, -- 统计月份
             i.project_area, -- 大区
             i.project_priority, -- 项目等级
             i.project_ft, -- 项目ft
             i.project_stage, -- 项目阶段
             i.project_area_group, -- 项目区域组
			 i.is_active, -- 是否活跃
             SUM(CASE WHEN tmp1.project_handover_end_time IS NOT NULL THEN 1 ELSE 0 END) AS handover_num, -- 交接项目数量
             SUM(CASE WHEN tmp1.project_handover_end_time is not null THEN nvl(tmp1.amount,0) ELSE 0 END) AS handover_amount -- 交接项目金额
      FROM 
      (
        SELECT CONCAT(year_date,'-',LPAD(CAST(month_date AS STRING),2,'0')) AS month_scope
        FROM ${dim_dbname}.dim_day_date
        WHERE days >= '2018-01-01' AND days <= '${pre1_date}'
        GROUP BY CONCAT(year_date,'-',LPAD(CAST(month_date AS STRING),2,'0'))
      ) td
      LEFT JOIN
      (
	    SELECT pcc.project_code_class, -- 项目编码分类
               pa.project_area, -- 大区
               pp.project_priority, -- 项目等级
               pf.project_ft, -- 项目ft
               ps.project_stage, -- 项目阶段
               pag.project_area_group, -- 项目区域组
			   ia.is_active -- 是否活跃
        FROM 
        (
          SELECT split(project_code_class,',') AS a, -- 项目编码分类
                 split(nvl(project_area,'未知'),',') AS b, -- 大区
                 split(nvl(project_priority,'未知'),',') AS c, -- 项目等级
                 split(nvl(project_ft,'未知'),',') AS d, -- 项目ft
                 split(nvl(project_stage,'未知'),',') AS e, -- 项目阶段
                 split(nvl(project_area_group,'未知'),',') AS f, -- 项目区域组
				 split(nvl(is_active,'未知'),',') AS g -- 是否活跃
          FROM ${tmp_dbname}.tmp_pms_project_general_view_detail
          WHERE project_code_class = 'A' -- 只筛选A开头的正式项目
          GROUP BY project_code_class,project_area,project_priority,project_ft,project_stage,project_area_group,is_active
        ) tmp
        LATERAL view explode(a) pcc AS project_code_class -- 项目编码分类
        LATERAL view explode(b) pa AS project_area -- 大区
        LATERAL view explode(c) pp AS project_priority -- 项目等级
        LATERAL view explode(d) pf AS project_ft -- 项目ft
        LATERAL view explode(e) ps AS project_stage -- 项目阶段
        LATERAL view explode(f) pag AS project_area_group -- 项目区域组
		LATERAL view explode(g) ia AS is_active -- 是否活跃
      ) i
      LEFT JOIN 
      (
	    SELECT *
	    FROM ${tmp_dbname}.tmp_pms_project_general_view_detail d
	    WHERE d.project_code_class = 'A' -- 只筛选A开头的正式项目
      ) tmp1
      ON nvl(i.project_code_class,'unknown1') = nvl(tmp1.project_code_class,'unknown2') AND nvl(td.month_scope,'unknown1') = nvl(date_format(tmp1.project_handover_end_time,'yyyy-MM'),'unknown2') AND nvl(i.project_area,'unknown1') = nvl(tmp1.project_area,'unknown2') AND nvl(i.project_priority,'unknown1') = nvl(tmp1.project_priority,'unknown2') AND nvl(i.project_ft,'unknown1') = nvl(tmp1.project_ft,'unknown2') AND nvl(i.project_stage,'unknown1') = nvl(tmp1.project_stage,'unknown2') AND nvl(i.project_area_group,'unknown1') = nvl(tmp1.project_area_group,'unknown2') AND nvl(i.is_active,'unknown1') = nvl(tmp1.is_active,'unknown2')
      GROUP BY i.project_code_class,td.month_scope,i.project_area,i.project_priority,i.project_ft,i.project_stage,i.project_area_group,i.is_active
    )total
    -- 上线阶段
    LEFT JOIN
    (
      SELECT tmp2.project_code_class, -- 项目编码分类
             tmp2.online_process_month, -- 上线审批完成月份
             tmp2.project_area, -- 大区
             tmp2.project_priority, -- 项目等级
             tmp2.project_ft, -- 项目ft
             tmp2.project_stage, -- 项目阶段
             tmp2.project_area_group, -- 项目区域组
			 tmp2.is_active, -- 是否活跃
             SUM(CASE WHEN tmp2.is_online = '已上线' THEN 1 ELSE 0 END) AS online_num, -- 上线项目数量
             SUM(CASE WHEN tmp2.is_online = '已上线' THEN nvl(tmp2.amount,0) ELSE 0 END) AS online_amount -- 上线项目数量
      FROM 
      (
	    SELECT *
	    FROM ${tmp_dbname}.tmp_pms_project_general_view_detail d
	    WHERE d.project_code_class = 'A' -- 只筛选A开头的正式项目
      )tmp2
      WHERE tmp2.is_online = '已上线' -- 只筛选已上线的
      GROUP BY tmp2.project_code_class,tmp2.online_process_month,tmp2.project_area,tmp2.project_priority,tmp2.project_ft,tmp2.project_stage,tmp2.project_area_group,tmp2.is_active
    )online
    ON nvl(total.project_code_class,'unknown1') = nvl(online.project_code_class,'unknown2') AND nvl(total.month_scope,'unknown1') = nvl(online.online_process_month,'unknown2') AND nvl(total.project_area,'unknown1') = nvl(online.project_area,'unknown2') AND nvl(total.project_priority,'unknown1') = nvl(online.project_priority,'unknown2') AND nvl(total.project_ft,'unknown1') = nvl(online.project_ft,'unknown2') AND nvl(total.project_stage,'unknown1') = nvl(online.project_stage,'unknown2') AND nvl(total.project_area_group,'unknown1') = nvl(online.project_area_group,'unknown2') AND nvl(total.is_active,'unknown1') = nvl(online.is_active,'unknown2')
    LEFT JOIN
    -- 验收极端
    (
      SELECT tmp3.project_code_class, -- 项目编码分类
             tmp3.final_inspection_process_month, -- 验收审批完成月份
             tmp3.project_area, -- 大区
             tmp3.project_priority, -- 项目等级
             tmp3.project_ft, -- 项目ft
             tmp3.project_stage, -- 项目阶段
             tmp3.project_area_group, -- 项目区域组
			 tmp3.is_active, -- 是否活跃
             SUM(CASE WHEN tmp3.is_final_inspection = '已验收' THEN 1 ELSE 0 END) AS final_inspection_num, -- 验收项目数量
             SUM(CASE WHEN tmp3.is_final_inspection = '已验收' THEN nvl(tmp3.amount,0) ELSE 0 END) AS final_inspection_amount -- 验收项目金额
      FROM 
      (
	    SELECT *
	    FROM ${tmp_dbname}.tmp_pms_project_general_view_detail d
	    WHERE d.project_code_class = 'A' -- 只筛选A开头的正式项目
      )tmp3
      WHERE tmp3.is_final_inspection = '已验收' -- 只筛选已验收的
      GROUP BY tmp3.project_code_class,tmp3.final_inspection_process_month,tmp3.project_area,tmp3.project_priority,tmp3.project_ft,tmp3.project_stage,tmp3.project_area_group,tmp3.is_active
    )final_inspection
    ON nvl(total.project_code_class,'unknown1') = nvl(final_inspection.project_code_class,'unknown2') AND nvl(total.month_scope,'unknown1') = nvl(final_inspection.final_inspection_process_month,'unknown2') AND nvl(total.project_area,'unknown1') = nvl(final_inspection.project_area,'unknown2') AND nvl(total.project_priority,'unknown1') = nvl(final_inspection.project_priority,'unknown2') AND nvl(total.project_ft,'unknown1') = nvl(final_inspection.project_ft,'unknown2') AND nvl(total.project_stage,'unknown1') = nvl(final_inspection.project_stage,'unknown2') AND nvl(total.project_area_group,'unknown1') = nvl(final_inspection.project_area_group,'unknown2') AND nvl(total.is_active,'unknown1') = nvl(final_inspection.is_active,'unknown2')
  )b
  ON nvl(a.project_code_class,'unknown1') = nvl(b.project_code_class,'unknown2') AND nvl(a.project_area,'unknown1') = nvl(b.project_area,'unknown2') AND nvl(a.project_area,'unknown1') = nvl(b.project_area,'unknown2') AND nvl(a.project_priority,'unknown1') = nvl(b.project_priority,'unknown2') AND nvl(a.project_ft,'unknown1') = nvl(b.project_ft,'unknown2') AND nvl(a.project_stage,'unknown1') = nvl(b.project_stage,'unknown2') AND nvl(a.project_area_group,'unknown1') = nvl(b.project_area_group,'unknown2') AND nvl(a.is_active,'unknown1') = nvl(b.is_active,'unknown2') AND a.month_scope >= b.month_scope
  GROUP BY a.project_code_class,a.month_scope,a.project_area,a.project_priority,a.project_ft,a.project_stage,a.project_area_group,a.is_active
)t1
-- 当月暂停项目数量
LEFT JOIN 
(
  SELECT b.project_code_class, -- 项目编码分类
         b.project_area, -- 大区
         b.project_priority, -- 项目等级
         b.project_ft, -- 项目ft
         b.project_stage, -- 项目阶段
         b.project_area_group, -- 项目区域组
		 b.is_active, -- 是否活跃
         DATE_FORMAT(a.end_time,'yyyy-MM') AS month_scope, -- 项目暂停审批完成月份
         COUNT(DISTINCT b.project_code) AS suspend_num_month, -- 暂停项目数量
         SUM(nvl(b.amount,0)) AS  suspend_amount_month -- 暂停项目金额
  FROM ${dwd_dbname}.dwd_bpm_project_suspend_apply_info_ful a -- bpm项目暂停申请单
  LEFT JOIN 
  (
    SELECT *
	FROM ${tmp_dbname}.tmp_pms_project_general_view_detail d
	WHERE d.project_code_class = 'A' -- 只筛选A开头的正式项目
  )b
  ON nvl(a.project_code,'unknown1') = nvl(b.project_code,'unknown2')
  WHERE a.approval_staus = 30 -- 审批完成 
    AND b.project_code_class IS NOT NULL
  GROUP BY b.project_code_class,b.project_area,date_format(a.end_time,'yyyy-MM'),b.project_priority,b.project_ft,b.project_stage,b.project_area_group,b.is_active
)t2
ON nvl(t1.project_code_class,'unknown1') = nvl(t2.project_code_class,'unknown2') AND nvl(t1.project_area,'unknown1') = nvl(t2.project_area,'unknown2') AND nvl(t1.month_scope,'unknown1') = nvl(t2.month_scope,'unknown2') AND nvl(t1.project_priority,'unknown1') = nvl(t2.project_priority,'unknown2') AND nvl(t1.project_ft,'unknown1') = nvl(t2.project_ft,'unknown2') AND nvl(t1.project_stage,'unknown1') = nvl(t2.project_stage,'unknown2') AND nvl(t1.project_area_group,'unknown1') = nvl(t2.project_area_group,'unknown2') AND nvl(t1.is_active,'unknown1') = nvl(t2.is_active,'unknown2')
-- PE人员数量
LEFT JOIN 
(
  SELECT SUBSTR(project_code,0,1) AS project_code_class, -- 项目编码分类
         project_area, -- 大区
         project_priority, -- 项目等级
         project_ft, -- 项目ft
         project_stage, -- 项目阶段
         project_area_group, -- 项目区域组
		 is_active, -- 是否活跃
         DATE_FORMAT(log_date,'yyyy-MM') AS month_scope, -- PE日志统计月份
         COUNT(DISTINCT emp_name) AS pe_num -- PE数量
  FROM pe_detail
  WHERE SUBSTR(project_code,0,1) = 'A' -- 只筛选A开头的正式项目
    AND ischeck = '已打卡' AND day_type != '全天请假'  AND is_valid = 1 -- 只取有效数据
  GROUP BY SUBSTR(project_code,0,1),project_area,project_priority,project_ft,project_stage,project_area_group,is_active,DATE_FORMAT(log_date,'yyyy-MM')
)t3
ON nvl(t1.project_code_class,'unknown1') = nvl(t3.project_code_class,'unknown2') AND nvl(t1.project_area,'unknown1') = nvl(t3.project_area,'unknown2') AND nvl( t1.month_scope,'unknown1') = nvl(t3.month_scope,'unknown2') AND nvl(t1.project_priority,'unknown1') = nvl(t3.project_priority,'unknown2') AND nvl(t1.project_ft,'unknown1') = nvl(t3.project_ft,'unknown2') AND nvl(t1.project_stage,'unknown1') = nvl(t3.project_stage,'unknown2') AND nvl(t1.project_area_group,'unknown1') = nvl(t3.project_area_group,'unknown2') AND nvl(t1.is_active,'unknown1') = nvl(t3.is_active,'unknown2');
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"