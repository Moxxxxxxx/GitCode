#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-10-14 创建
#-- 2 wangyingying 2022-10-20 融合pms项目数据
#-- 3 wangyingying 2022-10-20 替换项目大表临时表
#-- 4 wangyingying 2022-10-27 合并工时项目数据
#-- 5 wangyingying 2022-11-07 弥补空值造成的未筛选情况
#-- 6 wangyingying 2022-11-21 增加海外PE离线表数据
#-- 7 wangyingying 2022-11-22 增加海外劳务离线表数据
#-- 8 wangyingying 2022-11-22 优化代码
#-- 9 wangyingying 2022-11-26 更新海外pe费率
#-- 10 wangyingying 2022-12-07 增加项目阶段和项目状态
#-- 11 wangyingying 2022-12-13 增加AR费用
#-- 12 wangyingying 2022-12-14 增加合同币种
#-- 13 wangyingying 2022-12-16 增加出差数据源
#-- 14 wangyingying 2022-12-17 增加汇联易报销数据源
#-- 15 wangyingying 2022-12-23 外包硬件筛选条件修改
#-- 16 wangyingying 2022-12-27 增加项目集项目编码、是否为主项目
#-- 17 wangyingying 2022-12-28 增加项目类型字段
#-- 18 wangyingying 2023-01-05 调整工时人员范围
#-- 19 wangyingying 2023-01-11 调整出差逻辑
#-- 20 wangyingying 2023-01-12 增加项目集状态、类型字段
#-- 21 wangyingying 2023-01-28 增加上线日期、终验日期字段
#-- 22 wangyingying 2023-01-30 增加汇率、生效日期、失效日期字段
#-- 23 wangyingying 2023-02-13 增加是否活跃字段
#-- 24 wangyingying 2023-03-01 增加项目经理字段
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
-- set hive.execution.engine=mr;
set mapreduce.job.queuename=hive;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.vectorized.execution.enabled=false;
-------------------------------------------------------------------------------------------------------------00
--ads_pms_project_profit_detail    --pms项目利润表

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
      SELECT t.originator_user_id,
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
manhour_detail AS
(
  SELECT tud.team_ft, -- 一级部门
         tud.team_group, -- 二级部门
	     tud.team_sub_group, -- 三级部门
         tud.user_name AS team_member, -- 团队成员
         tud.emp_position, -- 成员职位
         tud.is_job, -- 是否在职
         tud.hired_date, -- 入职日期
         tud.quit_date, -- 离职日期
         tud.is_need_fill_manhour, -- 是否需要填写工时
         tud.org_role_type, -- 组织角色类型
         tud.virtual_role_type, -- 虚拟组角色类型
         tud.module_branch, -- 负责模块分支
         tud.virtual_org_name, -- 虚拟组组织架构
         tt.org_name_1 AS project_org_name, -- 项目所属组织
         tt.project_classify_name AS project_classify_name, -- 项目分类名称
         tt.sprint_classify_name AS sprint_classify_name, -- 迭代分类名称
         tt.external_project_code, -- 外部项目编码
         tt.external_project_name, -- 外部项目名称
         tt.project_bpm_code, -- 内部项目编码
         tt.project_bpm_name, -- 内部项目名称
         tt.project_type_name, -- 项目类型名称
         CAST(tt.stat_date AS DATE) AS work_create_date, -- 工作项创建时间
         tt.work_id, -- 工作项编码
         tt.summary AS work_summary, -- 工作项标题
         tt.task_desc AS work_desc, -- 工作项描述
         tt.work_type AS work_type, -- 工作项类型
         tt.work_status, -- 工作项状态
         CAST(tud.days AS DATE) AS work_check_date, -- 工作项登记日期
         IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) AS day_type, -- 日期类型 =>如果加班类型不为空的话输出<加班类型>，否则判断请假类型是否为空，如果请假类型为空的话输出<日期类型>，否则判断哺乳假是否在周末/节假日内，如果不在的话输出<日期类型-哺乳假>，在的话输出<日期类型>，否则输出<请假类型>
         CAST(nvl(tt3.travel_days,0) AS DECIMAL(10,2)) AS travel_days, -- 出差天数
         CAST(nvl(t2.work_hour,0) AS DECIMAL(10,2)) AS work_hour, -- 登记工时
         IF(tt.work_id IS NULL,0,t2.actual_date) AS actual_date, -- 登记相差天数
         CASE WHEN tt.project_type_name IS NULL and tt.work_id IS NOT NULL AND t2.actual_date > 7 THEN '无效工时&违规登记'
              WHEN (tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码')) AND t2.actual_date > 7 THEN '编码异常&违规登记'
              WHEN tt.project_type_name IS NULL AND tt.work_id IS NOT NULL THEN '无效工时'
              WHEN t2.actual_date > 7 THEN '违规登记'
              WHEN tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码') THEN '编码异常'
         ELSE '无异常' END AS error_type, -- 异常类型
         IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))) AS work_hour_total, -- 去掉编码异常&违规登记的登记工时
         nvl(CAST(nvl(t2.work_hour,0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0) AS work_hour_rate, -- 登记工时占比
         CASE WHEN tud.team_ft = '制造部' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 700 * 1 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '制造部' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天加班','上半天加班') THEN 700 * 0.5 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '制造部' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天请假','上半天请假','下半天请假-哺乳假','上半天请假-哺乳假') AND (tud.day_type = '工作日' OR tud.day_type = '调休') THEN 700 * 0.5 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '硬件自动化' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1000 * 1 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '硬件自动化' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天加班','上半天加班') THEN 1000 * 0.5 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '硬件自动化' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天请假','上半天请假','下半天请假-哺乳假','上半天请假-哺乳假') AND (tud.day_type = '工作日' OR tud.day_type = '调休') THEN 1000 * 0.5 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = 'AMR FT' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1300 * 1 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = 'AMR FT' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天加班','上半天加班') THEN 1300 * 0.5 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = 'AMR FT' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天请假','上半天请假','下半天请假-哺乳假','上半天请假-哺乳假') AND (tud.day_type = '工作日' OR tud.day_type = '调休') THEN 1300 * 0.5 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '系统中台' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1300 * 1 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '系统中台' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天加班','上半天加班') THEN 1300 * 0.5 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '系统中台' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天请假','上半天请假','下半天请假-哺乳假','上半天请假-哺乳假') AND (tud.day_type = '工作日' OR tud.day_type = '调休') THEN 1300 * 0.5 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '箱式FT' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1300 * 1 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '箱式FT' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天加班','上半天加班') THEN 1300 * 0.5 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '箱式FT' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天请假','上半天请假','下半天请假-哺乳假','上半天请假-哺乳假') AND (tud.day_type = '工作日' OR tud.day_type = '调休') THEN 1300 * 0.5 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '智能搬运FT' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1300 * 1 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '智能搬运FT' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天加班','上半天加班') THEN 1300 * 0.5 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '智能搬运FT' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天请假','上半天请假','下半天请假-哺乳假','上半天请假-哺乳假') AND (tud.day_type = '工作日' OR tud.day_type = '调休') THEN 1300 * 0.5 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '已离职' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1300 * 1 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '已离职' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天加班','上半天加班') THEN 1300 * 0.5 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
              WHEN tud.team_ft = '已离职' AND IF(tt2.work_overtime_type IS NULL,IF(tt1.leave_type IS NULL,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type NOT IN ('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type IN ('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天请假','上半天请假','下半天请假-哺乳假','上半天请假-哺乳假') AND (tud.day_type = '工作日' OR tud.day_type = '调休') THEN 1300 * 0.5 * nvl(CAST(nvl(t2.work_hour, 0) AS DECIMAL(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' OR tt.project_bpm_code = '未知项目编码'),0,CAST(nvl(t1.work_hour, 0) AS DECIMAL(10, 2))),0)
         ELSE 0 END AS cost_amount -- 研发费用 => 按部门按天按登记工时占比*费率计算
  FROM 
  (
    SELECT IF(tu.is_job = 0,'已离职',tu.team_ft) AS team_ft, -- 一级部门 => 离职人员补充<已离职>
           tu.team_group, -- 二级部门
           tu.team_sub_group, -- 三级部门
           tu.emp_id, -- 人员编码
           tu.user_name, -- 人员名称
           tu.user_email, -- 人员邮箱
           tu.org_role_type, -- 组织角色类型
           tu.virtual_role_type, -- 虚拟组角色类型
           tu.module_branch, -- 负责模块分支
           tu.virtual_org_name, -- 虚拟组组织架构
           tu.is_job, -- 是否在职
           tu.is_need_fill_manhour, -- 是否需要填写工时
           tu.hired_date, -- 入职日期
           tu.quit_date, -- 离职日期
           tu.emp_position, -- 成员职位
           td.days, -- 日期
           CASE WHEN td.day_type = 0 THEN '工作日'
                WHEN td.day_type = 1 THEN '周末'
                WHEN td.day_type = 2 THEN '节假日'
                WHEN td.day_type = 3 THEN '调休' END AS day_type -- 日期类型
    FROM 
    (
      SELECT tg.org_name_2 AS team_ft, -- 一级部门
             tg.org_name_3 AS team_group, -- 二级部门
             tg.org_name_4 AS team_sub_group, -- 三级部门
             te.emp_id, -- 人员编码
             te.emp_name AS user_name, -- 人员名称
             te.email AS user_email, -- 人员邮箱
             tmp.org_role_type AS org_role_type, -- 组织角色类型
             tt.role_type AS virtual_role_type, -- 虚拟组角色类型
             tt.module_branch, -- 负责模块分支
             tt.virtual_org_name, -- 虚拟组组织架构
             te.is_job, -- 是否在职
             tmp.is_need_fill_manhour, -- 是否需要填写工时
             te.hired_date, -- 入职日期
             te.quit_date, -- 离职日期
             te.emp_position -- 成员职位
      FROM ${dwd_dbname}.dwd_dtk_emp_info_df te -- 钉钉人员基础信息表
      LEFT JOIN 
      (
        SELECT m.emp_id, -- 人员编码
               m.emp_name, -- 人员名称
               m.org_id, -- 组织编码
               m.org_role_type, -- 组织角色类型
               m.is_need_fill_manhour, -- 是否需要填写工时
               ROW_NUMBER()OVER(PARTITION BY m.emp_id,m.emp_name ORDER BY m.is_need_fill_manhour DESC,m.org_role_type DESC,m.org_id ASC) rn -- 按是否需要填写工时、组织角色类型、组织编码排序
        FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
        WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.is_valid = 1 -- 只取有效的
        GROUP BY m.emp_id,m.emp_name,m.org_id,m.org_role_type,m.is_need_fill_manhour
      )tmp
      ON te.emp_id = tmp.emp_id AND tmp.rn = 1 -- 保证一个对应一个组织
      LEFT JOIN 
      (
        SELECT i.emp_code, -- 人员编码
               i.role_type, -- 角色类型
               i.module_branch, -- 负责模块分支
               i.virtual_org_name -- 虚拟组组织架构
        FROM ${dim_dbname}.dim_virtual_org_emp_info_offline i -- 虚拟组人员离线表
        WHERE i.is_active = 1 AND i.virtual_org_name = '凤凰项目' -- 只筛选凤凰项目组织
      )tt
      ON tt.emp_code = te.emp_id
      LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg -- 组织架构层级表
      ON tg.org_id = tmp.org_id AND tg.org_company_name = '上海快仓智能科技有限公司'  
      WHERE te.d = '${pre1_date}' AND te.org_company_name = '上海快仓智能科技有限公司'
        AND IF(te.is_job = 1,tg.org_name_2 IN ('AMR FT','智能搬运FT','硬件自动化','箱式FT','系统中台') OR (tg.org_name_2 IN ('制造部') AND tg.org_name_3 IN ('工程组','测试')) OR (tg.org_name_2 IN ('研发管理部') AND tg.org_name_3 IN ('产研质量组','效能工具组')),te.dept_name LIKE '%AMR FT%' OR te.dept_name LIKE '%智能搬运FT%' OR te.dept_name LIKE '%硬件自动化%' OR te.dept_name LIKE '%箱式FT%' OR te.dept_name LIKE '%系统中台%' OR te.dept_name LIKE '%制造部%' OR te.dept_name LIKE '%研发管理部%') -- 只筛选AMR FT、智能搬运FT、硬件自动化、箱式FT、系统中台、制造部、研发管理部
      GROUP BY tg.org_name_2,tg.org_name_3,tg.org_name_4,te.emp_id,te.emp_name,te.email,tmp.org_role_type,tt.role_type,tt.module_branch,tt.virtual_org_name,te.is_job,tmp.is_need_fill_manhour,te.hired_date,te.quit_date,te.emp_position
    )tu
    LEFT JOIN 
    (
      SELECT days, -- 日期
             day_type -- 日期类型
      FROM ${dim_dbname}.dim_day_date -- 日期维表
      WHERE days >= '2021-01-01' AND days <= '${pre1_date}'
      GROUP BY days,day_type
     )td
     WHERE td.days >= tu.hired_date AND td.days <= IF(tu.is_job = 0,tu.quit_date,'${pre1_date}') -- 入职时间作为补零开始时间，离职时间作为补零结束时间，未离职的补零至今
  )tud
  -- 工时统计
  LEFT JOIN 
  (
    SELECT TO_DATE(t.task_start_time) AS stat_date, -- 工时登记时间
           t.user_uuid, -- 人员编码
		   tou.user_email, -- 人员邮箱
		   round(COALESCE(SUM(t.task_spend_hours),0),2) AS work_hour -- 登记工时
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t -- ones工作项工时登记信息表
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou -- ones人员信息表
    ON tou.uuid = t.user_uuid
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t1 -- ones工作项信息表
    ON t.task_uuid = t1.uuid
    WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid IS NOT NULL -- 筛选登记工时类型 且 工时状态有效 且 人员不为空
  	  AND t1.status = 1 AND t1.issue_type_cname IN ('缺陷','任务','需求') -- 筛选工作项有效 且 工作项类型为缺陷、任务、需求
      AND (IF((t1.project_type_name = '外部客户项目' AND t1.external_project_code IS NULL),'未知项目编码',t1.external_project_code) != '未知项目编码' -- 筛选外部项目编码不为空的
	       OR IF((t1.project_type_name = '内部研发项目' AND t1.project_bpm_code IS NULL),'未知项目编码',t1.project_bpm_code) != '未知项目编码' -- 筛选内部项目编码不为空的
	       OR t1.project_type_name = '技术&管理工作') -- 筛选技术管理工作类型的
    GROUP BY TO_DATE(t.task_start_time),t.user_uuid,tou.user_email
  )t1
  ON t1.user_email = tud.user_email AND t1.stat_date = tud.days
  -- 工时明细
  LEFT JOIN 
  (
    SELECT TO_DATE(t.task_start_time) AS stat_date, -- 工时登记时间
           t.task_uuid, -- 工作项编码
           t.user_uuid, -- 人员编码
           t.project_classify_name, -- 项目分类名称
		   tou.user_email, -- 人员邮箱
	  	   DATEDIFF(TO_DATE(t.task_create_time),TO_DATE(t.task_start_time)) AS actual_date, -- 登记相差天数
           t.task_spend_hours AS work_hour -- 登记工时
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t -- ones工作项工时登记信息表
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou -- ones人员信息表
    ON tou.uuid = t.user_uuid
    WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid IS NOT NULL -- 筛选登记工时类型 且 工时状态有效 且 人员不为空
  )t2 
  ON t2.user_email = tud.user_email AND t2.stat_date = tud.days
  -- ones基础信息
  LEFT JOIN 
  (
    SELECT TO_DATE(t1.task_create_time) AS stat_date, -- 工作项创建时间
           t1.uuid, -- 工作项uuid
           t1.\`number\` AS work_id, -- 工作项编码
           t1.summary, -- 标题
           t1.task_desc, -- 描述
           t1.project_classify_name, -- 项目分类名称
           t1.sprint_classify_name, -- 迭代分类名称
		   t1.issue_type_cname AS work_type, -- 工作项类型
           t1.task_status_cname AS work_status, -- 工作项状态
           t1.org_name_1, -- 项目所属组织
           IF((t1.project_type_name = '外部客户项目' AND t1.external_project_code IS NULL) OR t1.project_type_name = '技术&管理工作','未知项目编码',nvl(b1.project_code,t1.external_project_code)) AS external_project_code, -- 外部项目编码
           IF((t1.project_type_name = '外部客户项目' AND t1.external_project_code IS NULL) OR t1.project_type_name = '技术&管理工作','未知项目名称',IF(t1.external_project_code = 'A00000','商机项目',b1.project_name)) AS external_project_name, -- 外部项目名称
           IF((t1.project_type_name = '内部研发项目' AND t1.project_bpm_code IS NULL) OR t1.project_type_name = '技术&管理工作','未知项目编码',t1.project_bpm_code) AS project_bpm_code, -- 内部项目编码
           IF((t1.project_type_name = '内部研发项目' AND t1.project_bpm_code IS NULL) OR t1.project_type_name = '技术&管理工作','未知项目名称',b2.project_name) AS project_bpm_name, -- 内部项目名称
           t1.project_type_name -- 项目类型名称
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t1 -- ones工作项信息表
    LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b1 -- pms项目基本信息汇总维表
    ON b1.d = '${pre1_date}' AND IF(t1.external_project_code LIKE 'S-%',SUBSTRING(t1.external_project_code,3) = b1.project_code,(t1.external_project_code = b1.project_code OR t1.external_project_code = b1.project_sale_code)) -- S-开头的项目只匹配后面的正式项目编码
    LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b2 -- pms项目基本信息汇总维表
    ON b2.d = '${pre1_date}' AND (t1.project_bpm_code = b2.project_code OR t1.project_bpm_code = b2.project_sale_code) AND ((b2.data_source = 'BPM' AND b2.project_type_name IN ('内部项目','公司级项目','硬件部项目','软件部项目') AND b2.project_operation_state NOT IN ('9.项目暂停','10.项目暂停','11.项目取消')) OR (b2.data_source = 'PMS' AND b2.is_external_project = 0)) -- 筛选内部项目
    WHERE t1.status = 1 AND t1.issue_type_cname IN ('缺陷','任务','需求') -- 筛选工作项有效 且 工作项类型为缺陷、任务、需求
  )tt 
  ON tt.uuid = t2.task_uuid 
  -- 请假数据
  LEFT JOIN 
  (
    SELECT l1.originator_user_id,
           l1.stat_date,
           CASE WHEN l2.leave_type IS NULL THEN l1.leave_type ELSE '全天请假' END AS leave_type
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
  ) tt1
  ON tt1.originator_user_id = tud.emp_id AND tt1.stat_date = tud.days
  -- 加班数据
  LEFT JOIN  
  (
    SELECT l1.applicant_userid,
           l1.stat_date,
           CASE WHEN l2.work_overtime_type IS NULL THEN l1.work_overtime_type ELSE '全天加班' END AS work_overtime_type
    FROM 
    (
      SELECT l.applicant_userid, -- 人员编码
             CAST(l.overtime_date AS DATE) AS stat_date, -- 加班日期
             CASE WHEN l.period_type = '全天' THEN '全天加班'
                  WHEN l.period_type = '下午' THEN '下半天加班'
                  WHEN l.period_type = '上午' THEN '上半天加班' END AS work_overtime_type, -- 加班类型
             ROW_NUMBER()OVER(PARTITION BY l.applicant_userid,CAST(l.overtime_date AS DATE) ORDER BY CASE WHEN l.period_type = '全天' THEN '全天加班'
                                                                                                          WHEN l.period_type = '下午' THEN '下半天加班'
                                                                                                          WHEN l.period_type = '上午' THEN '上半天加班' END ASC) rn -- 按加班类型排序
      FROM ${dwd_dbname}.dwd_dtk_process_work_overtime_dayily_info_df l -- 钉钉员工加班日天记录表
      WHERE l.d = '${pre1_date}' AND l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' -- 只取有效记录 且 审批通过 且 审批状态完成
    )l1
    LEFT JOIN 
    (
      SELECT l.applicant_userid, -- 人员编码
             CAST(l.overtime_date AS DATE) AS stat_date, -- 加班日期
             CASE WHEN l.period_type = '全天' THEN '全天加班'
                  WHEN l.period_type = '下午' THEN '下半天加班'
                  WHEN l.period_type = '上午' THEN '上半天加班' END AS work_overtime_type, -- 加班类型
             ROW_NUMBER()OVER(PARTITION BY l.applicant_userid,CAST(l.overtime_date AS DATE) ORDER BY CASE WHEN l.period_type = '全天' THEN '全天加班'
                                                                                                          WHEN l.period_type = '下午' THEN '下半天加班'
                                                                                                          WHEN l.period_type = '上午' THEN '上半天加班' END ASC) rn -- 按加班类型排序
      FROM ${dwd_dbname}.dwd_dtk_process_work_overtime_dayily_info_df l -- 钉钉员工加班日天记录表
      WHERE l.d = '${pre1_date}' AND l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' -- 只取有效记录 且 审批通过 且 审批状态完成
    )l2
    ON l1.applicant_userid = l2.applicant_userid AND l1.stat_date = l2.stat_date AND l1.work_overtime_type != l2.work_overtime_type -- 加班类型不同的
    WHERE l1.rn = 1 -- 保证一天只有一种加班类型输出
  ) tt2
  ON tt2.applicant_userid = tud.emp_id AND tt2.stat_date = tud.days
  -- 出差时长
  LEFT JOIN 
  (
    SELECT t1.originator_user_id,
           t1.stat_date,
           SUM(t1.travel_days) AS travel_days
    FROM travel_detail t1
    WHERE t1.is_valid = 1
    GROUP BY t1.originator_user_id,t1.stat_date
  ) tt3
  ON tt3.originator_user_id = tud.emp_id AND tt3.stat_date = tud.days
)

INSERT overwrite table ${ads_dbname}.ads_pms_project_profit_detail
SELECT '' AS id, -- 主键
       t1.project_code, -- 项目编码
       t1.project_sale_code, -- 售前编码
       t1.project_name, -- 项目名称
       t1.project_priority, -- 项目等级
       t1.project_dispaly_state_group, -- 项目阶段组
       t1.project_ft, -- 项目所属ft
	   t1.pm_name, -- 项目经理
       t1.project_area, -- 项目所在区域地点
	   t1.project_area_group, -- 项目区域组（国内|国外）
	   t1.pms_project_operation_state, -- pms项目运营状态
	   t1.pms_project_status, -- pms项目状态
	   t1.core_project_code, -- 项目集项目编码
	   t1.is_main_project, -- 是否为主项目
	   t1.pms_core_project_status, -- pms核心项目状态
	   t1.pms_core_project_operation_state, -- pms核心项目运营状态
	   t1.pms_core_project_type_name, -- pms核心项目类型
       t1.online_process_date, -- 上线审批完成日期
       t1.final_inspection_process_date, --验收审批完成日期
       t1.post_project_date, -- 结项日期
	   t1.online_date, -- 实际上线时间
	   t1.final_inspection_date, -- 实际终验时间
	   t1.is_active, -- 是否活跃
       t1.project_income, -- 项目收入
       nvl(t2.agv_num,0) as agv_num, -- agv数量 => 硬件 => 项目费用
       nvl(t7.agv_cost,0) as agv_cost, -- agv成本 => 硬件 => 项目费用
       nvl(t8.bucket_cost,0) as bucket_cost, -- 货架费用 => 硬件 => 项目费用
       nvl(t9.charging_cost,0) as charging_cost, -- 充电桩费用 => 硬件 => 项目费用
       nvl(t13.project_other_matters_cost,0) as project_other_matters_cost, -- 项目其他物料 => 硬件 => 项目费用
       nvl(t10.export_packing_cost,0) as export_packing_cost, -- 出口包装费 => 项目费用
       nvl(t11.transportation_cost,0) as transportation_cost, -- 运输费 => 项目费用
       nvl(t12.ectocyst_software_cost,0) as ectocyst_software_cost, -- 外包软件费用 => 项目费用
       nvl(t15.ectocyst_hardware_cost,0) as ectocyst_hardware_cost, -- 外包硬件费用 => 项目费用
       nvl(t3.pe_cost,0) as pe_cost, --PE => 人工费用 => 项目费用
       nvl(t18.mt_service_cost,0) as mt_service_cost, -- 维保 => 人工费用 => 项目费用
       nvl(t5.io_service_cost,0) + nvl(t16.io_service_cost_ago,0) + nvl(t23.os_service_cost,0) as io_service_cost, -- 外包实施劳务 + 海外外包劳务 => 人工费用 => 项目费用
       nvl(t4.op_service_cost,0) + nvl(t17.op_service_cost_ago,0) as op_service_cost, -- 外包运维劳务 => 人工费用 => 项目费用
       nvl(t19.te_cost,0) as te_cost, -- 研发 => 人工费用 => 项目费用
       nvl(t20.ctrip_car_cost,0) + nvl(t21.ctrip_flight_cost,0) + nvl(t22.ctrip_hotel_cost,0) as ctrip_amount, -- 携程商旅 => 差旅费 => 项目费用
       nvl(t6.reimburse_amount,0) as reimburse_amount, -- 个人报销 => 差旅费 => 项目费用
       nvl(t7.agv_cost,0) + nvl(t8.bucket_cost,0) + nvl(t9.charging_cost,0) + nvl(t13.project_other_matters_cost,0) + nvl(t10.export_packing_cost,0) + nvl(t11.transportation_cost,0) + nvl(t12.ectocyst_software_cost,0) + nvl(t15.ectocyst_hardware_cost,0) + nvl(t3.pe_cost,0) + nvl(t18.mt_service_cost,0) + nvl(t5.io_service_cost,0) + nvl(t16.io_service_cost_ago,0) + nvl(t23.os_service_cost,0) + nvl(t4.op_service_cost,0) + nvl(t17.op_service_cost_ago,0) + nvl(t19.te_cost,0) + nvl(t6.reimburse_amount,0) + nvl(t20.ctrip_car_cost,0) + nvl(t21.ctrip_flight_cost,0) + nvl(t22.ctrip_hotel_cost,0) as cost_sum, -- 成本费用合计
       nvl(t1.project_income,0) - (nvl(t7.agv_cost,0) + nvl(t8.bucket_cost,0) + nvl(t9.charging_cost,0) + nvl(t13.project_other_matters_cost,0) + nvl(t10.export_packing_cost,0) + nvl(t11.transportation_cost,0) + nvl(t12.ectocyst_software_cost,0) + nvl(t15.ectocyst_hardware_cost,0) + nvl(t3.pe_cost,0) + nvl(t18.mt_service_cost,0) + nvl(t5.io_service_cost,0) + nvl(t16.io_service_cost_ago,0) + nvl(t23.os_service_cost,0) + nvl(t4.op_service_cost,0) + nvl(t17.op_service_cost_ago,0) + nvl(t19.te_cost,0) + nvl(t6.reimburse_amount,0) + nvl(t20.ctrip_car_cost,0) + nvl(t21.ctrip_flight_cost,0) + nvl(t22.ctrip_hotel_cost,0)) as project_gp, -- 项目毛利
       nvl((nvl(t1.project_income,0) - (nvl(t7.agv_cost,0) + nvl(t8.bucket_cost,0) + nvl(t9.charging_cost,0) + nvl(t13.project_other_matters_cost,0) + nvl(t10.export_packing_cost,0) + nvl(t11.transportation_cost,0) + nvl(t12.ectocyst_software_cost,0) + nvl(t15.ectocyst_hardware_cost,0) + nvl(t3.pe_cost,0) + nvl(t18.mt_service_cost,0) + nvl(t5.io_service_cost,0) + nvl(t16.io_service_cost_ago,0) + nvl(t23.os_service_cost,0) + nvl(t4.op_service_cost,0) + nvl(t17.op_service_cost_ago,0) + nvl(t19.te_cost,0) + nvl(t6.reimburse_amount,0) + nvl(t20.ctrip_car_cost,0) + nvl(t21.ctrip_flight_cost,0) + nvl(t22.ctrip_hotel_cost,0))) / nvl(t1.project_income,0),0) as project_gp_rate, -- 项目毛利率
       t24.contract_code, -- 合同编号
       t24.sales_manager, -- 销售经理
       t24.project_type_name, -- 项目类型
       t1.contract_signed_date, -- 合同日期
       t24.contract_amount, -- 合同额
	   t24.currency, -- 合同币种
	   t24.exchange_rate, -- 直接汇率
	   t24.rate_begin_date, -- 生效日期
	   t24.rate_end_date, -- 失效日期
       t25.yf_collection_ratio, -- 预付比例
       t25.yf_collection_amount, -- 预付应收
       t25.yf_already_collection_amount, -- 预付已收
       t25.yf_overdays_amount, -- 预付逾期
       t25.yf_pay_amount_date, -- 预付应汇款日期
       t25.yf_overdays_days, -- 预付逾期天数
       t25.yf_entry_amount, -- 预付已开票金额
       t26.dh_date, -- 到货日期
       t26.dh_collection_ratio, -- 到货比例
       t26.dh_collection_amount, -- 到货应收
       t26.dh_already_collection_amount, -- 到货已收
       t26.dh_overdays_amount, -- 到货逾期
       t26.dh_pay_amount_date, -- 到货应汇款日期
       t26.dh_overdays_days, -- 到货逾期天数
       t26.dh_entry_amount, -- 到货已开票金额
       t27.ys_date, -- 验收日期
       t27.ys_collection_ratio, -- 验收比例
       t27.ys_collection_amount, -- 验收应收
       t27.ys_already_collection_amount, -- 验收已收
       t27.ys_overdays_amount, -- 验收逾期
       t27.ys_pay_amount_date, -- 验收应汇款日期
       t27.ys_overdays_days, -- 验收逾期天数
       t27.ys_entry_amount, -- 验收已开票金额
       t28.zb_date, -- 质保到期日期
       t28.zb_collection_ratio, -- 质保比例
       t28.wk_collection_amount, -- 尾款应收
       t28.wk_no_collection_amount, -- 尾款未收
       t28.wk_already_collection_amount, -- 尾款已收
       t28.wk_overdays_amount, -- 尾款逾期
       t28.wk_pay_amount_date, -- 尾款应汇款日期
       t28.wk_overdays_days, -- 尾款逾期天数
       t28.wk_entry_amount, -- 尾款已开票金额
	   t29.sx_date, -- 上线日期
       t29.sx_collection_ratio, -- 上线比例
       t29.sx_collection_amount, -- 上线应收
       t29.sx_already_collection_amount, -- 上线已收
       t29.sx_overdays_amount, -- 上线逾期
       t29.sx_pay_amount_date, -- 上线应汇款日期
       t29.sx_overdays_days, -- 上线逾期天数
       t29.sx_entry_amount, -- 上线已开票金额
	   date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM
-- 项目基础信息
(
  SELECT b.project_code, -- 项目编码
         b.project_sale_code, -- 前置项目编码
         b.project_name, -- 项目名称
         b.project_priority, -- 项目等级
         b.project_dispaly_state_group, -- 项目运营阶段组
         b.project_ft, -- 项目所属ft
		 b.pm_name, -- 项目经理
         b.project_area, -- 项目区域
         b.online_process_date, -- 上线审批完成时间
         b.final_inspection_process_date, -- 终验审批完成时间
         b.post_project_date, -- 项目结项时间
         b.amount AS project_income, -- 项目合同金额
		 b.project_area_group, -- 项目区域组（国内|国外）
		 b.pms_project_operation_state, -- pms项目运营状态
		 b.pms_project_status, -- pms项目状态
		 b.contract_signed_date, -- 合同日期
		 b.core_project_code, -- 项目集项目编码
	     b.is_main_project, -- 是否为主项目
		 b.pms_core_project_status, -- pms核心项目状态
		 b.pms_core_project_operation_state, -- pms核心项目运营状态
		 b.pms_core_project_type_name, --pms核心项目类型
		 b.online_date, -- 实际上线时间
		 b.final_inspection_date, -- 实际终验时间
		 b.is_active -- 是否活跃
  FROM ${tmp_dbname}.tmp_pms_project_general_view_detail b
)t1
-- AGV数量
LEFT JOIN
(
  SELECT b.project_code, -- 项目编码
         SUM(nvl(tmp.actual_sale_num,0)) AS agv_num -- agv销售数量
  FROM 
  (
    SELECT so.project_code, -- 项目编码
           nvl(so.real_qty,0) - nvl(sr.real_qty,0) AS actual_sale_num -- 实际销售数量 => 出库-退库
    FROM 
    -- 出库
    (
      SELECT so.project_code, -- 项目编码
             SUM(nvl(so.real_qty,0)) AS real_qty -- 出库数量
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m -- 无聊基础信息表
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so -- 销售出库单表体
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox  = 1 -- 物料属性为agv
        AND m.document_status = 'C' -- 数据状态：完成
        AND so.project_code IS NOT NULL
      GROUP BY so.project_code
    )so
    LEFT JOIN
    -- 退货
    (
      SELECT sr.project_code, -- 项目编码
             SUM(nvl(sr.real_qty,0)) AS real_qty -- 退货数量
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m -- 无聊基础信息表
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr -- 销售退货单表体
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox  = 1 -- 物料属性为agv
        AND m.document_status = 'C' -- 数据状态：完成
        AND sr.project_code IS NOT NULL
      GROUP BY sr.project_code
   )sr
   ON so.project_code = sr.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code IS NOT NULL
  GROUP BY b.project_code
)t2
ON t1.project_code = t2.project_code
-- PE费用
LEFT JOIN 
(
  SELECT b.project_code, -- 项目编码
         SUM(tmp.pe_cost) AS pe_cost -- pe费用
  FROM
  (
    SELECT tt.project_code, -- 项目编码
           SUM(CASE WHEN tt.day_type = '工作日' OR tt.day_type = '节假日' OR tt.day_type = '调休' OR tt.day_type = '周末' THEN 1 -- 全天工作
                    WHEN tt.day_type = '上半天请假' OR tt.day_type = '下半天请假' THEN 0.5 -- 半天工作
                    WHEN tt.day_type = '全天请假' THEN 0 END -- 全天请假
              ) AS pe_day, -- pe工作天数
           SUM(CASE WHEN (tt.day_type = '工作日' OR tt.day_type = '节假日' OR tt.day_type = '调休' OR tt.day_type = '周末') AND tt.data_source = 'abroad-offline' THEN 1 * 1671 -- 全天工作 * 离线数据1671
                    WHEN (tt.day_type = '上半天请假' OR tt.day_type = '下半天请假') AND tt.data_source = 'abroad-offline' THEN 0.5 * 1671 -- 半天工作 * 离线数据1671
                    WHEN (tt.day_type = '工作日' OR tt.day_type = '节假日' OR tt.day_type = '调休' OR tt.day_type = '周末') AND tt.org_name_2 = '箱式FT' THEN 1 * 700 -- 全天工作 * 箱式FT700
                    WHEN (tt.day_type = '工作日' OR tt.day_type = '节假日' OR tt.day_type = '调休' OR tt.day_type = '周末') AND tt.org_name_2 = '营销中心' AND (tt.org_name_3 = '华东大区' OR tt.org_name_3 = '华中大区' OR tt.org_name_3 = '华北大区' OR tt.org_name_3 = '华南大区' OR tt.org_name_3 = '西南大区' OR tt.org_name_3 = '福建子公司') THEN 1 * 700 -- 全天工作 * 营销中心700
                    WHEN (tt.day_type = '工作日' OR tt.day_type = '节假日' OR tt.day_type = '调休' OR tt.day_type = '周末') AND tt.org_name_2 = '项目部' THEN 1 * 500 -- 全天工作 * 项目部500
                    WHEN (tt.day_type = '工作日' OR tt.day_type = '节假日' OR tt.day_type = '调休' OR tt.day_type = '周末') AND tt.org_name_2 = '海外事业部' THEN 1 * 1671 -- 全天工作 * 海外事业部1671
                    WHEN (tt.day_type = '上半天请假' OR tt.day_type = '下半天请假') AND tt.org_name_2 = '箱式FT' THEN 0.5 * 700 -- 半天工作 * 箱式FT700
                    WHEN (tt.day_type = '上半天请假' OR tt.day_type = '下半天请假') AND tt.org_name_2 = '营销中心' AND (tt.org_name_3 = '华东大区' OR tt.org_name_3 = '华中大区' OR tt.org_name_3 = '华北大区' OR tt.org_name_3 = '华南大区' OR tt.org_name_3 = '西南大区' OR tt.org_name_3 = '福建子公司') THEN 0.5 * 700 -- 半天工作 * 营销中心700
                    WHEN (tt.day_type = '上半天请假' OR tt.day_type = '下半天请假') AND tt.org_name_2 = '项目部' THEN 0.5 * 500 -- 半天工作 * 项目部500
                    WHEN (tt.day_type = '上半天请假' OR tt.day_type = '下半天请假') AND tt.org_name_2 = '海外事业部' THEN 0.5 * 1671 -- 半天工作 * 海外事业部1671
                    WHEN tt.day_type = '全天请假' THEN 0 END -- 全天请假
              ) AS pe_cost -- pe费用
    FROM 
    (
      SELECT tud.org_name_2, -- 一级部门
             tud.org_name_3, -- 二级部门
             IF(t12.leave_type IS NOT NULL,t12.leave_type,tud.day_type) AS day_type, -- 日期类型 => 优先取请假类型，其次取正常日期类型
             t1.log_date, -- 日志日期
             t1.project_code, -- 项目编码
             t1.data_source -- 数据来源
      FROM 
      (
        SELECT tu.org_name_2, -- 一级部门
               tu.org_name_3, -- 二级部门
               tu.emp_id, -- 人员编码
               tu.emp_name, -- 人员姓名
               tu.emp_position, -- 人员职位
               tu.is_job, -- 是否在职
               tu.hired_date, -- 入职日期
               tu.quit_date, -- 离职日期
               td.days, -- 日期
               CASE WHEN td.day_type = 0 THEN '工作日'
                    WHEN td.day_type = 1 THEN '周末'
                    WHEN td.day_type = 2 THEN '节假日'
                    WHEN td.day_type = 3 THEN '调休' END AS day_type -- 日期类型
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
        SELECT p.log_date, -- 日志日期
               pvd.project_code, -- 项目编码
               p.applicant_user_id, -- 申请人编码
               p.data_source, -- 数据来源
               p.applicant_user_name, -- 申请人名称
               ROW_NUMBER()OVER(PARTITION BY p.log_date,pvd.project_code,p.applicant_user_id ORDER BY p.data_source DESC) rn -- 多来源数据 => 按来源排序，优先取pms的，其次是dtk，最后是离线表
        FROM ${dwd_dbname}.dwd_pms_project_emp_log_info_df p -- pms人员日志数据信息
        LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail pvd -- 项目大表
        ON p.project_code = pvd.project_code OR p.project_code = pvd.project_sale_code
        WHERE p.d = '${pre1_date}' AND p.role_type = 'PE' -- 筛选PE数据
      )t1
      ON t1.applicant_user_id = tud.emp_id AND tud.days = t1.log_date AND t1.rn = 1 -- 保证一人员一项目一天一条数据
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
      )t12 
      ON t12.emp_id = tud.emp_id AND t12.stat_date = tud.days
    )tt
    WHERE tt.project_code is not NULL -- 去掉项目编码为空的数据
    GROUP BY tt.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b -- 项目大表
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL -- 去掉项目编码为空的数据
  GROUP BY b.project_code
)t3
ON t1.project_code = t3.project_code
-- 外包劳务-运维费用 2022年之后的数据
LEFT JOIN
(
  SELECT b.project_code, -- 项目编码
         SUM(tmp.service_cost) AS op_service_cost -- 运维劳务费用
  FROM
  (
    SELECT tt.project_code, -- 项目编码
       	   SUM(tt.service_cost) AS service_cost -- 运维劳务费用
   	FROM 
    (
      SELECT tt1.cur_date, -- 统计日期
             tt1.project_code, -- 项目编码
           	 tt1.originator_user_name AS member_name, -- 劳务人员
             SUM(tt1.check_duration) AS check_duration_hour, -- 考勤时长（小时）
             CASE WHEN SUM(tt1.check_duration) < 4 THEN 0
                  WHEN SUM(tt1.check_duration) >= 4 AND SUM(tt1.check_duration) < 8 THEN 350
                  WHEN SUM(tt1.check_duration) >= 8 AND SUM(tt1.check_duration) <= 10 THEN 550
                  WHEN SUM(tt1.check_duration) > 10 THEN 550 + (SUM(tt1.check_duration) - 10) * 2 * 20 END AS service_cost -- 运维劳务费用
      FROM 
      (
        SELECT DATE(a.checkin_time) AS cur_date, -- 统计时间
               a.business_id, -- 审批编号
               a.project_code, -- 项目编号
               a.originator_dept_name, -- 团队名称
               IF(a.originator_user_name IS NULL,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) AS originator_user_name, -- 成员名称
               '运维劳务' AS service_type, -- 劳务类型
               IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) AS check_duration, -- 考勤时长（小时）,
               a.checkin_time, -- 考勤签到时间
               a.checkout_time, -- 考勤签退时间
               ROW_NUMBER()OVER(PARTITION BY DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name IS NULL,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) ORDER BY a.checkin_time,a.create_time) rn
       FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
       WHERE (a.service_type = '运维（陪产）劳务' OR a.service_type IS NULL) AND a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准,只取2022年之后的数据
      )tt1
      LEFT JOIN 
      (
      	SELECT DATE(a.checkin_time) AS cur_date, -- 统计时间
               a.business_id, -- 审批编号
               a.project_code, -- 项目编号
               a.originator_dept_name, -- 团队名称
               IF(a.originator_user_name IS NULL,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) AS originator_user_name, -- 成员名称
               '运维劳务' AS service_type, -- 劳务类型
               IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) AS check_duration, -- 考勤时长（小时）,
               a.checkin_time, -- 考勤签到时间
               a.checkout_time, -- 考勤签退时间
               ROW_NUMBER()OVER(PARTITION BY DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name IS NULL,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) ORDER BY a.checkin_time,a.create_time) rn
       FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
       WHERE (a.service_type = '运维（陪产）劳务' OR a.service_type IS NULL) AND a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准,只取2022年之后的数据
      )tt2
      ON tt1.cur_date = tt2.cur_date AND tt1.project_code = tt2.project_code AND tt1.originator_user_name = tt2.originator_user_name AND tt1.rn = tt2.rn + 1
      WHERE tt2.rn IS NULL OR tt1.checkin_time NOT BETWEEN tt2.checkin_time AND tt2.checkout_time
      GROUP BY tt1.cur_date,tt1.project_code,tt1.originator_user_name
      /*
      UNION ALL 
    
      SELECT TO_DATE(tt1.log_date) AS log_date,
             tt1.project_code,
             tt1.applicant_user_id,
             SUM(tt1.check_duration) AS check_duration_hour,
             CASE WHEN SUM(tt1.check_duration) < 4 THEN 0
                  WHEN SUM(tt1.check_duration) >= 4 AND SUM(tt1.check_duration) < 8 THEN 350
                  WHEN SUM(tt1.check_duration) >= 8 AND SUM(tt1.check_duration) <= 10 THEN 550
                  WHEN SUM(tt1.check_duration) > 10 THEN 550 + (SUM(tt1.check_duration) - 10) * 2 * 20 END AS service_cost
      FROM 
      (
        SELECT p.log_date,
               p.project_code,
               p.applicant_user_id,
               SUM(nvl(p.working_hours,0)) AS check_duration 
        FROM ${dwd_dbname}.dwd_pms_project_emp_log_info_df p
        WHERE p.d = '${pre1_date}' AND p.data_source = 'pms' AND p.role_type = 'OPS'
        GROUP BY p.log_date,p.project_code,p.applicant_user_id
      )tt1
      GROUP BY tt1.log_date,tt1.project_code,tt1.applicant_user_id
      */
    )tt
    GROUP BY tt.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code IS NOT NULL
  GROUP BY b.project_code
)t4
ON t1.project_code = t4.project_code
-- 外包劳务-实施费用 2022年之后的数据
LEFT JOIN
(
  SELECT b.project_code, -- 项目编码
         SUM(tmp.service_cost) AS io_service_cost -- 实施劳务费用
  FROM
  (
    SELECT tt.project_code, -- 项目编码
           SUM(tt.service_cost) AS service_cost -- 实施劳务费用
    FROM 
    (
      SELECT tt1.cur_date, -- 统计日期
             tt1.project_code, -- 项目编码
           	 tt1.originator_user_name AS member_name, -- 劳务人员
             SUM(tt1.check_duration) AS check_duration_hour, -- 考勤时长（小时）
             CASE WHEN SUM(tt1.check_duration) < 4 THEN 0
                  WHEN SUM(tt1.check_duration) >= 4 AND SUM(tt1.check_duration) < 8 THEN 350
                  WHEN SUM(tt1.check_duration) >= 8 AND SUM(tt1.check_duration) <= 10 THEN 550
                  WHEN SUM(tt1.check_duration) > 10 THEN 550 + (SUM(tt1.check_duration) - 10) * 2 * 20 END AS service_cost -- 实施劳务费用
      FROM 
      (
        SELECT DATE(a.checkin_time) AS cur_date, -- 统计时间
               a.business_id, -- 审批编号
               a.project_code, -- 项目编号
               a.originator_dept_name, -- 团队名称
               IF(a.originator_user_name IS NULL,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) AS originator_user_name, -- 成员名称
               '实施劳务' AS service_type, -- 劳务类型
               IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) AS check_duration, -- 考勤时长（小时）,
               a.checkin_time, -- 考勤签到时间
               a.checkout_time, -- 考勤签退时间
               ROW_NUMBER()OVER(PARTITION BY DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name IS NULL,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) ORDER BY a.checkin_time,a.create_time) rn
        FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
        WHERE a.service_type = '实施劳务' AND a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准，只取2022年之后的数据
      )tt1
      LEFT JOIN 
      (
     	SELECT DATE(a.checkin_time) AS cur_date, -- 统计时间
               a.business_id, -- 审批编号
               a.project_code, -- 项目编号
               a.originator_dept_name, -- 团队名称
               IF(a.originator_user_name IS NULL,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) AS originator_user_name, -- 成员名称
               '实施劳务' AS service_type, -- 劳务类型
               IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) AS check_duration, -- 考勤时长（小时）,
               a.checkin_time, -- 考勤签到时间
               a.checkout_time, -- 考勤签退时间
               ROW_NUMBER()OVER(PARTITION BY DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name IS NULL,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) ORDER BY a.checkin_time,a.create_time) rn
        FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
        WHERE a.service_type = '实施劳务' AND a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准，只取2022年之后的数据
      )tt2
      ON tt1.cur_date = tt2.cur_date AND tt1.project_code = tt2.project_code AND tt1.originator_user_name = tt2.originator_user_name AND tt1.rn = tt2.rn + 1
      WHERE tt2.rn IS NULL OR tt1.checkin_time NOT BETWEEN tt2.checkin_time AND tt2.checkout_time
      GROUP BY tt1.cur_date,tt1.project_code,tt1.originator_user_name
      /*
      UNION ALL 
    
      SELECT TO_DATE(tt1.log_date) AS log_date,
             tt1.project_code,
             tt1.applicant_user_id,
             SUM(tt1.check_duration) AS check_duration_hour,
             CASE WHEN SUM(tt1.check_duration) < 4 THEN 0
                  WHEN SUM(tt1.check_duration) >= 4 AND SUM(tt1.check_duration) < 8 THEN 350
                  WHEN SUM(tt1.check_duration) >= 8 AND SUM(tt1.check_duration) <= 10 THEN 550
                  WHEN SUM(tt1.check_duration) > 10 THEN 550 + (SUM(tt1.check_duration) - 10) * 2 * 20 END AS service_cost
      FROM 
      (
        SELECT p.log_date,
               p.project_code,
               p.applicant_user_id,
               SUM(nvl(p.working_hours,0)) AS check_duration 
        FROM ${dwd_dbname}.dwd_pms_project_emp_log_info_df p
        WHERE p.d = '${pre1_date}' AND p.data_source = 'pms' AND p.role_type = 'IMP'
        GROUP BY p.log_date,p.project_code,p.applicant_user_id
      )tt1
      GROUP BY tt1.log_date,tt1.project_code,tt1.applicant_user_id
      */
    )tt
    GROUP BY tt.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code IS NOT NULL
  GROUP BY b.project_code
)t5
ON t1.project_code = t5.project_code
-- 个人报销
LEFT JOIN 
(
  SELECT b.project_code, -- 项目编码
         SUM(tmp.reimburse_amount) AS reimburse_amount -- 个人报销金额
  FROM
  -- 个人报销
  (
    SELECT p.project_code, -- 项目编码
           SUM(nvl(p.reimburse_amount,0)) AS reimburse_amount -- 报销金额
    FROM
    (
      SELECT p.flow_id,-- 流程编码
             i.project_code, -- 项目编码
             i.total_amount AS reimburse_amount -- 报销金额
      FROM ${dwd_dbname}.dwd_bpm_personal_expense_account_info_ful p -- bpm个人费用报销单
      LEFT JOIN ${dwd_dbname}.dwd_bpm_personal_expense_account_item_info_ful i -- bpm个人账单报销明细
      ON p.flow_id = i.flow_id
      WHERE p.approve_status = 30 AND p.project_code IS NOT NULL AND p.currency_code = 'PRE001' -- 取货币类型为人民币的
    )p
    GROUP BY p.project_code
    
    UNION ALL 
    
    SELECT p.project_code, -- 项目编码
           SUM(nvl(p.reimburse_amount,0)) AS reimburse_amount -- 报销金额
    FROM
    (
      SELECT p.reimburse_project_codes AS project_code, -- 项目编码
             p.functional_currency_amount AS reimburse_amount -- 报销金额
      FROM ${dwd_dbname}.dwd_hly_personal_reimbursement_info_df p -- 汇联易费用报销单
      WHERE p.d = '${pre1_date}' AND p.data_source = 'HLY' AND p.reimburse_status IN ('待付款','已付款') AND p.reimburse_project_codes IS NOT NULL
    )p
    GROUP BY p.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code IS NOT NULL
  GROUP BY b.project_code
)t6
ON t1.project_code = t6.project_code
-- agv费用
LEFT JOIN 
(
  SELECT b.project_code, -- 项目编码
         SUM(tmp.actual_cost) AS agv_cost -- agv费用
  FROM
  (
    SELECT so.project_code, -- 项目编码
           nvl(so.finance_cost_amount_lc,0) - nvl(sr.finance_cost_amount_lc,0) AS actual_cost -- agv费用
    FROM 
    -- 出库
    (
      SELECT so.project_code, -- 项目编码
             SUM(IF(so.finance_cost_amount_lc != 0,so.finance_cost_amount_lc,(nvl(b.price_amount,0) * so.real_qty))) AS finance_cost_amount_lc -- 最终价格
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m -- 物料基础信息表
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so -- 销售出库单表体
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- 物料内码
               SUM(b.end_period_number) AS end_period_number, -- 期末数量
               SUM(b.end_period_amount) AS end_period_amount, -- 期末金额
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) AS DECIMAL(10,2)),0) AS price_amount -- 物料单个成本价
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b -- 物料期末结存视图表
        WHERE b.check_year = YEAR('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1 -- 取上月期末
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox  = 1 -- 物料属性为agv
        AND m.document_status = 'C' -- 数据状态：完成
        AND so.project_code IS NOT NULL
      GROUP BY so.project_code
    )so
    LEFT JOIN
    -- 退货
    (
      SELECT sr.project_code, -- 项目编码
             SUM(IF(sr.finance_cost_amount_lc != 0,sr.finance_cost_amount_lc,(nvl(b.price_amount,0) * sr.real_qty))) AS finance_cost_amount_lc -- 最终价格
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m -- 物料基础信息表
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr -- 销售退货单表体
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- 物料内码
               SUM(b.end_period_number) AS end_period_number, -- 期末数量
               SUM(b.end_period_amount) AS end_period_amount, -- 期末金额
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) AS DECIMAL(10,2)),0) AS price_amount -- 物料单个成本价
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b -- 物料期末结存视图表
        WHERE b.check_year = YEAR('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1 -- 取上月期末
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox  = 1 -- 物料属性为agv
        AND m.document_status = 'C' -- 数据状态：完成
        AND sr.project_code is not NULL
      GROUP BY sr.project_code
    )sr
    ON so.project_code = sr.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code IS NOT NULL
  GROUP BY b.project_code
)t7
ON t1.project_code = t7.project_code
-- 货架费用
LEFT JOIN 
(
  SELECT b.project_code, -- 项目编码
         SUM(tmp.actual_cost) AS bucket_cost -- 货架费用
  FROM
  (
    SELECT so.project_code, -- 项目编码
           nvl(so.finance_cost_amount_lc,0) - nvl(sr.finance_cost_amount_lc,0) AS actual_cost -- 货架费用 => 销售出库金额 - 销售退货金额
    FROM 
    (
      SELECT so.project_code, -- 项目编码
             SUM(IF(so.finance_cost_amount_lc != 0,so.finance_cost_amount_lc,(nvl(b.price_amount,0) * so.real_qty))) AS finance_cost_amount_lc -- 货架费用
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- 物料内码
               SUM(b.end_period_number) AS end_period_number, -- 期末数量
               SUM(b.end_period_amount) AS end_period_amount, -- 期末金额
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) AS DECIMAL(10,2)),0) AS price_amount -- 物料单个成本价
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = YEAR('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}' AND m.material_number LIKE 'RT04%' -- 货架
        AND so.project_code IS NOT NULL
      GROUP BY so.project_code
    )so
    LEFT JOIN
    (
      SELECT sr.project_code, -- 项目编码
             SUM(IF(sr.finance_cost_amount_lc != 0,sr.finance_cost_amount_lc,(nvl(b.price_amount,0) * sr.real_qty))) as finance_cost_amount_lc -- 货架费用
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- 物料内码
               SUM(b.end_period_number) AS end_period_number, -- 期末数量
               SUM(b.end_period_amount) AS end_period_amount, -- 期末金额
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) AS DECIMAL(10,2)),0) AS price_amount -- 物料单个成本价
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = YEAR('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}' AND m.material_number LIKE 'RT04%' -- 货架
        AND sr.project_code IS NOT NULL
      GROUP BY sr.project_code
    )sr
    ON so.project_code = sr.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code IS NOT NULL
  GROUP BY b.project_code
)t8
ON t1.project_code = t8.project_code
-- 充电桩费用
LEFT JOIN
(
  SELECT b.project_code, -- 项目编码
         SUM(tmp.actual_cost) AS charging_cost -- 充电桩费用
  FROM
  (
    SELECT so.project_code, -- 项目编码
           nvl(so.finance_cost_amount_lc,0) - nvl(sr.finance_cost_amount_lc,0) AS actual_cost -- 充电桩费用
    FROM 
    (
      SELECT so.project_code, -- 项目编码
             SUM(IF(so.finance_cost_amount_lc != 0,so.finance_cost_amount_lc,(nvl(b.price_amount,0) * so.real_qty))) AS finance_cost_amount_lc -- 充电桩费用
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- 物料内码
               SUM(b.end_period_number) AS end_period_number, -- 期末数量
               SUM(b.end_period_amount) AS end_period_amount, -- 期末金额
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) AS DECIMAL(10,2)),0) AS price_amount -- 物料单个成本价
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = YEAR('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}' AND m.material_number LIKE 'RT03%' -- 充电桩
        AND so.project_code IS NOT NULL
      GROUP BY so.project_code
    )so
    LEFT JOIN
    (
      SELECT sr.project_code, -- 项目编码
             SUM(IF(sr.finance_cost_amount_lc != 0,sr.finance_cost_amount_lc,(nvl(b.price_amount,0) * sr.real_qty))) AS finance_cost_amount_lc -- 充电桩费用
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- 物料内码
               SUM(b.end_period_number) AS end_period_number, -- 期末数量
               SUM(b.end_period_amount) AS end_period_amount, -- 期末金额
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) AS DECIMAL(10,2)),0) AS price_amount -- 物料单个成本价
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = YEAR('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}' AND m.material_number LIKE 'RT03%' -- 充电桩
        AND sr.project_code IS NOT NULL
      GROUP BY sr.project_code
    )sr
    ON so.project_code = sr.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code IS NOT NULL
  GROUP BY b.project_code
)t9
ON t1.project_code = t9.project_code
-- 出口包装费
LEFT JOIN
(  
  SELECT b.project_code, -- 项目编码
         SUM(tmp.actual_cost) AS export_packing_cost -- 出口包装费
  FROM
  (
    SELECT po.project_code, -- 项目编码
           nvl(po.finance_amount_lc,0) - nvl(pm.finance_cost_amount_lc,0) AS actual_cost -- 出口包装费
    FROM 
    (
      SELECT po.project_code, -- 项目编码
             SUM(nvl(po.finance_amount_lc,0)) AS finance_amount_lc -- 出口包装费
      FROM ${dim_dbname}.dim_kde_bd_material_group_info_ful g
      LEFT JOIN ${dwd_dbname}.dwd_kde_bd_material_info_df m
      ON g.id = m.material_group AND m.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
      ON m.material_id = po.material_id AND po.d = '${pre1_date}'
      WHERE g.materia_number ='P' -- 包装
        AND m.document_status = 'C' -- 数据状态：完成
        AND po.project_code IS NOT NULL
      GROUP BY po.project_code
    )po
    LEFT JOIN
    (
      SELECT pm.project_code, -- 项目编码
             SUM(nvl(pm.finance_cost_amount_lc,0)) AS finance_cost_amount_lc -- 出口包装费
      FROM ${dim_dbname}.dim_kde_bd_material_group_info_ful g
      LEFT JOIN ${dwd_dbname}.dwd_kde_bd_material_info_df m
      ON g.id = m.material_group AND m.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
      ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
      WHERE g.materia_number ='P' -- 包装
        AND m.document_status = 'C' -- 数据状态：完成
        AND pm.project_code IS NOT NULL
      GROUP BY pm.project_code
    )pm
    ON po.project_code = pm.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code IS NOT NULL
  GROUP BY b.project_code
)t10
ON t1.project_code = t10.project_code
-- 运输费
LEFT JOIN 
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as transportation_cost
  FROM
  (
    SELECT po.project_code,
           nvl(po.finance_amount_lc,0) - nvl(pm.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT po.project_code,
             SUM(nvl(po.finance_amount_lc,0)) as finance_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
      ON m.material_id = po.material_id AND po.d = '${pre1_date}'
      WHERE (m.material_number like 'R6S90077%' or m.material_number like 'R6S90078%') -- 国际物流费、国内物流费
        AND m.document_status = 'C' -- 数据状态：完成
        AND m.d = '${pre1_date}'
        AND po.project_code is not NULL
      GROUP BY po.project_code
    )po
    LEFT JOIN
    (
      SELECT pm.project_code,
             SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
      ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
      WHERE (m.material_number like 'R6S90077%' or m.material_number like 'R6S90078%') -- 国际物流费、国内物流费
        AND m.document_status = 'C' -- 数据状态：完成
        AND m.d = '${pre1_date}'
        AND pm.project_code is not NULL
      GROUP BY pm.project_code
    )pm
    ON po.project_code = pm.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t11
ON t1.project_code = t11.project_code
-- 外包软件
LEFT JOIN 
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as ectocyst_software_cost
  FROM
  (
    SELECT po.project_code,
           nvl(po.finance_amount_lc,0) - nvl(pm.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT po.project_code,
             SUM(nvl(po.finance_amount_lc,0)) as finance_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
      ON m.material_id = po.material_id AND po.d = '${pre1_date}'
      WHERE m.material_number in ('S99000046K010','S99L04660K010') -- 外包软件
        AND m.document_status = 'C' -- 数据状态：完成
        AND m.d = '${pre1_date}'
        AND po.project_code is not NULL
      GROUP BY po.project_code
    )po
    LEFT JOIN
    (
      SELECT pm.project_code,
             SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
      ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
      WHERE m.material_number in ('S99000046K010','S99L04660K010') -- 外包软件
        AND m.document_status = 'C' -- 数据状态：完成
        AND m.d = '${pre1_date}'
        AND pm.project_code is not NULL
      GROUP BY pm.project_code
    )pm
    ON po.project_code = pm.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t12
ON t1.project_code = t12.project_code
-- 其他物料费用
LEFT JOIN
(
  SELECT b.project_code,
         SUM(tmp.actual_cost) as project_other_matters_cost
  FROM
  (
    SELECT so.project_code,
           nvl(so.finance_cost_amount_lc,0) - nvl(sr.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT so.project_code,
             SUM(nvl(so.finance_cost_amount_lc,0)) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox != 1 -- 物料属性不为agv 
        AND m.material_number not like 'RT04%' -- 排除货架
        AND m.material_number not like 'RT03%' -- 排除充电桩
        AND (m.material_group not in ('111370','111373') OR m.material_group is null) -- 排除物料分组为P（包装）、S（软件）
        AND m.material_number not like 'R5S%'
        AND m.material_number not like 'R6S%'
        AND m.material_number not in ('S99000046K010','S99L04660K010','S99L00587K010','S99L00588K010','S99L04951K010') -- 特殊物料
        AND m.document_status = 'C' -- 数据状态已完成
        AND so.project_code is not NULL 
      GROUP BY so.project_code
    )so
    LEFT JOIN
    (
      SELECT sr.project_code,
             SUM(nvl(sr.finance_cost_amount_lc,0)) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox != 1 -- 物料属性不为agv 
        AND m.material_number not like 'RT04%' -- 排除货架
        AND m.material_number not like 'RT03%' -- 排除充电桩
        AND (m.material_group not in ('111370','111373') OR m.material_group is null) -- 排除物料分组为P（包装）、S（软件）
        AND m.material_number not like 'R5S%'
        AND m.material_number not like 'R6S%'
        AND m.material_number not in ('S99000046K010','S99L04660K010','S99L00587K010','S99L00588K010','S99L04951K010') -- 特殊物料
        AND m.document_status = 'C' -- 数据状态已完成
        AND sr.project_code is not NULL 
      GROUP BY sr.project_code
    )sr
    ON so.project_code = sr.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t13
ON t1.project_code = t13.project_code
-- 外包硬件
LEFT JOIN 
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as ectocyst_hardware_cost
  FROM
  (
    SELECT po.project_code,
           nvl(po.finance_amount_lc,0) - nvl(pm.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT po.project_code,
             SUM(nvl(po.finance_amount_lc,0)) as finance_amount_lc
      FROM ${dim_dbname}.dim_kde_bd_material_group_info_ful g
      LEFT JOIN ${dwd_dbname}.dwd_kde_bd_material_info_df m
      ON g.id = m.material_group AND m.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
      ON m.material_id = po.material_id AND po.d = '${pre1_date}'
      WHERE ((g.materia_number ='S' AND m.material_number not in ('S99L00587K010','S99L00588K010','S99L04951K010','S99000046K010','S99L04660K010')) OR m.material_number IN ('R5S90518','R5S90527','R5S90041'))
        AND m.document_status = 'C' -- 数据状态：完成
        AND po.project_code is not NULL
      GROUP BY po.project_code
    )po
    LEFT JOIN
    (
      SELECT pm.project_code,
             SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
      FROM ${dim_dbname}.dim_kde_bd_material_group_info_ful g
      LEFT JOIN ${dwd_dbname}.dwd_kde_bd_material_info_df m
      ON g.id = m.material_group AND m.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
      ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
      WHERE ((g.materia_number ='S' AND m.material_number not in ('S99L00587K010','S99L00588K010','S99L04951K010','S99000046K010','S99L04660K010')) OR m.material_number IN ('R5S90518','R5S90527','R5S90041'))
        AND m.document_status = 'C' -- 数据状态：完成
        AND pm.project_code is not NULL
      GROUP BY pm.project_code
    )pm
    ON po.project_code = pm.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t15
ON t1.project_code = t15.project_code
-- 外包劳务-实施费用 2022年之前的数据
LEFT JOIN
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as io_service_cost_ago
  FROM
  (
    SELECT po.project_code,
           nvl(po.finance_amount_lc,0) - nvl(pm.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT po.project_code,
             SUM(nvl(po.finance_amount_lc,0)) as finance_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
      ON m.material_id = po.material_id AND po.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_info_df p
      ON po.id = p.id AND p.d = '${pre1_date}'
      WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- 劳务类型数据
        AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- 不属于劳务类型数据
        AND m.material_number not in ('R5S90044','R5S90046','R5S90534') -- 运维劳务
		AND m.material_number not in ('R5S90041') -- 外包硬件
        AND m.document_status = 'C' -- 数据状态：完成
        AND p.bill_date <= '2021-12-31'
        AND m.d = '${pre1_date}'
        AND po.project_code is not NULL 
      GROUP BY po.project_code
    )po
    LEFT JOIN
    (
      SELECT pm.project_code,
             SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
      ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_info_df p
      ON pm.id = p.id AND p.d = '${pre1_date}'
      WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- 劳务类型数据
        AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- 不属于劳务类型数据
        AND m.material_number not in ('R5S90044','R5S90046','R5S90534') -- 运维劳务
		AND m.material_number not in ('R5S90041') -- 外包硬件
        AND p.bill_date <= '2021-12-31'
        AND m.d = '${pre1_date}'
        AND pm.project_code is not NULL 
      GROUP BY pm.project_code
    )pm
    ON po.project_code = pm.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t16
ON t1.project_code = t16.project_code
-- 外包劳务-运维费用 2022年之前的数据
LEFT JOIN
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as op_service_cost_ago
  FROM
  (
    SELECT po.project_code,
           nvl(po.finance_amount_lc,0) - nvl(pm.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT po.project_code,
             SUM(nvl(po.finance_amount_lc,0)) as finance_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
      ON m.material_id = po.material_id AND po.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_info_df p
      ON po.id = p.id AND p.d = '${pre1_date}'
      WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- 劳务类型数据
        AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- 不属于劳务类型数据
        AND m.material_number in ('R5S90044','R5S90046','R5S90534') -- 运维劳务
		AND m.material_number not in ('R5S90041') -- 外包硬件
        AND m.document_status = 'C' -- 数据状态：完成
        AND p.bill_date <= '2021-12-31'
        AND m.d = '${pre1_date}'
        AND po.project_code is not NULL
      GROUP BY po.project_code
    )po
    LEFT JOIN
    (
      SELECT pm.project_code,
             SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
      ON m.material_id = pm.material_id AND pm.d = '${pre1_date}'
      LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_info_df p
      ON pm.id = p.id AND p.d = '${pre1_date}' 
      WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- 劳务类型数据
        AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- 不属于劳务类型数据
        AND m.material_number in ('R5S90044','R5S90046','R5S90534') -- 运维劳务
		AND m.material_number not in ('R5S90041') -- 外包硬件
        AND m.document_status = 'C' -- 数据状态：完成
        AND p.bill_date <= '2021-12-31'
        AND m.d = '${pre1_date}'
        AND pm.project_code is not NULL
      GROUP BY pm.project_code
    )pm
    ON po.project_code = pm.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t17
ON t1.project_code = t17.project_code
-- 维保费用
LEFT JOIN
( 
  SELECT b.project_code,
         SUM(tmp.actual_cost) as mt_service_cost
  FROM
  (
    SELECT m.project_code,
           SUM(nvl(m.actual_cost,0)) as actual_cost
    FROM
    (
      SELECT m.log_date, -- 日志日期
             m.originator_user_name, -- 发起人
             m.project_code, -- 项目编码
             SUM(IF(m.working_hours is null,0,m.working_hours)) as working_hours, -- 工作时长
             SUM(tmp.working_hours_total) as working_hours_total, -- 人天总时长
             case when m.log_date < '2022-06-01' then SUM(IF(m.working_hours is null,0,m.working_hours)) * 0 -- 2022.6.1之前的数据费率为0
                  when m.log_date >= '2022-06-01' and SUM(tmp.working_hours_total) <= 8 then SUM(IF(m.working_hours is null,0,m.working_hours)) * 60 -- 2022.6.1之后的数据 总时长小于等于8小时 小时费率60*工作时长
                  when m.log_date >= '2022-06-01' and SUM(tmp.working_hours_total) > 8 then 480 / SUM(tmp.working_hours_total) * SUM(IF(m.working_hours is null,0,m.working_hours)) -- 2022.6.1之后的数据 总时长大于8小时 费率/总时长*工作时长
             end as actual_cost -- 维保费用
      FROM ${dwd_dbname}.dwd_dtk_process_maintenance_log_info_df m
      LEFT JOIN 
      (
        SELECT m.originator_user_name,
               m.log_date,
               SUM(IF(m.working_hours is null,0,m.working_hours)) as working_hours_total
        FROM ${dwd_dbname}.dwd_dtk_process_maintenance_log_info_df m
        WHERE m.org_name = '宝仓' AND m.project_code is not NULL AND m.d = '${pre1_date}' AND m.approval_result = 'agree' AND m.approval_status = 'COMPLETED'
        GROUP BY m.log_date,m.originator_user_name
      )tmp
      ON tmp.originator_user_name = m.originator_user_name AND tmp.log_date = m.log_date
      WHERE m.org_name = '宝仓' AND m.project_code is not NULL AND m.d = '${pre1_date}' AND m.approval_result = 'agree' AND m.approval_status = 'COMPLETED'
      GROUP BY m.log_date,m.originator_user_name,m.project_code
    )m
    GROUP BY m.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t18
ON t1.project_code = t18.project_code
-- 研发费用
LEFT JOIN 
(
  SELECT b.project_code,
         SUM(tmp.actual_cost) as te_cost
  FROM
  (
    SELECT md.external_project_code as project_code,
           SUM(md.cost_amount) as actual_cost
    FROM manhour_detail md
    WHERE md.cost_amount != 0 AND md.project_type_name IN ('外部客户项目')
    GROUP BY md.external_project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t19
ON t1.project_code = t19.project_code
-- 携程用车费用
LEFT JOIN
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as ctrip_car_cost
  FROM
  (
    SELECT c.project_code,
           SUM(IF(c.real_amount_haspost is null,0,c.real_amount_haspost)) as actual_cost
    FROM ${dwd_dbname}.dwd_ctrip_car_account_check_info_di c
    LEFT JOIN ${dim_dbname}.dim_day_date td 
    ON c.d = td.days
    WHERE c.project_code is not NULL AND (td.is_month_end = 1 OR td.days = '${pre1_date}')
    GROUP BY c.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t20
ON t1.project_code = t20.project_code
-- 携程机票费用
LEFT JOIN
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as ctrip_flight_cost
  FROM
  (
    SELECT f.project_code,
           SUM(IF(f.real_amount is null,0,f.real_amount)) as actual_cost
    FROM ${dwd_dbname}.dwd_ctrip_flight_account_check_info_di f
    LEFT JOIN ${dim_dbname}.dim_day_date td 
    ON f.d = td.days
    WHERE f.project_code is not NULL AND (td.is_month_end = 1 OR td.days = '${pre1_date}')
    GROUP BY f.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t21
ON t1.project_code = t21.project_code
-- 携程酒店费用
LEFT JOIN
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as ctrip_hotel_cost
  FROM
  (
    SELECT h.project_code,
           SUM(IF(h.amount is null,0,h.amount)) as actual_cost
    FROM ${dwd_dbname}.dwd_ctrip_hotel_account_check_info_di h
    LEFT JOIN ${dim_dbname}.dim_day_date td 
    ON h.d = td.days
    WHERE h.project_code is not NULL AND (td.is_month_end = 1 OR td.days = '${pre1_date}')
    GROUP BY h.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t22
ON t1.project_code = t22.project_code
-- 海外外包劳务
LEFT JOIN
(
  SELECT b.project_code,
         SUM(tmp.service_cost) as os_service_cost
  FROM
  (
    SELECT tt.project_code,
           SUM(tt.rmb_cost) as service_cost
    FROM ${dwd_dbname}.dwd_pms_overseas_labour_service_info_df tt
    WHERE tt.d = '${pre1_date}'
    GROUP BY tt.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t23
ON t1.project_code = t23.project_code
-- AR回款
LEFT JOIN 
(
  SELECT ar.project_code, -- 项目编码
         ar.contract_code, -- 合同编号
         ar.sales_manager, -- 销售经理
         ar.project_type_name, -- 项目类型
         ar.contract_amount, -- 合同额
		 ar.currency, -- 合同币种
		 ci.currency_code, -- 币种编码
		 ci.currency_id, -- 币种内码
		 IF(ar.currency = 'CNY',1,ri.exchange_rate) AS exchange_rate, -- 直接汇率
		 ri.begin_date AS rate_begin_date, -- 生效日期
		 ri.end_date AS rate_end_date -- 失效日期
  FROM ${dwd_dbname}.dwd_pms_project_ar_info_df ar
  LEFT JOIN ${dim_dbname}.dim_kde_bd_currency_info_ful ci 
  ON ar.currency = ci.currency_type_code AND ci.is_forbid = 1
  LEFT JOIN 
  (
    SELECT *,ROW_NUMBER() OVER (PARTITION BY rate_type_id,cy_for_id ORDER BY begin_date DESC) AS rn
    FROM ${dim_dbname}.dim_kde_bd_rate_info_ful
  )ri
  ON ri.cy_for_id = ci.currency_id AND ri.rn = 1
  WHERE ar.d = '${pre1_date}'
  GROUP BY ar.project_code,ar.contract_code,ar.sales_manager,ar.project_type_name,ar.contract_amount,ar.currency,ci.currency_code,ci.currency_id,IF(ar.currency = 'CNY',1,ri.exchange_rate),ri.begin_date,ri.end_date
)t24
ON t1.project_code = t24.project_code
-- AR回款:预付款
LEFT JOIN 
(
  SELECT ar.project_code, -- 项目编码
         CAST(ar.collection_ratio / 100 AS DECIMAL(10,2)) AS yf_collection_ratio, -- 预付比例
         ar.collection_amount AS yf_collection_amount, -- 预付应收
         ar.uncollected_amount AS yf_no_collection_amount, -- 预付未收
         ar.collection_amount - ar.uncollected_amount AS yf_already_collection_amount, -- 预付已收
         CASE WHEN ar.uncollected_amount != 0 AND TO_DATE(ar.ar_date) < '${pre1_date}' THEN ar.collection_amount - (ar.collection_amount - ar.uncollected_amount) END AS yf_overdays_amount, -- 预付逾期
         TO_DATE(ar.ar_date) AS yf_pay_amount_date, -- 预付应汇款日期
         CASE WHEN ar.uncollected_amount != 0 THEN IF(DATEDIFF(TO_DATE('${pre1_date}'),TO_DATE(ar.ar_date)) < 0,0,DATEDIFF(TO_DATE('${pre1_date}'),TO_DATE(ar.ar_date))) END AS yf_overdays_days, -- 预付逾期天数
         ar.erp_entry_amount AS yf_entry_amount -- 预付已开票金额
  FROM ${dwd_dbname}.dwd_pms_project_ar_info_df ar
  WHERE ar.d = '${pre1_date}' AND ar.collection_stage = '项目启动'
)t25
ON t1.project_code = t25.project_code
-- AR回款:到货
LEFT JOIN 
(
  SELECT ar.project_code, -- 项目编码
         ar.equitment_arrival_date AS dh_date, -- 到货日期
         CAST(ar.collection_ratio / 100 AS DECIMAL(10,2)) AS dh_collection_ratio, -- 到货比例
         ar.collection_amount AS dh_collection_amount, -- 到货应收
         ar.uncollected_amount AS dh_no_collection_amount, -- 到货未收
         ar.collection_amount - ar.uncollected_amount AS dh_already_collection_amount, -- 到货已收
         CASE WHEN ar.uncollected_amount != 0 AND TO_DATE(ar.ar_date) < '${pre1_date}' THEN ar.collection_amount - (ar.collection_amount - ar.uncollected_amount) END AS dh_overdays_amount, -- 到货逾期
         TO_DATE(ar.ar_date) AS dh_pay_amount_date, -- 到货应汇款日期
         CASE WHEN ar.uncollected_amount != 0 THEN IF(DATEDIFF(TO_DATE('${pre1_date}'),TO_DATE(ar.ar_date)) < 0,0,DATEDIFF(TO_DATE('${pre1_date}'),TO_DATE(ar.ar_date))) END AS dh_overdays_days, -- 到货逾期天数
         ar.erp_entry_amount AS dh_entry_amount -- 到货已开票金额
  FROM ${dwd_dbname}.dwd_pms_project_ar_info_df ar
  WHERE ar.d = '${pre1_date}' AND ar.collection_stage = '到货签收'
)t26
ON t1.project_code = t26.project_code
-- AR回款:验收
LEFT JOIN 
(
  SELECT ar.project_code, -- 项目编码
         ar.final_inspection_date AS ys_date, -- 验收日期
         CAST(ar.collection_ratio / 100 AS DECIMAL(10,2)) AS ys_collection_ratio, -- 验收比例
         ar.collection_amount AS ys_collection_amount, -- 验收应收
         ar.uncollected_amount AS ys_no_collection_amount, -- 验收未收
         ar.collection_amount - ar.uncollected_amount AS ys_already_collection_amount, -- 验收已收
         CASE WHEN ar.uncollected_amount != 0 AND TO_DATE(ar.ar_date) < '${pre1_date}' THEN ar.collection_amount - (ar.collection_amount - ar.uncollected_amount) END AS ys_overdays_amount, -- 验收逾期
         TO_DATE(ar.ar_date) AS ys_pay_amount_date, -- 验收应汇款日期
         CASE WHEN ar.uncollected_amount != 0 THEN IF(DATEDIFF(TO_DATE('${pre1_date}'),TO_DATE(ar.ar_date)) < 0,0,DATEDIFF(TO_DATE('${pre1_date}'),TO_DATE(ar.ar_date))) END AS ys_overdays_days, -- 验收逾期天数
         ar.erp_entry_amount AS ys_entry_amount -- 验收已开票金额
  FROM ${dwd_dbname}.dwd_pms_project_ar_info_df ar
  WHERE ar.d = '${pre1_date}' AND ar.collection_stage = '终验'
)t27
ON t1.project_code = t27.project_code
-- AR回款:质保
LEFT JOIN 
(
  SELECT ar.project_code, -- 项目编码
         ar.expiration_date AS zb_date, -- 质保到期日期
         CAST(ar.collection_ratio / 100 AS DECIMAL(10,2)) AS zb_collection_ratio, -- 质保比例
         ar.collection_amount AS wk_collection_amount, -- 尾款应收
         ar.uncollected_amount AS wk_no_collection_amount, -- 尾款未收
         ar.collection_amount - ar.uncollected_amount AS wk_already_collection_amount, -- 尾款已收
         CASE WHEN ar.uncollected_amount != 0 AND TO_DATE(ar.ar_date) < '${pre1_date}' THEN ar.collection_amount - (ar.collection_amount - ar.uncollected_amount) END AS wk_overdays_amount, -- 尾款逾期
         TO_DATE(ar.ar_date) AS wk_pay_amount_date, -- 尾款应汇款日期
         CASE WHEN ar.uncollected_amount != 0 THEN IF(DATEDIFF(TO_DATE('${pre1_date}'),TO_DATE(ar.ar_date)) < 0,0,DATEDIFF(TO_DATE('${pre1_date}'),TO_DATE(ar.ar_date))) END AS wk_overdays_days, -- 尾款逾期天数
         ar.erp_entry_amount AS wk_entry_amount -- 尾款已开票金额
  FROM ${dwd_dbname}.dwd_pms_project_ar_info_df ar
  WHERE ar.d = '${pre1_date}' AND ar.collection_stage = '项目结项'
)t28
ON t1.project_code = t28.project_code
-- AR回款:上线
LEFT JOIN 
(
  SELECT ar.project_code, -- 项目编码
         ar.online_date AS sx_date, -- 上线日期
         CAST(ar.collection_ratio / 100 AS DECIMAL(10,2)) AS sx_collection_ratio, -- 上线比例
         ar.collection_amount AS sx_collection_amount, -- 上线应收
         ar.uncollected_amount AS sx_no_collection_amount, -- 上线未收
         ar.collection_amount - ar.uncollected_amount AS sx_already_collection_amount, -- 上线已收
         CASE WHEN ar.uncollected_amount != 0 AND TO_DATE(ar.ar_date) < '${pre1_date}' THEN ar.collection_amount - (ar.collection_amount - ar.uncollected_amount) END AS sx_overdays_amount, -- 上线逾期
         TO_DATE(ar.ar_date) AS sx_pay_amount_date, -- 上线应汇款日期
         CASE WHEN ar.uncollected_amount != 0 THEN IF(DATEDIFF(TO_DATE('${pre1_date}'),TO_DATE(ar.ar_date)) < 0,0,DATEDIFF(TO_DATE('${pre1_date}'),TO_DATE(ar.ar_date))) END AS sx_overdays_days, -- 上线逾期天数
         ar.erp_entry_amount AS sx_entry_amount -- 上线已开票金额
  FROM ${dwd_dbname}.dwd_pms_project_ar_info_df ar
  WHERE ar.d = '${pre1_date}' AND ar.collection_stage = '上线'
)t29
ON t1.project_code = t29.project_code;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"