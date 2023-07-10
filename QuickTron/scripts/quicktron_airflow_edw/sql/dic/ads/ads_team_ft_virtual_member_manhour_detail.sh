#!/bin/bash

# ------------------------------------------------------------------------------------------------
#-- 修改历史 ： 修改人 修改时间 主要改动说明
#-- 1 wangyingying 2022-10-27 融合pms项目数据
#-- 2 wangyingying 2022-11-22 优化代码
#-- 3 wangyingying 2022-12-16 增加出差数据源
#-- 4 wangyingying 2023-01-05 调整人员范围
#-- 5 wangyingying 2023-01-11 调整出差逻辑
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
-- 团队小组成员工单登记工时明细 ads_team_ft_virtual_member_manhour_detail 

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
)

INSERT overwrite table ${ads_dbname}.ads_team_ft_virtual_member_manhour_detail
SELECT '' AS id, -- 主键
       tud.team_ft, -- 一级部门
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
       ELSE 0 END AS cost_amount, -- 研发费用 => 按部门按天按登记工时占比*费率计算
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS create_time,
       DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
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
    LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg  -- 组织架构层级表
    ON tg.org_id = tmp.org_id AND tg.org_company_name = '上海快仓智能科技有限公司'  
    WHERE te.d = '${pre1_date}' AND te.org_company_name = '上海快仓智能科技有限公司'
      AND IF(te.is_job = 1,tg.org_name_2 IN ('AMR FT','智能搬运FT','硬件自动化','箱式FT','系统中台') OR (tg.org_name_2 IN ('制造部') AND tg.org_name_3 IN ('工程组','测试')) OR (tg.org_name_2 IN ('研发管理部') AND tg.org_name_3 IN ('产研质量组','效能工具组')),te.dept_name LIKE '%AMR FT%' OR te.dept_name LIKE '%智能搬运FT%' OR te.dept_name LIKE '%硬件自动化%' OR te.dept_name LIKE '%箱式FT%' OR te.dept_name LIKE '%系统中台%' OR te.dept_name LIKE '%制造部%' OR te.dept_name LIKE '%研发管理部%') -- 只筛选AMR FT、智能搬运FT、硬件自动化、箱式FT、系统中台、制造部、研发管理部
    GROUP BY tg.org_name_2,tg.org_name_3,tg.org_name_4,te.emp_id,te.emp_name,te.email,tmp.org_role_type,tt.role_type,tt.module_branch,tt.virtual_org_name,te.is_job,tmp.is_need_fill_manhour,te.hired_date,te.quit_date,te.emp_position
  ) tu
  LEFT JOIN 
  (
    SELECT days, -- 日期
             day_type -- 日期类型
    FROM ${dim_dbname}.dim_day_date -- 日期维表
    WHERE days >= '2021-01-01' AND days <= '${pre1_date}'
    GROUP BY days,day_type
   ) td
   WHERE td.days >= tu.hired_date AND td.days <= IF(tu.is_job = 0,tu.quit_date,'${pre1_date}') -- 入职时间作为补零开始时间，离职时间作为补零结束时间，未离职的补零至今
) tud
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
) t1
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
) t2 
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
) tt 
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
ON tt3.originator_user_id = tud.emp_id AND tt3.stat_date = tud.days;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"