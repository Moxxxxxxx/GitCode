#!/bin/bash
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
--ads_pms_project_profit_detail    --pms项目利润表

-- 工时明细表
with manhour_detail as
(
  SELECT tud.team_ft,
         tud.team_group,
	     tud.team_sub_group,
         tud.user_name as team_member,
         tud.emp_position,
         tud.is_job,
         tud.hired_date,
         tud.quit_date,
         tud.is_need_fill_manhour,
         tud.org_role_type,
         tud.virtual_role_type,
         tud.module_branch,
         tud.virtual_org_name,
         tt.org_name_1 as project_org_name,
         tt.project_classify_name as project_classify_name,
         tt.sprint_classify_name as sprint_classify_name,
         tt.external_project_code,
         tt.external_project_name,
         tt.project_bpm_code,
         tt.project_bpm_name,
         tt.project_type_name,
         cast(tt.stat_date as date) as work_create_date,
         tt.work_id,
         tt.summary as work_summary,
         tt.task_desc as work_desc,
         tt.work_type as work_type,
         tt.work_status,
         cast(tud.days as date) as work_check_date,
         IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) as day_type,
         cast(nvl(tt3.travel_days, 0) as decimal(10, 2)) as travel_days,
         cast(nvl(t2.work_hour, 0) as decimal(10, 2)) as work_hour,
         IF(tt.work_id is null,0,t2.actual_date) as actual_date,
         CASE WHEN tt.project_type_name is null and tt.work_id is not null and t2.actual_date > 7 THEN '无效工时&违规登记'
              WHEN (tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码')) and t2.actual_date > 7 THEN '编码异常&违规登记'
              WHEN tt.project_type_name is null and tt.work_id is not null THEN '无效工时'
              WHEN t2.actual_date > 7 THEN '违规登记'
              WHEN tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码') THEN '编码异常'
         ELSE '无异常' END as error_type,
         IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2)))            as work_hour_total,
         nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0) as work_hour_rate,
         CASE WHEN tud.team_ft = '制造部' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 700 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '制造部' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天加班','上半天加班') THEN 700 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '制造部' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天请假','上半天请假','下半天请假-哺乳假','上半天请假-哺乳假') AND (tud.day_type = '工作日' or tud.day_type = '调休') THEN 700 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '硬件自动化' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1000 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '硬件自动化' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天加班','上半天加班') THEN 1000 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '硬件自动化' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天请假','上半天请假','下半天请假-哺乳假','上半天请假-哺乳假') AND (tud.day_type = '工作日' or tud.day_type = '调休') THEN 1000 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = 'AMR FT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1300 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = 'AMR FT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天加班','上半天加班') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = 'AMR FT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天请假','上半天请假','下半天请假-哺乳假','上半天请假-哺乳假') AND (tud.day_type = '工作日' or tud.day_type = '调休') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '系统中台' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1300 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '系统中台' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天加班','上半天加班') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '系统中台' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天请假','上半天请假','下半天请假-哺乳假','上半天请假-哺乳假') AND (tud.day_type = '工作日' or tud.day_type = '调休') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '箱式FT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1300 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '箱式FT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天加班','上半天加班') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '箱式FT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天请假','上半天请假','下半天请假-哺乳假','上半天请假-哺乳假') AND (tud.day_type = '工作日' or tud.day_type = '调休') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '智能搬运FT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1300 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '智能搬运FT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天加班','上半天加班') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '智能搬运FT' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天请假','上半天请假','下半天请假-哺乳假','上半天请假-哺乳假') AND (tud.day_type = '工作日' or tud.day_type = '调休') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '已离职' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1300 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '已离职' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天加班','上半天加班') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
              WHEN tud.team_ft = '已离职' and IF(tt2.work_overtime_type is null,IF(tt1.leave_type is null,tud.day_type,IF(tt1.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-',tt1.leave_type),IF(tt1.leave_type = '哺乳假' AND tud.day_type in('周末','节假日'),tud.day_type,tt1.leave_type))),tt2.work_overtime_type) IN ('下半天请假','上半天请假','下半天请假-哺乳假','上半天请假-哺乳假') AND (tud.day_type = '工作日' or tud.day_type = '调休') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
         ELSE 0 END as cost_amount
  FROM 
  (
    SELECT IF(tu.is_job = 0,'已离职',tu.team_ft) as team_ft,
           tu.team_group,
           tu.team_sub_group,
           tu.emp_id,
           tu.user_name,
           tu.user_email,
           tu.org_role_type,
           tu.virtual_role_type,
           tu.module_branch,
           tu.virtual_org_name,
           tu.is_job,
           tu.is_need_fill_manhour,
           tu.hired_date,
           tu.quit_date,
           tu.emp_position,
           td.days,
           CASE when td.day_type = 0 then '工作日'
                when td.day_type = 1 then '周末'
                when td.day_type = 2 then '节假日'
                when td.day_type = 3 then '调休' end as day_type
    FROM 
    (
      SELECT DISTINCT tg.org_name_2 as team_ft,
                      tg.org_name_3 as team_group,
                      tg.org_name_4 as team_sub_group,
                      te.emp_id,
                      te.emp_name   as user_name,
                      te.email      as user_email,
                      tmp.org_role_type as org_role_type,
                      tt.role_type as virtual_role_type,
                      tt.module_branch,
                      tt.virtual_org_name,
                      te.is_job,
                      tmp.is_need_fill_manhour,
                      te.hired_date,
                      te.quit_date,
                      te.emp_position
      FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
      LEFT JOIN 
      (
        SELECT DISTINCT m.emp_id,
                        m.emp_name,
                        m.org_id,
                        m.org_role_type,
                        m.is_need_fill_manhour,
                        row_number()over(PARTITION by m.emp_id,m.emp_name order by m.is_need_fill_manhour desc,m.org_role_type desc,m.org_id asc)rn
        FROM ${dim_dbname}.dim_dtk_emp_org_mapping_info m
        WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.is_valid = 1
      )tmp
      ON te.emp_id = tmp.emp_id AND tmp.rn = 1
      LEFT JOIN 
      (
        SELECT i.emp_code,
               i.role_type,
               i.module_branch,
               i.virtual_org_name
        FROM ${dim_dbname}.dim_virtual_org_emp_info_offline i
        WHERE i.is_active = 1 AND i.virtual_org_name = '凤凰项目'
      )tt
      ON tt.emp_code = te.emp_id
      LEFT JOIN ${dim_dbname}.dim_dtk_org_level_info tg 
      ON tg.org_id = tmp.org_id AND tg.org_company_name = '上海快仓智能科技有限公司'  
      WHERE te.d = '${pre1_date}' AND te.org_company_name = '上海快仓智能科技有限公司'
        AND (tg.org_name_2 IN ('AMR FT','智能搬运FT','硬件自动化','箱式FT','系统中台','制造部') OR (tg.org_name_2 is NULL AND te.is_job = 0))
    )tu
    LEFT JOIN 
    (
      SELECT DISTINCT days,
                      day_type
      FROM ${dim_dbname}.dim_day_date
      WHERE days >= '2021-01-01' AND days <= '${pre1_date}'
     )td
     WHERE td.days >= tu.hired_date AND td.days <= IF(tu.is_job = 0,tu.quit_date,'${pre1_date}') 
  )tud
  -- 工时统计
  LEFT JOIN 
  (
    SELECT to_date(t.task_start_time) as stat_date,
           t.user_uuid,
		   tou.user_email,
		   round(COALESCE(sum(t.task_spend_hours), 0), 2) as work_hour
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou 
    ON tou.uuid = t.user_uuid
    LEFT JOIN ${dwd_dbname}.dwd_ones_task_info_ful t1
    ON t.task_uuid = t1.uuid
    WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null
  	  AND t1.status = 1 AND t1.issue_type_cname in ('缺陷','任务','需求')
      AND (IF((t1.project_type_name = '外部客户项目' AND t1.external_project_code is null),'未知项目编码',t1.external_project_code) != '未知项目编码'
	       OR IF((t1.project_type_name = '内部研发项目' AND t1.project_bpm_code is null),'未知项目编码',t1.project_bpm_code) != '未知项目编码'
	       OR t1.project_type_name = '技术&管理工作')
    GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email
  )t1
  ON t1.user_email = tud.user_email AND t1.stat_date = tud.days
  -- 工时明细
  LEFT JOIN 
  (
    SELECT to_date(t.task_start_time) as stat_date,
           t.task_uuid,
           t.user_uuid,
           t.project_classify_name,
		   tou.user_email,
	  	   DATEDIFF(to_date(t.task_create_time),to_date(t.task_start_time)) as actual_date,
           t.task_spend_hours as work_hour
    FROM ${dwd_dbname}.dwd_ones_task_manhour_info_ful t
    LEFT JOIN ${dwd_dbname}.dwd_ones_org_user_info_ful tou on tou.uuid = t.user_uuid
    WHERE t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null
  )t2 
  ON t2.user_email = tud.user_email AND t2.stat_date = tud.days
  -- ones基础信息
  LEFT JOIN 
  (
    SELECT to_date(t1.task_create_time) as stat_date,
           t1.uuid,
           t1.\`number\` as work_id,
           t1.summary,
           t1.task_desc,
           t1.project_classify_name,
           t1.sprint_classify_name,
		   t1.issue_type_cname as work_type,
           t1.task_status_cname as work_status,
           t1.org_name_1,
           IF((t1.project_type_name = '外部客户项目' AND t1.external_project_code is null) or t1.project_type_name = '技术&管理工作','未知项目编码',t1.external_project_code) as external_project_code,
           IF((t1.project_type_name = '外部客户项目' AND t1.external_project_code is null) or t1.project_type_name = '技术&管理工作','未知项目名称',IF(t1.external_project_code = 'A00000','商机项目',b1.project_name)) as external_project_name,
           IF((t1.project_type_name = '内部研发项目' AND t1.project_bpm_code is null) or t1.project_type_name = '技术&管理工作','未知项目编码',t1.project_bpm_code) as project_bpm_code,
           IF((t1.project_type_name = '内部研发项目' AND t1.project_bpm_code is null) or t1.project_type_name = '技术&管理工作','未知项目名称',b2.project_name) as project_bpm_name,
           t1.project_type_name
    FROM ${dwd_dbname}.dwd_ones_task_info_ful t1
    LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b1
    ON b1.d = '${pre1_date}' AND IF(t1.external_project_code like 'S-%',SUBSTRING(t1.external_project_code,3) = b1.project_code,(t1.external_project_code = b1.project_code OR t1.external_project_code = b1.project_sale_code))
    LEFT JOIN ${dwd_dbname}.dwd_pms_share_project_base_info_df b2
    ON b2.d = '${pre1_date}' AND (t1.project_bpm_code = b2.project_code OR t1.project_bpm_code = b2.project_sale_code) AND ((b2.data_source = 'BPM' AND b2.project_type_name IN ('内部项目','公司级项目','硬件部项目','软件部项目') AND b2.project_operation_state NOT IN ('9.项目暂停','10.项目暂停','11.项目取消')) OR (b2.data_source = 'PMS' AND b2.is_external_project = 0))
    WHERE t1.status = 1 AND t1.issue_type_cname in ('缺陷','任务','需求')
  )tt 
ON tt.uuid = t2.task_uuid 
  -- 请假数据
  LEFT JOIN 
  (
    SELECT l1.originator_user_id,
           l1.stat_date,
           case when l2.leave_type is null THEN l1.leave_type else '全天请假' END as leave_type
    FROM 
    (
      SELECT l.originator_user_id,
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
  ) tt1
  ON tt1.originator_user_id = tud.emp_id AND tt1.stat_date = tud.days
  -- 加班数据
  LEFT JOIN  
  (
    SELECT l1.applicant_userid,
           l1.stat_date,
           case when l2.work_overtime_type is null THEN l1.work_overtime_type else '全天加班' END as work_overtime_type
    FROM 
    (
      SELECT l.applicant_userid,
             cast(l.overtime_date as date) as stat_date,
             CASE when l.period_type = '全天' THEN '全天加班'
                  when l.period_type = '下午' THEN '下半天加班'
                  when l.period_type = '上午' THEN '上半天加班' end as work_overtime_type,
             row_number()over(PARTITION by l.applicant_userid,cast(l.overtime_date as date) order by CASE when l.period_type = '全天' THEN '全天加班'
                                                                                                          when l.period_type = '下午' THEN '下半天加班'
                                                                                                          when l.period_type = '上午' THEN '上半天加班' end asc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_work_overtime_dayily_info_df l
      WHERE l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' AND l.d = '${pre1_date}'
    )l1
    LEFT JOIN 
    (
      SELECT l.applicant_userid,
             cast(l.overtime_date as date) as stat_date,
             CASE when l.period_type = '全天' THEN '全天加班'
                  when l.period_type = '下午' THEN '下半天加班'
                  when l.period_type = '上午' THEN '上半天加班' end as work_overtime_type,
             row_number()over(PARTITION by l.applicant_userid,cast(l.overtime_date as date) order by CASE when l.period_type = '全天' THEN '全天加班'
                                                                                                          when l.period_type = '下午' THEN '下半天加班'
                                                                                                          when l.period_type = '上午' THEN '上半天加班' end asc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_work_overtime_dayily_info_df l
      WHERE l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' AND l.d = '${pre1_date}'
    )l2
    ON l1.applicant_userid = l2.applicant_userid AND l1.stat_date = l2.stat_date AND l1.work_overtime_type != l2.work_overtime_type
    WHERE l1.rn = 1 
  ) tt2
  ON tt2.applicant_userid = tud.emp_id AND tt2.stat_date = tud.days
  -- 出差时长
  LEFT JOIN 
  (
    SELECT t1.originator_user_id,
           t1.stat_date,
           case when t2.travel_type is null THEN t1.travel_type else '全天出差' END as travel_type,
           case when (case when t2.travel_type is null THEN t1.travel_type else '全天出差' END) = '全天出差' THEN 1 
                when (case when t2.travel_type is null THEN t1.travel_type else '全天出差' END) like '%半天出差' THEN 0.5 
           else 0 end as travel_days    
    FROM 
    (
      SELECT t.originator_user_id,
             cast(t.travel_date as date) as stat_date,
             CASE when t.period_type = '全天' THEN '全天出差'
                  when t.period_type = '下午' THEN '下半天出差'
                  when t.period_type = '上午' THEN '上半天出差' end as travel_type,
             row_number()over(PARTITION by t.originator_user_id,cast(t.travel_date as date) order by CASE when t.period_type = '全天' THEN '全天出差'
                                                                                                          when t.period_type = '下午' THEN '下半天出差'
                                                                                                          when t.period_type = '上午' THEN '上半天出差' end asc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_business_travel_dayily_info_df t
      WHERE t.is_valid = 1 AND t.d = '${pre1_date}' AND t.approval_result = 'agree' AND t.approval_status = 'COMPLETED' 
    )t1
    LEFT JOIN 
    (
      SELECT t.originator_user_id,
             cast(t.travel_date as date) as stat_date,
             CASE when t.period_type = '全天' THEN '全天出差'
                  when t.period_type = '下午' THEN '下半天出差'
                  when t.period_type = '上午' THEN '上半天出差' end as travel_type,
             row_number()over(PARTITION by t.originator_user_id,cast(t.travel_date as date) order by CASE when t.period_type = '全天' THEN '全天出差'
                                                                                                          when t.period_type = '下午' THEN '下半天出差'
                                                                                                          when t.period_type = '上午' THEN '上半天出差' end asc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_business_travel_dayily_info_df t
      WHERE t.is_valid = 1 AND t.d = '${pre1_date}' AND t.approval_result = 'agree' AND t.approval_status = 'COMPLETED' 
    )t2
    ON t1.originator_user_id = t2.originator_user_id AND t1.stat_date = t2.stat_date AND t1.travel_type != t2.travel_type
    WHERE t1.rn = 1 
  ) tt3
  ON tt3.originator_user_id = tud.emp_id AND tt3.stat_date = tud.days
)

INSERT overwrite table ${ads_dbname}.ads_pms_project_profit_detail
SELECT '' as id, -- 主键
       t1.project_code, -- 项目编码
       t1.project_sale_code, -- 售前编码
       t1.project_name, -- 项目名称
       t1.project_priority, -- 项目等级
       t1.project_dispaly_state_group, -- 项目阶段组
       t1.project_ft, -- 项目所属ft
       t1.project_area, -- 项目所在区域地点
       t1.online_process_approval_time, -- 上线月份
       t1.final_inspection_process_approval_time, --验收月份
       t1.post_project_date, -- 结项月份
       nvl(t1.project_income,0) as project_income, -- 项目收入
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
       nvl(t5.io_service_cost,0) + nvl(t16.io_service_cost_ago,0) as io_service_cost, -- 外包实施劳务 => 人工费用 => 项目费用
       nvl(t4.op_service_cost,0) + nvl(t17.op_service_cost_ago,0) as op_service_cost, -- 外包运维劳务 => 人工费用 => 项目费用
       nvl(t19.te_cost,0) as te_cost, -- 研发 => 人工费用 => 项目费用
       nvl(t20.ctrip_car_cost,0) + nvl(t21.ctrip_flight_cost,0) + nvl(t22.ctrip_hotel_cost,0) as ctrip_amount, -- 携程商旅 => 差旅费 => 项目费用
       nvl(t6.reimburse_amount,0) as reimburse_amount, -- 个人报销 => 差旅费 => 项目费用
       nvl(t7.agv_cost,0) + nvl(t8.bucket_cost,0) + nvl(t9.charging_cost,0) + nvl(t13.project_other_matters_cost,0) + nvl(t10.export_packing_cost,0) + nvl(t11.transportation_cost,0) + nvl(t12.ectocyst_software_cost,0) + nvl(t15.ectocyst_hardware_cost,0) + nvl(t3.pe_cost,0) + nvl(t18.mt_service_cost,0) + nvl(t5.io_service_cost,0) + nvl(t16.io_service_cost_ago,0) + nvl(t4.op_service_cost,0) + nvl(t17.op_service_cost_ago,0) + nvl(t19.te_cost,0) + nvl(t6.reimburse_amount,0) + nvl(t20.ctrip_car_cost,0) + nvl(t21.ctrip_flight_cost,0) + nvl(t22.ctrip_hotel_cost,0) as cost_sum, -- 成本费用合计
       nvl(t1.project_income,0) - (nvl(t7.agv_cost,0) + nvl(t8.bucket_cost,0) + nvl(t9.charging_cost,0) + nvl(t13.project_other_matters_cost,0) + nvl(t10.export_packing_cost,0) + nvl(t11.transportation_cost,0) + nvl(t12.ectocyst_software_cost,0) + nvl(t15.ectocyst_hardware_cost,0) + nvl(t3.pe_cost,0) + nvl(t18.mt_service_cost,0) + nvl(t5.io_service_cost,0) + nvl(t16.io_service_cost_ago,0) + nvl(t4.op_service_cost,0) + nvl(t17.op_service_cost_ago,0) + nvl(t19.te_cost,0) + nvl(t6.reimburse_amount,0) + nvl(t20.ctrip_car_cost,0) + nvl(t21.ctrip_flight_cost,0) + nvl(t22.ctrip_hotel_cost,0)) as project_gp, -- 项目毛利
       nvl((nvl(t1.project_income,0) - (nvl(t7.agv_cost,0) + nvl(t8.bucket_cost,0) + nvl(t9.charging_cost,0) + nvl(t13.project_other_matters_cost,0) + nvl(t10.export_packing_cost,0) + nvl(t11.transportation_cost,0) + nvl(t12.ectocyst_software_cost,0) + nvl(t15.ectocyst_hardware_cost,0) + nvl(t3.pe_cost,0) + nvl(t18.mt_service_cost,0) + nvl(t5.io_service_cost,0) + nvl(t16.io_service_cost_ago,0) + nvl(t4.op_service_cost,0) + nvl(t17.op_service_cost_ago,0) + nvl(t19.te_cost,0) + nvl(t6.reimburse_amount,0) + nvl(t20.ctrip_car_cost,0) + nvl(t21.ctrip_flight_cost,0) + nvl(t22.ctrip_hotel_cost,0))) / nvl(t1.project_income,0),0) as project_gp_rate, -- 项目毛利率
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM
-- 项目基础信息
(
  SELECT b.project_code,
         b.project_sale_code,
         b.project_name,
         b.project_priority,
         b.project_dispaly_state_group,
         b.project_ft,
         b.project_area,
         b.online_process_month as online_process_approval_time,
         b.final_inspection_process_month as final_inspection_process_approval_time,
         date_format(b.post_project_date,'yyyy-MM') as post_project_date,
         b.amount as project_income
  FROM ${tmp_dbname}.tmp_pms_project_general_view_detail b
)t1
-- AGV数量
LEFT JOIN
(
  SELECT b.project_code,
         SUM(nvl(tmp.actual_sale_num,0)) as agv_num
  FROM 
  (
    SELECT so.project_code,
           nvl(so.real_qty,0) - nvl(sr.real_qty,0) as actual_sale_num
    FROM 
    -- 出库
    (
      SELECT so.project_code,
             SUM(nvl(so.real_qty,0)) as real_qty -- 出库数量
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox  = 1 -- 物料属性为agv
        AND m.document_status = 'C' -- 数据状态：完成
        AND so.project_code is not NULL
      GROUP BY so.project_code
    )so
    LEFT JOIN
    -- 退货
    (
      SELECT sr.project_code,
             SUM(nvl(sr.real_qty,0)) as real_qty -- 退货数量
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
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
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t2
ON t1.project_code = t2.project_code
-- PE费用
LEFT JOIN 
(
  SELECT b.project_code,
         SUM(tmp.pe_cost) as pe_cost
  FROM
  (
    SELECT tt.project_code,
           SUM(case when tt.day_type = '工作日' or tt.day_type = '节假日' or tt.day_type = '调休' or tt.day_type = '周末' then 1
                    when tt.day_type = '上半天请假' or tt.day_type = '下半天请假' then 0.5
                    when tt.day_type = '全天请假' then 0 end) as pe_day,
           SUM(case when (tt.day_type = '工作日' or tt.day_type = '节假日' or tt.day_type = '调休' or tt.day_type = '周末') AND tt.org_name_2 = '箱式FT' then 1*700
                    when (tt.day_type = '工作日' or tt.day_type = '节假日' or tt.day_type = '调休' or tt.day_type = '周末') AND tt.org_name_2 = '营销中心' AND (tt.org_name_3 = '华东大区' or tt.org_name_3 = '华中大区' or tt.org_name_3 = '华北大区' or tt.org_name_3 = '华南大区' or  tt.org_name_3 = '西南大区' or  tt.org_name_3 = '福建子公司') then 1*700
                    when (tt.day_type = '工作日' or tt.day_type = '节假日' or tt.day_type = '调休' or tt.day_type = '周末') AND tt.org_name_2 = '项目部' then 1*500
                    when (tt.day_type = '工作日' or tt.day_type = '节假日' or tt.day_type = '调休' or tt.day_type = '周末') AND tt.org_name_2 = '海外事业部' then 1*1500
                    when (tt.day_type = '上半天请假' or tt.day_type = '下半天请假') AND tt.org_name_2 = '箱式FT' then 0.5*700
                    when (tt.day_type = '上半天请假' or tt.day_type = '下半天请假') AND tt.org_name_2 = '营销中心' AND (tt.org_name_3 = '华东大区' or tt.org_name_3 = '华中大区' or tt.org_name_3 = '华北大区' or tt.org_name_3 = '华南大区' or  tt.org_name_3 = '西南大区' or  tt.org_name_3 = '福建子公司') then 0.5*700
                    when (tt.day_type = '上半天请假' or tt.day_type = '下半天请假') AND tt.org_name_2 = '项目部' then 0.5*500
                    when (tt.day_type = '上半天请假' or tt.day_type = '下半天请假') AND tt.org_name_2 = '海外事业部' then 0.5*1500
                    when tt.day_type = '全天请假' then 0 end) as pe_cost
    FROM 
    (
      SELECT tud.org_name_2,
             tud.org_name_3,
             IF(t12.leave_type is not null,t12.leave_type,tud.day_type) as day_type, -- 日期类型
             t1.log_date, -- 日志日期
             t1.project_code -- 项目编码
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
               CASE when td.day_type = 0 then '工作日'
                    when td.day_type = 1 then '周末'
                    when td.day_type = 2 then '节假日'
                    when td.day_type = 3 then '调休' end as day_type   
        FROM
        (
          SELECT tmp.org_name_2,
                 tmp.org_name_3,
                 tmp.emp_id,
                 tmp.emp_name,
                 tmp.emp_position,
                 tmp.is_job,
                 tmp.hired_date,
                 tmp.quit_date
          FROM
          (
            SELECT DISTINCT split(tg.org_path_name,'/')[1] as org_name_2,
                            split(tg.org_path_name,'/')[2] as org_name_3,
                            te.emp_id,
                            te.emp_name,
                            te.emp_position,
                            te.prg_path_name,
                            te.is_job,
                            date(te.hired_date) as hired_date,
                            date(te.quit_date) as quit_date,
                            row_number()over(PARTITION by te.emp_id order by split(tg.org_path_name,'/')[1] asc,split(tg.org_path_name,'/')[2] asc)rn
            FROM ${dwd_dbname}.dwd_dtk_emp_info_df te
            LEFT JOIN 
            (
              SELECT DISTINCT m.emp_id,
                              m.emp_name,
                              m.org_id,
                              m.org_role_type,
                              m.is_need_fill_manhour,
                              m.org_start_date,
                              m.org_end_date,
                              m.is_job
              FROM ${dwd_dbname}.dwd_dtk_emp_org_history_mapping_info_df m
              WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.d = '${pre1_date}' AND m.is_valid = 1 AND m.org_end_date = IF(m.is_job = 1,'9999-01-01',m.org_end_date)
            )tmp
            ON te.emp_id = tmp.emp_id
            LEFT JOIN ${dim_dbname}.dim_dtk_org_history_info_df tg 
            ON tg.org_id = tmp.org_id AND tg.d = IF(tmp.org_end_date = '9999-01-01','${pre1_date}',IF(tmp.is_job = 0 ,DATE_ADD(tmp.org_end_date, -1),tmp.org_end_date))
            WHERE te.d = '${pre1_date}' AND te.org_company_name = '上海快仓智能科技有限公司' AND te.is_active = 1 AND te.emp_function_role = 'PE'
          )tmp
          WHERE tmp.rn =1
        )tu  
        LEFT JOIN
        (
          SELECT DISTINCT days,
                          day_type
          FROM ${dim_dbname}.dim_day_date
          WHERE days >= '2021-07-01' AND days <= '${pre1_date}'
        )td
        ON td.days >= tu.hired_date AND td.days <= IF(tu.quit_date is NULL,'${pre1_date}',tu.quit_date)
      )tud
      LEFT JOIN 
      (
        SELECT p.log_date, -- 日志日期
               p.project_code, -- 项目编码
               p.applicant_user_id -- 人员编码
        FROM ${dwd_dbname}.dwd_pms_project_emp_log_info_df p
        WHERE p.d = '${pre1_date}' AND p.role_type = 'PE'
        GROUP BY p.log_date,p.project_code,p.applicant_user_id
      )t1
      ON t1.applicant_user_id = tud.emp_id AND tud.days = t1.log_date 
      LEFT JOIN 
      (
        SELECT l1.originator_user_id as emp_id,
               l1.stat_date,
               case when l2.leave_type is null THEN l1.leave_type else '全天请假' END as leave_type
        FROM 
        (
          SELECT l.originator_user_id,
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
      )t12 
      ON t12.emp_id = tud.emp_id AND t12.stat_date = tud.days
    )tt
    WHERE tt.project_code is not NULL 
    GROUP BY tt.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t3
ON t1.project_code = t3.project_code
-- 外包劳务-运维费用 2022年之后的数据
LEFT JOIN
(
  SELECT b.project_code,
         SUM(tmp.service_cost) as op_service_cost
  FROM
  (
    SELECT tt.project_code,
       	   SUM(tt.service_cost) as service_cost
   	FROM 
    (
      SELECT tt1.cur_date,
             tt1.project_code,
           	 tt1.originator_user_name as member_name,
             SUM(tt1.check_duration) as check_duration_hour,
             case when SUM(tt1.check_duration) < 4 then 0
                  when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then 350
                  when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then 550
                  when SUM(tt1.check_duration) > 10 then 550 + (SUM(tt1.check_duration) - 10)*2*20 END as service_cost
      FROM 
      (
        SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
               a.business_id, -- 审批编号
               a.project_code, -- 项目编号
               a.originator_dept_name, -- 团队名称
               IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
               '运维劳务' as service_type, -- 劳务类型
               IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
               a.checkin_time, -- 考勤签到时间
               a.checkout_time, -- 考勤签退时间
               row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
       FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
       WHERE (a.service_type = '运维（陪产）劳务' OR a.service_type is null) AND a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准,只取2022年之后的数据
      )tt1
      LEFT JOIN 
      (
      	SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
               a.business_id, -- 审批编号
               a.project_code, -- 项目编号
               a.originator_dept_name, -- 团队名称
               IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
               '运维劳务' as service_type, -- 劳务类型
               IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
               a.checkin_time, -- 考勤签到时间
               a.checkout_time, -- 考勤签退时间
               row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
      	FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
        WHERE (a.service_type = '运维（陪产）劳务' OR a.service_type is null) AND a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准，只取2022年之后的数据
      )tt2
      ON tt1.cur_date = tt2.cur_date AND tt1.project_code = tt2.project_code AND tt1.originator_user_name = tt2.originator_user_name AND tt1.rn = tt2.rn + 1
      WHERE tt2.rn is null or tt1.checkin_time not BETWEEN tt2.checkin_time and tt2.checkout_time
      GROUP BY tt1.cur_date,tt1.project_code,tt1.originator_user_name
    
      UNION ALL 
    
      SELECT TO_DATE(tt1.log_date) as log_date,
             tt1.project_code,
             tt1.applicant_user_id,
             SUM(tt1.check_duration) as check_duration_hour,
             case when SUM(tt1.check_duration) < 4 then 0
                  when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then 350
                  when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then 550
                  when SUM(tt1.check_duration) > 10 then 550 + (SUM(tt1.check_duration) - 10)*2*20 END as service_cost
      FROM 
      (
        SELECT p.log_date,
               p.project_code,
               p.applicant_user_id,
               SUM(nvl(p.working_hours,0)) as check_duration 
        FROM ${dwd_dbname}.dwd_pms_project_emp_log_info_df p
        WHERE p.d = '${pre1_date}' AND p.data_source = 'pms' AND p.role_type = 'OPS'
        GROUP BY p.log_date,p.project_code,p.applicant_user_id
      )tt1
      GROUP BY tt1.log_date,tt1.project_code,tt1.applicant_user_id
    )tt
    GROUP BY tt.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t4
ON t1.project_code = t4.project_code
-- 外包劳务-实施费用 2022年之后的数据
LEFT JOIN
(
  SELECT b.project_code,
         SUM(tmp.service_cost) as io_service_cost
  FROM
  (
    SELECT tt.project_code,
           SUM(tt.service_cost) as service_cost
    FROM 
    (
      SELECT tt1.cur_date,
             tt1.project_code,
           	 tt1.originator_user_name as member_name,
             SUM(tt1.check_duration) as check_duration_hour,
             case when SUM(tt1.check_duration) < 4 then 0
                  when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then 350
                  when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then 550
                  when SUM(tt1.check_duration) > 10 then 550 + (SUM(tt1.check_duration) - 10)*2*20 END as service_cost
      FROM 
      (
        SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
               a.business_id, -- 审批编号
               a.project_code, -- 项目编号
               a.originator_dept_name, -- 团队名称
               IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
               '实施劳务' as service_type, -- 劳务类型
               IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
               a.checkin_time, -- 考勤签到时间
               a.checkout_time, -- 考勤签退时间
               row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
        FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
        WHERE a.service_type = '实施劳务' AND a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准，只取2022年之后的数据
      )tt1
      LEFT JOIN 
      (
     	SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
               a.business_id, -- 审批编号
               a.project_code, -- 项目编号
               a.originator_dept_name, -- 团队名称
               IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
               '实施劳务' as service_type, -- 劳务类型
               IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
               a.checkin_time, -- 考勤签到时间
               a.checkout_time, -- 考勤签退时间
               row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
     	FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
        WHERE a.service_type = '实施劳务' AND a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准，只取2022年之后的数据
      )tt2
      ON tt1.cur_date = tt2.cur_date AND tt1.project_code = tt2.project_code AND tt1.originator_user_name = tt2.originator_user_name AND tt1.rn = tt2.rn + 1
      WHERE tt2.rn is null or tt1.checkin_time not BETWEEN tt2.checkin_time and tt2.checkout_time
      GROUP BY tt1.cur_date,tt1.project_code,tt1.originator_user_name
    
      UNION ALL 
    
      SELECT TO_DATE(tt1.log_date) as log_date,
             tt1.project_code,
             tt1.applicant_user_id,
             SUM(tt1.check_duration) as check_duration_hour,
             case when SUM(tt1.check_duration) < 4 then 0
                  when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then 350
                  when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then 550
                  when SUM(tt1.check_duration) > 10 then 550 + (SUM(tt1.check_duration) - 10)*2*20 END as service_cost
      FROM 
      (
        SELECT p.log_date,
               p.project_code,
               p.applicant_user_id,
               SUM(nvl(p.working_hours,0)) as check_duration 
        FROM ${dwd_dbname}.dwd_pms_project_emp_log_info_df p
        WHERE p.d = '${pre1_date}' AND p.data_source = 'pms' AND p.role_type = 'IMP'
        GROUP BY p.log_date,p.project_code,p.applicant_user_id
      )tt1
      GROUP BY tt1.log_date,tt1.project_code,tt1.applicant_user_id
    )tt
    GROUP BY tt.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t5
ON t1.project_code = t5.project_code
-- 个人报销
LEFT JOIN 
(
  SELECT b.project_code,
         SUM(tmp.reimburse_amount) as reimburse_amount
  FROM
  -- 个人报销
  (
    SELECT p.project_code,
           SUM(nvl(p.reimburse_amount,0)) as reimburse_amount -- 报销金额
    FROM
    (
      SELECT p.flow_id,
             i.project_code,
             i.total_amount as reimburse_amount -- 报销金额
      FROM ${dwd_dbname}.dwd_bpm_personal_expense_account_info_ful p
      LEFT JOIN ${dwd_dbname}.dwd_bpm_personal_expense_account_item_info_ful i
      ON p.flow_id = i.flow_id
      WHERE p.approve_status = 30 AND p.project_code is not NULL AND p.currency_code = 'PRE001' -- 取货币类型为人民币的
    )p
    GROUP BY p.project_code
  )tmp
  -- 合并FH和A项目数据
  LEFT JOIN ${tmp_dbname}.tmp_pms_project_general_view_detail b
  ON b.project_code = tmp.project_code OR b.project_sale_code = tmp.project_code
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t6
ON t1.project_code = t6.project_code
-- agv费用
LEFT JOIN 
(
  SELECT b.project_code,
         SUM(tmp.actual_cost) as agv_cost
  FROM
  (
    SELECT so.project_code,
           nvl(so.finance_cost_amount_lc,0) - nvl(sr.finance_cost_amount_lc,0) as actual_cost
    FROM 
    -- 出库
    (
      SELECT so.project_code,
             SUM(IF(so.finance_cost_amount_lc != 0,so.finance_cost_amount_lc,(nvl(b.price_amount,0) * so.real_qty))) as finance_cost_amount_lc -- 最终价格
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- 物料内码
               SUM(b.end_period_number) as end_period_number, -- 期末数量
               SUM(b.end_period_amount) as end_period_amount, -- 期末金额
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- 物料单个成本价
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = year('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}'
        AND m.paez_checkbox  = 1 -- 物料属性为agv
        AND m.document_status = 'C' -- 数据状态：完成
        AND so.project_code is not NULL
      GROUP BY so.project_code
    )so
    LEFT JOIN
    -- 退货
    (
      SELECT sr.project_code,
             SUM(IF(sr.finance_cost_amount_lc != 0,sr.finance_cost_amount_lc,(nvl(b.price_amount,0) * sr.real_qty))) as finance_cost_amount_lc -- 最终价格
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- 物料内码
               SUM(b.end_period_number) as end_period_number, -- 期末数量
               SUM(b.end_period_amount) as end_period_amount, -- 期末金额
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- 物料单个成本价
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = year('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
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
  WHERE b.project_code is not NULL 
  GROUP BY b.project_code
)t7
ON t1.project_code = t7.project_code
-- 货架费用
LEFT JOIN 
(
  SELECT b.project_code,
         SUM(tmp.actual_cost) as bucket_cost
  FROM
  (
    SELECT so.project_code,
           nvl(so.finance_cost_amount_lc,0) - nvl(sr.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT so.project_code,
             SUM(IF(so.finance_cost_amount_lc != 0,so.finance_cost_amount_lc,(nvl(b.price_amount,0) * so.real_qty))) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- 物料内码
               SUM(b.end_period_number) as end_period_number, -- 期末数量
               SUM(b.end_period_amount) as end_period_amount, -- 期末金额
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- 物料单个成本价
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = year('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}' AND m.material_number like 'RT04%' -- 货架
        AND so.project_code is not NULL
      GROUP BY so.project_code
    )so
    LEFT JOIN
    (
      SELECT sr.project_code,
             SUM(IF(sr.finance_cost_amount_lc != 0,sr.finance_cost_amount_lc,(nvl(b.price_amount,0) * sr.real_qty))) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- 物料内码
               SUM(b.end_period_number) as end_period_number, -- 期末数量
               SUM(b.end_period_amount) as end_period_amount, -- 期末金额
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- 物料单个成本价
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = year('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}' AND m.material_number like 'RT04%' -- 货架
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
)t8
ON t1.project_code = t8.project_code
-- 充电桩费用
LEFT JOIN
(
  SELECT b.project_code,
         SUM(tmp.actual_cost) as charging_cost
  FROM
  (
    SELECT so.project_code,
           nvl(so.finance_cost_amount_lc,0) - nvl(sr.finance_cost_amount_lc,0) as actual_cost
    FROM 
    (
      SELECT so.project_code,
             SUM(IF(so.finance_cost_amount_lc != 0,so.finance_cost_amount_lc,(nvl(b.price_amount,0) * so.real_qty))) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so
      ON m.material_id = so.material_id AND so.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- 物料内码
               SUM(b.end_period_number) as end_period_number, -- 期末数量
               SUM(b.end_period_amount) as end_period_amount, -- 期末金额
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- 物料单个成本价
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = year('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}' AND m.material_number like 'RT03%' -- 充电桩
        AND so.project_code is not NULL
      GROUP BY so.project_code
    )so
    LEFT JOIN
    (
      SELECT sr.project_code,
             SUM(IF(sr.finance_cost_amount_lc != 0,sr.finance_cost_amount_lc,(nvl(b.price_amount,0) * sr.real_qty))) as finance_cost_amount_lc
      FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
      LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
      ON m.material_id = sr.material_id AND sr.d = '${pre1_date}'
      LEFT JOIN 
      (
        SELECT b.material_id, -- 物料内码
               SUM(b.end_period_number) as end_period_number, -- 期末数量
               SUM(b.end_period_amount) as end_period_amount, -- 期末金额
               nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- 物料单个成本价
        FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
        WHERE b.check_year = year('${pre1_date}') AND b.check_period = MONTH('${pre1_date}') - 1
        GROUP BY b.material_id
      )b
      ON m.material_id = b.material_id
      WHERE m.d = '${pre1_date}' AND m.material_number like 'RT03%' -- 充电桩
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
)t9
ON t1.project_code = t9.project_code
-- 出口包装费
LEFT JOIN
(  
  SELECT b.project_code,
         SUM(tmp.actual_cost) as export_packing_cost
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
      WHERE g.materia_number ='P' -- 包装
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
      WHERE g.materia_number ='P' -- 包装
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
        AND m.material_group != '111370' -- 排除物料分组为P（包装）
        AND m.material_group != '111373' -- 排除物料分组为S（软件）
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
        AND m.material_group != '111370' -- 排除物料分组为P（包装）
        AND m.material_group != '111373' -- 排除物料分组为S（软件）
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
      WHERE ((g.materia_number ='S' AND m.material_number not in ('S99L00587K010','S99L00588K010','S99L04951K010')) OR m.material_number = 'R5S90518')
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
      WHERE ((g.materia_number ='S' AND m.material_number not in ('S99L00587K010','S99L00588K010','S99L04951K010')) OR m.material_number = 'R5S90518' )
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
ON t1.project_code = t22.project_code;
-----------------------------------------------------------------------------------------------------------------------------00

"

$hive -e "$sql"



echo "----------------------------------------------------------------------------------###########hive ads => mysql ads#########------------------------------------------------------------------------------------------------------------- "


#!/bin/bash

HOSTNAME="007.bg.qkt"                                        
PORT="3306"
USERNAME="root"
PASSWORD="quicktron123456"
DBNAME="ads"                                     


ssh -tt 008.bg.qkt <<effo
mysql -h${HOSTNAME}  -P${PORT}  -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "

truncate table ads_pms_project_profit_detail;

"
exit
effo


echo "-------------------------------------------------------------------------------------------------######## 向接口表插入数据 #######----------------------------------------------------------------------------------------------- "


##表：ads_pms_project_profit_detail    --pms项目利润表
sqoop export -D mapred.job.queue.name=hive \
--connect "jdbc:mysql://007.bg.qkt:3306/ads?useUnicode=true&characterEncoding=utf-8" \
--username root \
--password quicktron123456 \
--table ads_pms_project_profit_detail \
--export-dir hdfs://001.bg.qkt:8020/user/hive/warehouse/ads.db/ads_pms_project_profit_detail \
--num-mappers 1  \
--input-null-string '\\N' \
--input-null-non-string '\\N' \
--input-fields-terminated-by "\t" \
--columns "id,project_code,project_sale_code,project_name,project_priority,project_dispaly_state_group,project_ft,project_area,online_process_approval_time,final_inspection_process_approval_time,post_project_date,project_income,agv_num,agv_cost,bucket_cost,charging_cost,project_other_matters_cost,export_packing_cost,transportation_cost,ectocyst_software_cost,ectocyst_hardware_cost,pe_cost,mt_service_cost,io_service_cost,op_service_cost,te_cost,ctrip_amount,reimburse_amount,cost_sum,project_gp,project_gp_rate,create_time,update_time"




echo "-----------------------------------------###########end############------------------------------------------------------------------------------------------- "





