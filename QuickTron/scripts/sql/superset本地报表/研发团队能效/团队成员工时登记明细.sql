-- 团队小组成员工单登记工时明细 ads_team_ft_virtual_member_manhour_detail 

INSERT overwrite table ${ads_dbname}.ads_team_ft_virtual_member_manhour_detail
SELECT '' as id,
       tud.team_ft,
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
            ELSE 0 END as cost_amount,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
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
    WHERE 1 = 1
      AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
      AND (tg.org_name_2 IN ('AMR FT','智能搬运FT','硬件自动化','箱式FT','系统中台','制造部') OR (tg.org_name_2 is NULL AND te.is_job = 0))
  ) tu
  LEFT JOIN 
  (
    SELECT DISTINCT days,
                    day_type
    FROM ${dim_dbname}.dim_day_date
    WHERE 1 = 1
      AND days >= '2021-01-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
   ) td
   WHERE td.days >= tu.hired_date AND td.days <= IF(tu.is_job = 0,tu.quit_date,DATE_ADD(CURRENT_DATE(), -1)) 
) tud
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
  WHERE 1 = 1
	AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null
	AND t1.status = 1 AND t1.issue_type_cname in ('缺陷','任务','需求')
	AND (IF((t1.project_type_name = '外部客户项目' AND t1.external_project_code is null),'未知项目编码',t1.external_project_code) != '未知项目编码'
	     OR IF((t1.project_type_name = '内部研发项目' AND t1.project_bpm_code is null),'未知项目编码',t1.project_bpm_code) != '未知项目编码'
	     OR t1.project_type_name = '技术&管理工作')
  GROUP BY to_date(t.task_start_time),t.user_uuid,tou.user_email
) t1
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
  WHERE 1 = 1
	AND t.task_type = 1 AND t.status = 1 AND t.user_uuid is not null
) t2 
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
  LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b1
  ON IF(t1.external_project_code like 'S-%',SUBSTRING(t1.external_project_code,3) = b1.project_code,t1.external_project_code = b1.project_code) AND b1.d = DATE_ADD(CURRENT_DATE(), -1)
  LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful b2
  ON t1.project_bpm_code = b2.project_code AND b2.project_type IN ('内部项目','公司级项目','硬件部项目','软件部项目') AND b2.project_status NOT IN ('9.项目暂停','10.项目暂停','11.项目取消')
  WHERE t1.status = 1 AND t1.issue_type_cname in ('缺陷','任务','需求')
) tt 
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
    WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1) 
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
    WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1)
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
    WHERE l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1)
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
    WHERE l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1)
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
    WHERE t.is_valid = 1 AND t.d = DATE_ADD(CURRENT_DATE(), -1) AND t.approval_result = 'agree' AND t.approval_status = 'COMPLETED' 
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
    WHERE t.is_valid = 1 AND t.d = DATE_ADD(CURRENT_DATE(), -1) AND t.approval_result = 'agree' AND t.approval_status = 'COMPLETED' 
  )t2
  ON t1.originator_user_id = t2.originator_user_id AND t1.stat_date = t2.stat_date AND t1.travel_type != t2.travel_type
  WHERE t1.rn = 1 
) tt3
ON tt3.originator_user_id = tud.emp_id AND tt3.stat_date = tud.days;