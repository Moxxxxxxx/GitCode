--ads_project_profit_detail    --项目利润表

-- 工时明细表
with manhour_detail as
(
SELECT tud.team_ft,
       tud.team_group,
	   tud.team_sub_group,
       tud.user_name                                           as team_member,
       tud.emp_position,
       tud.is_job,
       tud.hired_date,
       tud.quit_date,
       tud.is_need_fill_manhour,
       tud.org_role_type,
       tud.virtual_role_type,
       tud.module_branch,
       tud.virtual_org_name,
       tt.org_name_1                                           as project_org_name,
       tt.project_classify_name                                as project_classify_name,
       tt.sprint_classify_name                                 as sprint_classify_name,
       tt.external_project_code,
       tt.external_project_name,
       tt.project_bpm_code,
       tt.project_bpm_name,
       tt.project_type_name,
       cast(tt.stat_date as date)                              as work_create_date,
       tt.work_id,
       tt.summary                                              as work_summary,
       tt.task_desc                                            as work_desc,
       tt.work_type                                            as work_type,
       tt.work_status,
       cast(tud.days as date)                                  as work_check_date,
       IF(t3.day_type is not null,t3.day_type,IF(t13.work_overtime_type is not null,t13.work_overtime_type,IF(t3.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) as day_type,
       cast(nvl(t2.work_hour, 0) as decimal(10, 2))            as work_hour,
       IF(tt.work_id is null,0,t2.actual_date) as actual_date,
       CASE WHEN tt.project_type_name is null and tt.work_id is not null and t2.actual_date > 7 THEN '无效工时&违规登记'
            WHEN (tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码')) and t2.actual_date > 7 THEN '编码异常&违规登记'
            WHEN tt.project_type_name is null and tt.work_id is not null THEN '无效工时'
            WHEN t2.actual_date > 7 THEN '违规登记'
            WHEN tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码') THEN '编码异常'
            ELSE '无异常' END as error_type,
       IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2)))            as work_hour_total,
       nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0) as work_hour_rate,
       CASE WHEN tud.team_ft = '制造部' and IF(t3.day_type is not null,t3.day_type,IF(t13.work_overtime_type is not null,t13.work_overtime_type,IF(t3.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 700 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
            WHEN tud.team_ft = '制造部' and IF(t3.day_type is not null,t3.day_type,IF(t13.work_overtime_type is not null,t13.work_overtime_type,IF(t3.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) IN ('下半天请假','上半天请假','下半天加班','上半天加班','下半天请假-哺乳假','上半天请假-哺乳假') THEN 700 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
            WHEN tud.team_ft = '硬件自动化' and IF(t3.day_type is not null,t3.day_type,IF(t13.work_overtime_type is not null,t13.work_overtime_type,IF(t3.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1000 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
            WHEN tud.team_ft = '硬件自动化' and IF(t3.day_type is not null,t3.day_type,IF(t13.work_overtime_type is not null,t13.work_overtime_type,IF(t3.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) IN ('下半天请假','上半天请假','下半天加班','上半天加班','下半天请假-哺乳假','上半天请假-哺乳假') THEN 1000 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
            WHEN tud.team_ft = 'AMR FT' and IF(t3.day_type is not null,t3.day_type,IF(t13.work_overtime_type is not null,t13.work_overtime_type,IF(t3.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1300 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
            WHEN tud.team_ft = 'AMR FT' and IF(t3.day_type is not null,t3.day_type,IF(t13.work_overtime_type is not null,t13.work_overtime_type,IF(t3.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) IN ('下半天请假','上半天请假','下半天加班','上半天加班','下半天请假-哺乳假','上半天请假-哺乳假') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
            WHEN tud.team_ft = '系统中台' and IF(t3.day_type is not null,t3.day_type,IF(t13.work_overtime_type is not null,t13.work_overtime_type,IF(t3.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1300 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
            WHEN tud.team_ft = '系统中台' and IF(t3.day_type is not null,t3.day_type,IF(t13.work_overtime_type is not null,t13.work_overtime_type,IF(t3.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) IN ('下半天请假','上半天请假','下半天加班','上半天加班','下半天请假-哺乳假','上半天请假-哺乳假') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
            WHEN tud.team_ft = '箱式FT' and IF(t3.day_type is not null,t3.day_type,IF(t13.work_overtime_type is not null,t13.work_overtime_type,IF(t3.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1300 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
            WHEN tud.team_ft = '箱式FT' and IF(t3.day_type is not null,t3.day_type,IF(t13.work_overtime_type is not null,t13.work_overtime_type,IF(t3.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) IN ('下半天请假','上半天请假','下半天加班','上半天加班','下半天请假-哺乳假','上半天请假-哺乳假') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
            WHEN tud.team_ft = '智能搬运FT' and IF(t3.day_type is not null,t3.day_type,IF(t13.work_overtime_type is not null,t13.work_overtime_type,IF(t3.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1300 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
            WHEN tud.team_ft = '智能搬运FT' and IF(t3.day_type is not null,t3.day_type,IF(t13.work_overtime_type is not null,t13.work_overtime_type,IF(t3.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) IN ('下半天请假','上半天请假','下半天加班','上半天加班','下半天请假-哺乳假','上半天请假-哺乳假') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
            WHEN tud.team_ft = '已离职' and IF(t3.day_type is not null,t3.day_type,IF(t13.work_overtime_type is not null,t13.work_overtime_type,IF(t3.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) IN ('工作日','调休','全天加班','工作日-哺乳假','调休-哺乳假') THEN 1300 * 1 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
            WHEN tud.team_ft = '已离职' and IF(t3.day_type is not null,t3.day_type,IF(t13.work_overtime_type is not null,t13.work_overtime_type,IF(t3.leave_type = '哺乳假' AND tud.day_type not in('周末','节假日'),CONCAT(tud.day_type,'-','哺乳假'),tud.day_type))) IN ('下半天请假','上半天请假','下半天加班','上半天加班','下半天请假-哺乳假','上半天请假-哺乳假') THEN 1300 * 0.5 * nvl(cast(nvl(t2.work_hour, 0) as decimal(10, 2))/IF(tt.project_type_name != '技术&管理工作' AND (tt.external_project_code ='未知项目编码' or tt.project_bpm_code = '未知项目编码'),0,cast(nvl(t1.work_hour, 0) as decimal(10, 2))),0)
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
                      row_number()over(PARTITION by m.emp_id,m.emp_name order by m.is_need_fill_manhour desc,m.org_role_type desc)rn
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
         IF((t1.project_type_name = '外部客户项目' AND t1.external_project_code is null) or t1.project_type_name = '技术&管理工作','未知项目名称',b1.project_name) as external_project_name,
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
  SELECT tmp1.stat_date,
         tmp1.originator_user_id,
         tmp1.email,
         IF(tmp2.stat_date is not null,IF(tmp2.day_type = '全天请假' OR tmp2.leave_type != '哺乳假',tmp2.day_type,CONCAT(tmp2.day_type,'-',tmp1.leave_type)),tmp1.day_type) as day_type,
         tmp1.leave_type
  FROM
  (
    SELECT tud.days as stat_date,
           l.originator_user_id,
	  	   te.email,
		   CASE when (tud.days > l.start_date and tud.days < l.end_date and l.leave_type != '哺乳假')
                  or (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '下午')
                  or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '上午') 
                  or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '下午') then '全天请假'
                when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '上午')
                  or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '上午') then '上半天请假'
                when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '下午' and l.end_time_period = '下午')
                  or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '下午') then '下半天请假'
           end as day_type,
           l.leave_type
    FROM 
    (
      SELECT IF(l1.business_id is not null,l1.business_id,l.business_id) as business_id,
             l.originator_user_id,
             IF(l1.business_id is not null,l.start_date,l.start_date) as start_date,
             IF(l1.business_id is not null,l.start_time_period,l.start_time_period) as start_time_period,
             IF(l1.business_id is not null,l1.end_date,l.end_date) as end_date,
             IF(l1.business_id is not null,l1.end_time_period,l.end_time_period) as end_time_period,
             IF(l.leave_type != '哺乳假','正常请假','哺乳假') as leave_type,
             row_number()over(PARTITION by IF(l1.business_id is not null,l1.business_id,l.business_id) order by l.start_date asc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_leave_info_df l
      LEFT JOIN ${dwd_dbname}.dwd_dtk_process_leave_info_df l1
      ON l.originator_user_id = l1.originator_user_id AND l.end_date = l1.start_date AND l.start_date != l.end_date AND l.d = l1.d AND l.process_result = l1.process_result AND l.process_status = l1.process_status AND l.is_valid =l1.is_valid
      WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1) 
    )l
    LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
    ON l.originator_user_id = te.emp_id	
    LEFT JOIN ${dim_dbname}.dim_day_date tud
    ON l.start_date <= tud.days and l.end_date >= tud.days
    WHERE l.rn = 1
      AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
  )tmp1
  LEFT JOIN 
  (
    SELECT tud.days as stat_date,
           l.originator_user_id,
	  	   te.email,
		   CASE when (tud.days > l.start_date and tud.days < l.end_date and l.leave_type != '哺乳假')
                  or (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '下午')
                  or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '上午') 
                  or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '下午') then '全天请假'
                when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '上午')
                  or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '上午') then '上半天请假'
                when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '下午' and l.end_time_period = '下午')
                  or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '下午') then '下半天请假'
           end as day_type,
           l.leave_type
    FROM 
    (
      SELECT IF(l1.business_id is not null,l1.business_id,l.business_id) as business_id,
             l.originator_user_id,
             IF(l1.business_id is not null,l.start_date,l.start_date) as start_date,
             IF(l1.business_id is not null,l.start_time_period,l.start_time_period) as start_time_period,
             IF(l1.business_id is not null,l1.end_date,l.end_date) as end_date,
             IF(l1.business_id is not null,l1.end_time_period,l.end_time_period) as end_time_period,
             IF(l.leave_type != '哺乳假','正常请假','哺乳假') as leave_type,
             row_number()over(PARTITION by IF(l1.business_id is not null,l1.business_id,l.business_id) order by l.start_date asc)rn
      FROM ${dwd_dbname}.dwd_dtk_process_leave_info_df l
      LEFT JOIN ${dwd_dbname}.dwd_dtk_process_leave_info_df l1
      ON l.originator_user_id = l1.originator_user_id AND l.end_date = l1.start_date AND l.start_date != l.end_date AND l.d = l1.d AND l.process_result = l1.process_result AND l.process_status = l1.process_status AND l.is_valid =l1.is_valid
      WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1) 
    )l
    LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
    ON l.originator_user_id = te.emp_id	
    LEFT JOIN ${dim_dbname}.dim_day_date tud
    ON l.start_date <= tud.days and l.end_date >= tud.days
    WHERE l.rn = 1
      AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
  )tmp2
  ON tmp1.stat_date = tmp2.stat_date and tmp1.originator_user_id = tmp2.originator_user_id and tmp1.email = tmp2.email and tmp1.leave_type != tmp2.leave_type   
) t3
ON t3.email = tud.user_email AND t3.stat_date = tud.days
-- 加班数据
LEFT JOIN 
(
  SELECT tud.days as stat_date,
         l.applicant_userid,
		 te.email,
		 CASE when (tud.days > l.start_date and tud.days < l.end_date)
                or (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '下午')
                or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '上午') 
                or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '下午')  then '全天加班'
              when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '上午')
                or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '上午') then '上半天加班'
              when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '下午' and l.end_time_period = '下午')
                or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '下午') then '下半天加班'
         end as work_overtime_type
  FROM 
  (
    SELECT IF(l1.business_id is not null,l1.business_id,l.business_id) as business_id,
           l.applicant_userid,
           IF(l1.business_id is not null,l.start_date,l.start_date) as start_date,
           IF(l1.business_id is not null,l.start_time_period,l.start_time_period) as start_time_period,
           IF(l1.business_id is not null,l1.end_date,l.end_date) as end_date,
           IF(l1.business_id is not null,l1.end_time_period,l.end_time_period) as end_time_period,
           row_number()over(PARTITION by IF(l1.business_id is not null,l1.business_id,l.business_id) order by l.start_date asc)rn
    FROM ${dwd_dbname}.dwd_dtk_process_work_overtime_info_df l
    LEFT JOIN ${dwd_dbname}.dwd_dtk_process_work_overtime_info_df l1
    ON l.applicant_userid = l1.applicant_userid AND l.end_date = l1.start_date AND l.start_date != l.end_date AND l.d = l1.d AND l.approval_result = l1.approval_result AND l.approval_status = l1.approval_status AND l.is_valid =l1.is_valid
    WHERE l.is_valid = 1 AND l.approval_result = 'agree' AND l.approval_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1)
  )l
  LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
  ON l.applicant_userid = te.emp_id	
  LEFT JOIN ${dim_dbname}.dim_day_date tud
  on l.start_date <= tud.days and l.end_date >= tud.days
  WHERE l.rn = 1
    AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
) t13
ON t13.email = tud.user_email AND t13.stat_date = tud.days
)

INSERT overwrite table ${ads_dbname}.ads_project_profit_detail
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
       nvl(t14.contract_amount,0) as project_income, -- 项目收入
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
       nvl(t14.contract_amount,0) - (nvl(t7.agv_cost,0) + nvl(t8.bucket_cost,0) + nvl(t9.charging_cost,0) + nvl(t13.project_other_matters_cost,0) + nvl(t10.export_packing_cost,0) + nvl(t11.transportation_cost,0) + nvl(t12.ectocyst_software_cost,0) + nvl(t15.ectocyst_hardware_cost,0) + nvl(t3.pe_cost,0) + nvl(t18.mt_service_cost,0) + nvl(t5.io_service_cost,0) + nvl(t16.io_service_cost_ago,0) + nvl(t4.op_service_cost,0) + nvl(t17.op_service_cost_ago,0) + nvl(t19.te_cost,0) + nvl(t6.reimburse_amount,0) + nvl(t20.ctrip_car_cost,0) + nvl(t21.ctrip_flight_cost,0) + nvl(t22.ctrip_hotel_cost,0)) as project_gp, -- 项目毛利
       nvl((nvl(t14.contract_amount,0) - (nvl(t7.agv_cost,0) + nvl(t8.bucket_cost,0) + nvl(t9.charging_cost,0) + nvl(t13.project_other_matters_cost,0) + nvl(t10.export_packing_cost,0) + nvl(t11.transportation_cost,0) + nvl(t12.ectocyst_software_cost,0) + nvl(t15.ectocyst_hardware_cost,0) + nvl(t3.pe_cost,0) + nvl(t18.mt_service_cost,0) + nvl(t5.io_service_cost,0) + nvl(t16.io_service_cost_ago,0) + nvl(t4.op_service_cost,0) + nvl(t17.op_service_cost_ago,0) + nvl(t19.te_cost,0) + nvl(t6.reimburse_amount,0) + nvl(t20.ctrip_car_cost,0) + nvl(t21.ctrip_flight_cost,0) + nvl(t22.ctrip_hotel_cost,0))) / nvl(t14.contract_amount,0),0) as project_gp_rate, -- 项目毛利率
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM
-- 项目基础信息（FH和A合并）
(
  SELECT tt.true_project_code as project_code,
         tt.true_project_sale_code as project_sale_code,
         tt.project_name,
         tt.project_priority,
         tt.project_dispaly_state_group,
         tt.project_ft,
         tt.project_area,
         date_format(tt.online_process_approval_time,'yyyy-MM') as online_process_approval_time,
         date_format(tt.final_inspection_process_approval_time,'yyyy-MM') as final_inspection_process_approval_time,
         date_format(tt.post_project_date,'yyyy-MM') as post_project_date
  FROM 
  (
    SELECT b.project_code as true_project_code, -- 项目编码
           b.project_sale_code as true_project_sale_code, -- 售前编码
           b.project_name, -- 项目名称
           b.project_priority, -- 项目等级
           case when b.project_dispaly_state = '0.未开始' OR b.project_dispaly_state = '1.立项/启动阶段' OR b.project_dispaly_state = '2.需求确认/分解' OR b.project_dispaly_state = '3.设计开发/测试' then '需求确认/分解阶段'
                when b.project_dispaly_state = '4.采购/生产' OR b.project_dispaly_state = '5.发货/现场实施' then '发货阶段'
                when b.project_dispaly_state = '6.上线/初验/用户培训' then '上线实施阶段'
                when b.project_dispaly_state = '7.终验' then '验收阶段'
                when b.project_dispaly_state like '8.移交运维/转售后' then '售后移交阶段'
                when b.project_dispaly_state = '9.项目结项' then '项目结项'
                when b.project_dispaly_state = '10.项目暂停' then '项目暂停'
                when b.project_dispaly_state = '11.项目取消' then '项目取消'
                else NULL end as project_dispaly_state_group, -- 项目阶段组
           IF(nvl(b.project_attr_ft,'')='','未知',b.project_attr_ft) as project_ft, -- 大区/FT => <技术方案评审>ft
           IF(IF(b.project_code LIKE 'C%' AND b.project_type_id = 8 AND b.project_area_place is null,'销售',b.project_area_place) is null,'未知',IF(b.project_code LIKE 'C%' AND b.project_type_id = 8 AND b.project_area_place is null,'销售',b.project_area_place)) as project_area, -- 项目所在区域地点
           b.online_process_approval_time, -- 上线流程审批完成时间
           b.final_inspection_process_approval_time, -- 终验流程审批完成时间
           b.post_project_date, -- 项目结项时间
           h.end_time, -- 项目交接完成时间
           row_number()over(PARTITION by b2.project_sale_code order by b2.project_code,h.start_time desc)rn
    FROM ${dwd_dbname}.dwd_share_project_base_info_df b
    LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b2
    ON (b.project_code = b2.project_code or b.project_sale_code = b2.project_sale_code) AND b.d =b2.d 
    LEFT JOIN 
    (
      SELECT h.project_code,
             h.pre_sale_code,
             h.start_time,
             h.end_time,
             row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
      FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
      WHERE h.approval_staus = 30 
    )h
    ON b.project_code = h.project_code AND h.rn = 1
    WHERE b.d = DATE_ADD(CURRENT_DATE(), -1)   
      AND b.project_type_id IN (0,1,4,7,8,9) -- 只保留外部项目/公司外部项目/售前项目/硬件部项目/纯硬件项目/自营仓项目
      AND b.project_dispaly_state != '11.项目取消'
  )tt
  WHERE (tt.true_project_sale_code IS NULL OR tt.rn = 1)
)t1
-- AGV数量
LEFT JOIN
(
  SELECT a.true_project_code,
         SUM(a.actual_sale_num) as agv_num
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_sale_num,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
      FROM 
      (
        SELECT so.project_code,
               nvl(so.real_qty,0) - nvl(sr.real_qty,0) as actual_sale_num
        FROM 
        (
          SELECT so.project_code,
                 SUM(nvl(so.real_qty,0)) as real_qty
          FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
          LEFT JOIN ${dwd_dbname}.dwd_kde_sal_outstock_entry_info_df so
          ON m.material_id = so.material_id
          WHERE m.d = DATE_ADD(CURRENT_DATE(), -1) AND so.d = DATE_ADD(CURRENT_DATE(), -1) 
            AND m.paez_checkbox  = 1 -- 物料属性为agv
            AND m.document_status = 'C' -- 数据状态：完成
          GROUP BY so.project_code
        )so
        LEFT JOIN
        (
          SELECT sr.project_code,
                 SUM(nvl(sr.real_qty,0)) as real_qty
          FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
          LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
          ON m.material_id = sr.material_id
          WHERE m.d = DATE_ADD(CURRENT_DATE(), -1) AND sr.d = DATE_ADD(CURRENT_DATE(), -1) 
            AND m.paez_checkbox  = 1 -- 物料属性为agv
            AND m.document_status = 'C' -- 数据状态：完成
          GROUP BY sr.project_code
        )sr
        ON so.project_code = sr.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t2
ON t1.project_code = t2.true_project_code
-- PE费用
LEFT JOIN 
(
  SELECT a.true_project_code,
         SUM(a.pe_cost) as pe_cost
  FROM
  (
    SELECT tmp.*
    FROM 
    (
      SELECT tt.project_code,
             tt.pe_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tt.project_code order by h.start_time desc)rn
      FROM 
      (
        SELECT tmp.project_code,
               SUM(case when tmp.day_type = '工作日' or tmp.day_type = '节假日' or tmp.day_type = '调休' or tmp.day_type = '周末' then 1
                        when tmp.day_type = '上半天请假' or tmp.day_type = '下半天请假' then 0.5
                        when tmp.day_type = '全天请假' then 0 end) as pe_day,
               SUM(case when (tmp.day_type = '工作日' or tmp.day_type = '节假日' or tmp.day_type = '调休' or tmp.day_type = '周末') AND tmp.org_name_2 = '箱式FT' then 1*700
                        when (tmp.day_type = '工作日' or tmp.day_type = '节假日' or tmp.day_type = '调休' or tmp.day_type = '周末') AND tmp.org_name_2 = '营销中心' AND (tmp.org_name_3 = '华东大区' or tmp.org_name_3 = '华中大区' or tmp.org_name_3 = '华北大区' or tmp.org_name_3 = '华南大区' or  tmp.org_name_3 = '西南大区' or  tmp.org_name_3 = '福建子公司') then 1*700
                        when (tmp.day_type = '工作日' or tmp.day_type = '节假日' or tmp.day_type = '调休' or tmp.day_type = '周末') AND tmp.org_name_2 = '项目部' then 1*500
                        when (tmp.day_type = '工作日' or tmp.day_type = '节假日' or tmp.day_type = '调休' or tmp.day_type = '周末') AND tmp.org_name_2 = '海外事业部' then 1*1500
                        when (tmp.day_type = '上半天请假' or tmp.day_type = '下半天请假') AND tmp.org_name_2 = '箱式FT' then 0.5*700
                        when (tmp.day_type = '上半天请假' or tmp.day_type = '下半天请假') AND tmp.org_name_2 = '营销中心' AND (tmp.org_name_3 = '华东大区' or tmp.org_name_3 = '华中大区' or tmp.org_name_3 = '华北大区' or tmp.org_name_3 = '华南大区' or  tmp.org_name_3 = '西南大区' or  tmp.org_name_3 = '福建子公司') then 0.5*700
                        when (tmp.day_type = '上半天请假' or tmp.day_type = '下半天请假') AND tmp.org_name_2 = '项目部' then 0.5*500
                        when (tmp.day_type = '上半天请假' or tmp.day_type = '下半天请假') AND tmp.org_name_2 = '海外事业部' then 0.5*1500
                        when tmp.day_type = '全天请假' then 0 end) as pe_cost
        FROM 
        (
          SELECT tud.org_name_2,
                 tud.org_name_3,
                 tud.emp_name, -- 人员名称
                 tud.emp_position,
                 tud.is_job, -- 是否在职
                 tud.days, -- 日期
                 IF(t12.leave_type is not null,t12.leave_type,tud.day_type) as day_type, -- 日期类型
                 IF(t1.log_date is not null or t12.leave_type is not null,'已打卡','未打卡') as ischeck, -- 是否打卡
                 t1.work_status, -- 出勤状态
                 t1.job_content, -- 工作内容
                 t1.log_date, -- 日志日期
                 t1.project_code, -- 项目编码
                 t1.project_name, -- 项目名称
                 t1.project_manage, -- 项目经理
                 t1.business_id, -- 审批编号
                 t1.create_time, -- 创建时间
                 t1.originator_user_name, -- 发起人
                 t1.approval_status, -- 审批状态
                 CASE when t1.log_date is not null and t1.work_status IN ('出差/On business trip','远程支持/Remote support') and i.project_code is null then '编码不存在' end as unusual_res -- 异常原因
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
                  WHERE m.org_company_name = '上海快仓智能科技有限公司' AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND m.is_valid = 1 AND m.org_end_date = IF(m.is_job = 1,'9999-01-01',m.org_end_date)
                )tmp
                ON te.emp_id = tmp.emp_id
                LEFT JOIN ${dim_dbname}.dim_dtk_org_history_info_df tg 
                ON tg.org_id = tmp.org_id AND tg.d = IF(tmp.org_end_date = '9999-01-01',DATE_ADD(CURRENT_DATE(), -1),IF(tmp.is_job = 0 ,DATE_ADD(tmp.org_end_date, -1),tmp.org_end_date))
                WHERE 1 = 1
                  AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司' AND te.is_active = 1
                  -- 筛选PE全部人员
                  AND ((split(tg.org_path_name,'/')[1] IN ('营销中心','箱式FT') AND te.emp_position IN ('项目工程师','实施工程师','项目实施工程师','项目实施','华北项目实施','实施调试工程师','实习生'))
                    OR (split(tg.org_path_name,'/')[1] IN ('项目部') AND split(tg.org_path_name,'/')[2] IN ('箱式交付组','项目交付组') AND te.emp_position IN ('项目工程师','实施工程师','项目实施工程师','项目实施','华北项目实施','实施调试工程师','实习生'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('欧洲分公司') AND split(tg.org_path_name,'/')[3] IN ('项目交付及运营组') AND te.emp_position IN ('海外交付工程师','PE','实习生','项目工程师'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('北美分公司') AND split(tg.org_path_name,'/')[3] IN ('项目交付及运营组') AND te.emp_position IN ('PE','实习生'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('东南亚及中国台湾区域') AND split(tg.org_path_name,'/')[3] IN ('项目交付及运营组') AND te.emp_position IN ('PE','项目工程师'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('韩国子公司') AND te.emp_position IN ('项目工程师'))
                    OR (split(tg.org_path_name,'/')[1] IN ('海外事业部') AND split(tg.org_path_name,'/')[2] IN ('日本分公司') AND split(tg.org_path_name,'/')[3] IN ('项目交付组') AND te.emp_position IN ('维保技术员','项目工程师')))
              )tmp
              WHERE tmp.rn =1
            )tu  
            LEFT JOIN
            (
              SELECT DISTINCT days,
                              day_type
              FROM ${dim_dbname}.dim_day_date
              WHERE 1 = 1 AND days >= '2021-07-01' AND days <= DATE_ADD(CURRENT_DATE(), -1)
            )td
            ON td.days >= tu.hired_date AND td.days <= IF(tu.quit_date is NULL,DATE_ADD(CURRENT_DATE(), -1),tu.quit_date)
          )tud
          LEFT JOIN 
          (
            select *
            FROM ${dwd_dbname}.dwd_dtk_process_pe_log_info_df p
            WHERE d = DATE_ADD(CURRENT_DATE(), -1) AND p.approval_status != 'TERMINATED' AND p.create_time >= '2021-07-01 00:00:00'-- 审批状态剔除终止，起始时间从2021-07-01开始
          )t1
          ON t1.originator_user_id = tud.emp_id AND tud.days = t1.log_date 
          LEFT JOIN ${dwd_dbname}.dwd_bpm_project_info_ful i 
          ON t1.project_code = i.project_code
          LEFT JOIN 
          (
            SELECT tud.days as stat_date,
                   l.originator_user_id,
		           te.emp_id,
		           CASE when (tud.days > l.start_date and tud.days < l.end_date)
                          or (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '下午')
                          or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '上午') 
                          or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '下午')  then '全天请假'
                        when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '上午' and l.end_time_period = '上午')
                          or (tud.days != l.start_date and tud.days = l.end_date and l.end_time_period = '上午') then '上半天请假'
                        when (tud.days = l.start_date and tud.days = l.end_date and l.start_time_period = '下午' and l.end_time_period = '下午')
                          or (tud.days = l.start_date and tud.days != l.end_date and l.start_time_period = '下午') then '下半天请假'
                   end as leave_type
            FROM 
            (
              SELECT IF(l1.business_id is not null,l1.business_id,l.business_id) as business_id,
                     l.originator_user_id,
                     IF(l1.business_id is not null,l.start_date,l.start_date) as start_date,
                     IF(l1.business_id is not null,l.start_time_period,l.start_time_period) as start_time_period,
                     IF(l1.business_id is not null,l1.end_date,l.end_date) as end_date,
                     IF(l1.business_id is not null,l1.end_time_period,l.end_time_period) as end_time_period,
                     IF(l.leave_type != '哺乳假','正常请假','哺乳假') as leave_type,
                     row_number()over(PARTITION by IF(l1.business_id is not null,l1.business_id,l.business_id) order by l.start_date asc)rn
              FROM ${dwd_dbname}.dwd_dtk_process_leave_info_df l
              LEFT JOIN ${dwd_dbname}.dwd_dtk_process_leave_info_df l1
              ON l.originator_user_id = l1.originator_user_id AND l.end_date = l1.start_date AND l.start_date != l.end_date AND l.d = l1.d AND l.process_result = l1.process_result AND l.process_status = l1.process_status AND l.is_valid =l1.is_valid
              WHERE l.is_valid = 1 AND l.process_result = 'agree' AND l.process_status = 'COMPLETED' AND l.d = DATE_ADD(CURRENT_DATE(), -1) 
            )l
            LEFT JOIN ${dwd_dbname}.dwd_dtk_emp_info_df te
            ON l.originator_user_id = te.emp_id	
            LEFT JOIN ${dim_dbname}.dim_day_date tud
            on l.start_date <= tud.days and l.end_date >= tud.days
            WHERE l.rn = 1
            AND te.d = DATE_ADD(CURRENT_DATE(), -1) AND te.org_company_name = '上海快仓智能科技有限公司'
          ) t12 
          ON t12.emp_id = tud.emp_id AND t12.stat_date = tud.days
        )tmp
        WHERE tmp.project_code is not NULL 
        GROUP BY tmp.project_code
      )tt
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tt.project_code or s.project_sale_code = tt.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tmp
    WHERE tmp.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t3
ON t1.project_code = t3.true_project_code
-- 外包劳务-运维费用 2022年之后的数据
LEFT JOIN
(
  SELECT a.true_project_code,
         SUM(a.service_cost) as op_service_cost
  FROM
  (
    SELECT tmp.*
    FROM 
    (   
      SELECT tt.project_code,
             tt.service_type,
             tt.service_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tt.project_code order by h.start_time desc)rn
      FROM 
      (
        SELECT tmp.project_code,
        	   tmp.service_type,
         	   CONCAT(SUM(substring_index(check_duration_day,'天',1)),'天',SUM(IF(substring_index(substring_index(check_duration_day,'天',-1),'小时',1) ='',0,substring_index(substring_index(check_duration_day,'天',-1),'小时',1))),'小时') as check_duration_day,
               SUM(tmp.service_cost) as service_cost
      	FROM 
        (
          SELECT tt1.cur_date,
                 tt1.project_code,
             	 tt1.project_name,
                 tt1.project_ft,
               	 tt1.project_operation_state,
               	 tt1.originator_dept_name as team_name,
               	 tt1.originator_user_name as member_name,
                 tt1.service_type,
                 SUM(tt1.check_duration) as check_duration_hour,
                 case when SUM(tt1.check_duration) < 4 then '0天'
                      when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then '0.5天'
                      when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then '1天'
                      when SUM(tt1.check_duration) > 10 then CONCAT('1天',(SUM(tt1.check_duration) - 10),'小时') END as check_duration_day,
                 case when SUM(tt1.check_duration) < 4 then 0
                      when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then 350
                      when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then 550
                      when SUM(tt1.check_duration) > 10 then 550 + (SUM(tt1.check_duration) - 10)*2*20 END as service_cost
          FROM 
          (
            SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
                   a.business_id, -- 审批编号
                   a.project_code, -- 项目编号
                   b.project_name, -- 项目名称
                   IF(b.project_attr_ft is null,'未知',b.project_attr_ft) as project_ft, -- 所属产品线
                   b.project_operation_state, -- 项目运营阶段
                   a.originator_dept_name, -- 团队名称
                   IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
                   case when a.service_type = '实施劳务' then '实施劳务'
                        when a.service_type = '运维（陪产）劳务' OR a.service_type is null then '运维劳务'
                        end as service_type, -- 劳务类型
                   IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
                   a.checkin_time, -- 考勤签到时间
                   a.checkout_time, -- 考勤签退时间
                   row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
            FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
          	LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
           	ON a.project_code = b.project_code
            WHERE a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准,只取2022年之后的数据
              AND b.d = DATE_ADD(CURRENT_DATE(), -1)
          )tt1
          LEFT JOIN 
          (
        	SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
                   a.business_id, -- 审批编号
                   a.project_code, -- 项目编号
                   b.project_name, -- 项目名称
                   IF(b.project_attr_ft is null,'未知',b.project_attr_ft) as project_ft, -- 所属产品线
                   b.project_operation_state, -- 项目运营阶段
                   a.originator_dept_name, -- 团队名称
                   IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
                   case when a.service_type = '实施劳务' then '实施劳务'
                        when a.service_type = '运维（陪产）劳务' OR a.service_type is null then '运维劳务'
                        end as service_type, -- 劳务类型
                   IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
                   a.checkin_time, -- 考勤签到时间
                   a.checkout_time, -- 考勤签退时间
                   row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
        	FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
            LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
            ON a.project_code = b.project_code
            WHERE a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准，只取2022年之后的数据
              AND b.d = DATE_ADD(CURRENT_DATE(), -1)
          )tt2
          ON tt1.cur_date = tt2.cur_date AND tt1.project_code = tt2.project_code AND tt1.originator_user_name = tt2.originator_user_name AND tt1.rn = tt2.rn + 1
          WHERE tt2.rn is null or tt1.checkin_time not BETWEEN tt2.checkin_time and tt2.checkout_time
          GROUP BY tt1.cur_date,tt1.project_code,tt1.project_name,tt1.project_ft,tt1.project_operation_state,tt1.originator_dept_name,tt1.originator_user_name,tt1.service_type
      	)tmp
        WHERE tmp.service_type = '运维劳务'
        GROUP BY tmp.project_code,tmp.service_type
      )tt
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tt.project_code or s.project_sale_code = tt.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tmp
    WHERE tmp.rn = 1
  )a 
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t4
ON t1.project_code = t4.true_project_code
-- 外包劳务-实施费用 2022年之后的数据
LEFT JOIN
(
  SELECT a.true_project_code,
         SUM(a.service_cost) as io_service_cost
  FROM
  (
    SELECT tmp.*
    FROM 
    (   
      SELECT tt.project_code,
             tt.service_type,
             tt.service_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tt.project_code order by h.start_time desc)rn
      FROM 
      (
        SELECT tmp.project_code,
        	   tmp.service_type,
         	   CONCAT(SUM(substring_index(check_duration_day,'天',1)),'天',SUM(IF(substring_index(substring_index(check_duration_day,'天',-1),'小时',1) ='',0,substring_index(substring_index(check_duration_day,'天',-1),'小时',1))),'小时') as check_duration_day,
               SUM(tmp.service_cost) as service_cost
      	FROM 
        (
          SELECT tt1.cur_date,
                 tt1.project_code,
             	 tt1.project_name,
                 tt1.project_ft,
               	 tt1.project_operation_state,
               	 tt1.originator_dept_name as team_name,
               	 tt1.originator_user_name as member_name,
                 tt1.service_type,
                 SUM(tt1.check_duration) as check_duration_hour,
                 case when SUM(tt1.check_duration) < 4 then '0天'
                      when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then '0.5天'
                      when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then '1天'
                      when SUM(tt1.check_duration) > 10 then CONCAT('1天',(SUM(tt1.check_duration) - 10),'小时') END as check_duration_day,
                 case when SUM(tt1.check_duration) < 4 then 0
                      when SUM(tt1.check_duration) >= 4 and SUM(tt1.check_duration) < 8 then 350
                      when SUM(tt1.check_duration) >= 8 and SUM(tt1.check_duration) <= 10 then 550
                      when SUM(tt1.check_duration) > 10 then 550 + (SUM(tt1.check_duration) - 10)*2*20 END as service_cost
          FROM 
          (
            SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
                   a.business_id, -- 审批编号
                   a.project_code, -- 项目编号
                   b.project_name, -- 项目名称
                   IF(b.project_attr_ft is null,'未知',b.project_attr_ft) as project_ft, -- 所属产品线
                   b.project_operation_state, -- 项目运营阶段
                   a.originator_dept_name, -- 团队名称
                   IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
                   case when a.service_type = '实施劳务' then '实施劳务'
                        when a.service_type = '运维（陪产）劳务' OR a.service_type is null then '运维劳务'
                        end as service_type, -- 劳务类型
                   IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
                   a.checkin_time, -- 考勤签到时间
                   a.checkout_time, -- 考勤签退时间
                   row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
            FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
          	LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
           	ON a.project_code = b.project_code
            WHERE a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准，只取2022年之后的数据
              AND b.d = DATE_ADD(CURRENT_DATE(), -1)
          )tt1
          LEFT JOIN 
          (
        	SELECT DATE(a.checkin_time) as cur_date, -- 统计时间
                   a.business_id, -- 审批编号
                   a.project_code, -- 项目编号
                   b.project_name, -- 项目名称
                   IF(b.project_attr_ft is null,'未知',b.project_attr_ft) as project_ft, -- 所属产品线
                   b.project_operation_state, -- 项目运营阶段
                   a.originator_dept_name, -- 团队名称
                   IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) as originator_user_name, -- 成员名称
                   case when a.service_type = '实施劳务' then '实施劳务'
                        when a.service_type = '运维（陪产）劳务' OR a.service_type is null then '运维劳务'
                        end as service_type, -- 劳务类型
                   IF(SUBSTR(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[1],1,1)<5,CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','0'),CONCAT(split(((unix_timestamp(a.checkout_time) - unix_timestamp(a.checkin_time))/3600),'\\\\.')[0],'\.','5')) as check_duration, -- 考勤时长（小时）,
                   a.checkin_time, -- 考勤签到时间
                   a.checkout_time, -- 考勤签退时间
                   row_number()over(PARTITION by DATE(a.checkin_time),a.project_code,a.originator_dept_name,IF(a.originator_user_name is null,SUBSTRING_INDEX(a.approval_title,'提',1),a.originator_user_name) order by a.checkin_time)rn
        	FROM ${dwd_dbname}.dwd_dtk_implementers_attendamce_di a
            LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df b
            ON a.project_code = b.project_code
            WHERE a.project_code NOT LIKE '%S%' AND a.approval_status = 'COMPLETED' AND a.approval_result = 'agree' AND a.is_project_matching = '1' AND a.create_time >= '2022-01-01 00:00:00' -- 剔除S开头项目编号的售后劳务,审批状态:已结束,审批结果:已通过,项目以<有效匹配即1>的为准，只取2022年之后的数据
              AND b.d = DATE_ADD(CURRENT_DATE(), -1)
          )tt2
          ON tt1.cur_date = tt2.cur_date AND tt1.project_code = tt2.project_code AND tt1.originator_user_name = tt2.originator_user_name AND tt1.rn = tt2.rn + 1
          WHERE tt2.rn is null or tt1.checkin_time not BETWEEN tt2.checkin_time and tt2.checkout_time
          GROUP BY tt1.cur_date,tt1.project_code,tt1.project_name,tt1.project_ft,tt1.project_operation_state,tt1.originator_dept_name,tt1.originator_user_name,tt1.service_type
      	)tmp
        WHERE tmp.service_type = '实施劳务'
        GROUP BY tmp.project_code,tmp.service_type
      )tt
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tt.project_code or s.project_sale_code = tt.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tmp
    WHERE tmp.rn = 1
  )a 
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t5
ON t1.project_code = t5.true_project_code
-- 个人报销
LEFT JOIN 
(
  SELECT a.true_project_code,
         SUM(a.reimburse_amount) as reimburse_amount
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.reimburse_amount,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
      FROM 
      (
        SELECT p.project_code,
               SUM(p.reimburse_amount) as reimburse_amount 
        FROM
        (
          SELECT DISTINCT p.flow_id,
                          tbl.project_code,
                          tbl.reimburse_amount
          FROM dwd.dwd_bpm_personal_expense_account_info_ful p
          lateral view explode(split(p.row_project_codes,',')) tbl as project_code
          lateral view explode(split(p.row_reimburse_amounts,',')) tbl as reimburse_amount
          WHERE p.approve_status = 30 AND p.currency_code = 'PRE001' -- 取货币类型为人民币的
        )p
        GROUP BY p.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t6
ON t1.project_code = t6.true_project_code
-- agv费用
LEFT JOIN 
(
  SELECT a.true_project_code,
         SUM(a.actual_cost) as agv_cost
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
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
          ON m.material_id = so.material_id
          LEFT JOIN 
          (
            SELECT b.material_id, -- 物料内码
                   SUM(b.end_period_number) as end_period_number, -- 期末数量
                   SUM(b.end_period_amount) as end_period_amount, -- 期末金额
                   nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- 物料单个成本价
            FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
            WHERE b.check_year = year(DATE_ADD(CURRENT_DATE(), -1)) AND b.check_period = MONTH(DATE_ADD(CURRENT_DATE(), -1)) - 1
            GROUP BY b.material_id
          )b
          ON m.material_id = b.material_id
          WHERE m.d = DATE_ADD(CURRENT_DATE(), -1) AND so.d = DATE_ADD(CURRENT_DATE(), -1) 
            AND m.paez_checkbox  = 1 -- 物料属性为agv
            AND m.document_status = 'C' -- 数据状态：完成
          GROUP BY so.project_code
        )so
        LEFT JOIN
        (
          SELECT sr.project_code,
                 SUM(IF(sr.finance_cost_amount_lc != 0,sr.finance_cost_amount_lc,(nvl(b.price_amount,0) * sr.real_qty))) as finance_cost_amount_lc
          FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
          LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
          ON m.material_id = sr.material_id
          LEFT JOIN 
          (
            SELECT b.material_id, -- 物料内码
                   SUM(b.end_period_number) as end_period_number, -- 期末数量
                   SUM(b.end_period_amount) as end_period_amount, -- 期末金额
                   nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- 物料单个成本价
            FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
            WHERE b.check_year = year(DATE_ADD(CURRENT_DATE(), -1)) AND b.check_period = MONTH(DATE_ADD(CURRENT_DATE(), -1)) - 1
            GROUP BY b.material_id
          )b
          ON m.material_id = b.material_id
          WHERE m.d = DATE_ADD(CURRENT_DATE(), -1) AND sr.d = DATE_ADD(CURRENT_DATE(), -1) 
            AND m.paez_checkbox  = 1 -- 物料属性为agv
            AND m.document_status = 'C' -- 数据状态：完成
          GROUP BY sr.project_code
        )sr
        ON so.project_code = sr.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t7
ON t1.project_code = t7.true_project_code
-- 货架费用
LEFT JOIN 
(
  SELECT a.true_project_code,
         SUM(a.actual_cost) as bucket_cost
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
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
          ON m.material_id = so.material_id
          LEFT JOIN 
          (
            SELECT b.material_id, -- 物料内码
                   SUM(b.end_period_number) as end_period_number, -- 期末数量
                   SUM(b.end_period_amount) as end_period_amount, -- 期末金额
                   nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- 物料单个成本价
            FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
            WHERE b.check_year = year(DATE_ADD(CURRENT_DATE(), -1)) AND b.check_period = MONTH(DATE_ADD(CURRENT_DATE(), -1)) - 1
            GROUP BY b.material_id
          )b
          ON m.material_id = b.material_id
          WHERE m.d = DATE_ADD(CURRENT_DATE(), -1) AND so.d = DATE_ADD(CURRENT_DATE(), -1) 
            AND m.material_number like 'RT04%' -- 货架
          GROUP BY so.project_code
        )so
        LEFT JOIN
        (
          SELECT sr.project_code,
                 SUM(IF(sr.finance_cost_amount_lc != 0,sr.finance_cost_amount_lc,(nvl(b.price_amount,0) * sr.real_qty))) as finance_cost_amount_lc
          FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
          LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
          ON m.material_id = sr.material_id
          LEFT JOIN 
          (
            SELECT b.material_id, -- 物料内码
                   SUM(b.end_period_number) as end_period_number, -- 期末数量
                   SUM(b.end_period_amount) as end_period_amount, -- 期末金额
                   nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- 物料单个成本价
            FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
            WHERE b.check_year = year(DATE_ADD(CURRENT_DATE(), -1)) AND b.check_period = MONTH(DATE_ADD(CURRENT_DATE(), -1)) - 1
            GROUP BY b.material_id
          )b
          ON m.material_id = b.material_id
          WHERE m.d = DATE_ADD(CURRENT_DATE(), -1) AND sr.d = DATE_ADD(CURRENT_DATE(), -1) 
            AND m.material_number like 'RT04%' -- 货架
          GROUP BY sr.project_code
        )sr
        ON so.project_code = sr.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t8
ON t1.project_code = t8.true_project_code
-- 充电桩费用
LEFT JOIN
(
  SELECT a.true_project_code,
         SUM(a.actual_cost) as charging_cost
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
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
          ON m.material_id = so.material_id
          LEFT JOIN 
          (
            SELECT b.material_id, -- 物料内码
                   SUM(b.end_period_number) as end_period_number, -- 期末数量
                   SUM(b.end_period_amount) as end_period_amount, -- 期末金额
                   nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- 物料单个成本价
            FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
            WHERE b.check_year = year(DATE_ADD(CURRENT_DATE(), -1)) AND b.check_period = MONTH(DATE_ADD(CURRENT_DATE(), -1)) - 1
            GROUP BY b.material_id
          )b
          ON m.material_id = b.material_id
          WHERE m.d = DATE_ADD(CURRENT_DATE(), -1) AND so.d = DATE_ADD(CURRENT_DATE(), -1) 
            AND m.material_number like 'RT03%' -- 充电桩
          GROUP BY so.project_code
        )so
        LEFT JOIN
        (
          SELECT sr.project_code,
                 SUM(IF(sr.finance_cost_amount_lc != 0,sr.finance_cost_amount_lc,(nvl(b.price_amount,0) * sr.real_qty))) as finance_cost_amount_lc
          FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
          LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
          ON m.material_id = sr.material_id
          LEFT JOIN 
          (
            SELECT b.material_id, -- 物料内码
                   SUM(b.end_period_number) as end_period_number, -- 期末数量
                   SUM(b.end_period_amount) as end_period_amount, -- 期末金额
                   nvl(CAST(SUM(b.end_period_amount)/SUM(b.end_period_number) as decimal(10,2)),0) as price_amount -- 物料单个成本价
            FROM ${dwd_dbname}.dwd_kde_material_final_balance_info_ful b 
            WHERE b.check_year = year(DATE_ADD(CURRENT_DATE(), -1)) AND b.check_period = MONTH(DATE_ADD(CURRENT_DATE(), -1)) - 1
            GROUP BY b.material_id
          )b
          ON m.material_id = b.material_id
          WHERE m.d = DATE_ADD(CURRENT_DATE(), -1) AND sr.d = DATE_ADD(CURRENT_DATE(), -1) 
            AND m.material_number like 'RT03%' -- 充电桩
          GROUP BY sr.project_code
        )sr
        ON so.project_code = sr.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t9
ON t1.project_code = t9.true_project_code
-- 出口包装费
LEFT JOIN
(  
  SELECT a.true_project_code,
         SUM(a.actual_cost) as export_packing_cost
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
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
          ON g.id = m.material_group
          LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
          ON m.material_id = po.material_id
          WHERE g.materia_number ='P' -- 包装
            AND m.document_status = 'C' -- 数据状态：完成
            AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND po.d = DATE_ADD(CURRENT_DATE(), -1) 
          GROUP BY po.project_code
        )po
        LEFT JOIN
        (
          SELECT pm.project_code,
                 SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
          FROM ${dim_dbname}.dim_kde_bd_material_group_info_ful g
          LEFT JOIN ${dwd_dbname}.dwd_kde_bd_material_info_df m
          ON g.id = m.material_group
          LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
          ON m.material_id = pm.material_id
          WHERE g.materia_number ='P' -- 包装
            AND m.document_status = 'C' -- 数据状态：完成
            AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND pm.d = DATE_ADD(CURRENT_DATE(), -1) 
          GROUP BY pm.project_code
        )pm
        ON po.project_code = pm.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t10
ON t1.project_code = t10.true_project_code
-- 运输费
LEFT JOIN 
(  
  SELECT a.true_project_code,
         SUM(a.actual_cost) as transportation_cost
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
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
          ON m.material_id = po.material_id
          WHERE (m.material_number like 'R6S90077%' or m.material_number like 'R6S90078%') -- 国际物流费、国内物流费
            AND m.document_status = 'C' -- 数据状态：完成
            AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND po.d = DATE_ADD(CURRENT_DATE(), -1) 
          GROUP BY po.project_code
        )po
        LEFT JOIN
        (
          SELECT pm.project_code,
                 SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
          FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
          LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
          ON m.material_id = pm.material_id
          WHERE (m.material_number like 'R6S90077%' or m.material_number like 'R6S90078%') -- 国际物流费、国内物流费
            AND m.document_status = 'C' -- 数据状态：完成
            AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND pm.d = DATE_ADD(CURRENT_DATE(), -1) 
          GROUP BY pm.project_code
        )pm
        ON po.project_code = pm.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t11
ON t1.project_code = t11.true_project_code
-- 外包软件
LEFT JOIN 
(  
  SELECT a.true_project_code,
         SUM(a.actual_cost) as ectocyst_software_cost
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
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
          ON m.material_id = po.material_id
          WHERE m.material_number in ('S99000046K010','S99L04660K010') -- 外包软件
            AND m.document_status = 'C' -- 数据状态：完成
            AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND po.d = DATE_ADD(CURRENT_DATE(), -1) 
          GROUP BY po.project_code
        )po
        LEFT JOIN
        (
          SELECT pm.project_code,
                 SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
          FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
          LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
          ON m.material_id = pm.material_id
          WHERE m.material_number in ('S99000046K010','S99L04660K010') -- 外包软件
            AND m.document_status = 'C' -- 数据状态：完成
            AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND pm.d = DATE_ADD(CURRENT_DATE(), -1) 
          GROUP BY pm.project_code
        )pm
        ON po.project_code = pm.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t12
ON t1.project_code = t12.true_project_code
-- 其他物料费用
LEFT JOIN
(
  SELECT a.true_project_code,
         SUM(a.actual_cost) as project_other_matters_cost
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
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
          ON m.material_id = so.material_id
          WHERE m.d = DATE_ADD(CURRENT_DATE(), -1) AND so.d = DATE_ADD(CURRENT_DATE(), -1) 
            AND m.paez_checkbox != 1 -- 物料属性不为agv 
            AND m.material_number not like 'RT04%' -- 排除货架
            AND m.material_number not like 'RT03%' -- 排除充电桩
            AND m.material_group != '111370' -- 排除物料分组为P（包装）
            AND m.material_group != '111373' -- 排除物料分组为S（软件）
            AND m.material_number not like 'R5S%'
            AND m.material_number not like 'R6S%'
            AND m.material_number not in ('S99000046K010','S99L04660K010','S99L00587K010','S99L00588K010','S99L04951K010') -- 特殊物料
            AND m.document_status = 'C' -- 数据状态已完成
          GROUP BY so.project_code
        )so
        LEFT JOIN
        (
          SELECT sr.project_code,
                 SUM(nvl(sr.finance_cost_amount_lc,0)) as finance_cost_amount_lc
          FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
          LEFT JOIN ${dwd_dbname}.dwd_kde_sal_returnstock_entry_info_df sr
          ON m.material_id = sr.material_id
          WHERE m.d = DATE_ADD(CURRENT_DATE(), -1) AND sr.d = DATE_ADD(CURRENT_DATE(), -1) 
            AND m.paez_checkbox != 1 -- 物料属性不为agv 
            AND m.material_number not like 'RT04%' -- 排除货架
            AND m.material_number not like 'RT03%' -- 排除充电桩
            AND m.material_group != '111370' -- 排除物料分组为P（包装）
            AND m.material_group != '111373' -- 排除物料分组为S（软件）
            AND m.material_number not like 'R5S%'
            AND m.material_number not like 'R6S%'
            AND m.material_number not in ('S99000046K010','S99L04660K010','S99L00587K010','S99L00588K010','S99L04951K010') -- 特殊物料
            AND m.document_status = 'C' -- 数据状态已完成
          GROUP BY sr.project_code
        )sr
        ON so.project_code = sr.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t13
ON t1.project_code = t13.true_project_code
-- 项目收入
LEFT JOIN
(
  SELECT a.project_code,
         a.contract_amount
  FROM ${dwd_dbname}.dwd_bpm_contract_amount_offline_info_ful a 
)t14
ON t1.project_code = t14.project_code
-- 外包硬件
LEFT JOIN 
(  
  SELECT a.true_project_code,
         SUM(a.actual_cost) as ectocyst_hardware_cost
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
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
          ON g.id = m.material_group
          LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_entry_info_df po
          ON m.material_id = po.material_id
          WHERE ((g.materia_number ='S' AND m.material_number not in ('S99L00587K010','S99L00588K010','S99L04951K010')) OR m.material_number = 'R5S90518')
            AND m.document_status = 'C' -- 数据状态：完成
            AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND po.d = DATE_ADD(CURRENT_DATE(), -1) 
          GROUP BY po.project_code
        )po
        LEFT JOIN
        (
          SELECT pm.project_code,
                 SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
          FROM ${dim_dbname}.dim_kde_bd_material_group_info_ful g
          LEFT JOIN ${dwd_dbname}.dwd_kde_bd_material_info_df m
          ON g.id = m.material_group
          LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
          ON m.material_id = pm.material_id
          WHERE ((g.materia_number ='S' AND m.material_number not in ('S99L00587K010','S99L00588K010','S99L04951K010')) OR m.material_number = 'R5S90518' )
            AND m.document_status = 'C' -- 数据状态：完成
            AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND pm.d = DATE_ADD(CURRENT_DATE(), -1) 
          GROUP BY pm.project_code
        )pm
        ON po.project_code = pm.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t15
ON t1.project_code = t15.true_project_code
-- 外包劳务-实施费用 2022年之前的数据
LEFT JOIN
(  
  SELECT a.true_project_code,
         SUM(a.actual_cost) as io_service_cost_ago
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
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
          ON m.material_id = po.material_id
          LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_info_df p
          ON po.id = p.id
          WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- 劳务类型数据
            AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- 不属于劳务类型数据
            AND m.material_number not in ('R5S90044','R5S90046','R5S90534') -- 运维劳务
            AND m.document_status = 'C' -- 数据状态：完成
            AND p.bill_date <= '2021-12-31'
            AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND po.d = DATE_ADD(CURRENT_DATE(), -1) AND p.d = DATE_ADD(CURRENT_DATE(), -1) 
          GROUP BY po.project_code
        )po
        LEFT JOIN
        (
          SELECT pm.project_code,
                 SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
          FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
          LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
          ON m.material_id = pm.material_id
          LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_info_df p
          ON pm.id = p.id
          WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- 劳务类型数据
            AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- 不属于劳务类型数据
            AND m.material_number not in ('R5S90044','R5S90046','R5S90534') -- 运维劳务
            AND p.bill_date <= '2021-12-31'
            AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND pm.d = DATE_ADD(CURRENT_DATE(), -1) AND p.d = DATE_ADD(CURRENT_DATE(), -1) 
          GROUP BY pm.project_code
        )pm
        ON po.project_code = pm.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t16
ON t1.project_code = t16.true_project_code
-- 外包劳务-运维费用 2022年之前的数据
LEFT JOIN
(  
  SELECT a.true_project_code,
         SUM(a.actual_cost) as op_service_cost_ago
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
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
          ON m.material_id = po.material_id
          LEFT JOIN ${dwd_dbname}.dwd_kde_pur_poorder_info_df p
          ON po.id = p.id
          WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- 劳务类型数据
            AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- 不属于劳务类型数据
            AND m.material_number in ('R5S90044','R5S90046','R5S90534') -- 运维劳务
            AND m.document_status = 'C' -- 数据状态：完成
            AND p.bill_date <= '2021-12-31'
            AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND po.d = DATE_ADD(CURRENT_DATE(), -1) AND p.d = DATE_ADD(CURRENT_DATE(), -1) 
          GROUP BY po.project_code
        )po
        LEFT JOIN
        (
          SELECT pm.project_code,
                 SUM(nvl(pm.finance_cost_amount_lc,0)) as finance_cost_amount_lc
          FROM ${dwd_dbname}.dwd_kde_bd_material_info_df m
          LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_entry_info_df pm
          ON m.material_id = pm.material_id
          LEFT JOIN ${dwd_dbname}.dwd_kde_pur_mrb_info_df p
          ON pm.id = p.id
          WHERE (m.material_number like 'R5S%' OR m.material_number like 'R6S%') -- 劳务类型数据
            AND m.material_number not in ('R5S90518','R5S90527','R6S90077','R6S90078') -- 不属于劳务类型数据
            AND m.material_number in ('R5S90044','R5S90046','R5S90534') -- 运维劳务
            AND m.document_status = 'C' -- 数据状态：完成
            AND p.bill_date <= '2021-12-31'
            AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND pm.d = DATE_ADD(CURRENT_DATE(), -1) AND p.d = DATE_ADD(CURRENT_DATE(), -1) 
          GROUP BY pm.project_code
        )pm
        ON po.project_code = pm.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t17
ON t1.project_code = t17.true_project_code
-- 维保费用
LEFT JOIN
(  
  SELECT a.true_project_code,
         SUM(a.actual_cost) as mt_service_cost
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
      FROM 
      (
        SELECT m.project_code,
               SUM(m.actual_cost) as actual_cost
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
            WHERE m.org_name = '宝仓' AND m.project_code is not null AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND m.approval_result = 'agree' AND m.approval_status = 'COMPLETED'
            GROUP BY m.log_date,m.originator_user_name
          )tmp
          ON tmp.originator_user_name = m.originator_user_name AND tmp.log_date = m.log_date
          WHERE m.org_name = '宝仓' AND m.project_code is not null AND m.d = DATE_ADD(CURRENT_DATE(), -1) AND m.approval_result = 'agree' AND m.approval_status = 'COMPLETED'
          GROUP BY m.log_date,m.originator_user_name,m.project_code
        )m
        GROUP BY m.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t18
ON t1.project_code = t18.true_project_code
-- 研发费用
LEFT JOIN 
(
  SELECT a.true_project_code,
         SUM(a.actual_cost) as te_cost
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
      FROM 
      (
        SELECT md.external_project_code as project_code,
               SUM(md.cost_amount) as actual_cost
        FROM manhour_detail md
        WHERE md.cost_amount != 0 AND md.project_type_name IN ('外部客户项目')
        GROUP BY md.external_project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t19
ON t1.project_code = t19.true_project_code
-- 携程用车费用
LEFT JOIN
(  
  SELECT a.true_project_code,
         SUM(a.actual_cost) as ctrip_car_cost
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
      FROM 
      (
        SELECT c.project_code,
               SUM(IF(c.real_amount_haspost is null,0,c.real_amount_haspost)) as actual_cost
        FROM ${dwd_dbname}.dwd_ctrip_car_account_check_info_di c
        LEFT JOIN ${dim_dbname}.dim_day_date td 
        ON c.d = td.days
        WHERE td.is_month_end = 1 OR td.days = DATE_ADD(CURRENT_DATE(), -1)
        GROUP BY c.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t20
ON t1.project_code = t20.true_project_code
-- 携程机票费用
LEFT JOIN
(  
  SELECT a.true_project_code,
         SUM(a.actual_cost) as ctrip_flight_cost
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
      FROM 
      (
        SELECT f.project_code,
               SUM(IF(f.real_amount is null,0,f.real_amount)) as actual_cost
        FROM ${dwd_dbname}.dwd_ctrip_flight_account_check_info_di f
        LEFT JOIN ${dim_dbname}.dim_day_date td 
        ON f.d = td.days
        WHERE td.is_month_end = 1 OR td.days = DATE_ADD(CURRENT_DATE(), -1)
        GROUP BY f.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t21
ON t1.project_code = t21.true_project_code
-- 携程酒店费用
LEFT JOIN
(  
  SELECT a.true_project_code,
         SUM(a.actual_cost) as ctrip_hotel_cost
  FROM
  (
    SELECT tt.*
    FROM 
    (
      SELECT tmp.project_code,
             tmp.actual_cost,
             s.project_code as true_project_code,
             s.project_sale_code,
             row_number()over(PARTITION by tmp.project_code order by h.start_time desc)rn
      FROM 
      (
        SELECT h.project_code,
               SUM(IF(h.amount is null,0,h.amount)) as actual_cost
        FROM ${dwd_dbname}.dwd_ctrip_hotel_account_check_info_di h
        LEFT JOIN ${dim_dbname}.dim_day_date td 
        ON h.d = td.days
        WHERE td.is_month_end = 1 OR td.days = DATE_ADD(CURRENT_DATE(), -1)
        GROUP BY h.project_code
      )tmp
      LEFT JOIN ${dwd_dbname}.dwd_share_project_base_info_df s
      ON s.d = DATE_ADD(CURRENT_DATE(), -1) AND (s.project_code = tmp.project_code or s.project_sale_code = tmp.project_code) AND s.project_type_id IN (0,1,4,7,8,9) AND s.project_dispaly_state != '11.项目取消'
      LEFT JOIN 
      (
        SELECT h.project_code,
               h.pre_sale_code,
               h.start_time,
               h.end_time,
               h.contract_code,
               row_number()over(PARTITION by IF(h.pre_sale_code is NULL OR length(h.pre_sale_code) = 0,h.project_code,h.pre_sale_code) order by h.start_time desc)rn
        FROM ${dwd_dbname}.dwd_bpm_external_project_handover_info_ful h
        WHERE h.approval_staus = 30 
      )h
      ON s.project_code = h.project_code AND h.rn = 1 
    )tt
    WHERE tt.rn = 1
  )a
  WHERE a.true_project_code is not null
  GROUP BY a.true_project_code
)t22
ON t1.project_code = t22.true_project_code;