--ones工时拆解明细 ads_ones_manhour_dismemberment_detail

INSERT overwrite table ${ads_dbname}.ads_ones_manhour_dismemberment_detail
SELECT '' as id,
       total_member.team_ft,
       total_member.team_group,
       total_member.team_sub_group,
       total_member.team_member,
       total_member.emp_position,
       total_member.is_job,
       total_member.hired_date,
       total_member.quit_date,
       total_member.is_need_fill_manhour,
       total_member.org_role_type,
       total_member.virtual_role_type,
       total_member.module_branch,
       total_member.virtual_org_name,
       m.project_org_name,
       m.project_classify_name,
       m.sprint_classify_name,
       m.external_project_code,
       m.external_project_name,
       m.project_bpm_code,
       m.project_bpm_name,
       m.project_type_name,
       m.work_create_date,
       m.work_id,
       m.work_summary,
       m.work_desc,
       m.work_type,
       m.work_status,
       total_day.work_check_date,
       total_day.day_type,
       m.work_hour,
       m.actual_date,
       m.error_type,
       m.work_hour_total,
       m.work_hour_rate,
       m.cost_amount,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as create_time,
       date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') as update_time
FROM 
(
  SELECT DISTINCT m.team_ft,
                  m.team_group,
                  m.team_sub_group,
                  m.team_member,
                  m.emp_position,
                  m.is_job,
                  m.hired_date,
                  m.quit_date,
                  m.is_need_fill_manhour,
                  m.org_role_type,
                  m.virtual_role_type,
                  m.module_branch,
                  m.virtual_org_name
  FROM ads.ads_team_ft_virtual_member_manhour_detail m
  WHERE m.project_type_name is not null
)total_member
LEFT JOIN 
(
  SELECT DISTINCT m.team_member,
                  m.work_check_date,
                  m.day_type
  FROM ads.ads_team_ft_virtual_member_manhour_detail m
)total_day
ON total_member.team_member = total_day.team_member
LEFT JOIN 
(
-- 技术&管理工作(5-10%)
SELECT m.team_ft,
       m.team_group,
       m.team_sub_group,
       m.team_member,
       m.emp_position,
       m.is_job,
       m.hired_date,
       m.quit_date,
       m.is_need_fill_manhour,
       m.org_role_type,
       m.virtual_role_type,
       m.module_branch,
       m.virtual_org_name,
       m.project_org_name,
       m.project_classify_name,
       m.sprint_classify_name,
       m.external_project_code,
       m.external_project_name,
       m.project_bpm_code,
       m.project_bpm_name,
       m.project_type_name,
       m.work_create_date,
       m.work_id,
       m.work_summary,
       m.work_desc,
       m.work_type,
       m.work_status,
       m.work_check_date,
       m.day_type,
       m.work_hour,
       m.actual_date,
       m.error_type,
       m.work_hour_total,
       m.work_hour_rate,
       m.cost_amount
FROM ads.ads_team_ft_virtual_member_manhour_detail m
WHERE m.project_type_name = '技术&管理工作'
  AND (m.team_ft in ('制造部','硬件自动化','AMR FT') 
   OR (m.team_ft not in ('制造部','硬件自动化','AMR FT') AND m.emp_position rlike '主管|FTO|负责人|合伙人|创始人|总监|副总监'))

UNION ALL

-- 外部客户项目(3-5%)
SELECT m.team_ft,
       m.team_group,
       m.team_sub_group,
       m.team_member,
       m.emp_position,
       m.is_job,
       m.hired_date,
       m.quit_date,
       m.is_need_fill_manhour,
       m.org_role_type,
       m.virtual_role_type,
       m.module_branch,
       m.virtual_org_name,
       m.project_org_name,
       m.project_classify_name,
       m.sprint_classify_name,
       m.external_project_code,
       m.external_project_name,
       m.project_bpm_code,
       m.project_bpm_name,
       m.project_type_name,
       m.work_create_date,
       m.work_id,
       m.work_summary,
       m.work_desc,
       m.work_type,
       m.work_status,
       m.work_check_date,
       m.day_type,
       m.work_hour,
       m.actual_date,
       m.error_type,
       m.work_hour_total,
       m.work_hour_rate,
       m.cost_amount
FROM ads.ads_team_ft_virtual_member_manhour_detail m
WHERE m.project_type_name = '外部客户项目' 
  AND ((m.team_ft = '智能搬运FT' AND m.team_group = '标准搬运研发' AND m.team_sub_group ='研发定制组')
	  OR (m.team_ft = '智能搬运FT' AND m.team_group = 'LES产品研发' AND m.team_sub_group ='LES项目交付组')
	  OR (m.team_ft = '箱式FT' AND m.team_group = '箱式FT软件研发' AND m.team_sub_group ='项目交付组')
	  OR (m.team_ft = '箱式FT' AND m.team_group = '箱式FT软件研发' AND m.team_sub_group ='研发交付组'))

UNION ALL

-- 不满足条件的技术&管理工作
SELECT m.team_ft,
       m.team_group,
       m.team_sub_group,
       m.team_member,
       m.emp_position,
       m.is_job,
       m.hired_date,
       m.quit_date,
       m.is_need_fill_manhour,
       m.org_role_type,
       m.virtual_role_type,
       m.module_branch,
       m.virtual_org_name,
       m.project_org_name,
       m.project_classify_name,
       m.sprint_classify_name,
       null as external_project_code,
       null as external_project_name,
       IF(m.project_type_name in ('外部客户项目','技术&管理工作') or (m.project_type_name in ('内部研发项目') AND m.project_bpm_code = '未知项目编码'),m2.project_bpm_code,m.project_bpm_code) as project_bpm_code,
       IF(m.project_type_name in ('外部客户项目','技术&管理工作') or (m.project_type_name in ('内部研发项目') AND m.project_bpm_name = '未知项目名称'),m2.project_bpm_name,m.project_bpm_name) as project_bpm_name,
       '内部研发项目' as project_type_name,
       m.work_create_date,
       m.work_id,
       m.work_summary,
       m.work_desc,
       m.work_type,
       m.work_status,
       m.work_check_date,
       m.day_type,
       m.work_hour,
       m.actual_date,
       m.error_type,
       m.work_hour_total,
       m.work_hour_rate,
       m.cost_amount
FROM ads.ads_team_ft_virtual_member_manhour_detail m
-- 按日期伪随机
LEFT JOIN
(
  SELECT d.days,m.project_bpm_code,m.project_bpm_name
  FROM dim.dim_day_date d
  LEFT JOIN 
  (
    SELECT t1.project_bpm_code,t1.project_bpm_name,row_number()over(order by t1.num desc)rn 
    FROM 
    (
      SELECT m.project_bpm_code,m.project_bpm_name,count(*) as num
      FROM ads.ads_team_ft_virtual_member_manhour_detail m
      WHERE m.project_type_name = '内部研发项目' AND m.project_bpm_code != '未知项目编码'
      GROUP BY m.project_bpm_code,m.project_bpm_name
      HAVING count(*)>80
    )t1
  )m
  ON d.day_date = m.rn
  WHERE d.days >= '2021-01-01'
)m2
ON m.work_create_date = m2.days
WHERE ((m.project_type_name = '技术&管理工作' AND m.team_ft not in ('制造部','硬件自动化','AMR FT') AND m.emp_position not rlike '主管|FTO|负责人|合伙人|创始人|总监|副总监')
   OR (m.project_type_name = '内部研发项目')
   OR (m.project_type_name = '外部客户项目' AND (m.team_ft not in ('智能搬运FT','箱式FT') 
                                         OR (m.team_ft = '智能搬运FT' AND m.team_group not in ('标准搬运研发','LES产品研发'))
                                         OR (m.team_ft = '智能搬运FT' AND m.team_group = '标准搬运研发' AND (m.team_sub_group not in ('研发定制组') or m.team_sub_group is null))
                                         OR (m.team_ft = '智能搬运FT' AND m.team_group = 'LES产品研发' AND (m.team_sub_group not in ('LES项目交付组') or m.team_sub_group is null))
                                         OR (m.team_ft = '箱式FT' AND m.team_group not in ('箱式FT软件研发'))
                                         OR (m.team_ft = '箱式FT' AND m.team_group = '箱式FT软件研发' AND (m.team_sub_group not in ('项目交付组','研发交付组') or m.team_sub_group is null)))))
)m
ON total_member.team_member = m.team_member AND total_day.work_check_date = m.work_check_date;