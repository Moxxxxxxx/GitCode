SELECT 
	work_date,
	case 
	when d.team_member = '曹琛' then '曹琛(前端)'
	when d.team_member = '张莉' then '张莉(前端)'
	when d.team_member = '李治宇' then '李治宇(前端)'
	when d.team_member = '李强' then '李强(前端)'
	when d.team_member = '张晖' then '张晖(前端)'
	when d.team_member = '高陈华' then '高陈华(前端)'
	else d.team_member
	end as team_member,
	add_lines_count,
	e.team_ft,
	e.module_branch
FROM	
(
SELECT
	work_date,
	team_member,
	SUM(add_lines_count) as add_lines_count
FROM
(
SELECT 
days as work_date, team_member, add_lines_count
FROM
(SELECT 
	DISTINCT emp_name as team_member, 0 as add_lines_count
FROM 
	ads.ads_virtual_org_emp_info_offline
WHERE 
	role_type = '研发' and is_active = 1) a,
(
select days from ads.dim_day_date where days between '2021-11-13' and current_date()
) b
UNION ALL
SELECT work_date,team_member,add_lines_count
FROM ads.ads_team_ft_member_git_detail
WHERE (second_level_directory = 'phoenix' or git_repository='hardware/upper_computer/upper_computer.git')
and team_member in (
select distinct emp_name from ads.ads_virtual_org_emp_info_offline where role_type = '研发' and is_active = 1
)
ORDER BY team_member,work_date desc
) c
GROUP BY work_date,team_member
) d
right join 
(
SELECT emp_name as team_member,org_name as team_ft,module_branch from ads.ads_virtual_org_emp_info_offline where virtual_org_name = '凤凰项目' group by emp_name,org_name,module_branch
) e on d.team_member=e.team_member