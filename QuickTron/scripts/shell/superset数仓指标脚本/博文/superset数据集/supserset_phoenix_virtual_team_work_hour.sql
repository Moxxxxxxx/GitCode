SELECT
	c.work_date,
	c.day_type,
	c.team_member,
	c.work_hour,
	d.team_ft,
	d.module_branch,
	d.role_type
FROM
(
SELECT
	work_date,
	team_member,
	SUM(work_hour) as work_hour,
	SUM(day_type) as day_type
FROM
(
SELECT
	team_member,
	work_date,
	0 as day_type,
	work_hour 
FROM
	ads.ads_team_ft_virtual_member_work_efficiency 
WHERE
	project_classify_name = '3.0凤凰项目' 

UNION ALL

SELECT
	emp_name AS team_member,
	days AS work_date,
	day_type,
	0 AS work_hour 
FROM
	( SELECT DISTINCT emp_name FROM ads.ads_virtual_org_emp_info_offline WHERE virtual_org_name = '凤凰项目' and is_active = 1) a,
	( SELECT days,day_type FROM ads.dim_day_date WHERE days BETWEEN '2021-11-13' AND CURRENT_DATE ()) b
) c

GROUP BY 
	work_date,
	team_member
) c left join 
(
SELECT emp_name as team_member,org_name as team_ft,module_branch,role_type from ads.ads_virtual_org_emp_info_offline where virtual_org_name = '凤凰项目' and is_active = 1 group by emp_name,org_name,module_branch,role_type
) d on c.team_member=d.team_member