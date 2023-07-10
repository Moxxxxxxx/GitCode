-- 外部客户项目工时 
with external_p as 
(select 
	* 
from 
	ads.ads_team_ft_virtual_member_manhour_detail 
where 
	(project_type_name = '外部客户项目' and external_project_code <> '未知项目编码')  	
	and ((team_ft = '智能搬运FT' and team_group = '标准搬运研发' and team_sub_group ='研发定制组')
	or(team_ft = '智能搬运FT' and team_group = 'LES产品研发' and team_sub_group ='LES项目交付组')
	or(team_ft = '箱式FT' and team_group = '箱式FT软件研发' and team_sub_group ='项目交付组')
	--or(team_ft = '智能搬运FT' and team_group = '标准搬运研发' and team_sub_group ='研发交付组')
	or(team_ft = '箱式FT' and team_group = '箱式FT软件研发' and team_sub_group ='研发交付组')))

-- 内部客户项目	
, internal_p as 
(select 
	*
from 
	ads.ads_team_ft_virtual_member_manhour_detail 
where 
	 (project_type_name = '内部研发项目' and project_bpm_code <> '未知项目编码'))
 	 
-- 技术&管理工作
, mgmt as 
(
SELECT 
	*
FROM 
	ads.ads_team_ft_virtual_member_manhour_detail 
WHERE 
	project_type_name = '技术&管理工作'
	and (team_ft IN ('制造部','硬件自动化') or emp_position rlike '主管|FTO|负责人|合伙人|创始人|总监|副总监') 
	and work_hour <> 0
)
,ilist as
(
select rank() over(order by project_bpm_code) as id,project_bpm_code,project_bpm_name
from ads.ads_team_ft_virtual_member_manhour_detail 
where project_type_name = '内部研发项目' and project_bpm_code not like '%临时%' and project_bpm_code not in ('C35067') group by project_bpm_code,project_bpm_name
)
,clist as 
(
SELECT 
	*,FLOOR(RAND()*35+1) as bpm_project_id
FROM 
	ads.ads_team_ft_virtual_member_manhour_detail 
WHERE 
	(project_type_name = '技术&管理工作' and (team_ft NOT IN ('制造部','硬件自动化') and emp_position not rlike '主管|FTO|负责人|合伙人|创始人|总监|副总监'))
	or (project_type_name = '内部研发项目' and project_bpm_code = '未知项目编码')
	or (project_type_name = '外部客户项目' and external_project_code = '未知项目编码')
	or (project_type_name = '外部客户项目' and concat(team_ft,'-',team_group,'-',team_sub_group) not in ('智能搬运FT-标准搬运研发-研发定制组','智能搬运FT-LES产品研发-LES项目交付组','箱式FT-箱式FT软件研发-项目交付组','箱式FT-箱式FT软件研发-研发交付组'))
)
select * from (
select clist.id,team_ft,team_group,team_sub_group,team_member,emp_position,is_job,hired_date,quit_date,is_need_fill_manhour,org_role_type,virtual_role_type,module_branch,virtual_org_name,project_org_name,project_classify_name,sprint_classify_name,external_project_code,external_project_name,ilist.project_bpm_code as project_bpm_code,ilist.project_bpm_name as project_bpm_name,'内部研发项目' as project_type_name,work_create_date,work_id,work_summary,work_desc,work_type,work_status,work_check_date,day_type,work_hour,actual_date,error_type,work_hour_total,work_hour_rate,cost_amount,create_time,update_time 
from 
	clist left join ilist on clist.bpm_project_id = ilist.id
) a
union all
select * from external_p 
union all 
select * from internal_p
union all 
select * from mgmt