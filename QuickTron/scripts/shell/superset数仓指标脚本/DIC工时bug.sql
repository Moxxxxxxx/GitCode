
select * from ads.ads_single_project_agv_type_info where cur_date = '2022-09-12' and project_code = 'A51118'

select * from ads.ads_team_ft_role_member_work_efficiency 
where team_member = '陈真' and work_date >= '2022-09-05' and work_date < '2022-09-12'

select team_member,work_date,count(1) 
from ads.ads_team_ft_role_member_work_efficiency group by team_member,work_date having count(1)>1

select * from ads.ads_team_ft_role_member_work_efficiency 
where team_member = '张聪' and work_date = '2022-07-16' 


select * from dwd.dwd_dtk_emp_attendance_checkin_day_info_di 
where emp_name = '田先富' and work_date = '2022-08-09' 

select * from ads.ads_team_ft_virtual_member_manhour_detail where team_member = '田先富'

and work_date = '2022-09-08'


select * from ads.ads_team_ft_role_member_work_efficiency where team_member = '田先富'

select * from ads.ads_team_ft_virtual_member_manhour_detail where team_member = '杨萍' and work_check_date = '2022-09-09'
select * from ads.ads_team_ft_role_member_work_efficiency where team_member = '杨萍' and work_date = '2022-09-09'

select * from dwd.dwd_ones_task_manhour_info_ful where user_name = '杨萍' and task_start_time >= '2022-09-09' and task_start_time < '2022-09-10' 



select * from dwd.dwd_ones_task_info_ful  where  uuid IN ('S2cvHLwNkmPeFxWi','S2cvHLwNJGAeVL9u','S2cvHLwNkmPeFxWi','S2cvHLwNJGAeVL9u')
