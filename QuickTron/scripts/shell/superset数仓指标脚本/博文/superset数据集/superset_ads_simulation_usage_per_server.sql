SELECT a.date_node,a.server_ip,a.user_ip,sum(a.duration_second) as total_duration, count(case when a.action_detail = '仿真场景运行' then 1 else null end) as use_quantity
FROM 
(
SELECT *, str_to_date(DATE_FORMAT(start_time,'%Y-%m-%d'),'%Y-%m-%d') as date_node
FROM ads_simulation_utility_detail
) a 
GROUP BY  a.server_ip, a.user_ip, a.date_node