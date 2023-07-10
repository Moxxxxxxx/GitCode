SELECT
	a.use_date AS date_node,
	COUNT(DISTINCT (case when a.simulation_type='搬运业务' AND a.simulation_sub_type='纯料箱搬运' THEN a.user_name ELSE NULL END)) AS container_usage_count,
	COUNT(DISTINCT (case when a.simulation_type='搬运业务' AND a.simulation_sub_type='货架搬运(潜伏式)' THEN a.user_name ELSE NULL END)) AS bucket_usage_count,
	COUNT(DISTINCT (case when a.simulation_type='搬运业务' AND a.simulation_sub_type='纯料箱搬运' THEN a.user_name ELSE NULL END))/5 AS container_target_usage_rate,
	COUNT(DISTINCT (case when a.simulation_type='搬运业务' AND a.simulation_sub_type='货架搬运(潜伏式)' THEN a.user_name ELSE NULL END))/10 AS bucket_target_usage_rate
FROM
(
SELECT
	simulation_type,
	simulation_sub_type,
	user_ip,
	user_name,
	str_to_date(CONCAT(DATE_FORMAT(start_time,'%Y-%m')  ,'-01 00:00:00'),'%Y-%m-%d') AS use_date
FROM
	ads_simulation_utility_detail
WHERE
  user_name <> 'admin'
) a
GROUP BY a.use_date

