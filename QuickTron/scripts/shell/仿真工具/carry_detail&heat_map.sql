









--搬运ads表
with job_path_plan_point_travel as (
SELECT 
	service_ip,
	simulation_id,
	simulation_job_created_id,
	job_id,
	agv_code,
	job_point_order,
	way_point_code,
	CASE 
	WHEN current_point_x = next_point_x or current_point_y = next_point_y THEN 'LINE'
	WHEN current_point_x = next_point_x and current_point_y = next_point_y THEN 'POINT'
	ELSE 'CURVE' END as move_behaviour,
	abs(next_point_x-current_point_x)+abs(next_point_y-current_point_y) as line_travel_dist
FROM 
	dwd.dwd_simulation_agv_path_plan_info_di
GROUP BY
	service_ip,
	simulation_id,
	simulation_job_created_id,
	job_id,
	agv_code,
	job_point_order,
	way_point_code,
	current_point_x,
	current_point_y,
	next_point_x,
	next_point_y
ORDER BY
	job_id,job_point_order 
), 
job_travel_dist as (
SELECT 
	service_ip,
	simulation_id,
	simulation_job_created_id,
	job_id,
	SUM(line_travel_dist) as job_travel_dist
FROM 
	job_path_plan_point_travel
GROUP BY 
	service_ip,
	simulation_id,
	simulation_job_created_id,
	job_id
),
simulation_start_time_dim as (
SELECT 
    MIN(CASE 
    WHEN job_type = 'WORKBIN_MOVE' THEN job_state_time_map['WAITING_NEXTSTOP']
    WHEN job_type = 'G2P_BUCKET_MOVE' THEN job_state_time_map['WAITING_RESOURCE']
    WHEN job_type = 'SI_CARRY' THEN job_state_time_map['INIT']
    WHEN job_type = 'SI_BUCKET_MOVE' THEN job_state_time_map['WAITING_NEXTSTOP']
    ELSE NULL END) as min_job_start_time,
    simulation_id ,
    simulation_job_created_id,
    service_ip 
FROM 
    ${dws_dbname}.dws_simulation_job_info_dscount
WHERE 
    job_allot_response_ms IS NOT NULL AND job_allot_ms IS NOT NULL AND job_transport_ms IS NOT NULL 
GROUP BY 
    simulation_id ,
    simulation_job_created_id,
    service_ip 
)
SELECT 
	a.service_ip,
	a.simulation_id,
	a.simulation_job_created_id,
	a.job_id,
	a.agv_code,
	from_unixtime(unix_timestamp(min_job_start_time)+FLOOR((unix_timestamp(job_accept_time)-unix_timestamp(min_job_start_time))/3600)*3600,'yyyy-MM-dd HH:mm:ss') as period_start,
    from_unixtime(unix_timestamp(min_job_start_time)+FLOOR(1+(unix_timestamp(job_accept_time)-unix_timestamp(min_job_start_time))/3600)*3600,'yyyy-MM-dd HH:mm:ss') as period_end,
	concat(from_unixtime(unix_timestamp(min_job_start_time)+FLOOR((unix_timestamp(job_accept_time)-unix_timestamp(min_job_start_time))/3600)*3600,'yyyy-MM-dd HH:mm:ss'),from_unixtime(unix_timestamp(min_job_start_time)+FLOOR(1+(unix_timestamp(job_accept_time)-unix_timestamp(min_job_start_time))/3600)*3600,'yyyy-MM-dd HH:mm:ss')) as time_interval,
    a.job_accept_time,
	a.job_execute_time,
	a.job_finish_time,
	a.job_duration,
	a.job_sequence_name,
	a.job_sub_job_name,
	a.job_sub_source_area,
	a.job_sub_target_area,
	c.job_travel_dist
FROM 
	dwd.dwd_simulation_agv_job_history_info_di a
LEFT JOIN 
	simulation_start_time_dim b 
	on a.simulation_id = b.simulation_id
	and a.simulation_job_created_id = b.simulation_job_created_id
	and a.service_ip = b.service_ip
LEFT JOIN
	job_travel_dist c 
	on a.simulation_id = c.simulation_id
	and a.simulation_job_created_id = c.simulation_job_created_id
	and a.service_ip = c.service_ip
	and a.job_id = c.job_id
WHERE 
	a.job_accept_time > b.min_job_start_time
ORDER BY
	job_accept_time DESC

	

--ads热力图
--with job_path_plan_point_occupation as (
--SELECT 
--	a.service_ip,
--	a.simulation_id,
--	a.simulation_job_created_id,
--	a.job_id,
--	a.agv_id,
--	a.job_point_order,
--	a.way_point_code,
--	CASE 
--	WHEN b.way_point_code IS NULL THEN a.way_point_code
--	ELSE b.way_point_code 
--	END as next_point_code,
--	a.data_time as current_point_enter_time,
--	CASE 
--	WHEN b.data_time IS NULL THEN a.data_time
--	ELSE b.data_time 
--	END as current_point_exit_time	
--FROM 
--	tmp.tmp_simulation_agv_path_plan_info_di a
--LEFT JOIN 
--	tmp.tmp_simulation_agv_path_plan_info_di b 
--	ON a.service_ip = b.service_ip 
--	AND a.simulation_id = b.simulation_id 
--	AND a.simulation_job_created_id = b.simulation_job_created_id 
--	AND a.job_id = b.job_id 
--	AND a.job_point_order = b.job_point_order - 1
--
--ORDER BY
--	job_id,job_point_order 
--), 
--job_path_plan_point_occupation_v2 as (
--SELECT 
--	service_ip,
--	simulation_id,
--	simulation_job_created_id,
--	job_id,
--	agv_id,
--	job_point_order,
--	way_point_code,
--	current_point_enter_time,
--	unix_timestamp(current_point_exit_time)-unix_timestamp(current_point_enter_time) as occupation	
--FROM 
--	job_path_plan_point_occupation
--ORDER BY 
--	job_id,job_point_order
--)
--SELECT 
--	a.service_ip,
--	a.simulation_id,
--	a.simulation_job_created_id,
--	a.job_id,
--	a.agv_id,
--	a.job_point_order,
--	a.way_point_code,
--	a.current_point_enter_time,
--	a.occupation,
--	from_unixtime((unix_timestamp(current_point_enter_time)+cast(60*((ROW_NUMBER()OVER(PARTITION BY job_id,way_point_code))-1) as int)),'yyyy-MM-dd HH:mm:ss') as slice_enter_time,
--	t.slice_occupation,
--	ROW_NUMBER()OVER(PARTITION BY job_id,way_point_code) slice_order
--FROM 
--	job_path_plan_point_occupation_v2 a
--	lateral view explode(split((CASE 
--								WHEN occupation = 60 THEN '60'
--								ELSE concat(repeat('60,',cast(floor(occupation/60) as int)),cast(occupation%60 as int))
--								END),',')) t as slice_occupation
--ORDER BY 
--	job_id,job_point_order,slice_order
	
	
	
	
	
	
	
	
	

	
	
	
	
	
	
	
	
	