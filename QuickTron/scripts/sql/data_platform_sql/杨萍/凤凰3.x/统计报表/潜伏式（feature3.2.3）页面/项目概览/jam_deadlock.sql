SELECT
	SUM(CASE WHEN tag = 'deadlock' THEN num ELSE 0 END) AS deadlock_num
	,SUM(CASE WHEN tag = 'jam' THEN num ELSE 0 END) AS jam_num
FROM
	(
		SELECT
			'deadlock' AS tag
			,COUNT(DISTINCT log_id) AS num
		FROM
		    local_report.dead_lock_trace
		WHERE
			happen_time BETWEEN {now_start_time} AND {now_start_time}
			AND
			robot_code is not null
		    AND
		    point_x is not null
		    AND
		    point_y is not null

		UNION

		SELECT
			'jam' AS tag
			,NULL AS num
		FROM
		    local_report.jam_map
		WHERE
			sta_time BETWEEN {now_start_time} AND {now_start_time}
	) tmp
