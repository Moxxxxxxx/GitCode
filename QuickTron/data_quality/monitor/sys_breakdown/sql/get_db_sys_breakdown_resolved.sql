select
    MD5(CONCAT(project_code,"##",item_name)) AS pk_md5
    ,MAX(breakdown_time) AS breakdown_time
FROM
    sys_breakdown
WHERE
    breakdown_level = 'error'
    AND item_status = 'resolved'
    AND breakdown_time >= DATE_SUB(NOW(), interval 24 HOUR)
    AND item_name not like '%exporter%'
GROUP BY
    MD5(CONCAT(project_code,"##",item_name))