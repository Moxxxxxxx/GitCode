SELECT
    tmp1.*
FROM
     sys_breakdown tmp1
INNER JOIN
     (
        select
            t1.project_code,
            t1.item_name,
            max(t1.id) id
        from
            sys_breakdown t1
        inner join
            (
                select
                    project_code,
                    item_name,
                    MAX(breakdown_time) AS breakdown_time
                FROM
                     sys_breakdown
                WHERE
                    breakdown_level = 'error'
                    AND breakdown_time >= DATE_SUB(NOW(), interval 24 HOUR)
                GROUP BY
                   project_code,
                   item_name
            ) t2
        ON
            t1.project_code = t2.project_code
            and t1.item_name = t2.item_name
            and t1.breakdown_time = t2.breakdown_time
        GROUP BY
            t1.project_code,
            t1.item_name
     ) tmp2
ON
    tmp1.id = tmp2.id
WHERE
    tmp1.dingtalk_status = '0'
    AND tmp1.item_name not like '%组件状态异常%'
    AND tmp1.item_name not like '%exporter%'
    AND tmp1.project_name != '科力普'
;
