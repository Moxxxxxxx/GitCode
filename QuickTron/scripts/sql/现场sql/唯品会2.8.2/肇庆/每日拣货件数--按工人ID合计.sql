SELECT
       tmp1.display_name AS '工人姓名',
       tmp1.date1 AS '统计日期',
       tmp1.qty  AS '拣货件数'
FROM 
(
SELECT d.capacity_date AS date1,
       u.display_name,
       CAST(SUM(d.qty) AS DECIMAL(10,0)) AS qty
FROM evo_vip.vip_production_capacity d
LEFT JOIN auth.user u
ON d.staff_no = u.username
WHERE d.capacity_date >= '{begin_time}' and d.capacity_date <= '{end_time}' AND d.state = 'DONE' AND d.transaction_type = '拣货'
GROUP BY u.display_name,d.capacity_date

UNION ALL

SELECT '--',
       CONCAT(u.display_name,'-','汇总') AS display_name, 
       CAST(SUM(d.qty) AS DECIMAL(10,0)) AS qty
FROM evo_vip.vip_production_capacity d
LEFT JOIN auth.user u
ON d.staff_no = u.username
WHERE d.capacity_date >= '{begin_time}' and d.capacity_date <= '{end_time}' AND d.state = 'DONE' AND d.transaction_type = '拣货'
GROUP BY u.display_name
)tmp1
ORDER BY tmp1.display_name ,tmp1.date1 ASC