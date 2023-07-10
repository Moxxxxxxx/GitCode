SELECT d.capacity_date AS '日期',
       CAST(SUM(d.qty) AS DECIMAL(10,0)) AS '出库件数'
FROM evo_vip.vip_production_capacity d
WHERE d.capacity_date >= '{begin_time}' and d.capacity_date <= '{end_time}' AND d.state = 'DONE' AND d.transaction_type = '拣货'
GROUP BY d.capacity_date