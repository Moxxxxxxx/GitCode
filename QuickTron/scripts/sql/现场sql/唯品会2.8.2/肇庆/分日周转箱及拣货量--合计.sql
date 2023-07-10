SELECT tmp1.time as '日期',
tmp1.package_max_num as '日周转箱最大件数',
tmp1.package_min_num as '日周转箱最小件数',
tmp1.package_avg_num as '日周转箱平均件数',
tmp2.picking_max_num as '小时完成拣货最大箱数',
tmp2.picking_min_num as '小时完成拣货最小箱数',
tmp2.picking_avg_num as '小时完成拣货平均箱数',
tmp1.package_num as '完成拣货箱总数',
tmp3.bucket_num as '使用货架总量'
FROM
(
SELECT tt.time, -- 按每天作为时间维度
MAX(tt.num) as package_max_num, -- 周转箱内件数
MIN(tt.num) as package_min_num, -- 周转箱内件数
CAST(SUM(tt.num)/COUNT(DISTINCT tt.package_uuid) AS DECIMAL(10,2)) as package_avg_num, -- 周转箱内件数
COUNT(DISTINCT tt.package_uuid) as package_num
FROM
(
SELECT DATE_FORMAT(d.last_updated_date,'%Y-%m-%d')time,
d.package_uuid, -- 周转箱编码
SUM(d.quantity) as num -- 按周转箱统计件数
FROM evo_wes_picking.picking_order_fulfill_detail d
WHERE d.last_updated_date >= '{begin_time} 00:00:00' and d.last_updated_date <= '{end_time} 23:59:59' AND d.quantity IS NOT NULL AND d.state = 'DONE'
GROUP BY d.package_uuid,DATE_FORMAT(d.last_updated_date,'%Y-%m-%d')
)tt
GROUP BY DATE_FORMAT(tt.time,'%Y-%m-%d')
)tmp1
LEFT JOIN
(
SELECT DATE_FORMAT(tt.time,'%Y-%m-%d') as time, -- 按每小时作为时间维度
MAX(tt.package_num) as picking_max_num, -- 周转箱箱数
MIN(tt.package_num) as picking_min_num, -- 周转箱箱数
CAST(SUM(tt.package_num)/COUNT(DISTINCT tt.time) AS DECIMAL(10,2)) as picking_avg_num -- 周转箱箱数
FROM
(
SELECT DATE_FORMAT(d.last_updated_date,'%Y-%m-%d %H:00:00')time, -- 按每天作为时间维度
COUNT(DISTINCT d.package_uuid) as package_num
FROM evo_wes_picking.picking_order_fulfill_detail d
WHERE d.last_updated_date >= '{begin_time} 00:00:00' and d.last_updated_date <= '{end_time} 23:59:59' AND d.quantity IS NOT NULL AND d.state = 'DONE'
GROUP BY DATE_FORMAT(d.last_updated_date,'%Y-%m-%d %H:00:00')
)tt
GROUP BY DATE_FORMAT(tt.time,'%Y-%m-%d')
)tmp2
ON DATE_FORMAT(tmp1.time,'%Y-%m-%d') = DATE_FORMAT(tmp2.time,'%Y-%m-%d')
LEFT JOIN
(
SELECT DATE_FORMAT(j.updated_date,'%Y-%m-%d') as time,COUNT(j.id) as bucket_num
FROM evo_wcs_g2p.bucket_move_job j
WHERE j.updated_date >= '{begin_time} 00:00:00' and j.updated_date <= '{end_time} 23:59:59' AND j.job_type = 'G2P_BUCKET_MOVE' AND j.state = 'DONE' AND j.bucket_move_type = 'G2P_ONLINE_PICK'
GROUP BY DATE_FORMAT(j.updated_date,'%Y-%m-%d')
)tmp3
ON DATE_FORMAT(tmp1.time,'%Y-%m-%d') = DATE_FORMAT(tmp3.time,'%Y-%m-%d')