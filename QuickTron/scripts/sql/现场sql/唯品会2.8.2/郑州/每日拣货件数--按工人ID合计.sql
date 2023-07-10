SELECT
IF(tmp1.operator is NULL OR tmp1.operator ='','--',tmp1.operator) AS '工人ID',
tmp1.display_name AS '工人姓名',
tmp1.date1 AS '统计日期',
tmp1.quantity AS '拣货件数',
tmp1.out_quantity AS '移出件数',
tmp1.quantity - tmp1.out_quantity AS '正常拣货件数'
FROM
(
SELECT DATE_FORMAT(d.last_updated_date,'%Y-%m-%d') AS date1,
IF(d.operator is NULL OR d.operator ='','--',d.operator) AS operator,
u.display_name,
SUM(d.quantity) AS quantity,
SUM(IF(po.order_type = 'INV_MOVE_OUT',d.quantity,0)) AS out_quantity
FROM evo_wes_picking.picking_order po
LEFT JOIN evo_wes_picking.picking_order_detail pod
ON po.id = pod.picking_order_id
LEFT JOIN evo_wes_picking.picking_order_fulfill_detail d
ON pod.id = d.picking_order_detail_id
LEFT JOIN auth.user u
ON d.operator = u.username
WHERE d.last_updated_date >= '{begin_time} 00:00:00' and d.last_updated_date <= '{end_time} 23:59:59' AND d.quantity IS NOT NULL
GROUP BY d.operator,DATE_FORMAT(d.last_updated_date,'%Y-%m-%d')

UNION ALL

SELECT '--' ,
CONCAT(IF(d.operator is NULL OR d.operator ='','--',d.operator),'-','汇总') AS operator,
'--',
SUM(d.quantity) AS quantity ,
SUM(IF(po.order_type = 'INV_MOVE_OUT',d.quantity,0)) AS out_quantity
FROM evo_wes_picking.picking_order po
LEFT JOIN evo_wes_picking.picking_order_detail pod
ON po.id = pod.picking_order_id
LEFT JOIN evo_wes_picking.picking_order_fulfill_detail d
ON pod.id = d.picking_order_detail_id
LEFT JOIN auth.user u
ON d.operator = u.username
WHERE d.last_updated_date >= '{begin_time} 00:00:00' and d.last_updated_date <= '{end_time} 23:59:59' AND d.quantity IS NOT NULL
GROUP BY d.operator
)tmp1
ORDER BY tmp1.operator,DATE_FORMAT(tmp1.date1,'%Y-%m-%d')DESC