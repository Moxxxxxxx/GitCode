-- 每日出入库量及库容
SET @begin_time = '2021-11-08 00:00:00'; -- 设置开始时间
SET @end_time = '2021-11-09 00:00:00'; -- 设置结束时间

SELECT 
DATE(ro.last_updated_date) as '上架日期',                               -- 取值于replenish_order表的更新时间
COUNT(DISTINCT rod.pack_id) as '上架箱数',                              -- 取值于replenish_order_detail表的周转箱数
SUM(rod.fulfill_quantity) as '上架sku件数',                             -- 取值于replenish_order_detail表的数量
IFNULL(tt1.picking_num,0) as '出库sku件数',                             -- 取值于picking_order_detail表的数量
tt2.inventory_num as '剩余sku总件数',                                   -- 取值于level3_inventory表的数量
tt3.slot_num_total as '总库位数',                                       -- 取值于basic_slot表的slot_code数量
tt4.slot_num_used as '已使用库位数',                                    -- 取值于 level3_inventory表的bucket_slot_code数量
tt3.slot_num_total - tt4.slot_num_used as '剩余空库位数',               -- 取值于总库位数-已使用库位数
CAST(tt4.slot_num_used/tt3.slot_num_total as DECIMAL(10,2)) as '使用率' -- 取值于已使用库位数/总库位数
FROM evo_wes_replenish.replenish_order ro
LEFT JOIN evo_wes_replenish.replenish_order_detail rod
ON rod.replenish_order_id = ro.id
LEFT JOIN 
(
SELECT DATE(pod.last_updated_date) as picking_date,SUM(pod.fulfill_quantity) as picking_num -- 出库sku件数
FROM evo_wes_picking.picking_order_detail pod
WHERE pod.quantity = pod.fulfill_quantity
GROUP BY DATE(pod.last_updated_date)
)tt1
ON DATE(ro.last_updated_date) = tt1.picking_date
JOIN
(
SELECT SUM(li.quantity) as inventory_num -- 剩余sku总件数
FROM evo_wes_inventory.level3_inventory li
)tt2
JOIN
(
SELECT COUNT(bs.slot_code) as slot_num_total -- 总库位数
FROM evo_basic.basic_slot bs
LEFT JOIN evo_basic.basic_bucket bb
ON bb.id = bs.bucket_id
WHERE bs.slot_type_id = '11' AND bs.enabled = 1 AND bs.state = 'effective' AND bb.point_code IS NOT NULL
)tt3
JOIN
(
SELECT COUNT(DISTINCT li.bucket_slot_code) AS slot_num_used  -- 已使用库位数
FROM evo_wes_inventory.level3_inventory li
WHERE li.bucket_slot_code NOT IN ('INVENTORY_DIFF','TEMPORARY_DELIVERY','TEMPORARY_RECEIVING','TEMPORARY_RETURN')
)tt4
WHERE ro.state = 'DONE' AND ro.last_updated_date >= @begin_time AND ro.last_updated_date < @end_time
GROUP BY DATE(ro.last_updated_date)