SELECT
tmp.`统计日期` as '统计日期',
SUM(tmp.`出库单数`) as '出库单数',
SUM(tmp.`出库件数`) as '出库件数',
SUM(tmp.`MM订单数`) as 'MM订单数',
SUM(tmp.`MM订件数`) as 'MM订件数',
SUM(tmp.`F-S单数`) as 'F-S单数',
SUM(tmp.`F-S件数`) as 'F-S件数',
SUM(tmp.`F-N单数`) as 'F-N单数',
SUM(tmp.`F-N件数`) as 'F-N件数',
SUM(tmp.`F-MS单数`) as 'F-MS单数',
SUM(tmp.`F-MS件数`) as 'F-MS件数',
-- SUM(tmp.`F-FN单数`) as 'F-FN单数',
-- SUM(tmp.`F-FN件数`) as 'F-FN件数',
-- SUM(tmp.`F-LN单数`) as 'F-LN单数',
-- SUM(tmp.`F-LN件数`) as 'F-LN件数',
-- SUM(tmp.`SS订单数`) as 'SS订单数',
-- SUM(tmp.`SS订件数`) as 'SS订件数',
-- SUM(tmp.`SM订单数`) as 'SM订单数',
-- SUM(tmp.`SM订件数`) as 'SM订件数',
SUM(tmp.`下架单数`) as '下架单数',
SUM(tmp.`下架件数`) as '下架件数',
SUM(tmp.`移库单数`) as '移库单数',
SUM(tmp.`移库件数`) as '移库件数',
SUM(tmp.`入库单数`) as '入库单数',
SUM(tmp.`入库件数`) as '入库件数',
SUM(tmp.`库存数量`) as '库存数量'
FROM
(
SELECT
DATE_FORMAT(po.last_updated_date,'%Y-%m-%d') as '统计日期',
IF(po.order_type != 'OUB_SHIP_PICK' AND po.order_type != 'INV_MOVE_OUT',IF(COUNT(DISTINCT po.picking_order_number) is NULL,0,COUNT(DISTINCT po.picking_order_number)),0) as '出库单数',
IF(po.order_type != 'OUB_SHIP_PICK' AND po.order_type != 'INV_MOVE_OUT',IF(SUM(pod.fulfill_quantity) is NULL,0,SUM(pod.fulfill_quantity)),0) as '出库件数',
IF(po.order_type = 'OUB_SALE_MM_PICK' OR po.order_type = 'OUB_SALE_MM_SINGLE_PICK',COUNT(DISTINCT po.picking_order_number),0) as 'MM订单数',
IF(po.order_type = 'OUB_SALE_MM_PICK' OR po.order_type = 'OUB_SALE_MM_SINGLE_PICK',SUM(pod.fulfill_quantity),0) as 'MM订件数',
IF(po.order_type = 'FLOWPICK_S',COUNT(DISTINCT po.picking_order_number),0) as 'F-S单数',
IF(po.order_type = 'FLOWPICK_S',SUM(pod.fulfill_quantity),0) as 'F-S件数',
IF(po.order_type = 'FLOWPICK_N',COUNT(DISTINCT po.picking_order_number),0) as 'F-N单数',
IF(po.order_type = 'FLOWPICK_N',SUM(pod.fulfill_quantity),0) as 'F-N件数',
IF(po.order_type = 'FLOWPICK_MS',COUNT(DISTINCT po.picking_order_number),0) as 'F-MS单数',
IF(po.order_type = 'FLOWPICK_MS',SUM(pod.fulfill_quantity),0) as 'F-MS件数',
-- IF(po.order_type = 'FLOWPICK_FN',COUNT(DISTINCT po.picking_order_number),0) as 'F-FN单数',
-- IF(po.order_type = 'FLOWPICK_FN',SUM(pod.fulfill_quantity),0) as 'F-FN件数',
-- IF(po.order_type = 'FLOWPICK_LN',COUNT(DISTINCT po.picking_order_number),0) as 'F-LN单数',
-- IF(po.order_type = 'FLOWPICK_LN',SUM(pod.fulfill_quantity),0) as 'F-LN件数',
-- IF(po.order_type = 'OUB_SALE_SS_PICK',COUNT(DISTINCT po.picking_order_number),0) as 'SS订单数',
-- IF(po.order_type = 'OUB_SALE_SS_PICK',SUM(pod.fulfill_quantity),0) as 'SS订件数',
-- IF(po.order_type = 'OUB_SALE_SM_PICK',COUNT(DISTINCT po.picking_order_number),0) as 'SM订单数',
-- IF(po.order_type = 'OUB_SALE_SM_PICK',SUM(pod.fulfill_quantity),0) as 'SM订件数',
IF(po.order_type = 'OUB_SHIP_PICK',COUNT(DISTINCT po.picking_order_number),0) as '下架单数',
IF(po.order_type = 'OUB_SHIP_PICK',SUM(pod.fulfill_quantity),0) as '下架件数',
IF(po.order_type = 'INV_MOVE_OUT',COUNT(DISTINCT po.picking_order_number),0) as '移库单数',
IF(po.order_type = 'INV_MOVE_OUT',SUM(pod.fulfill_quantity),0) as '移库件数',
0 as '入库单数',
0 as '入库件数',
0 as '库存数量'
FROM evo_wes_picking.picking_order po
LEFT JOIN evo_wes_picking.picking_order_detail pod
ON po.id = pod.picking_order_id
WHERE po.state = 'DONE' AND po.last_updated_date >= '{begin_time}' AND po.last_updated_date <= '{end_time}'
GROUP BY DATE_FORMAT(po.last_updated_date,'%Y-%m-%d'),po.order_type

UNION ALL

SELECT
DATE_FORMAT(ro.last_updated_date,'%Y-%m-%d') as '统计日期',
0 as '出库单数',
0 as '出库件数',
0 as 'MM订单数',
0 as 'MM订件数',
0 as 'F-S单数',
0 as 'F-S件数',
0 as 'F-N单数',
0 as 'F-N件数',
0 as 'F-MS单数',
0 as 'F-MS件数',
-- 0 as 'F-FN单数',
-- 0 as 'F-FN件数',
-- 0 as 'F-LN单数',
-- 0 as 'F-LN件数',
-- 0 as 'SS订单数',
-- 0 as 'SS订件数',
-- 0 as 'SM订单数',
-- 0 as 'SM订件数',
0 as '下架单数',
0 as '下架件数',
0 as '移库单数',
0 as '移库件数',
IF(ro.order_type = 'INB_RC_SHELF' OR ro.order_type = 'INV_MOVE_IN',IF(COUNT(DISTINCT ro.replenish_order_number) is NULL,0,COUNT(DISTINCT ro.replenish_order_number)),0) as '入库单数',
IF(ro.order_type = 'INB_RC_SHELF' OR ro.order_type = 'INV_MOVE_IN',IF(SUM(rod.fulfill_quantity) is NULL,0,SUM(rod.fulfill_quantity)),0) as '入库件数',
0 as '库存数量'
FROM evo_wes_replenish.replenish_order ro
LEFT JOIN evo_wes_replenish.replenish_order_detail rod
ON ro.id = rod.replenish_order_id
WHERE ro.state = 'DONE' AND ro.last_updated_date >= '{begin_time}' AND ro.last_updated_date <= '{end_time}'
GROUP BY DATE_FORMAT(ro.last_updated_date,'%Y-%m-%d'),ro.order_type

UNION ALL

SELECT
DATE_FORMAT(vidr.report_date,'%Y-%m-%d') as '统计日期',
0 as '出库单数',
0 as '出库件数',
0 as 'MM订单数',
0 as 'MM订件数',
0 as 'F-S单数',
0 as 'F-S件数',
0 as 'F-N单数',
0 as 'F-N件数',
0 as 'F-MS单数',
0 as 'F-MS件数',
-- 0 as 'F-FN单数',
-- 0 as 'F-FN件数',
-- 0 as 'F-LN单数',
-- 0 as 'F-LN件数',
-- 0 as 'SS订单数',
-- 0 as 'SS订件数',
-- 0 as 'SM订单数',
-- 0 as 'SM订件数',
0 as '下架单数',
0 as '下架件数',
0 as '移库单数',
0 as '移库件数',
0 as '入库单数',
0 as '入库件数',
IF(SUM(vidr.quantity) is NULL,0,SUM(vidr.quantity)) as '库存数量'
FROM evo_vip.vip_inventory_daily_report vidr
WHERE vidr.report_date >= '{begin_time}' AND vidr.report_date <= '{end_time}'
GROUP BY DATE_FORMAT(vidr.report_date,'%Y-%m-%d')
)tmp
GROUP BY tmp.`统计日期`