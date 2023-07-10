SELECT tmp.*,tmp1.*,tmp2.*,tmp3.*
FROM
(

SELECT tt.sku_id,IF(tt.sku_id is not NULL,'C',NULL) as '类型',tt.`出库量`
FROM
(
SELECT a.sku_id,a.`出库量`,(@sum3 := @sum3 + a.`出库量`) as qty
FROM
(
SELECT tmp.sku_id,tmp.`出库量`
FROM
(
SELECT t.sku_id,t.`出库量`,(@sum := @sum + t.`出库量`) as qty
FROM
(
SELECT pod.sku_id,SUM(pod.fulfill_quantity) as '出库量'
FROM evo_wes_picking.picking_order_detail pod,(SELECT @sum :=0) tmp
WHERE pod.last_updated_date >= '2021-09-01 00:00:00' AND pod.last_updated_date < '2021-09-02 00:00:00' AND pod.project_code = 'A51118'
GROUP BY pod.sku_id
ORDER BY `出库量` DESC
)t
GROUP BY t.sku_id
ORDER BY t.`出库量` DESC
)tmp
LEFT JOIN
(
SELECT tt.sku_id,IF(tt.sku_id is not NULL,'A',NULL) as '类型',tt.`出库量`
FROM
(
SELECT t.sku_id,t.`出库量`,(@sum1 := @sum1 + t.`出库量`) as qty
FROM
(
SELECT pod.sku_id,SUM(pod.fulfill_quantity) as '出库量'
FROM evo_wes_picking.picking_order_detail pod,(SELECT @sum1 :=0) tmp
WHERE pod.last_updated_date >= '2021-09-01 00:00:00' AND pod.last_updated_date < '2021-09-02 00:00:00' AND pod.project_code = 'A51118'
GROUP BY pod.sku_id
ORDER BY `出库量` DESC
)t
GROUP BY t.sku_id
ORDER BY t.`出库量` DESC
)tt
WHERE tt.qty <= (SELECT SUM(pod.fulfill_quantity)*0.7 FROM evo_wes_picking.picking_order_detail pod WHERE pod.last_updated_date >= '2021-09-01 00:00:00' AND pod.last_updated_date < '2021-09-02 00:00:00' AND pod.project_code = 'A51118')
)tmp1
ON tmp.sku_id = tmp1.sku_id AND tmp.`出库量` = tmp1.`出库量`
LEFT JOIN
(
SELECT tt.sku_id,IF(tt.sku_id is not NULL,'B',NULL) as '类型',tt.`出库量`
FROM
(
SELECT a.sku_id,a.`出库量`,(@sum2 := @sum2 + a.`出库量`) as qty
FROM
(
SELECT tmp.sku_id,tmp.`出库量`
FROM
(
SELECT t.sku_id,t.`出库量`,(@sum0 := @sum0 + t.`出库量`) as qty
FROM
(
SELECT pod.sku_id,SUM(pod.fulfill_quantity) as '出库量'
FROM evo_wes_picking.picking_order_detail pod,(SELECT @sum0 :=0) tmp
WHERE pod.last_updated_date >= '2021-09-01 00:00:00' AND pod.last_updated_date < '2021-09-02 00:00:00' AND pod.project_code = 'A51118'
GROUP BY pod.sku_id
ORDER BY `出库量` DESC
)t
GROUP BY t.sku_id
ORDER BY t.`出库量` DESC
)tmp
LEFT JOIN
(
SELECT tt.sku_id,IF(tt.sku_id is not NULL,'A',NULL) as '类型',tt.`出库量`
FROM
(
SELECT t.sku_id,t.`出库量`,(@sum11 := @sum11 + t.`出库量`) as qty
FROM
(
SELECT pod.sku_id,SUM(pod.fulfill_quantity) as '出库量'
FROM evo_wes_picking.picking_order_detail pod,(SELECT @sum11 :=0) tmp
WHERE pod.last_updated_date >= '2021-09-01 00:00:00' AND pod.last_updated_date < '2021-09-02 00:00:00' AND pod.project_code = 'A51118'
GROUP BY pod.sku_id
ORDER BY `出库量` DESC
)t
GROUP BY t.sku_id
ORDER BY t.`出库量` DESC
)tt
WHERE tt.qty <= (SELECT SUM(pod.fulfill_quantity)*0.7 FROM evo_wes_picking.picking_order_detail pod WHERE pod.last_updated_date >= '2021-09-01 00:00:00' AND pod.last_updated_date < '2021-09-02 00:00:00' AND pod.project_code = 'A51118')
)tmp1
ON tmp.sku_id = tmp1.sku_id AND tmp.`出库量` = tmp1.`出库量`
WHERE tmp1.sku_id is NULL
)a,(SELECT @sum2 :=0) tmp
)tt
WHERE tt.qty <= (SELECT SUM(pod.fulfill_quantity)*0.2 FROM evo_wes_picking.picking_order_detail pod WHERE pod.last_updated_date >= '2021-09-01 00:00:00' AND pod.last_updated_date < '2021-09-02 00:00:00' AND pod.project_code = 'A51118')
)tmp2
ON tmp.sku_id = tmp2.sku_id AND tmp.`出库量` = tmp2.`出库量`
WHERE tmp1.sku_id is NULL AND tmp2.sku_id is NULL
)a,(SELECT @sum3 :=0) tmp
)tt
WHERE tt.qty <= (SELECT SUM(pod.fulfill_quantity)*0.1 FROM evo_wes_picking.picking_order_detail pod WHERE pod.last_updated_date >= '2021-09-01 00:00:00' AND pod.last_updated_date < '2021-09-02 00:00:00' AND pod.project_code = 'A51118')

UNION ALL

SELECT tt.sku_id,IF(tt.sku_id is not NULL,'B',NULL) as '类型',tt.`出库量`
FROM
(
SELECT a.sku_id,a.`出库量`,(@sum2 := @sum2 + a.`出库量`) as qty
FROM
(
SELECT tmp.sku_id,tmp.`出库量`
FROM
(
SELECT t.sku_id,t.`出库量`,(@sum0 := @sum0 + t.`出库量`) as qty
FROM
(
SELECT pod.sku_id,SUM(pod.fulfill_quantity) as '出库量'
FROM evo_wes_picking.picking_order_detail pod,(SELECT @sum0 :=0) tmp
WHERE pod.last_updated_date >= '2021-09-01 00:00:00' AND pod.last_updated_date < '2021-09-02 00:00:00' AND pod.project_code = 'A51118'
GROUP BY pod.sku_id
ORDER BY `出库量` DESC
)t
GROUP BY t.sku_id
ORDER BY t.`出库量` DESC
)tmp
LEFT JOIN
(
SELECT tt.sku_id,IF(tt.sku_id is not NULL,'A',NULL) as '类型',tt.`出库量`
FROM
(
SELECT t.sku_id,t.`出库量`,(@sum11 := @sum11 + t.`出库量`) as qty
FROM
(
SELECT pod.sku_id,SUM(pod.fulfill_quantity) as '出库量'
FROM evo_wes_picking.picking_order_detail pod,(SELECT @sum11 :=0) tmp
WHERE pod.last_updated_date >= '2021-09-01 00:00:00' AND pod.last_updated_date < '2021-09-02 00:00:00' AND pod.project_code = 'A51118'
GROUP BY pod.sku_id
ORDER BY `出库量` DESC
)t
GROUP BY t.sku_id
ORDER BY t.`出库量` DESC
)tt
WHERE tt.qty <= (SELECT SUM(pod.fulfill_quantity)*0.7 FROM evo_wes_picking.picking_order_detail pod WHERE pod.last_updated_date >= '2021-09-01 00:00:00' AND pod.last_updated_date < '2021-09-02 00:00:00' AND pod.project_code = 'A51118')
)tmp1
ON tmp.sku_id = tmp1.sku_id AND tmp.`出库量` = tmp1.`出库量`
WHERE tmp1.sku_id is NULL
)a,(SELECT @sum2 :=0) tmp
)tt
WHERE tt.qty <= (SELECT SUM(pod.fulfill_quantity)*0.2 FROM evo_wes_picking.picking_order_detail pod WHERE pod.last_updated_date >= '2021-09-01 00:00:00' AND pod.last_updated_date < '2021-09-02 00:00:00' AND pod.project_code = 'A51118')

UNION ALL

SELECT tt.sku_id,IF(tt.sku_id is not NULL,'A',NULL) as '类型',tt.`出库量`
FROM
(
SELECT t.sku_id,t.`出库量`,(@sum1 := @sum1 + t.`出库量`) as qty
FROM
(
SELECT pod.sku_id,SUM(pod.fulfill_quantity) as '出库量'
FROM evo_wes_picking.picking_order_detail pod,(SELECT @sum1 :=0) tmp
WHERE pod.last_updated_date >= '2021-09-01 00:00:00' AND pod.last_updated_date < '2021-09-02 00:00:00' AND pod.project_code = 'A51118'
GROUP BY pod.sku_id
ORDER BY `出库量` DESC
)t
GROUP BY t.sku_id
ORDER BY t.`出库量` DESC
)tt
WHERE tt.qty <= (SELECT SUM(pod.fulfill_quantity)*0.7 FROM evo_wes_picking.picking_order_detail pod WHERE pod.last_updated_date >= '2021-09-01 00:00:00' AND pod.last_updated_date < '2021-09-02 00:00:00' AND pod.project_code = 'A51118')