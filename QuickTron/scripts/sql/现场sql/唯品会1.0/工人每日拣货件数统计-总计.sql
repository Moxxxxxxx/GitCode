SET @begin_time = '2021-08-01';
SET @end_time = '2021-08-29';

SELECT DATE_FORMAT(d.lastUpdatedDate,'%Y-%m-%d') AS '日期',
       SUM(d.fulfillQuantity) AS '出库件数'
FROM walle_erp.picking_order_details d
WHERE d.lastUpdatedDate >= @begin_time and d.lastUpdatedDate <= @end_time AND d.fulfillQuantity IS NOT NULL
GROUP BY DATE_FORMAT(d.lastUpdatedDate,'%Y-%m-%d')