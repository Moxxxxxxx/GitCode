SET @begin_time = '2021-08-01';
SET @end_time = '2021-08-29';

SELECT
       tmp1.date1 AS '日期',
       tmp1.createdUser AS '工人ID',
       tmp1.name AS '工人姓名',
       tmp1.fulfillQuantity AS '件数'
FROM 
(
SELECT DATE_FORMAT(d.lastUpdatedDate,'%Y-%m-%d') AS date1,
       SUM(d.fulfillQuantity) AS fulfillQuantity,
       d.createdUser AS createdUser,
       u.name AS name
FROM walle_erp.picking_order_details d
LEFT JOIN walle_admin.users u
ON d.createdUser = u.account
WHERE d.lastUpdatedDate >= @begin_time and d.lastUpdatedDate <= @end_time AND d.fulfillQuantity IS NOT NULL
GROUP BY d.createdUser,DATE_FORMAT(d.lastUpdatedDate,'%Y-%m-%d')

UNION ALL

SELECT DATE_FORMAT(d.lastUpdatedDate,'%Y-%m-%d') AS date1,
       SUM(d.fulfillQuantity) AS fulfillQuantity,
       '汇总',
       '汇总' 
FROM walle_erp.picking_order_details d
WHERE d.lastUpdatedDate >= @begin_time and d.lastUpdatedDate <= @end_time AND d.fulfillQuantity IS NOT NULL
GROUP BY DATE_FORMAT(d.lastUpdatedDate,'%Y-%m-%d')

UNION ALL

SELECT '汇总',
       SUM(d.fulfillQuantity) AS fulfillQuantity,
       d.createdUser AS createdUser,
       u.name AS name
FROM walle_erp.picking_order_details d
LEFT JOIN walle_admin.users u
ON d.createdUser = u.account
WHERE d.lastUpdatedDate >= @begin_time and d.lastUpdatedDate <= @end_time AND d.fulfillQuantity IS NOT NULL
GROUP BY d.createdUser
)tmp1
ORDER BY DATE_FORMAT(tmp1.date1,'%Y-%m-%d')DESC,tmp1.createdUser