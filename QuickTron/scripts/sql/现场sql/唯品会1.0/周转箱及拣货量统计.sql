SET @begin_time = '2021-08-25';
SET @end_time = '2021-08-29';

SELECT tmp1.date1 as '日期',
       tmp1.max as '日周转箱最大件数',
       tmp1.min as '日周转箱最小件数',
       tmp1.avg as '日周转箱平均件数',
       tmp2.max as '小时完成拣货最大量',
       tmp2.min as '小时完成拣货最小量',
       tmp1.box_num as '完成拣货箱总数',
       tmp2.avg as '小时完成拣货平均量'
FROM 
(
SELECT DATE_FORMAT(tt1.date1,'%Y-%m-%d')date1,MAX(tt1.quantity_num) max,MIN(tt1.quantity_num) min,CAST(SUM(tt1.quantity_num)/SUM(tt1.box_num) AS DECIMAL(10,2)) avg,SUM(box_num) box_num
FROM
(
SELECT DATE_FORMAT(d.lastUpdatedDate,'%Y-%m-%d')date1,d.boxID,SUM(d.quantity) quantity_num,count(DISTINCT d.boxID) box_num
FROM walle_erp.box_details_history d
WHERE d.lastUpdatedDate >= @begin_time and d.lastUpdatedDate <= @end_time AND d.quantity IS NOT NULL AND d.sourceBillType = 'PICKING_ORDER'
GROUP BY d.boxID,DATE_FORMAT(d.lastUpdatedDate,'%Y-%m-%d')
)tt1
GROUP BY DATE_FORMAT(tt1.date1,'%Y-%m-%d')
)tmp1
LEFT JOIN
(
SELECT DATE_FORMAT(tt2.date1,'%Y-%m-%d')date1,MAX(tt2.quantity_num) max,MIN(tt2.quantity_num) min,CAST(SUM(tt2.quantity_num)/SUM(tt2.num) AS DECIMAL(10,2)) avg
FROM
(
SELECT DATE_FORMAT(d.lastUpdatedDate,'%Y-%m-%d %H:00:00')date1,SUM(d.fulfillQuantity) quantity_num,count(DISTINCT DATE_FORMAT(d.lastUpdatedDate,'%Y-%m-%d %H:00:00')) num
FROM walle_erp.picking_order_details d
WHERE d.lastUpdatedDate >= @begin_time and d.lastUpdatedDate <= @end_time AND d.fulfillQuantity IS NOT NULL
GROUP BY DATE_FORMAT(d.lastUpdatedDate,'%Y-%m-%d %H:00:00')
)tt2
GROUP BY DATE_FORMAT(tt2.date1,'%Y-%m-%d')
)tmp2
ON DATE_FORMAT(tmp1.date1,'%Y-%m-%d') = DATE_FORMAT(tmp2.date1,'%Y-%m-%d')