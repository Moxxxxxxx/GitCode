SET @begin_time = '2021-11-21 07:00:00';
SET @end_time = '2021-11-26 07:00:00';

SELECT DATE(DATE_FORMAT(rw.last_updated_date,'%Y-%m-%d %H:00:00')) as '日期',
       COUNT(DISTINCT rw.source_order_id)as '每天上架订单数',
       COUNT(DISTINCT rwd.sku_id)as '每天上架sku数',
       SUM(rwd.fulfill_quantity) as '每天上架订单量'
FROM evo_wes_replenish.replenish_work rw
LEFT JOIN evo_wes_replenish.replenish_work_detail rwd
ON rw.id = rwd.replenish_work_id
WHERE rw.state = 'DONE' AND rw.last_updated_date >= @begin_time and rw.last_updated_date < @end_time AND rw.project_code = 'A51118' AND rwd.project_code = 'A51118' 
GROUP BY DATE(DATE_FORMAT(rw.last_updated_date,'%Y-%m-%d %H:00:00'))