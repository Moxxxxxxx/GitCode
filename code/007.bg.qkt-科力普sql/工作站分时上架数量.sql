SET @begin_time = '2021-11-25 07:00:00';
SET @end_time = '2021-11-26 07:00:00';

SELECT DATE_FORMAT(rw.last_updated_date,'%Y-%m-%d %H:00:00') as '时间段',rw.station_code as '工作站编码',SUM(rwd.fulfill_quantity) as '上架货品数量'
FROM evo_wes_replenish.replenish_work rw
LEFT JOIN evo_wes_replenish.replenish_work_detail rwd
ON rw.id = rwd.replenish_work_id
WHERE rw.state = 'DONE' AND rw.last_updated_date >= @begin_time and rw.last_updated_date < @end_time AND rw.project_code = 'A51118' AND rwd.project_code = 'A51118' 
GROUP BY DATE_FORMAT(rw.last_updated_date,'%Y-%m-%d %H:00:00'),rw.station_code