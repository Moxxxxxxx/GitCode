-- 货架进站调度时间明细
SET @begin_time = '2021-11-16 00:00:00'; -- 设置开始时间
SET @end_time = '2021-11-17 00:00:00'; -- 设置结束时间

SELECT 
po.picking_order_number as '拣选订单号',
pj.job_id as '任务号',
pj.bucket_code as '货架编号',
'进站' as '任务类型',
c1.updated_date as '开始时间',
c2.updated_date as '结束时间',
TIMESTAMPDIFF(SECOND,c1.updated_date,c2.updated_date) as '时长/s'
FROM evo_wes_picking.picking_order po
LEFT JOIN evo_wcs_g2p.picking_job pj
ON po.id = pj.order_id
LEFT JOIN evo_wcs_g2p.job_state_change c1
ON pj.job_id = c1.job_id
LEFT JOIN evo_wcs_g2p.job_state_change c2
ON c1.job_id = c2.job_id
WHERE po.state = 'DONE' AND c1.state = 'INIT_JOB' AND c2.state = 'START_EXECUTOR' AND po.done_date >= @begin_time AND po.done_date < @end_time