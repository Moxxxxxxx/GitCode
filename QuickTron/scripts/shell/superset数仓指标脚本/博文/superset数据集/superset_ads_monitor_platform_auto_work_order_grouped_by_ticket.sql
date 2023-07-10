SELECT
	ticket_id as '工单号',work_order_status as '工单状态',work_order_create_time as '工单创建时间'
FROM
	ads_monitor_platform_auto_work_order
GROUP BY ticket_id,work_order_status,work_order_create_time