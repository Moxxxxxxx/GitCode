SELECT
report_date as '统计日期',
po_no as '采购订单号',
vendor_code as '供应商编码',
item_code as '商品条码',
quantity as '数量'
FROM evo_vip.vip_inventory_daily_report
WHERE report_date >= '{begin_time}' and report_date <= '{end_time}'