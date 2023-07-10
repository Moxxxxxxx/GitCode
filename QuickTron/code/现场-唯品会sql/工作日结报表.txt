SELECT
report_date as '统计日期',
po_no as '采购订单号',
vendor_code as '供应商编码',
item_code as '商品条码',
brand_name as '品牌',
item_cat1_code as '一级分类代码',
item_cat1_name as '一级分类名称',
item_cat2_code as '二级分类代码',
item_cat2_name as '二级分类名称',
item_cat3_code as '三级分类代码',
item_cat3_name as '三级分类名称',
work_type as '工作类型',
quantity as '数量'
FROM evo_vip.vip_work_daily_report
WHERE report_date >= '{begin_time}' and report_date <= '{end_time}'