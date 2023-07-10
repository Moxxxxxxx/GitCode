SELECT '汇总' as 'id','--' as '库区','--' as '货架编码',COUNT(DISTINCT i.bucket_slot_code) as '槽位编码',COUNT(DISTINCT i.sku_id) as 'sku编码',COUNT(DISTINCT s.udf1) as '品牌',SUM(i.quantity) as '库存数量','--' as '创建时间','--' as '更新时间'
FROM evo_wes_inventory.level3_inventory i
LEFT JOIN evo_wes_basic.basic_sku s
ON i.sku_id = s.id

UNION ALL

SELECT i.id,i.zone_code as '库区',i.bucket_code as '货架编码',i.bucket_slot_code as '槽位编码',i.sku_id as 'sku编码',s.udf1 as '品牌',i.quantity as '库存数量',i.created_date as '创建时间',i.last_updated_date as '更新时间'
FROM evo_wes_inventory.level3_inventory i
LEFT JOIN evo_wes_basic.basic_sku s
ON i.sku_id = s.id