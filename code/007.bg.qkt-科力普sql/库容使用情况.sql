SELECT SUM(li.quantity) as '当前库存数量',
       COUNT(DISTINCT li.sku_id) as '当前sku种类数',
       SUM(li.quantity)/COUNT(DISTINCT li.bucket_code) as '平均每货架存储件数(件/货架)',
       CONCAT(CAST((COUNT(DISTINCT li.bucket_code)/COUNT(DISTINCT bb.bucket_code)*100) as DECIMAL(10,2)),'%') as '当前货架占用率',
       CONCAT(CAST((COUNT(DISTINCT li.bucket_slot_code)/COUNT(DISTINCT bs.slot_code)*100) as DECIMAL(10,2)),'%') as '当前货位占用率'
FROM evo_basic.basic_bucket bb
LEFT JOIN evo_basic.basic_slot bs
ON bb.id = bs.bucket_id
LEFT JOIN evo_wes_inventory.level3_inventory li
ON bb.bucket_code = li.bucket_code AND bs.slot_code = li.bucket_slot_code
WHERE bb.bucket_code != 'INVENTORY_DIFF' AND bb.bucket_code !='TEMPORARY_DELIVERY' AND bb.bucket_code != 'TEMPORARY_RECEIVING' AND bb.bucket_code != 'TEMPORARY_RETURN' 
  AND bb.project_code = 'A51118' AND bs.project_code = 'A51118' AND li.project_code = 'A51118'
  AND li.quantity > 0 AND bb.bucket_type_id = '57'