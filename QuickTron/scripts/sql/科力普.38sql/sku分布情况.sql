SELECT li.sku_id,COUNT(DISTINCT li.bucket_code) as '货架数'
FROM evo_basic.basic_bucket bb
LEFT JOIN evo_wes_inventory.level3_inventory li
ON bb.bucket_code = li.bucket_code
WHERE li.bucket_code != 'INVENTORY_DIFF' AND li.bucket_code !='TEMPORARY_DELIVERY' AND li.bucket_code != 'TEMPORARY_RECEIVING' AND li.bucket_code != 'TEMPORARY_RETURN' 
  AND li.project_code = 'A51118' AND li.quantity > 0 AND bb.bucket_type_id = '57' AND bb.project_code = 'A51118'
GROUP BY li.sku_id
ORDER BY COUNT(DISTINCT li.bucket_code)

SELECT li.bucket_code,COUNT(DISTINCT li.sku_id) as 'sku数'
FROM evo_basic.basic_bucket bb
LEFT JOIN evo_wes_inventory.level3_inventory li
ON bb.bucket_code = li.bucket_code
WHERE li.bucket_code != 'INVENTORY_DIFF' AND li.bucket_code !='TEMPORARY_DELIVERY' AND li.bucket_code != 'TEMPORARY_RECEIVING' AND li.bucket_code != 'TEMPORARY_RETURN' 
  AND li.project_code = 'A51118' AND li.quantity > 0 AND bb.bucket_type_id = '57' AND bb.project_code = 'A51118'
GROUP BY li.bucket_code
ORDER BY COUNT(DISTINCT li.sku_id)