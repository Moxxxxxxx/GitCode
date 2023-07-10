SELECT tt.`库存件数`,COUNT(DISTINCT tt.sku_id) as 'sku数量',SUM(tt.quantity) as '件数',
       CONCAT(CAST((COUNT(DISTINCT tt.sku_id)/(SELECT COUNT(DISTINCT li.sku_id)FROM evo_wes_inventory.level3_inventory li WHERE li.bucket_code != 'INVENTORY_DIFF' AND li.bucket_code !='TEMPORARY_DELIVERY' AND li.bucket_code != 'TEMPORARY_RECEIVING' AND li.bucket_code != 'TEMPORARY_RETURN' AND li.project_code = 'A51118' AND li.quantity > 0)*100) as DECIMAL(10,2)),'%') as 'sku占比',
       CONCAT(CAST((SUM(tt.quantity)/(SELECT SUM(li.quantity)FROM evo_wes_inventory.level3_inventory li WHERE li.bucket_code != 'INVENTORY_DIFF' AND li.bucket_code !='TEMPORARY_DELIVERY' AND li.bucket_code != 'TEMPORARY_RECEIVING' AND li.bucket_code != 'TEMPORARY_RETURN' AND li.project_code = 'A51118' AND li.quantity > 0)*100) as DECIMAL(10,2)),'%') as '件数占比'
FROM
(
SELECT CASE WHEN SUM(li.quantity) = 1 THEN '1件'
            WHEN SUM(li.quantity) = 2 THEN '2件'
            WHEN SUM(li.quantity) >= 3 AND SUM(li.quantity) < 10 THEN '3-10件'
            WHEN SUM(li.quantity) >= 10 THEN '10+件' END AS '库存件数',
       li.sku_id,SUM(li.quantity) as quantity
FROM evo_basic.basic_bucket bb
LEFT JOIN evo_wes_inventory.level3_inventory li
ON bb.bucket_code = li.bucket_code
WHERE li.bucket_code != 'INVENTORY_DIFF' AND li.bucket_code !='TEMPORARY_DELIVERY' AND li.bucket_code != 'TEMPORARY_RECEIVING' AND li.bucket_code != 'TEMPORARY_RETURN' 
  AND li.project_code = 'A51118' AND li.quantity > 0 AND bb.bucket_type_id = '57' AND bb.project_code = 'A51118'
GROUP BY li.sku_id
)tt
GROUP BY tt.`库存件数`
ORDER BY tt.`库存件数`+'0'